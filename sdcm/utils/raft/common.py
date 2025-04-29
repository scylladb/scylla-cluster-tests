import logging
import contextlib

from typing import Iterable, Callable
from functools import partial

from sdcm.exceptions import BootstrapStreamErrorFailure
from sdcm.cluster import BaseNode, BaseScyllaCluster, BaseMonitorSet
from sdcm.wait import wait_for
from sdcm.sct_events.group_common_events import decorate_with_context, \
    ignore_stream_mutation_fragments_errors, ignore_ycsb_connection_refused, ignore_raft_topology_cmd_failing, ignore_raft_transport_failing
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.common import ParallelObject


LOGGER = logging.getLogger(__name__)


class RaftException(Exception):
    """Raise if raft feature mode differs on nodes"""


def validate_raft_on_nodes(nodes: list["BaseNode"]) -> None:
    LOGGER.debug("Check that raft is enabled on all the nodes")
    raft_enabled_on_nodes = [node.raft.is_enabled for node in nodes]
    if len(set(raft_enabled_on_nodes)) != 1:
        raise RaftException("Raft configuration is not the same on all the nodes")

    if not all(raft_enabled_on_nodes):
        LOGGER.debug("Raft feature is disabled)")
        return
    LOGGER.debug("Raft feature is enabled)")
    LOGGER.debug("Check raft feature status on nodes")
    nodes_raft_status = []
    for node in nodes:
        if raft_ready := node.raft.is_ready():
            nodes_raft_status.append(raft_ready)
            continue
        LOGGER.error("Node %s has raft status: %s", node.name, node.raft.get_status())
    if not all(nodes_raft_status):
        raise RaftException("Raft is not ready")
    LOGGER.debug("Raft is ready!")


class NodeBootstrapAbortManager:
    INSTANCE_START_TIMEOUT = 600
    SUCCESS_BOOTSTRAP_TIMEOUT = 3600

    def __init__(self, bootstrap_node: BaseNode, verification_node: BaseNode):
        self.bootstrap_node = bootstrap_node
        self.verification_node = verification_node
        self.db_cluster: BaseScyllaCluster = self.verification_node.parent_cluster
        self.monitors: BaseMonitorSet = self.verification_node.test_config.tester_obj().monitors

    @property
    def host_id_searcher(self) -> Iterable[str]:
        return self.bootstrap_node.follow_system_log(patterns=['Setting local host id to'])

    def _set_wait_stop_event(self):
        if not self.bootstrap_node.stop_wait_db_up_event.is_set():
            self.bootstrap_node.stop_wait_db_up_event.set()
        LOGGER.debug("Stop event was set for node %s", self.bootstrap_node.name)

    def _start_bootstrap(self):
        try:
            LOGGER.debug("Starting bootstrap process %s", self.bootstrap_node.name)
            self.bootstrap_node.parent_cluster.node_setup(self.bootstrap_node, verbose=True)
            self.bootstrap_node.parent_cluster.node_startup(self.bootstrap_node, verbose=True)
            LOGGER.debug("Node %s was bootstrapped", self.bootstrap_node.name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.error("Setup failed for node %s with err %s", self.bootstrap_node.name, exc)
        finally:
            self._set_wait_stop_event()

    def _abort_bootstrap(self, abort_action: Callable, log_message: str, timeout: int = 600):
        LOGGER.debug("Stop bootstrap process after log message: '%s'", log_message)
        log_follower = self.bootstrap_node.follow_system_log(patterns=[log_message])
        try:
            wait_for(func=lambda: list(log_follower), step=5,
                     text="Waiting log message to stop scylla...",
                     timeout=timeout,
                     throw_exc=True,
                     stop_event=self.bootstrap_node.stop_wait_db_up_event)
            abort_action()
            LOGGER.info("Scylla was stopped successfully on node %s", self.bootstrap_node.name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.warning("Abort was failed on node %s with error %s", self.bootstrap_node.name, exc)
        finally:
            self._set_wait_stop_event()

    @decorate_with_context(ignore_ycsb_connection_refused)
    def clean_unbootstrapped_node(self):
        node_host_ids = []
        if found_stings := list(self.host_id_searcher):
            for line in found_stings:
                new_node_host_id = line.split(" ")[-1].strip()
                LOGGER.debug("Node %s has host id: %s in log", self.bootstrap_node.name, new_node_host_id)
                node_host_ids.append(new_node_host_id)
        self.bootstrap_node.log.debug("New host was not properly bootstrapped. Terminate it")
        self.db_cluster.terminate_node(self.bootstrap_node)
        self.monitors.reconfigure_scylla_monitoring()
        self.verification_node.raft.clean_group0_garbage(raise_exception=True)
        if node_host_ids:
            for host_id in node_host_ids:
                self.verification_node.run_nodetool(
                    f"removenode {host_id}", ignore_status=True, retry=3, warning_event_on_exception=True)

        assert self.verification_node.raft.is_cluster_topology_consistent(), \
            "Group0, Token Ring and number of node in cluster are differs. Check logs"
        self.verification_node.parent_cluster.check_nodes_up_and_normal()
        LOGGER.info("Failed bootstrapped node %s was removed. Cluster is in initial state", self.bootstrap_node.name)

    def run_bootstrap_and_abort_with_action(self, terminate_pattern, abort_action: Callable, abort_action_timeout: int = 300):
        watcher = partial(self._abort_bootstrap,
                          abort_action=abort_action,
                          log_message=terminate_pattern.log_message,
                          timeout=self.INSTANCE_START_TIMEOUT + terminate_pattern.timeout + abort_action_timeout)

        wait_operations_timeout = (self.SUCCESS_BOOTSTRAP_TIMEOUT + self.INSTANCE_START_TIMEOUT
                                   + terminate_pattern.timeout + abort_action_timeout)
        with ignore_stream_mutation_fragments_errors(), ignore_raft_topology_cmd_failing(), \
                ignore_raft_transport_failing(), contextlib.ExitStack() as stack:
            for expected_start_failed_context in self.verification_node.raft.get_severity_change_filters_scylla_start_failed(
                    terminate_pattern.timeout):
                stack.enter_context(expected_start_failed_context)
            try:
                ParallelObject(objects=[self._start_bootstrap, watcher],
                               timeout=wait_operations_timeout).call_objects(ignore_exceptions=True)
            finally:
                self._set_wait_stop_event()

        LOGGER.debug("Clear stop event for wait_for on node %s", self.bootstrap_node.name)
        self.bootstrap_node.stop_wait_db_up_event.clear()

    def clean_and_restart_bootstrap_after_abort(self):
        if self.bootstrap_node.db_up():
            LOGGER.debug("Node %s was bootstrapped")
            return
        # stop scylla if it was started by scylla-manager-client during setup
        self.bootstrap_node.stop_scylla_server(ignore_status=True, timeout=600)
        # Clean garbage from group 0 and scylla data and restart setup
<<<<<<< HEAD
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
        if self.verification_node.raft.get_diff_group0_token_ring_members() or \
                self.verification_node.raft.get_group0_non_voters():
            self.verification_node.raft.clean_group0_garbage(raise_exception=True)
        if not self.is_bootstrapped_successfully():
            LOGGER.debug("Clean old scylla data and restart scylla service")
            self.bootstrap_node.clean_scylla_data()
        watcher_startup_failed = partial(self.watch_startup_failed, timeout=3600)
=======
        if self.verification_node.raft.search_inconsistent_host_ids():
            self.verification_node.raft.clean_group0_garbage(raise_exception=True)
        if not self.is_bootstrapped_successfully():
            LOGGER.debug("Clean old scylla data and restart scylla service")
            self.bootstrap_node.clean_scylla_data()
        watcher_startup_failed = partial(self.watch_startup_failed, timeout=3600)
>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
        try:
            if self.verification_node.raft.get_diff_group0_token_ring_members():
                self.verification_node.raft.clean_group0_garbage(raise_exception=True)
                LOGGER.debug("Clean old scylla data and restart scylla service")
                self.bootstrap_node.clean_scylla_data()
            with ignore_raft_topology_cmd_failing(), ignore_raft_transport_failing(), \
                    adaptive_timeout(operation=Operations.NEW_NODE, node=self.verification_node, timeout=3600) as bootstrap_timeout:

                self.bootstrap_node.start_scylla_server(verify_up_timeout=bootstrap_timeout, verify_down=True)
                self.bootstrap_node.start_scylla_jmx()
            self.db_cluster.check_nodes_up_and_normal(
                nodes=[self.bootstrap_node], verification_node=self.verification_node)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.error("Scylla service restart failed: %s", exc)
            self.clean_unbootstrapped_node()
            raise BootstrapStreamErrorFailure(f"Rebootstrap failed with error: {exc}") from exc

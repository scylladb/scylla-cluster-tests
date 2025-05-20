import logging
import contextlib
import time
import traceback
import re

from typing import Iterable, Callable
from functools import partial
from json import loads

from sdcm.sct_events.decorators import raise_event_on_failure
from sdcm.exceptions import BootstrapStreamErrorFailure, ExitByEventError
from sdcm.utils.action_logger import ActionLogger
from sdcm.wait import wait_for

from sdcm.sct_events.group_common_events import decorate_with_context, \
    ignore_ycsb_connection_refused
from sdcm.utils.common import ParallelObject
from sdcm.utils.raft import get_node_status_from_system_by
from sdcm.cluster import BaseMonitorSet, NodeSetupFailed, BaseScyllaCluster, BaseNode
from sdcm.exceptions import RaftTopologyCoordinatorNotFound
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.utils.decorators import retrying


LOGGER = logging.getLogger(__name__)
UUID_REGEX = re.compile(r"([0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12})")


class RaftException(Exception):
    """Raise if raft feature mode differs on nodes"""


def validate_raft_on_nodes(nodes: list[BaseNode]) -> None:
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


@retrying(n=3, allowed_exceptions=(RaftTopologyCoordinatorNotFound, ), message="Waiting topology coordinator election ...")
def get_topology_coordinator_node(node: BaseNode) -> BaseNode:
    active_nodes: list[BaseNode] = node.parent_cluster.get_nodes_up_and_normal(node)
    stm = "select description from system.group0_history where key = 'history' and \
            description LIKE 'Starting new topology coordinator%' ALLOW FILTERING;"
    with node.parent_cluster.cql_connection_patient(node) as session:
        result = list(session.execute(stm))
    coordinators_ids = []
    for row in result:
        if match := UUID_REGEX.search(row.description):
            coordinators_ids.append(match.group(1))
    if not coordinators_ids:
        raise RaftTopologyCoordinatorNotFound("No host ids were found in raft group0 history")
    LOGGER.debug("All coordinators history ids: %s", coordinators_ids)
    for active_node in active_nodes:
        node_hostid = loads(StorageServiceClient(active_node).get_local_hostid().stdout)
        LOGGER.debug("Node %s host id is %s", active_node.name, node_hostid)
        if node_hostid == coordinators_ids[0]:
            return active_node
    raise RaftTopologyCoordinatorNotFound(f"The node with host id {coordinators_ids[0]} was not found")


class NodeBootstrapAbortManager:
    INSTANCE_START_TIMEOUT = 600
    SUCCESS_BOOTSTRAP_TIMEOUT = 3600

    def __init__(self, bootstrap_node: BaseNode, verification_node: BaseNode, actions_log: ActionLogger):
        self.bootstrap_node = bootstrap_node
        self.verification_node = verification_node
        self.db_cluster: BaseScyllaCluster = verification_node.parent_cluster
        self.monitors: BaseMonitorSet = self.verification_node.test_config.tester_obj().monitors
        self.actions_log = actions_log

    @property
    def host_id_searcher(self) -> Iterable[str]:
        return self.bootstrap_node.follow_system_log(patterns=['Setting local host id to'], start_from_beginning=True)

    def get_host_ids_from_log(self) -> list[str]:
        node_host_ids = []
        found_strings = list(self.host_id_searcher)
        LOGGER.debug("Found local host ids: %s", found_strings)
        if found_strings:
            for line in found_strings:
                host_id = line.split(" ")[-1].strip()
                node_host_ids.append(host_id)
        LOGGER.debug("Found host ids %s for node %s", node_host_ids, self.bootstrap_node.name)
        return node_host_ids

    def _set_wait_stop_event(self):
        if not self.bootstrap_node.stop_wait_db_up_event.is_set():
            self.bootstrap_node.stop_wait_db_up_event.set()
        LOGGER.debug("Stop event was set for node %s", self.bootstrap_node.name)

    @raise_event_on_failure
    def _start_bootstrap(self):
        try:
            self.actions_log.info("Node bootstrap start", target=self.bootstrap_node.name)
            self.bootstrap_node.parent_cluster.node_setup(self.bootstrap_node, verbose=True)
            self.bootstrap_node.parent_cluster.node_startup(self.bootstrap_node, verbose=True)
            self.actions_log.info("Node bootstrap finished", target=self.bootstrap_node.name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Setup failed for node %s with err %s", self.bootstrap_node.name, exc)
            self.actions_log.error("Node bootstrap failed", target=self.bootstrap_node.name)
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
            if self.bootstrap_node.db_up():
                LOGGER.info("Node %s is bootstrapped. Cancel abort action", self.bootstrap_node.name)
                return
            self.actions_log.info("Aborting bootstrap", target=self.bootstrap_node.name,
                                  metadata={"action": abort_action.__name__})
            abort_action()
            LOGGER.info("Scylla was stopped successfully on node %s", self.bootstrap_node.name)
            self.actions_log.info("Bootstrap abort finished", target=self.bootstrap_node.name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Abort was failed on node %s with error %s", self.bootstrap_node.name, exc)
            self.actions_log.warning("Bootstrap abort failed", target=self.bootstrap_node.name)
        finally:
            self._set_wait_stop_event()

    @decorate_with_context(ignore_ycsb_connection_refused)
    def clean_unbootstrapped_node(self):
        node_host_ids = self.get_host_ids_from_log()
        self.bootstrap_node.log.debug("New host was not properly bootstrapped. Terminate it")
        self.db_cluster.terminate_node(self.bootstrap_node)
        self.monitors.reconfigure_scylla_monitoring()
        if node_host_ids:
            for host_id in set(node_host_ids):
                self.verification_node.run_nodetool(
                    f"removenode {host_id}", ignore_status=True, retry=3)
        self.verification_node.raft.clean_group0_garbage(raise_exception=True)

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
        with contextlib.ExitStack() as stack:
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

    def _rebootstrap_node(self):
        self.bootstrap_node.start_scylla_server(verify_up_timeout=3600, verify_down=True)
        self.bootstrap_node.start_scylla_jmx()
        self.db_cluster.check_nodes_up_and_normal(
            nodes=[self.bootstrap_node], verification_node=self.verification_node)
        self._set_wait_stop_event()

    def watch_startup_failed(self, timeout=600):
        start_time = time.perf_counter()
        log_follower = self.bootstrap_node.follow_system_log(patterns=[".*Startup failed.*"])
        while time.perf_counter() - start_time < timeout and not self.bootstrap_node.stop_wait_db_up_event.is_set():
            found_errors = list(log_follower)
            if found_errors:
                self._set_wait_stop_event()
                raise NodeSetupFailed(node=self.bootstrap_node, error_msg=str(found_errors))
            time.sleep(1)
        self._set_wait_stop_event()

    def is_bootstrapped_successfully(self):
        """Check that bootstrap node was added to token ring and group0 on each node"""
        host_ids = self.get_host_ids_from_log()
        all_nodes_token_ring = []
        all_nodes_group0 = []
        if not host_ids:
            return False
        # check only latest host_id.
        host_id = host_ids[-1]
        LOGGER.info("Check group0 and token ring")
        for node in [node for node in self.verification_node.parent_cluster.nodes if node != self.bootstrap_node]:
            token_ring = node.get_token_ring_members()
            group0 = node.raft.get_group0_members()
            all_nodes_token_ring.append(host_id in [n["host_id"] for n in token_ring])

            for n in group0:
                if host_id == n["host_id"] and n['voter']:
                    all_nodes_group0.append(True)
                    break
            else:
                all_nodes_group0.append(False)
        return all(all_nodes_group0) and all(all_nodes_token_ring)

    def clean_and_restart_bootstrap_after_abort(self):
        if self.bootstrap_node.db_up():
            LOGGER.debug("Node %s was bootstrapped")
            return
        # stop scylla if it was started by scylla-manager-client during setup
        with self.actions_log.action_scope("Stop Scylla server", target=self.bootstrap_node.name):
            self.bootstrap_node.stop_scylla_server(ignore_status=True, timeout=600)
        # Clean garbage from group 0 and scylla data and restart setup
        if self.verification_node.raft.search_inconsistent_host_ids():
            with self.actions_log.action_scope("Clean group0 garbage", target=self.verification_node.name):
                self.verification_node.raft.clean_group0_garbage(raise_exception=True)
        if not self.is_bootstrapped_successfully():
            LOGGER.debug("Clean old scylla data and restart scylla service")
            with self.actions_log.action_scope("Clean Scylla data", target=self.bootstrap_node.name):
                self.bootstrap_node.clean_scylla_data()
        watcher_startup_failed = partial(self.watch_startup_failed, timeout=3600)
        try:
            LOGGER.debug("Start rebootstrap as new node")
            self.actions_log.info("Rebootstrap node start", target=self.bootstrap_node.name)
            ParallelObject(objects=[self._rebootstrap_node, watcher_startup_failed], timeout=3800).call_objects()
            LOGGER.debug("Node is up")
        except NodeSetupFailed as exc:
            LOGGER.error("Scylla service restart failed: %s", exc)
            self.actions_log.error("Rebootstrap node failed", target=self.bootstrap_node.name)
            self.clean_unbootstrapped_node()
            raise BootstrapStreamErrorFailure(f"Rebootstrap failed with error: {exc}") from exc
        except ExitByEventError as exc:
            LOGGER.error("Event stopped: %s", exc)
            if self.bootstrap_node.db_up():
                LOGGER.info("Node is up")
            else:
                LOGGER.info("Clean node")
                self.clean_unbootstrapped_node()

        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Scylla service restart failed: %s", exc)
            self.clean_unbootstrapped_node()
            raise BootstrapStreamErrorFailure(f"Rebootstrap failed with error: {exc}") from exc
        finally:
            self.bootstrap_node.stop_wait_db_up_event.clear()


class FailedDecommissionOperationMonitoring:
    """Monitor decommission after operation failing

    Sometimes decommission operation could fail for some reason
    but decommission process is still running on the node and
    could finished successfully.
    The context manager allows to check whether decommission is still running
    and it waits while decommission will be finished. Operation status is checked by
    records in system table: system.cluster_status by target node ip

    CM should be initialized before decommission operation started. It
    get 2 required parameter:
     - target node which will be decommissioning
     - verification node which will be used to get target node status
    """

    def __init__(self, target_node: BaseNode, verification_node: BaseNode, timeout=7200):
        self.timeout = timeout
        self.target_node = target_node
        self.db_cluster = verification_node.parent_cluster
        self.target_node_ip = target_node.ip_address
        self.verification_node = verification_node

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            LOGGER.warning("Decommission failed with error: %s",
                           "".join(traceback.format_exception(exc_type, exc_val, exc_tb)))
            LOGGER.debug("Check is decommission running..")
            decommission_is_running = self.is_node_decommissioning()
            if decommission_is_running:
                wait_for(func=lambda: not self.is_node_decommissioning(), step=15,
                         timeout=self.timeout,
                         text=f"Waiting decommission is finished for {self.target_node.name}...")
            self.db_cluster.verify_decommission(self.target_node)
            return True

    def is_node_decommissioning(self):
        node_status = get_node_status_from_system_by(
            self.verification_node, ip_address=self.target_node_ip)
        return node_status["state"] == "DECOMMISSIONING" if node_status else False

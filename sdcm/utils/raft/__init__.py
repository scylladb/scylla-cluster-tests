import contextlib
import logging
import random

from enum import Enum
from typing import Protocol, NamedTuple, Mapping, Iterable

from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events import Severity
<<<<<<< HEAD
from sdcm.utils.features import is_consistent_topology_changes_feature_enabled, is_consistent_cluster_management_feature_enabled
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
from sdcm.utils.features import is_consistent_topology_changes_feature_enabled, is_consistent_cluster_management_feature_enabled
from sdcm.utils.health_checker import HealthEventsGenerator
from sdcm.wait import wait_for
from sdcm.rest.raft_api import RaftApi
=======
from sdcm.utils.features import (is_consistent_topology_changes_feature_enabled,
                                 is_consistent_cluster_management_feature_enabled,
                                 is_group0_limited_voters_enabled)
from sdcm.utils.health_checker import HealthEventsGenerator
from sdcm.wait import wait_for
from sdcm.rest.raft_api import RaftApi
>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)


LOGGER = logging.getLogger(__name__)
RAFT_DEFAULT_SCYLLA_VERSION = "5.5.0-dev"


class Group0MembersNotConsistentWithTokenRingMembersException(Exception):
    pass


class LogPosition(Enum):
    BEGIN = 0
    END = 1


class TopologyOperations(Enum):
    DECOMMISSION = "decommission"
    BOOTSTRAP = "bootstrap"


class MessagePosition(NamedTuple):
    log_message: str
    position: LogPosition


class MessageTimeout(NamedTuple):
    log_message: str
    timeout: int


BACKEND_TIMEOUTS: dict[str, Mapping[LogPosition, int]] = {
    "aws": {LogPosition.BEGIN: 300, LogPosition.END: 3600},
    "gce": {LogPosition.BEGIN: 300, LogPosition.END: 3600},
    "azure": {LogPosition.BEGIN: 1200, LogPosition.END: 7200},
}

ABORT_DECOMMISSION_LOG_PATTERNS: Iterable[MessagePosition] = [
    MessagePosition("DECOMMISSIONING: unbootstrap starts", LogPosition.BEGIN),
    MessagePosition("DECOMMISSIONING: unbootstrap done", LogPosition.END),
    MessagePosition("becoming a group 0 non-voter", LogPosition.END),
    MessagePosition("became a group 0 non-voter", LogPosition.END),
    MessagePosition("leaving token ring", LogPosition.END),
    MessagePosition("left token ring", LogPosition.END),
    MessagePosition("Finished token ring movement", LogPosition.END)
]

ABORT_BOOTSTRAP_LOG_PATTERNS: Iterable[MessagePosition] = [
    MessagePosition("Starting to bootstrap", LogPosition.BEGIN),
    MessagePosition("setup_group0: joining group 0", LogPosition.BEGIN),
    MessagePosition("setup_group0: successfully joined group 0", LogPosition.BEGIN),
    MessagePosition("setup_group0: the cluster is ready to use Raft. Finishing", LogPosition.BEGIN),
    MessagePosition("entering BOOTSTRAP mode", LogPosition.BEGIN),
    MessagePosition("storage_service - Bootstrap completed", LogPosition.END),
    MessagePosition("finish_setup_after_join: becoming a voter in the group 0 configuration", LogPosition.END),
    MessagePosition("raft_group0 - finish_setup_after_join: group 0 ID present, loading server info", LogPosition.END),
    MessagePosition("became a group 0 voter", LogPosition.END),
]


class RaftFeatureOperations(Protocol):
    _node: "BaseNode"
    TOPOLOGY_OPERATION_LOG_PATTERNS: dict[TopologyOperations, Iterable[MessagePosition]]

    @property
    def is_enabled(self) -> bool:
        ...

    @property
    def is_consistent_topology_changes_enabled(self) -> bool:
        ...

    def get_status(self) -> str:
        ...

    def is_ready(self) -> bool:
        ...

    def get_group0_members(self) -> list[str]:
        ...

    def get_group0_non_voters(self) -> list[dict[str, str]]:
        ...

    def clean_group0_garbage(self, raise_exception: bool = False) -> None:
        ...

    def get_message_waiting_timeout(self, message_position: MessagePosition) -> MessageTimeout:
        backend = self._node.parent_cluster.params["cluster_backend"]
        if backend not in BACKEND_TIMEOUTS:
            backend = "aws"
        return MessageTimeout(message_position.log_message,
                              BACKEND_TIMEOUTS[backend][message_position.position])

    def get_random_log_message(self, operation: TopologyOperations, seed: int | None = None):
        random.seed(seed)
        log_patterns = self.TOPOLOGY_OPERATION_LOG_PATTERNS.get(operation)
        log_pattern = random.choice(log_patterns)
        return self.get_message_waiting_timeout(log_pattern)

    def get_all_messages_timeouts(self, operation: TopologyOperations):
        log_patterns = self.TOPOLOGY_OPERATION_LOG_PATTERNS.get(operation)
        return list(map(self.get_message_waiting_timeout, log_patterns))

<<<<<<< HEAD
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
    def call_read_barrier(self):
        ...

=======
    def call_read_barrier(self):
        """Trigger raft global read barrier """

    def search_inconsistent_host_ids(self) -> list[str]:
        """ Search host id of node which violate group0 and token ring consistency"""

>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)

class RaftFeature(RaftFeatureOperations):
    TOPOLOGY_OPERATION_LOG_PATTERNS: dict[TopologyOperations, Iterable[MessagePosition]] = {
        TopologyOperations.DECOMMISSION: ABORT_DECOMMISSION_LOG_PATTERNS,
        TopologyOperations.BOOTSTRAP: ABORT_BOOTSTRAP_LOG_PATTERNS,
    }

    def __init__(self, node: "BaseNode") -> None:
        super().__init__()
        self._node = node

    @property
    def is_enabled(self) -> bool:
        return True

    @property
    def is_consistent_topology_changes_enabled(self) -> bool:
        """Check whether CONSISTENT_TOPOLOGY_CHANGES feature is enabled"""
        with self._node.parent_cluster.cql_connection_patient(node=self._node) as session:
            return is_consistent_topology_changes_feature_enabled(session)

    def get_status(self) -> str:
        """ get raft status """
        with self._node.parent_cluster.cql_connection_patient_exclusive(node=self._node) as session:
            row = session.execute("SELECT * FROM system.scylla_local WHERE key = 'group0_upgrade_state'").one()
            return getattr(row, 'value', "")

    def is_ready(self) -> bool:
        """check if raft running

        According to https://docs.scylladb.com/branch-5.2/architecture/raft.html
        """
        state = self.get_status()
        return state == "use_post_raft_procedures"

    def get_group0_members(self) -> list[dict[str, str]]:
        LOGGER.debug("Get group0 members")
        group0_members = []
        try:
            with self._node.parent_cluster.cql_connection_patient_exclusive(node=self._node) as session:
                row = session.execute("select value from system.scylla_local where key = 'raft_group0_id'").one()
                if not row:
                    return []
                raft_group0_id = row.value

                rows = session.execute(f"select server_id, can_vote from system.raft_state  \
                                        where group_id = {raft_group0_id} and disposition = 'CURRENT'").all()

                for row in rows:
                    group0_members.append({"host_id": str(row.server_id),
                                           "voter": row.can_vote})
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            err_msg = f"Get group0 members failed with error: {exc}"
            LOGGER.error(err_msg)

        LOGGER.debug("Group0 members: %s", group0_members)
        return group0_members

    def get_group0_non_voters(self) -> list[str]:
        LOGGER.debug("Get group0 members in status non-voter")
        # Add host id which cann't vote after decommission was aborted because it is fast rebooted / terminated")
        return [member['host_id'] for member in self.get_group0_members() if not member['voter']]

    def get_diff_group0_token_ring_members(self) -> list[str]:
        LOGGER.debug("Get diff group0 from token ring")
        group0_members = self.get_group0_members()
        group0_members_ids = {member["host_id"] for member in group0_members}
        token_ring_members = self._node.get_token_ring_members()
        token_ring_member_ids = {member["host_id"] for member in token_ring_members}
        LOGGER.debug("Token rings members ids: %s", token_ring_member_ids)
        LOGGER.debug("Group0 members ids: %s", group0_members_ids)

        diff = list(group0_members_ids - token_ring_member_ids)
        LOGGER.debug("Group0 differs from token ring: %s", diff)
        return diff

    def clean_group0_garbage(self, raise_exception: bool = False) -> None:
        LOGGER.debug("Clean group0 non-voter's members")
<<<<<<< HEAD
        host_ids = self.get_diff_group0_token_ring_members()
        if not host_ids:
            LOGGER.debug("Node could return to token ring but not yet bootstrap")
            host_ids = self.get_group0_non_voters()
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
        host_ids = self.get_diff_group0_token_ring_members()
        if not host_ids:
            LOGGER.debug("Node could return to token ring but not yet bootstrap")
            host_ids = self.get_group0_non_voters()
        attempt = 3
=======
        host_ids = self.search_inconsistent_host_ids()
        attempt = 3
>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
        while host_ids:
            removing_host_id = host_ids.pop(0)
            ingore_dead_nodes_opt = f"--ignore-dead-nodes {','.join(host_ids)}" if host_ids else ""

            result = self._node.run_nodetool(f"removenode {removing_host_id} {ingore_dead_nodes_opt}",
                                             ignore_status=True,
                                             verbose=True,
                                             retry=3)
            if not result.ok:
                LOGGER.error("Removenode with host_id %s failed with %s",
                             removing_host_id, result.stdout + result.stderr)
            if not host_ids:
                break

<<<<<<< HEAD
        if missing_host_ids := self.get_diff_group0_token_ring_members():
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
        missing_host_ids = self.get_diff_group0_token_ring_members() or self.get_group0_non_voters()
        if missing_host_ids:
=======
        missing_host_ids = self.search_inconsistent_host_ids()
        if missing_host_ids:
>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
            token_ring_members = self._node.get_token_ring_members()
            group0_members = self.get_group0_members()
            error_msg = f"Token ring {token_ring_members} and group0 {group0_members} are differs on: {missing_host_ids}"
            LOGGER.error(error_msg)
            if raise_exception:
                raise Group0MembersNotConsistentWithTokenRingMembersException(error_msg)
        LOGGER.debug("Group0 cleaned")

    @staticmethod
    def get_severity_change_filters_scylla_start_failed(timeout: int | None = None) -> tuple:
        return (
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=r".*storage_service - decommission.*Operation failed",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=r".*node_ops - decommission.*seastar::sleep_aborted",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=r".*This node was decommissioned and will not rejoin the ring",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=r".*This node was decommissioned and will not rejoin the ring",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=r".*Startup failed: std::runtime_error.*is removed from the cluster",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=r".*gossip - is_safe_for_restart.*status=LEFT",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=r".*init - Startup failed: std::runtime_error.*already exists, cancelling join",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=r".*init - Startup failed: std::runtime_error.*repair_reason=bootstrap.*aborted_by_user=true",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent,
                                        regex=".*init - Startup failed.*"),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=r".*node_ops - bootstrap.*Operation failed.*seastar::abort_requested_exception",
                                        extra_time_to_expiration=timeout),
        )

    def is_cluster_topology_consistent(self) -> bool:
        group0_ids = [member["host_id"] for member in self.get_group0_members()]
        LOGGER.debug("Group0 member ids %s", group0_ids)
        token_ring_ids = [member["host_id"] for member in self._node.get_token_ring_members()]
        LOGGER.debug("Token ring member ids: %s", token_ring_ids)
        diff = set(group0_ids) - set(token_ring_ids) or set(token_ring_ids) - set(group0_ids)
        LOGGER.debug("Difference between group0 and token ring: %s", diff)
        num_of_nodes = len(self._node.parent_cluster.nodes)
        LOGGER.debug("Number of nodes in sct cluster %s", num_of_nodes)
        non_voters_ids = self.search_inconsistent_host_ids()

        return not diff and not non_voters_ids and len(group0_ids) == len(token_ring_ids) == num_of_nodes

<<<<<<< HEAD
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
    def check_group0_tokenring_consistency(
            self, group0_members: list[dict[str, str]],
            tokenring_members: list[dict[str, str]]) -> HealthEventsGenerator:
        LOGGER.debug("Check group0 and token ring consistency on node %s (host_id=%s)...",
                     self._node.name, self._node.host_id)
        token_ring_node_ids = [member["host_id"] for member in tokenring_members]
        for member in group0_members:
            if member["voter"] and member["host_id"] in token_ring_node_ids:
                continue
            error_message = f"Node {self._node.name} has group0 member with host_id {member['host_id']} with " \
                f"can_vote {member['voter']} and " \
                f"presents in token ring {member['host_id'] in token_ring_node_ids}. " \
                f"Inconsistency between group0: {group0_members} " \
                f"and tokenring: {tokenring_members}"
            LOGGER.error(error_message)
            yield ClusterHealthValidatorEvent.Group0TokenRingInconsistency(
                severity=Severity.ERROR,
                node=self._node.name,
                error=error_message,
            )
        LOGGER.debug("Group0 and token-ring are consistent on node %s (host_id=%s)...",
                     self._node.name, self._node.host_id)

    def call_read_barrier(self):
        """ Wait until the node applies all previously committed Raft entries

        Any schema/topology changes are committed with Raft group0 on node. Before
        change is written to node, raft group0 checks that all previous schema/
        topology changes were applied. Raft triggers read_barrier on node, and
        node applies all previous changes(commits) before new schema/operation
        write will be applied. After read barrier finished, it guarantees that
        node has all schema/topology changes, which was done in cluster before
        read_barrier started on node.

        """
        with self._node.parent_cluster.cql_connection_patient_exclusive(node=self._node) as session:
            raft_group0_id = self.get_group0_id(session)
            assert raft_group0_id, "Group0 id was not found"
        try:
            api = RaftApi(self._node)
            result = api.read_barrier(group_id=raft_group0_id)
            LOGGER.debug("Api response %s", result)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.error("Trigger read-barrier via rest api failed %s", exc)

=======
    def check_group0_tokenring_consistency(
            self, group0_members: list[dict[str, str]],
            tokenring_members: list[dict[str, str]]) -> HealthEventsGenerator:
        LOGGER.debug("Check group0 and token ring consistency on node %s (host_id=%s)...",
                     self._node.name, self._node.host_id)
        token_ring_node_ids = [member["host_id"] for member in tokenring_members]
        broken_hosts = self.search_inconsistent_host_ids()
        for member in group0_members:
            if member["host_id"] in token_ring_node_ids and member["host_id"] not in broken_hosts:
                continue
            error_message = f"Node {self._node.name} has group0 member with host_id {member['host_id']} with " \
                f"can_vote {member['voter']} and " \
                f"presents in token ring {member['host_id'] in token_ring_node_ids}. " \
                f"Inconsistency between group0: {group0_members} " \
                f"and tokenring: {tokenring_members}"
            LOGGER.error(error_message)
            yield ClusterHealthValidatorEvent.Group0TokenRingInconsistency(
                severity=Severity.ERROR,
                node=self._node.name,
                error=error_message,
            )
        LOGGER.debug("Group0 and token-ring are consistent on node %s (host_id=%s)...",
                     self._node.name, self._node.host_id)

    def call_read_barrier(self):
        """ Wait until the node applies all previously committed Raft entries

        Any schema/topology changes are committed with Raft group0 on node. Before
        change is written to node, raft group0 checks that all previous schema/
        topology changes were applied. Raft triggers read_barrier on node, and
        node applies all previous changes(commits) before new schema/operation
        write will be applied. After read barrier finished, it guarantees that
        node has all schema/topology changes, which was done in cluster before
        read_barrier started on node.

        """
        with self._node.parent_cluster.cql_connection_patient_exclusive(node=self._node) as session:
            raft_group0_id = self.get_group0_id(session)
            assert raft_group0_id, "Group0 id was not found"
        try:
            api = RaftApi(self._node)
            result = api.read_barrier(group_id=raft_group0_id)
            LOGGER.debug("Api response %s", result)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.error("Trigger read-barrier via rest api failed %s", exc)

    def search_inconsistent_host_ids(self) -> list[str]:
        """ Search inconsistent hosts in group zero and token ring

        Find difference between group0 and tokenring and return hostid
        of nodes which should be removed for restoring consistency
                """
        with self._node.parent_cluster.cql_connection_patient_exclusive(node=self._node) as session:
            limited_voters_feature_enabled = is_group0_limited_voters_enabled(session)
        host_ids = self.get_diff_group0_token_ring_members()

        LOGGER.debug("Difference between group0 and token ring: %s", host_ids)
        # Starting from 2025.2 not all alive nodes are voters. get
        # non voters node only for older versions < 2025.2
        if not host_ids and limited_voters_feature_enabled:
            LOGGER.debug("Get non-voter member hostids")
            host_ids = self.get_group0_non_voters()
        return host_ids

>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)

class NoRaft(RaftFeatureOperations):
    TOPOLOGY_OPERATION_LOG_PATTERNS = {
        TopologyOperations.DECOMMISSION: [
            MessagePosition("DECOMMISSIONING: unbootstrap starts", LogPosition.BEGIN)],
        TopologyOperations.BOOTSTRAP: [
            MessagePosition("entering BOOTSTRAP mode", LogPosition.BEGIN)
        ]
    }

    def __init__(self, node: "BaseNode") -> None:
        super().__init__()
        self._node = node

    @property
    def is_enabled(self) -> bool:
        return False

    @property
    def is_consistent_topology_changes_enabled(self) -> bool:
        return False

    def get_status(self) -> str:
        return ""

    def is_ready(self) -> bool:
        return False

    def get_group0_members(self) -> list[dict[str, str]]:
        return []

    def get_group0_non_voters(self) -> list[str]:
        return []

    def clean_group0_garbage(self, raise_exception: bool = False) -> None:
        return

    def get_diff_group0_token_ring_members(self) -> list[str]:
        return []

    @staticmethod
    def get_severity_change_filters_scylla_start_failed(timeout: int = None) -> tuple:
        return (contextlib.nullcontext(),)

    def is_cluster_topology_consistent(self) -> bool:
        token_ring_ids = [member["host_id"] for member in self._node.get_token_ring_members()]
        LOGGER.debug("Token ring member ids: %s", token_ring_ids)
        num_of_nodes = len(self._node.parent_cluster.nodes)
        LOGGER.debug("Number of nodes in sct cluster %s", num_of_nodes)

        return len(token_ring_ids) == num_of_nodes

<<<<<<< HEAD
||||||| parent of 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)
    def check_group0_tokenring_consistency(
            self, group0_members: list[dict[str, str]],
            tokenring_members: list[dict[str, str]]) -> Generator[None, None, None]:
        LOGGER.debug("Raft feature is disabled on node %s (host_id=%s)", self._node.name, self._node.host_id)

        yield None

    def call_read_barrier(self):
        ...

=======
    def check_group0_tokenring_consistency(
            self, group0_members: list[dict[str, str]],
            tokenring_members: list[dict[str, str]]) -> Generator[None, None, None]:
        LOGGER.debug("Raft feature is disabled on node %s (host_id=%s)", self._node.name, self._node.host_id)

        yield None

    def call_read_barrier(self):
        ...

    def search_inconsistent_host_ids(self) -> list[str]:
        return []

>>>>>>> 67c34d2e3 (fix(limited_voters): validate non-voters as failed for scylla <=2025.1)

def get_raft_mode(node) -> RaftFeature | NoRaft:
    with node.parent_cluster.cql_connection_patient(node) as session:
        return RaftFeature(node) if is_consistent_cluster_management_feature_enabled(session) else NoRaft(node)


__all__ = ["get_raft_mode",
           "Group0MembersNotConsistentWithTokenRingMembersException",
           ]

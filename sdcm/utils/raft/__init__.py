import contextlib
import logging

from typing import Protocol
from collections import namedtuple

from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsSeverityChangerFilter
from sdcm.sct_events import Severity


LOGGER = logging.getLogger(__name__)


class Group0MembersNotConsistentWithTokenRingMembersException(Exception):
    pass


class RaftFeatureOperations(Protocol):
    _node: 'BaseNode'

    @property
    def is_enabled(self) -> bool:
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

    def get_diff_group0_token_ring_members(self) -> list[str]:
        ...

    def is_cluster_topology_consistent(self) -> bool:
        ...


class Raft(RaftFeatureOperations):
    def __init__(self, node: "BaseNode") -> None:
        super().__init__()
        self._node = node

    @property
    def is_enabled(self) -> bool:
        return True

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
        except Exception as exc:  # pylint: disable=broad-except
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
        host_ids = self.get_diff_group0_token_ring_members()
        if not host_ids:
            LOGGER.debug("Node could return to token ring but not yet bootstrap")
            host_ids = self.get_group0_non_voters()
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

        if missing_host_ids := self.get_diff_group0_token_ring_members():
            token_ring_members = self._node.get_token_ring_members()
            group0_members = self.get_group0_members()
            error_msg = f"Token ring {token_ring_members} and group0 {group0_members} are differs on: {missing_host_ids}"
            LOGGER.error(error_msg)
            if raise_exception:
                raise Group0MembersNotConsistentWithTokenRingMembersException(error_msg)
        LOGGER.debug("Group0 cleaned")

    @staticmethod
    def get_log_patterns_for_decommission_abort() -> list[str]:
        return ["DECOMMISSIONING: unbootstrap starts",
                "DECOMMISSIONING: unbootstrap done",
                "becoming a group 0 non-voter",
                "became a group 0 non-voter",
                "leaving token ring",
                "left token ring",
                "Finished token ring movement"]

    @staticmethod
    def get_log_pattern_for_bootstrap_abort() -> list[str]:
        return ["Starting to bootstrap",
                "setup_group0: joining group 0",
                "setup_group0: successfully joined group 0",
                "setup_group0: the cluster is ready to use Raft. Finishing",
                "entering BOOTSTRAP mode",
                "storage_service - Bootstrap completed!",
                "finish_setup_after_join: becoming a voter in the group 0 configuration",
                "raft_group0 - finish_setup_after_join: group 0 ID present, loading server info",
                "became a group 0 voter",
                ]

    @staticmethod
    def get_severity_change_filters_scylla_start_failed(timeout: int | None = None) -> tuple:
        return (
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=".*storage_service - decommission.*Operation failed",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=".*This node was decommissioned and will not rejoin the ring",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=".*Startup failed: std::runtime_error.*is removed from the cluster",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.DATABASE_ERROR,
                                        regex=".*gossip - is_safe_for_restart.*status=LEFT",
                                        extra_time_to_expiration=timeout),
            EventsSeverityChangerFilter(new_severity=Severity.WARNING,
                                        event_class=DatabaseLogEvent.RUNTIME_ERROR,
                                        regex=".*init - Startup failed: std::runtime_error.*already exists, cancelling join",
                                        extra_time_to_expiration=timeout)
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

        return not diff and len(group0_ids) == len(token_ring_ids) == num_of_nodes


class NoRaft(RaftFeatureOperations):
    def __init__(self, node: "BaseNode") -> None:
        super().__init__()
        self._node = node

    @property
    def is_enabled(self) -> bool:
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
    def get_log_patterns_for_decommission_abort() -> list[str]:
        return ["DECOMMISSIONING: unbootstrap starts"]

    @staticmethod
    def get_log_pattern_for_bootstrap_abort() -> list[str]:
        return ["entering BOOTSTRAP mode"]

    @staticmethod
    def get_severity_change_filters_scylla_start_failed(timeout: int = None) -> tuple:
        return (contextlib.nullcontext(),)

    def is_cluster_topology_consistent(self) -> bool:
        token_ring_ids = [member["host_id"] for member in self._node.get_token_ring_members()]
        LOGGER.debug("Token ring member ids: %s", token_ring_ids)
        num_of_nodes = len(self._node.parent_cluster.nodes)
        LOGGER.debug("Number of nodes in sct cluster %s", num_of_nodes)

        return len(token_ring_ids) == num_of_nodes


def get_raft_mode(node) -> Raft | NoRaft:
    with node.remote_scylla_yaml() as scylla_yaml:
        if node.is_kubernetes():
            consistent_cluster_management = scylla_yaml.get('consistent_cluster_management')
        else:
            consistent_cluster_management = scylla_yaml.consistent_cluster_management
        node.log.debug("consistent_cluster_management : %s", consistent_cluster_management)
        return Raft(node) if consistent_cluster_management else NoRaft(node)


__all__ = ["get_raft_mode",
           "Group0MembersNotConsistentWithTokenRingMembersException",
           ]

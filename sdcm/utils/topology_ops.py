import logging
import traceback

from typing import TypeVar
from uuid import UUID

from sdcm.wait import wait_for
from sdcm.exceptions import KillNemesis
from sdcm.utils.raft import get_node_status_from_system_by

LOGGER = logging.getLogger(__name__)
BaseNode = TypeVar("BaseNode")
BaseScyllaCluster = TypeVar("BaseScyllaCluster")


class FailedDecommissionOperationMonitoring:
    """Monitor status of decommission operation after the command ends

    If decommission process is still running in raft topology (the command failed,
    or the node was rebooted meanwhile), wait while it be finished.
    If the command failed, also check operation status.
    """

    def __init__(self, target_node: "BaseNode", verification_node: "BaseNode", timeout: int | float = 7200):
        self.timeout = timeout
        self.target_node = target_node
        self.db_cluster: "BaseScyllaCluster" = target_node.parent_cluster
        self.verification_node = verification_node
        # Taken while the node is still up and known to gossip
        host_id = (
            target_node.host_id
            or get_node_status_from_system_by(verification_node, ip_address=target_node.ip_address).host_id
        )
        if not host_id:
            raise ValueError(f"Failed to get host id of {target_node.name}")
        self.target_host_id = UUID(host_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        decommission_in_progress = self.is_node_decommissioning()
        if not exc_type:
            # The command returned, but the node could be rebooted meanwhile: the coordinator keeps the
            # request and finishes (or rolls back) it once the node is back. Let the topology settle,
            # the caller checks the result.
            if decommission_in_progress:
                self._wait_decommission_is_finished()
            return None
        # Do not check decommission status if SCT raise KillNemesis
        if exc_type != KillNemesis or decommission_in_progress:
            LOGGER.warning("Decommission failed with error: %s", traceback.format_exception(exc_type, exc_val, exc_tb))
            if self.is_node_decommissioning():
                self._wait_decommission_is_finished()
            self.db_cluster.verify_decommission(self.target_node)
            return True

    def _wait_decommission_is_finished(self):
        LOGGER.debug("Wait decommission to be done...")
        wait_for(
            func=lambda: not self.is_node_decommissioning(),
            step=15,
            timeout=self.timeout,
            text=f"Waiting decommission is finished for {self.target_node.name}...",
        )

    def is_node_decommissioning(self) -> bool:
        """Whether the raft topology still has a pending or running decommission of the target node.

        Read system.topology rather than system.cluster_status: the latter reports a rebooted node as
        'shutdown' until gossip sees it again, although the coordinator is going to resume the decommission.
        The table is local, so read it from the verification node only, never from the rebooted target.
        """
        with self.db_cluster.cql_connection_patient_exclusive(node=self.verification_node) as session:
            session.default_timeout = 300
            row = session.execute(
                "SELECT node_state, topology_request FROM system.topology WHERE key = 'topology' AND host_id = %s",
                (self.target_host_id,),
            ).one()
        LOGGER.debug("The node %s raft topology state: %s", self.target_node.name, row)
        return bool(row) and (row.node_state == "decommissioning" or row.topology_request == "leave")

import logging
import traceback

from dataclasses import dataclass
from enum import Enum
from typing import TypeVar

from sdcm.wait import wait_for
from sdcm.exceptions import KillNemesis

LOGGER = logging.getLogger(__name__)
BaseNode = TypeVar("BaseNode")
BaseScyllaCluster = TypeVar("BaseScyllaCluster")
Session = TypeVar("Session")


class OperationState(Enum):
    decommission = "DECOMMISSIONING"
    shutdown = "shutdown"
    normal = "NORMAL"


@dataclass
class NodeStatus:
    ip_address: str
    host_id: str
    status: str
    up: bool


class FailedDecommissionOperationMonitoring:
    """Monitor status of decommission operation after fail

    If decommission command was failed by any reason but decommission
    process still running, wait while it be finished and check operation status
    """

    def __init__(self, target_node: "BaseNode", verification_node: "BaseNode", timeout: int | float = 7200):
        self.timeout = timeout
        self.target_node = target_node
        self.db_cluster: "BaseScyllaCluster" = target_node.parent_cluster
        self.target_node_ip = target_node.ip_address
        self.verification_node = verification_node

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        decommission_in_progress = self.is_node_decommissioning()
        # Do not check decommission status if SCT raise KillNemesis
        if (exc_type and exc_type != KillNemesis) or decommission_in_progress:
            LOGGER.warning("Decommission failed with error: %s", traceback.format_exception(exc_type, exc_val, exc_tb))
            decommission_in_progress = self.is_node_decommissioning()
            if not decommission_in_progress:
                self.db_cluster.verify_decommission(self.target_node)
                return True
            else:
                LOGGER.debug("Wait decommission to be done...")
                wait_for(func=lambda: not self.is_node_decommissioning(), step=15,
                         timeout=self.timeout,
                         text=f"Waiting decommission is finished for {self.target_node.name}...")
                self.db_cluster.verify_decommission(self.target_node)
                return True

    def is_node_decommissioning(self):
        with self.db_cluster.cql_connection_patient(node=self.verification_node) as session:
            node_status = get_node_state_by_ip(session, ip_address=self.target_node_ip)
            LOGGER.debug("The node %s state after decommission failed: %s", self.target_node.name, node_status)
            return node_status.status == OperationState.decommission.value if node_status else False


def get_node_state_by_ip(session: "Session", ip_address: str) -> NodeStatus | None:
    nodes_states = get_nodes_states_from_system_table(session)
    for node_state in nodes_states:
        if node_state.ip_address == ip_address:
            return node_state


def get_node_state_by_hostid(session: "Session", host_id: str) -> NodeStatus | None:
    nodes_states = get_nodes_states_from_system_table(session)
    for node_state in nodes_states:
        if node_state.ip_address == host_id:
            return node_state


def get_nodes_states_from_system_table(session) -> list[NodeStatus]:
    """Get cluster status from system.cluster_status table

    The table contains current cluster status for each node and updated
    by raft accordingly
    """
    query = "select peer, host_id, status, up from system.cluster_status"
    session.default_timeout = 300
    results = session.execute(query)
    nodes_status = []
    for row in results:
        nodes_status.append(NodeStatus(row.peer, str(row.host_id), row.status, row.up))
    LOGGER.debug("All nodes status: %s", nodes_status)
    return nodes_status

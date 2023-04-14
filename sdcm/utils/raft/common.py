import logging


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

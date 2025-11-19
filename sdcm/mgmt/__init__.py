from typing import Union

from .common import ScyllaManagerError, TaskStatus, HostStatus, HostSsl, HostRestStatus
from .cli import ScyllaManagerToolRedhatLike, ScyllaManagerToolNonRedhat, ManagerCluster
from .operator import ScyllaManagerToolOperator, OperatorManagerCluster


AnyManagerTool = Union[ScyllaManagerToolOperator, ScyllaManagerToolRedhatLike, ScyllaManagerToolNonRedhat]
AnyManagerCluster = Union[OperatorManagerCluster, ManagerCluster]


def get_scylla_manager_tool(manager_node, scylla_cluster=None) -> AnyManagerTool:
    if manager_node.is_kubernetes():
        return ScyllaManagerToolOperator(manager_node=manager_node, scylla_cluster=scylla_cluster)
    if manager_node.distro.is_rhel_like:
        return ScyllaManagerToolRedhatLike(manager_node=manager_node)
    return ScyllaManagerToolNonRedhat(manager_node=manager_node)


__all__ = [
    "ScyllaManagerError",
    "TaskStatus",
    "HostStatus",
    "HostSsl",
    "HostRestStatus",
    "AnyManagerTool",
    "AnyManagerCluster",
    "get_scylla_manager_tool",
]

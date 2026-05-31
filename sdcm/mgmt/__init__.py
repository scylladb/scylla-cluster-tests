from typing import Union

from .common import ScyllaManagerError, TaskStatus, HostStatus, HostSsl, HostRestStatus
from .cli import ManagerCluster, ScyllaManagerToolDocker, ScyllaManagerToolNonRedhat, ScyllaManagerToolRedhatLike
from .operator import ScyllaManagerToolOperator, OperatorManagerCluster


AnyManagerTool = Union[
    ScyllaManagerToolOperator, ScyllaManagerToolRedhatLike, ScyllaManagerToolNonRedhat, ScyllaManagerToolDocker
]
AnyManagerCluster = Union[OperatorManagerCluster, ManagerCluster]


def get_scylla_manager_tool(manager_node, scylla_cluster=None) -> AnyManagerTool:
    if manager_node.is_kubernetes():
        return ScyllaManagerToolOperator(manager_node=manager_node, scylla_cluster=scylla_cluster)
    if manager_node.is_docker():
        # The container name is validated in create_sctool() (sdcm/mgmt/cli.py), which every
        # sctool user goes through -- no need to repeat the check here.
        parent_cluster = getattr(manager_node, "parent_cluster", None)
        return ScyllaManagerToolDocker(
            manager_node=manager_node,
            manager_container_name=getattr(parent_cluster, "manager_container_name", None),
        )
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

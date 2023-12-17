from .cli import ManagerCluster, ScyllaManagerToolNonRedhat, ScyllaManagerToolRedhatLike
from .common import HostRestStatus, HostSsl, HostStatus, ScyllaManagerError, TaskStatus
from .operator import OperatorManagerCluster, ScyllaManagerToolOperator

AnyManagerTool = ScyllaManagerToolOperator | ScyllaManagerToolRedhatLike | ScyllaManagerToolNonRedhat
AnyManagerCluster = OperatorManagerCluster | ManagerCluster


def get_scylla_manager_tool(manager_node, scylla_cluster=None) -> AnyManagerTool:
    if manager_node.is_kubernetes():
        return ScyllaManagerToolOperator(manager_node=manager_node, scylla_cluster=scylla_cluster)
    if manager_node.distro.is_rhel_like:
        return ScyllaManagerToolRedhatLike(manager_node=manager_node)
    return ScyllaManagerToolNonRedhat(manager_node=manager_node)

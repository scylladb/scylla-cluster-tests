import contextlib
import logging
import random

from typing import Optional

from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def block_scylla_ports(target_node: BaseNode, ports: list[int] | None = None):
    ports = ports or [7001, 7000, 9042, 9142, 19042, 19142]
    target_node.install_package("iptables")
    target_node.start_service("iptables", ignore_status=True)
    target_node.log.debug("Block connections %s", target_node.name)
    for port in ports:
        target_node.remoter.sudo(f"iptables -A INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"iptables -A OUTPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"ip6tables -A INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"ip6tables -A OUTPUT -p tcp --dport {port} -j DROP")
    yield
    target_node.log.debug("Remove all iptable rules %s", target_node.name)
    for port in ports:
        target_node.remoter.sudo(f"iptables -D INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"iptables -D OUTPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"ip6tables -D INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"ip6tables -D OUTPUT -p tcp --dport {port} -j DROP")
    target_node.stop_service("iptables", ignore_status=True)


@contextlib.contextmanager
def pause_scylla_with_sigstop(target_node: BaseNode):
    target_node.log.debug("Send signal SIGSTOP to scylla process on node %s", target_node.name)
    target_node.remoter.sudo("pkill --signal SIGSTOP -e scylla", timeout=60)
    yield
    target_node.log.debug("Send signal SIGCONT to scylla process on node %s", target_node.name)
    target_node.remoter.sudo(cmd="pkill --signal SIGCONT -e scylla", timeout=60)


@contextlib.contextmanager
def block_loaders_payload_for_scylla_node(scylla_node: BaseNode, loader_nodes: list[BaseNode]):
    """ Block connections from loaders to cql ports on scylla node

    Make the Scylla node inaccessible to loaders by blocking
    any subsequent connections to the Scylla node.
    This ensures that the stress tool can continue to operate without failure
    even if the Scylla node is banned and removed from the cluster.
    """
    ports = [9042, 9142, 19042, 19142]
    scylla_node.install_package("iptables")
    scylla_node.start_service("iptables", ignore_status=True)
    loader_nodes_names = [node.name for node in loader_nodes]
    blocking_ips = [node.ip_address for node in loader_nodes]
    scylla_node.log.debug("Block connections on %s from loader nodes %s", scylla_node.name, loader_nodes_names)
    for port in ports:
        scylla_node.remoter.sudo(
            f"iptables -A INPUT -s {','.join(blocking_ips)} -p tcp --dport {port} -j DROP", ignore_status=True)
        scylla_node.remoter.sudo(
            f"ip6tables -A INPUT -s {','.join(blocking_ips)} -p tcp --dport {port} -j DROP", ignore_status=True)
    yield
    # if scylla_node is alive, then delete the iptables rules
    if scylla_node.remoter.is_up():
        for port in ports:
            scylla_node.remoter.sudo(
                f"iptables -D INPUT -s {','.join(blocking_ips)} -p tcp --dport {port} -j DROP", ignore_status=True)
            scylla_node.remoter.sudo(
                f"ip6tables -D INPUT -s {','.join(blocking_ips)} -p tcp --dport {port} -j DROP", ignore_status=True)
        scylla_node.stop_service("iptables", ignore_status=True)


def is_node_removed_from_cluster(removed_node: BaseNode, verification_node: BaseNode) -> bool:
    LOGGER.debug("Verification node %s", verification_node.name)
    cluster_status: Optional[dict] = removed_node.parent_cluster.get_nodetool_status(
        verification_node=verification_node)
    if not cluster_status:
        return False
    result = []
    for dc in cluster_status:
        result.append(removed_node.ip_address not in cluster_status[dc].keys())
    return all(result)


def is_node_seen_as_down(down_node: BaseNode, verification_node: BaseNode) -> bool:
    LOGGER.debug("Verification node %s", verification_node.name)
    nodes_status = verification_node.parent_cluster.get_nodetool_status(verification_node, dc_aware=False)
    down_node_status = nodes_status.get(down_node.ip_address)
    return (not down_node_status or down_node_status["state"] == "DN")


def handle_failed_removenode(node_to_remove: BaseNode, verification_node: BaseNode) -> None:
    """
    Attempt recovery steps if a removed node is still in the cluster:
    - Clean up garbage Raft host IDs.
    - Remove from dead_nodes_list if consistent with the ring.
    """
    LOGGER.error(f"nodetool removenode failed for node {node_to_remove}")

    garbage_host_ids = verification_node.raft.get_diff_group0_token_ring_members()
    LOGGER.debug("Difference between token ring and group0 is %s", garbage_host_ids)

    if garbage_host_ids:
        verification_node.raft.clean_group0_garbage()
    else:
        LOGGER.debug(f"Attempting to remove node {node_to_remove} from dead_nodes_list")
        node = next(
            (n for n in node_to_remove.parent_cluster.dead_nodes_list if n.ip_address == node_to_remove.ip_address),
            None
        )
        if node:
            node_to_remove.parent_cluster.dead_nodes_list.remove(node)
        else:
            LOGGER.debug(f"Node {node_to_remove.name} not found in dead_nodes_list")


def ensure_seed_node_after_removal(cluster, nodes_to_remove):
    """
    Ensures that the cluster will not lose all seed nodes after removing the given nodes.
    Promotes one of the surviving nodes to be a seed if needed.
    """
    surviving_nodes = [node for node in cluster.nodes if node not in nodes_to_remove]
    surviving_seeds = [node for node in surviving_nodes if node.is_seed]

    if not surviving_seeds:
        candidate = random.choice(surviving_nodes)
        candidate.set_seed_flag(True)
        cluster.update_seed_provider()
        LOGGER.info(
            f"Promoted {candidate} to seed to maintain availability after removing {[n.name for n in nodes_to_remove]}"
        )

def ensure_rack_symmetry(cluster, selected_nodes):
    """
    Ensure one node is selected per rack across all racks in the cluster.
    """
    available_racks = {
        (dc, rack_idx)
        for dc, racks in cluster.get_nodes_per_datacenter_and_rack_idx().items()
        for rack_idx in racks
    }

    return len(selected_nodes) == len(available_racks)

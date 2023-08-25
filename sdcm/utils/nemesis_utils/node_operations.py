import contextlib
import logging

from typing import Optional
from random import choice

from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def use_network_firewall(target_node: "BaseNode"):
    ports = [7001, 7000, 9042, 9142, 19042, 19142]
    target_node.install_package("iptables")
    target_node.log.debug("Block connections %s", target_node.name)
    for port in ports:
        target_node.remoter.sudo(f"iptables -A INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"iptables -A OUTPUT -p tcp --dport {port} -j DROP")
    yield
    target_node.log.debug("Remove all iptable rules %s", target_node.name)
    for port in ports:
        target_node.remoter.sudo(f"iptables -D INPUT -p tcp --dport {port} -j DROP")
        target_node.remoter.sudo(f"iptables -D OUTPUT -p tcp --dport {port} -j DROP")


@contextlib.contextmanager
def use_send_signal_process(target_node: "BaseNode"):
    target_node.log.debug("Send signal SIGSTOP to scylla process on node %s", target_node.name)
    target_node.remoter.sudo("pkill --signal SIGSTOP -e scylla", timeout=60)
    yield
    target_node.log.debug("Send signal SIGCONT to scylla process on node %s", target_node.name)
    target_node.remoter.sudo(cmd="pkill --signal SIGCONT -e scylla", timeout=60)


def get_random_free_verification_node(node: "BaseNode") -> "BaseNode":
    return choice([active_node for active_node in node.parent_cluster.nodes if not active_node.running_nemesis])


def check_node_removed_from_cluster(node: "BaseNode"):
    LOGGER.debug("Get random verification node without running nemesis")
    verification_node = get_random_free_verification_node(node)
    LOGGER.debug("Chosen verification node %s", verification_node.name)
    cluster_status: Optional[dict] = node.parent_cluster.get_nodetool_status(verification_node=verification_node)
    if not cluster_status:
        return False
    result = []
    for dc in cluster_status:
        result.append(node.ip_address not in list(cluster_status[dc].keys()))
    return all(result)


def check_node_seen_as_down_in_cluster(down_node: BaseNode):
    LOGGER.debug("Get random verification node without running nemesis")
    verification_node = get_random_free_verification_node(down_node)
    LOGGER.debug("Chosen verification node %s", verification_node.name)
    return down_node not in down_node.parent_cluster.get_nodes_up_and_normal(verification_node)


def simulate_node_unavailability(use_ip_tables: bool = False):
    if use_ip_tables:
        return use_network_firewall
    else:
        return use_send_signal_process

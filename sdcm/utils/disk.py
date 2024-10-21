from sct_ssh import ScyllaDBCluster
from sdcm.cluster import BaseNode


def get_node_disk_usage(node: BaseNode) -> dict[str, int]:
    """Returns disk usage data for a node"""
    result = node.remoter.run(
        "df -h -BG --output=size,used,avail,pcent /var/lib/scylla | sed 1d | sed 's/G//g' | sed 's/%//'")
    size, used, avail, pcent = result.stdout.strip().split()
    return {
        'Total': int(size),
        'Used': int(used),
        'Available': int(avail),
        'Usage %': int(pcent)
    }


def get_cluster_disk_usage(cluster: ScyllaDBCluster) -> dict[str, dict[str, int]]:
    """Returns disk usage data for all nodes and cluster totals"""
    data: dict[str, dict[str, int]] = {}

    for idx, node in enumerate(cluster.nodes, 1):
        info = get_node_disk_usage(node)
        data[f"node_{idx}"] = info

    cluster_data = {key: sum(node_data[key] for node_data in data.values()) for key in data['node_1'].keys()}
    cluster_data["Usage %"] = cluster_data["Usage %"] // len(cluster.nodes)
    data["cluster"] = cluster_data

    return data

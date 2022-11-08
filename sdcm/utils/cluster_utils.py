from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module
from sdcm.utils.docker_utils import running_in_docker


def get_cluster_driver(docker_scylla) -> Cluster:
    if running_in_docker():
        address = f"{docker_scylla.internal_ip_address}:9042"
    else:
        address = docker_scylla.get_port("9042")
    node_ip, port = address.split(":")
    port = int(port)

    return Cluster([node_ip], port=port)

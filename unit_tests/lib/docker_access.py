import sys

from sdcm.utils.docker_utils import running_in_docker


def container_ip_reachable() -> bool:
    """Whether the test process can reach docker containers by their bridge IP and container port.

    True inside a container (CI) and on a Linux host, where the docker bridge is routable. Elsewhere only the
    published host ports are reachable.
    """
    return running_in_docker() or sys.platform == "linux"

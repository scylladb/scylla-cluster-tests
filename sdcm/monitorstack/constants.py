from sdcm.utils.common import get_free_port

GRAFANA_DOCKER_NAME = "agraf"
PROMETHEUS_DOCKER_NAME = "aprom"
ALERT_DOCKER_NAME = "aalert"

GRAFANA_DOCKER_PORT = get_free_port(
    ports_to_try=(
        3000,
        0,
    )
)
ALERT_DOCKER_PORT = get_free_port(
    ports_to_try=(
        6000,
        0,
    )
)
PROMETHEUS_DOCKER_PORT = get_free_port(
    ports_to_try=(
        9090,
        0,
    )
)
COMMAND_TIMEOUT = 1800

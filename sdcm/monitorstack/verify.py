import logging
import time

from sdcm.db_stats import PrometheusDBStats
from sdcm.logcollector import GrafanaEntity, MonitoringStack
from sdcm.remote import LocalCmdRunner

from sdcm.monitorstack.constants import (
    GRAFANA_DOCKER_NAME,
    GRAFANA_DOCKER_PORT,
    PROMETHEUS_DOCKER_NAME,
    PROMETHEUS_DOCKER_PORT,
)

LOGGER = logging.getLogger(name="monitoringstack")


def verify_monitoring_stack(containers_ports):
    checked_statuses = [
        verify_dockers_are_running(containers_ports=containers_ports),
        verify_grafana_is_available(grafana_docker_port=containers_ports["grafana_docker_port"]),
        verify_prometheus_is_available(prometheus_docker_port=containers_ports["prometheus_docker_port"]),
    ]
    return all(checked_statuses)


def verify_dockers_are_running(containers_ports):
    result = LocalCmdRunner().run("docker ps --format '{{.Names}}'", ignore_status=True)
    docker_names = result.stdout.strip().split()
    grafana_docker_port = containers_ports["grafana_docker_port"]
    prometheus_docker_port = containers_ports["prometheus_docker_port"]
    if result.ok and docker_names:
        if (
            f"{GRAFANA_DOCKER_NAME}-{grafana_docker_port}" in docker_names
            and f"{PROMETHEUS_DOCKER_NAME}-{prometheus_docker_port}" in docker_names
        ):
            LOGGER.info("Monitoring stack docker containers are running.\n%s", result.stdout)
            return True
    LOGGER.error("Monitoring stack containers are not running\nStdout:\n%s\nstderr:%s", result.stdout, result.stderr)
    return False


def verify_grafana_is_available(grafana_docker_port=GRAFANA_DOCKER_PORT):
    grafana_statuses = []
    for dashboard in GrafanaEntity.base_grafana_dashboards:
        try:
            LOGGER.info("Check dashboard %s", dashboard.title)
            result = MonitoringStack.get_dashboard_by_title(
                grafana_ip="localhost", port=grafana_docker_port, title=dashboard.title
            )
            grafana_statuses.append(result)
            LOGGER.info(f"Dashboard {dashboard.title} is available")
        except Exception as details:  # noqa: BLE001
            LOGGER.error("Dashboard %s is not available. Error: %s", dashboard.title, details)
            grafana_statuses.append(False)

    result = any(grafana_statuses)

    if not result:
        LOGGER.error("None of the expected dashboards are available.")

    return result


def verify_prometheus_is_available(prometheus_docker_port=PROMETHEUS_DOCKER_PORT):
    """Validate that request to Prometheus container is not failed.

    :returns: True if request is successful, False otherwise
    :rtype: {bool}
    """
    time_end = time.time()
    time_start = time_end - 600
    try:
        LOGGER.info("Send request to Prometheus")
        prom_client = PrometheusDBStats("localhost", port=prometheus_docker_port)
        prom_client.get_throughput(time_start, time_end)
        LOGGER.info("Prometheus is up")
        return True
    except Exception as details:  # noqa: BLE001
        LOGGER.error("Error requesting prometheus %s", details)
        return False

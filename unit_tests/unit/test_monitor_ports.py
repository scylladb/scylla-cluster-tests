"""Tests for moving the monitoring stack off ports a shared host already uses.

A lab host can run its own services on the stack's defaults, e.g. Fedora's Cockpit on 9090.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseMonitorSet
from sdcm.db_stats import PrometheusDBStats
from sdcm.logcollector import find_monitoring_container


def ss_listening(*ports: int) -> str:
    return "".join(f"LISTEN 0      4096   *:{port} *:*\n" for port in ports) + "LISTEN 0 128 0.0.0.0:22 0.0.0.0:*\n"


def pick(*taken: int) -> MagicMock:
    monitor_set = MagicMock(
        grafana_port=3000,
        prometheus_port=9090,
        alertmanager_port=9093,
        loki_port=3100,
        promtail_port=9080,
        monitoring_port_flags=None,
    )
    node = MagicMock()
    node.remoter.run.return_value.stdout = ss_listening(*taken)
    BaseMonitorSet.pick_monitoring_ports(monitor_set, node)
    return monitor_set


def test_free_defaults_keep_the_host_network_start():
    monitor_set = pick()

    assert monitor_set.monitoring_port_flags == ""
    assert (monitor_set.grafana_port, monitor_set.prometheus_port, monitor_set.alertmanager_port) == (3000, 9090, 9093)


def test_cockpit_on_9090_moves_only_prometheus():
    monitor_set = pick(9090)

    assert monitor_set.prometheus_port == 9091
    assert monitor_set.monitoring_port_flags == "-p 9091"
    assert monitor_set.grafana_port == 3000


@pytest.mark.parametrize(
    "taken, expected_prometheus",
    [((9090, 9091), 9092), ((9090, 9091, 9092), 9094)],
    ids=["next-free", "skips-alertmanager-default"],
)
def test_moved_port_is_free_and_not_another_service_default(taken, expected_prometheus):
    assert pick(*taken).prometheus_port == expected_prometheus


def test_every_taken_default_gets_its_own_flag():
    monitor_set = pick(3000, 9093, 3100, 9080)

    assert monitor_set.monitoring_port_flags == "-g 3001 -m 9094 --loki-port 3101 --promtail-port 9081"


def test_ports_are_picked_once():
    """A restart must reuse the ports, kill-all.sh finds the running containers by them."""
    monitor_set = MagicMock(monitoring_port_flags="-p 9091")
    node = MagicMock()

    BaseMonitorSet.pick_monitoring_ports(monitor_set, node)

    node.remoter.run.assert_not_called()


@pytest.mark.parametrize(
    "containers, name, expected",
    [
        ("aprom\nagraf\nagrafrender\naalert\n", "aprom", ("aprom", 9090)),
        ("aprom-9091\nagraf\nagrafrender\n", "aprom", ("aprom-9091", 9091)),
        ("agrafrender\nagraf-3001\n", "agraf", ("agraf-3001", 3001)),
        ("", "aalert", ("aalert", 9093)),
    ],
    ids=["defaults", "moved-prometheus", "renderer-is-not-grafana", "nothing-running"],
)
def test_log_collector_finds_the_container_and_its_port(containers, name, expected):
    """Log collection runs in its own process after the test, so it reads the ports off the container names."""
    node = MagicMock()
    node.remoter.run.return_value.stdout = containers

    assert find_monitoring_container(node, name) == expected


def test_prometheus_client_uses_the_port_the_monitor_set_picked():
    monitor_node = MagicMock(external_address="10.0.0.60")
    monitor_node.parent_cluster.prometheus_port = 9091

    with patch.object(PrometheusDBStats, "get_configuration", return_value={}):
        client = PrometheusDBStats.for_monitor(monitor_node)

    assert (client.host, client.port) == ("10.0.0.60", 9091)

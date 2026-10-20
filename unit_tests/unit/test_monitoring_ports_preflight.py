"""Tests for the monitoring ports part of the monitor host preflight check.

On the lab host in SCT-1215 Fedora's Cockpit held 9090, and Prometheus could not start there.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseMonitorSet, NodeSetupFailed, ensure_monitoring_ports_free

SSH_ONLY = 'LISTEN 0 128 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=900,fd=3))\n'
COCKPIT = (
    'LISTEN 0 4096 *:9090 *:* users:(("systemd",pid=1,fd=93))\n'
    'LISTEN 0 4096 [::]:9090 [::]:* users:(("systemd",pid=1,fd=94))\n'
)


def node_listening(ss_output: str) -> MagicMock:
    node = MagicMock()
    node.remoter.sudo.return_value.stdout = ss_output
    return node


def test_free_ports_pass():
    ensure_monitoring_ports_free(node_listening(SSH_ONLY))


def test_taken_port_fails_naming_the_port_service_and_holder_once():
    with pytest.raises(NodeSetupFailed) as failure:
        ensure_monitoring_ports_free(node_listening(SSH_ONLY + COCKPIT))

    message = str(failure.value)
    assert '9090 (Prometheus) is held by users:(("systemd",pid=1,fd=93))' in message
    assert message.count("9090") == 1, "an IPv4 and an IPv6 listener on one port are reported once"


def test_a_port_the_stack_opens_besides_the_ui_ones_is_checked_too():
    ss_output = 'LISTEN 0 4096 0.0.0.0:9094 0.0.0.0:* users:(("alertmanager",pid=5,fd=3))\n'

    with pytest.raises(NodeSetupFailed, match=r"9094 \(Alertmanager gossip\)"):
        ensure_monitoring_ports_free(node_listening(ss_output))


def test_every_taken_port_is_reported():
    ss_output = COCKPIT + 'LISTEN 0 4096 127.0.0.1:3000 0.0.0.0:* users:(("grafana",pid=7,fd=8))\n'

    with pytest.raises(NodeSetupFailed, match=r"9090 \(Prometheus\).*3000 \(Grafana\)"):
        ensure_monitoring_ports_free(node_listening(ss_output))


@pytest.mark.parametrize("formal_image", [False, True], ids=["plain-host", "formal-monitoring-image"])
def test_ports_are_not_checked_on_a_formal_monitoring_image(monkeypatch, formal_image):
    """A formal monitoring image may start its own stack at boot, on the same ports."""
    monkeypatch.setattr("sdcm.cluster.docker_host_preflight", MagicMock())
    ports_check = MagicMock()
    monkeypatch.setattr("sdcm.cluster.ensure_monitoring_ports_free", ports_check)

    BaseMonitorSet.preflight_check(MagicMock(is_formal_monitor_image=formal_image), MagicMock())

    assert ports_check.called is not formal_image

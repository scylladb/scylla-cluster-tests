"""Tests for the monitoring ports part of the monitor host preflight check.

On the lab host in SCT-1215 Fedora's Cockpit held 9090, and Prometheus could not start there.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.cluster import NodeSetupFailed, ensure_monitoring_ports_free

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


def test_every_taken_port_is_reported():
    ss_output = COCKPIT + 'LISTEN 0 4096 127.0.0.1:3000 0.0.0.0:* users:(("grafana",pid=7,fd=8))\n'

    with pytest.raises(NodeSetupFailed, match=r"9090 \(Prometheus\).*3000 \(Grafana\)"):
        ensure_monitoring_ports_free(node_listening(ss_output))

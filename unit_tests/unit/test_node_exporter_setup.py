# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

import os
import subprocess
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseMonitorSet
from sdcm.node_exporter_setup import NODE_EXPORTER_PORT_PROBE, NODE_EXPORTER_VERSION, NodeExporterSetup
from sdcm.remote import shell_script_cmd


def _remoter(machine, probe="free 9100\n"):
    """A remoter whose `uname -m` prints `machine` and whose install script ends with what its port probe found."""
    remoter = MagicMock()
    remoter.run.return_value = MagicMock(stdout=f"{machine}\n")
    remoter.sudo.return_value = MagicMock(stdout=f"tar output\n{probe}")
    return remoter


@pytest.mark.parametrize(
    "machine, expected",
    [("x86_64", "amd64"), ("amd64", "amd64"), ("aarch64", "arm64"), ("arm64", "arm64")],
)
def test_install_downloads_the_release_for_the_loader_machine(machine, expected):
    remoter = _remoter(machine)

    NodeExporterSetup.install(remoter=remoter)

    remoter.run.assert_any_call("uname -m", verbose=False)
    script = remoter.sudo.call_args.args[0]
    release = f"node_exporter-{NODE_EXPORTER_VERSION}.linux-{expected}"
    assert f"{release}.tar.gz" in script
    assert f"mv {release}/node_exporter /usr/local/bin" in script
    other = "arm64" if expected == "amd64" else "amd64"
    assert f"linux-{other}" not in script


def test_install_rejects_a_machine_with_no_node_exporter_release():
    remoter = _remoter("riscv64")

    with pytest.raises(ValueError, match="riscv64"):
        NodeExporterSetup.install(remoter=remoter)

    remoter.sudo.assert_not_called()


def test_install_requires_a_node_or_a_remoter():
    with pytest.raises(AssertionError):
        NodeExporterSetup.install()


def test_running_node_exporter_is_reused():
    """A distro node_exporter, or the one another node on the same host started, is scraped as is.

    The install script probes under a host lock, so a loader and a monitor on one host share an exporter.
    """
    remoter = _remoter("x86_64", probe="running 9100\n")

    assert NodeExporterSetup.install(remoter=remoter, scrape_host="10.0.0.7") == 9100


def test_install_listens_on_the_free_port_the_probe_found():
    remoter = _remoter("x86_64", probe="free 9101\n")

    assert NodeExporterSetup.install(remoter=remoter, scrape_host="10.0.0.7") == 9101


def test_install_fails_when_every_port_is_taken():
    remoter = _remoter("x86_64", probe="free \n")

    with pytest.raises(RuntimeError, match="No free port"):
        NodeExporterSetup.install(remoter=remoter, scrape_host="10.0.0.7")


def test_install_without_a_scrape_host_skips_the_probe():
    """DB nodes and the runner are scraped at the default port, so they don't move off it."""
    remoter = _remoter("x86_64", probe="free 9100\n")

    assert NodeExporterSetup.install(remoter=remoter) == 9100
    script = remoter.sudo.call_args.args[0]
    assert "port=9100" in script
    assert "ss -Hltn" not in script


FAKE_SS = """#!/bin/sh
# `ss -Hltn sport = :<port>` lists a socket when <port> is in $LISTENING
case " $LISTENING " in *" ${4#:} "*) echo "LISTEN 0 4096 *:${4#:} *:*" ;; esac
"""

FAKE_CURL = """#!/bin/sh
# `curl -s --max-time 2 http://<host>:<port>/metrics` answers as a node_exporter when <host>:<port> is in $EXPORTERS
address=${4#http://}
case " $EXPORTERS " in *" ${address%/metrics} "*) echo 'node_exporter_build_info{version="1.8.2"} 1' ;; esac
"""


@pytest.fixture
def run_probe(tmp_path):
    """Runs the probe NodeExporterSetup.install sends under its lock, against fake `ss` and `curl`, and returns what
    it prints."""
    for name, script in (("ss", FAKE_SS), ("curl", FAKE_CURL)):
        (tmp_path / name).write_text(script)
        (tmp_path / name).chmod(0o755)

    def run(scrape_host, listening="", exporters=""):
        remoter = _remoter("x86_64")
        NodeExporterSetup.install(remoter=remoter, scrape_host=scrape_host)
        probe = f"host={scrape_host}\n{NODE_EXPORTER_PORT_PROBE}"
        assert probe in remoter.sudo.call_args.args[0]
        probe = shell_script_cmd(f'{probe}echo "$state $port"', quote="'")
        env = dict(os.environ, PATH=f"{tmp_path}:{os.environ['PATH']}", LISTENING=listening, EXPORTERS=exporters)
        return subprocess.run(probe, shell=True, env=env, capture_output=True, text=True, check=True).stdout.strip()

    return run


@pytest.mark.parametrize(
    "listening, exporters, expected",
    [
        pytest.param("", "", "free 9100", id="nothing-listens"),
        pytest.param("9100", "", "free 9101", id="9100-held-by-something-else"),
        pytest.param("9100 9101", "10.0.0.7:9101", "running 9101", id="exporter-on-9101"),
        pytest.param("9100", "127.0.0.1:9100", "free 9101", id="exporter-bound-to-loopback-only"),
        pytest.param(" ".join(str(p) for p in range(9100, 9120)), "", "free", id="every-port-taken"),
    ],
)
def test_probe_finds_an_exporter_at_the_address_prometheus_scrapes(run_probe, listening, exporters, expected):
    """An exporter bound to loopback doesn't answer at the scrape address, so it isn't reused."""
    assert run_probe("10.0.0.7", listening=listening, exporters=exporters) == expected


def _monitor_set(monkeypatch, loaders=(), params=None):
    """A monitor set with one monitor on 10.0.0.7:9101, and the list its node_exporter_servers.yml is written to."""
    monkeypatch.setattr("sdcm.cluster.get_my_ip", lambda: "10.0.0.1")
    exporter_targets = []
    monitor = MagicMock(private_ip_address="10.0.0.7", node_exporter_port=9101)
    monitor._remote_yaml.return_value.__enter__.return_value = exporter_targets
    monitor_set = MagicMock(nodes=[monitor], targets={"loaders": MagicMock(nodes=list(loaders))})
    monitor_set.DB_NODES_IP_ADDRESS = BaseMonitorSet.DB_NODES_IP_ADDRESS
    monitor_set.params.get.side_effect = (params or {}).get
    return monitor_set, exporter_targets


def test_monitor_target_uses_the_port_its_exporter_serves_on(monkeypatch):
    monitor_set, exporter_targets = _monitor_set(monkeypatch)

    BaseMonitorSet.reconfigure_scylla_monitoring(monitor_set)

    assert {"labels": {"dc": "monitor"}, "targets": ["10.0.0.7:9101"]} in exporter_targets


def test_loader_target_follows_the_port_the_loader_picked(monkeypatch):
    """The monitor may write the targets before the loader's setup picked its port; the next call fixes it."""
    loader = MagicMock(ip_address="10.0.0.8", region="eu-west-1", node_exporter_port=9100)
    monitor_set, exporter_targets = _monitor_set(monkeypatch, loaders=[loader])

    BaseMonitorSet.reconfigure_scylla_monitoring(monitor_set)
    loader.node_exporter_port = 9101
    BaseMonitorSet.reconfigure_scylla_monitoring(monitor_set)

    assert {"labels": {"dc": "eu-west-1"}, "targets": ["10.0.0.8:9101"]} in exporter_targets
    assert not [dc for dc in exporter_targets if "10.0.0.8:9100" in dc["targets"]]


def test_cloud_prometheus_keeps_only_the_loader_targets(monkeypatch):
    loader = MagicMock(ip_address="10.0.0.8", region="eu-west-1", node_exporter_port=9101)
    monitor_set, exporter_targets = _monitor_set(
        monkeypatch, loaders=[loader], params={"cloud_prom_bearer_token": "token"}
    )

    BaseMonitorSet.reconfigure_scylla_monitoring(monitor_set)

    assert exporter_targets == [{"labels": {"dc": "eu-west-1"}, "targets": ["10.0.0.8:9101"]}]

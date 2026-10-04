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

"""The cleanup phase (`hydra clean-resources`) cleans up the hosts of a bare-metal run."""

from unittest.mock import MagicMock

import pytest

from sdcm import cluster_baremetal, keystore, remote
from sdcm.utils import resources_cleanup

TEST_ID = "c0ffee00-0000-4000-8000-000000000001"
HOSTS = {
    "db_nodes": {"username": "fedora", "node_list": [{"public_ip": "1.0.0.1", "private_ip": "10.0.0.1"}]},
    "loader_nodes": {"username": "fedora", "node_list": [{"public_ip": "1.0.0.2", "private_ip": "10.0.0.2"}]},
    "monitor_nodes": {"username": "fedora", "node_list": [{"public_ip": "1.0.0.3", "private_ip": "10.0.0.3"}]},
}


class Config(dict):
    region_names = []


@pytest.fixture(name="cleaned")
def fixture_cleaned(monkeypatch):
    """Record what the cleanup does instead of doing it: the containers removed and each host's cleanup."""
    calls = []
    monkeypatch.setattr(keystore.KeyStore, "get_baremetal_config", lambda self, name: HOSTS)
    monkeypatch.setattr(
        resources_cleanup, "clean_resources_docker", lambda tags, dry_run: calls.append(("containers", tags))
    )
    monkeypatch.setattr(
        remote.RemoteCmdRunnerBase, "create_remoter", lambda **kwargs: MagicMock(login=(kwargs["hostname"], kwargs))
    )

    def clean_up_host(host, tunnel_ports, scylla_disk_setup):
        calls.append((str(host), tunnel_ports, scylla_disk_setup))
        assert host.remoter.login[1]["user"] == "fedora"
        assert host.remoter.login[1]["key_file"] == "~/.ssh/key"

    monkeypatch.setattr(cluster_baremetal.PhysicalHost, "clean_up_host", clean_up_host)
    return calls


def _config(**params):
    defaults = {
        "cluster_backend": "baremetal",
        "s3_baremetal_config": "hosts",
        "user_credentials_path": "~/.ssh/key",
        "ip_ssh_connections": "public",
    }
    return Config(defaults | params)


def test_post_behavior_selects_the_hosts_to_clean(cleaned):
    tags = {"TestId": TEST_ID, "NodeType": ["scylla-db", "loader"]}

    resources_cleanup.clean_resources_baremetal(tags, config=_config())

    assert cleaned == [
        ("containers", tags),
        ("scylla-db 1.0.0.1", True, True),
        ("loader 1.0.0.2", True, False),
    ]


def test_all_hosts_are_cleaned_without_post_behavior(cleaned):
    resources_cleanup.clean_resources_baremetal({"TestId": TEST_ID}, config=_config(ip_ssh_connections="private"))

    assert cleaned == [
        ("scylla-db 10.0.0.1", False, True),
        ("loader 10.0.0.2", False, False),
        ("monitor 10.0.0.3", False, False),
    ]


def test_nothing_is_cleaned_when_post_behavior_keeps_everything(cleaned):
    resources_cleanup.clean_resources_baremetal({"TestId": TEST_ID, "NodeType": []}, config=_config())

    assert cleaned == []


def test_a_preinstalled_scylla_keeps_its_disk_setup(cleaned):
    resources_cleanup.clean_resources_baremetal(
        {"TestId": TEST_ID, "NodeType": ["scylla-db"]}, config=_config(use_preinstalled_scylla=True)
    )

    assert cleaned[1:] == [("scylla-db 1.0.0.1", True, False)]


def test_dry_run_touches_no_host(cleaned, monkeypatch):
    monkeypatch.setattr(remote.RemoteCmdRunnerBase, "create_remoter", MagicMock(side_effect=AssertionError("SSH")))

    resources_cleanup.clean_resources_baremetal({"TestId": TEST_ID}, config=_config(), dry_run=True)

    assert [call for call in cleaned if call[0] != "containers"] == []


def test_clean_cloud_resources_cleans_up_baremetal_hosts(monkeypatch):
    clean_baremetal = MagicMock()
    monkeypatch.setattr(resources_cleanup, "clean_resources_baremetal", clean_baremetal)
    monkeypatch.setattr(resources_cleanup, "clean_instances_aws", MagicMock(side_effect=AssertionError("AWS")))
    config = _config()

    assert resources_cleanup.clean_cloud_resources({"TestId": TEST_ID}, config=config) is True

    clean_baremetal.assert_called_once_with({"TestId": TEST_ID}, config=config, dry_run=False)

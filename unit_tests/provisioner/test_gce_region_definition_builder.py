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
from collections import namedtuple
from pathlib import Path
from unittest.mock import MagicMock, patch

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig


GCE_CONFIG = str(Path(__file__).parent.parent.parent / "defaults" / "gce_config.yaml")


def _make_config(monkeypatch, extra_env: dict | None = None):
    EnvConfig = namedtuple(
        "EnvConfig",
        [
            "SCT_CLUSTER_BACKEND",
            "SCT_TEST_ID",
            "SCT_CONFIG_FILES",
            "SCT_GCE_DATACENTER",
            "SCT_N_DB_NODES",
            "SCT_N_LOADERS",
            "SCT_N_MONITOR_NODES",
            "SCT_USER_PREFIX",
        ],
    )
    test_id = "3923f974-bf0e-4c3c-9f52-3f6473b8a0b7"
    test_config = TestConfig()
    test_config.set_test_id_only(test_id)
    env = EnvConfig(
        SCT_CLUSTER_BACKEND="gce",
        SCT_TEST_ID=test_config.test_id(),
        SCT_CONFIG_FILES=f'["{GCE_CONFIG}"]',
        SCT_GCE_DATACENTER="us-east1",
        SCT_N_DB_NODES="3",
        SCT_N_LOADERS="2",
        SCT_N_MONITOR_NODES="1",
        SCT_USER_PREFIX="unit",
    )
    for key, value in env._asdict().items():
        monkeypatch.setenv(key, value)
    if extra_env:
        for key, value in extra_env.items():
            monkeypatch.setenv(key, value)
    config = SCTConfiguration()
    return config, test_config


def _build_region_defs(config, test_config):
    """Build region definitions with SSH key fetching mocked out."""
    fake_ssh_key = MagicMock()
    fake_ssh_key.name = "fake-key"
    fake_ssh_key.public_key = b"ssh-ed25519 AAAA fake"
    with patch("sdcm.sct_provision.region_definition_builder.KeyStore") as mock_ks:
        mock_ks.return_value.get_ssh_key_pair.return_value = fake_ssh_key
        builder = region_definition_builder.get_builder(params=config, test_config=test_config)
        return builder.build_all_region_definitions()


def test_loader_root_disk_type_flows_into_instance_definition(monkeypatch):
    """root_disk_type from gce_root_disk_type_loader must appear in the InstanceDefinition for loader nodes."""
    config, test_config = _make_config(monkeypatch, {"SCT_GCE_ROOT_DISK_TYPE_LOADER": "pd-ssd"})

    region_defs = _build_region_defs(config, test_config)

    loader_defs = [d for d in region_defs[0].definitions if d.name and "loader" in d.name]
    assert loader_defs, "No loader definitions found"
    for loader_def in loader_defs:
        assert loader_def.root_disk_type == "pd-ssd", (
            f"Expected root_disk_type='pd-ssd' for loader, got '{loader_def.root_disk_type}'"
        )


def test_db_root_disk_type_flows_into_instance_definition(monkeypatch):
    """root_disk_type from gce_root_disk_type_db must appear in the InstanceDefinition for DB nodes."""
    config, test_config = _make_config(monkeypatch, {"SCT_GCE_ROOT_DISK_TYPE_DB": "pd-ssd"})

    region_defs = _build_region_defs(config, test_config)

    db_defs = [d for d in region_defs[0].definitions if d.name and "db" in d.name]
    assert db_defs, "No DB definitions found"
    for db_def in db_defs:
        assert db_def.root_disk_type == "pd-ssd", (
            f"Expected root_disk_type='pd-ssd' for DB node, got '{db_def.root_disk_type}'"
        )


def test_monitor_root_disk_type_flows_into_instance_definition(monkeypatch):
    """root_disk_type from gce_root_disk_type_monitor must appear in InstanceDefinition for monitor nodes."""
    config, test_config = _make_config(monkeypatch, {"SCT_GCE_ROOT_DISK_TYPE_MONITOR": "pd-standard"})

    region_defs = _build_region_defs(config, test_config)

    monitor_defs = [d for d in region_defs[0].definitions if d.name and "monitor" in d.name]
    assert monitor_defs, "No monitor definitions found"
    for monitor_def in monitor_defs:
        assert monitor_def.root_disk_type == "pd-standard", (
            f"Expected root_disk_type='pd-standard' for monitor, got '{monitor_def.root_disk_type}'"
        )

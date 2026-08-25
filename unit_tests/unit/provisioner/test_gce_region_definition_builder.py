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
from unittest.mock import MagicMock, patch

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig


@pytest.fixture
def make_config(monkeypatch, defaults_dir):
    gce_config = str(defaults_dir / "gce_config.yaml")
    base_env = {
        "SCT_CLUSTER_BACKEND": "gce",
        "SCT_CONFIG_FILES": f'["{gce_config}"]',
        "SCT_GCE_DATACENTER": "us-east1",
        "SCT_N_DB_NODES": "3",
        "SCT_N_LOADERS": "2",
        "SCT_N_MONITOR_NODES": "1",
        "SCT_USER_PREFIX": "unit",
    }

    def _factory(extra_env: dict | None = None):
        test_config = TestConfig()
        test_config.set_test_id_only("3923f974-bf0e-4c3c-9f52-3f6473b8a0b7")
        env = {**base_env, "SCT_TEST_ID": test_config.test_id(), **(extra_env or {})}
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return SCTConfiguration(), test_config

    return _factory


@pytest.fixture
def build_region_defs():
    def _factory(config, test_config):
        fake_ssh_key = MagicMock()
        fake_ssh_key.name = "fake-key"
        fake_ssh_key.public_key = b"ssh-ed25519 AAAA fake"
        with patch("sdcm.sct_provision.region_definition_builder.KeyStore") as mock_ks:
            mock_ks.return_value.get_ssh_key_pair.return_value = fake_ssh_key
            builder = region_definition_builder.get_builder(params=config, test_config=test_config)
            return builder.build_all_region_definitions()

    return _factory


@pytest.mark.parametrize(
    "extra_env,node_type,expected_disk_type",
    [
        ({"SCT_GCE_ROOT_DISK_TYPE_LOADER": "pd-ssd"}, "loader", "pd-ssd"),
        ({"SCT_GCE_ROOT_DISK_TYPE_DB": "pd-ssd"}, "scylla-db", "pd-ssd"),
        ({"SCT_GCE_ROOT_DISK_TYPE_MONITOR": "pd-standard"}, "monitor", "pd-standard"),
    ],
)
def test_root_disk_type_flows_into_instance_definition(
    make_config, build_region_defs, extra_env, node_type, expected_disk_type
):
    """root_disk_type config must appear in the InstanceDefinition for the corresponding node type."""
    config, test_config = make_config(extra_env)
    region_defs = build_region_defs(config, test_config)

    defs = [d for d in region_defs[0].definitions if d.tags.get("NodeType") == node_type]
    assert defs, f"No definitions with NodeType='{node_type}' found"
    for defn in defs:
        assert defn.root_disk_type == expected_disk_type, (
            f"Expected root_disk_type='{expected_disk_type}' for NodeType='{node_type}', got '{defn.root_disk_type}'"
        )


def test_oracle_db_instance_definition(make_config):
    """oracle-db must use oracle GCE params and the oracle-specific node name prefix."""
    config, test_config = make_config(
        {
            "SCT_DB_TYPE": "mixed_scylla",
            "SCT_N_TEST_ORACLE_DB_NODES": "1",
            "SCT_AMI_ID_DB_SCYLLA_DESC": "unit",
            "SCT_ORACLE_SCYLLA_VERSION": "",
            "SCT_GCE_IMAGE_DB_ORACLE": (
                "https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/fake-oracle"
            ),
            "SCT_GCE_INSTANCE_TYPE_DB_ORACLE": "z3-highmem-8-standardlssd",
        }
    )
    fake_ssh_key = MagicMock()
    fake_ssh_key.name = "fake-key"
    fake_ssh_key.public_key = b"ssh-ed25519 AAAA fake"

    with patch("sdcm.sct_provision.region_definition_builder.KeyStore") as mock_ks:
        mock_ks.return_value.get_ssh_key_pair.return_value = fake_ssh_key
        builder = region_definition_builder.get_builder(params=config, test_config=test_config)
        definition = builder.build_instance_definition(region="us-east1", node_type="oracle-db", index=1)

    assert definition.name == f"unit-oracle-db-node-{test_config.test_id()[:8]}-0-1"
    assert definition.image_id.endswith("fake-oracle")
    assert definition.type == "z3-highmem-8-standardlssd"
    assert definition.tags["NodeType"] == "oracle-db"

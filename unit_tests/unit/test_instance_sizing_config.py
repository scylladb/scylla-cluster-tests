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

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from sdcm import sct_config
from sdcm.utils.cloud_catalog.instance_catalog import InstanceCatalog, InstanceTypeInfo

logging.basicConfig(level=logging.ERROR)

_MINIMAL_CONFIG = "unit_tests/test_configs/minimal_test_case.yaml"

_AWS_INSTANCE = InstanceTypeInfo(
    instance_type="i8g.2xlarge",
    cloud="aws",
    family="i8g",
    vcpus=8,
    memory_gb=64.0,
    local_disk_gb=1875.0,
    local_disk_count=1,
    arch="arm64",
    price_per_hour=None,
)

_GCE_INSTANCE = InstanceTypeInfo(
    instance_type="n2-standard-8",
    cloud="gce",
    family="n2",
    vcpus=8,
    memory_gb=32.0,
    local_disk_gb=0.0,
    local_disk_count=0,
    arch="x86_64",
    price_per_hour=None,
)


def _make_catalog(*instances: InstanceTypeInfo) -> InstanceCatalog:
    cat = InstanceCatalog()
    cat.instances = list(instances)
    cat.cloud_defaults = {"aws": {"arch": "arm64"}, "gce": {"arch": "x86_64"}}
    cat.preferred_families = {
        "db": {"aws": ["i8g"], "gce": ["n2"]},
        "loader": {"aws": ["c6i"], "gce": ["n2"]},
        "monitor": {"aws": ["t3"], "gce": ["n2"]},
    }
    return cat


@pytest.fixture(autouse=True)
def silence_loggers():
    logging.getLogger("anyconfig").setLevel(logging.ERROR)
    logging.getLogger("botocore").setLevel(logging.CRITICAL)
    logging.getLogger("boto3").setLevel(logging.CRITICAL)


def test_dict_constraint_resolved_to_instance_type(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_literal_instance_type_passes_through_unchanged(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i4i.large"


def test_docker_backend_skips_resolution(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)

    catalog_loaded = []
    real_from_dir = InstanceCatalog.from_directory

    def tracking_from_dir(path: Path) -> InstanceCatalog:
        catalog_loaded.append(path)
        return real_from_dir(path)

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", side_effect=tracking_from_dir):
        sct_config.SCTConfiguration()

    assert not catalog_loaded, "catalog should not be loaded for docker backend"


def test_missing_vcpu_in_constraint_raises_value_error(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)),
        pytest.raises(ValueError, match="Invalid constraint for instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_no_matching_instance_raises_value_error(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "9999")

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)),
        pytest.raises(ValueError, match="Cannot resolve instance_type_db"),
    ):
        sct_config.SCTConfiguration()


def test_env_var_dot_notation_vcpu_and_memory_resolved(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.memory", "64")

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf = sct_config.SCTConfiguration()

    assert conf.get("instance_type_db") == "i8g.2xlarge"


def test_missing_catalog_directory_logs_warning_and_skips(monkeypatch, caplog):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB.vcpu", "8")

    sct_config._SIZING_RESOLUTION_CACHE.clear()

    with (
        patch("sdcm.sct_config.InstanceCatalog.from_directory", side_effect=FileNotFoundError("not found")),
        caplog.at_level(logging.WARNING, logger="sdcm.sct_config"),
    ):
        conf = sct_config.SCTConfiguration()

    assert "catalog" in caplog.text.lower() or "not found" in caplog.text.lower()
    assert conf.get("instance_type_db") != "i8g.2xlarge"


def test_resolve_instance_sizes_method_direct(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", _MINIMAL_CONFIG)

    conf = sct_config.SCTConfiguration()

    fake_env = {
        "cluster_backend": "aws",
        "instance_type_db": {"vcpu": "8"},
    }

    with patch("sdcm.sct_config.InstanceCatalog.from_directory", return_value=_make_catalog(_AWS_INSTANCE)):
        conf._resolve_instance_sizes(fake_env)

    assert fake_env["instance_type_db"] == "i8g.2xlarge"

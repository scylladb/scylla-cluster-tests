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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for stress upgrade parameter split."""

import pytest

from sdcm import sct_config


@pytest.fixture(autouse=True)
def fixture_env(monkeypatch):
    """Set up minimal environment for testing."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "5.4.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")


def test_stress_before_upgrade_parameter_exists():
    """Test that stress_before_upgrade parameter is defined."""
    conf = sct_config.SCTConfiguration()
    # Parameter should exist and be accessible via get()
    assert "stress_before_upgrade" in [param["name"] for param in conf.config_options]


def test_large_partition_stress_during_upgrade_parameter_exists():
    """Test that large_partition_stress_during_upgrade parameter is defined."""
    conf = sct_config.SCTConfiguration()
    # Parameter should exist and be accessible via get()
    assert "large_partition_stress_during_upgrade" in [param["name"] for param in conf.config_options]


def test_stress_before_upgrade_from_env(monkeypatch):
    """Test that stress_before_upgrade can be set from environment variable."""
    test_cmd = "cassandra-stress write cl=ALL n=1000000"
    monkeypatch.setenv("SCT_STRESS_BEFORE_UPGRADE", test_cmd)
    conf = sct_config.SCTConfiguration()
    assert conf.get("stress_before_upgrade") == test_cmd


def test_large_partition_stress_during_upgrade_from_env(monkeypatch):
    """Test that large_partition_stress_during_upgrade can be set from environment variable."""
    test_cmd = "cassandra-stress write cl=QUORUM n=1000000"
    monkeypatch.setenv("SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE", test_cmd)
    conf = sct_config.SCTConfiguration()
    assert conf.get("large_partition_stress_during_upgrade") == test_cmd


def test_both_stress_parameters_can_coexist(monkeypatch):
    """Test that both stress_before_upgrade and large_partition_stress_during_upgrade can be set simultaneously."""
    before_cmd = "cassandra-stress write cl=ALL n=1000000"
    during_cmd = "cassandra-stress write cl=QUORUM n=2000000"
    monkeypatch.setenv("SCT_STRESS_BEFORE_UPGRADE", before_cmd)
    monkeypatch.setenv("SCT_LARGE_PARTITION_STRESS_DURING_UPGRADE", during_cmd)
    conf = sct_config.SCTConfiguration()
    assert conf.get("stress_before_upgrade") == before_cmd
    assert conf.get("large_partition_stress_during_upgrade") == during_cmd


def test_stress_parameters_independent():
    """Test that stress parameters are independent - setting one doesn't affect the other."""
    conf = sct_config.SCTConfiguration()
    # When not set, both should return None or empty
    before = conf.get("stress_before_upgrade")
    during = conf.get("large_partition_stress_during_upgrade")
    # They should be independent (both None or both empty, but not the same object)
    assert before is None or before == ""
    assert during is None or during == ""

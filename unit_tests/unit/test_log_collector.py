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
# Copyright (c) 2022 ScyllaDB

import uuid
from unittest.mock import patch, MagicMock

import pytest

from sdcm.logcollector import (
    Collector,
    BaseSCTLogCollector,
    PythonSCTLogCollector,
    SchemaLogCollector,
    FailureStatisticsCollector,
)
from sdcm.provision import provisioner_factory
from unit_tests.lib.fake_resources import prepare_fake_region
from sdcm.utils import common


@pytest.fixture(scope="session")
def test_id():
    return f"{str(uuid.uuid4())}"


@pytest.fixture
def baremetal_config():
    """Sample baremetal configuration matching BareMetalCredentials structure."""
    return {
        "db_nodes": {
            "username": "scylla",
            "node_list": [
                {"public_ip": "10.0.0.1", "private_ip": "192.168.1.1"},
                {"public_ip": "10.0.0.2", "private_ip": "192.168.1.2"},
                {"public_ip": "10.0.0.3", "private_ip": "192.168.1.3"},
            ],
        },
        "loader_nodes": {
            "username": "loader_user",
            "node_list": [
                {"public_ip": "10.0.1.1", "private_ip": "192.168.2.1"},
                {"public_ip": "10.0.1.2", "private_ip": "192.168.2.2"},
            ],
        },
        "monitor_nodes": {
            "username": "monitor_user",
            "node_list": [
                {"public_ip": "10.0.2.1", "private_ip": "192.168.3.1"},
            ],
        },
    }


def test_create_collecting_nodes(test_id, tmp_path_factory):
    test_dir = tmp_path_factory.mktemp("log-collector")
    prepare_fake_region(test_id, "region_1", n_db_nodes=3, n_loaders=2, n_monitor_nodes=1)
    collector = Collector(
        test_id=test_id, test_dir=test_dir, params={"cluster_backend": "fake", "use_cloud_manager": False}
    )
    collector.create_collecting_nodes()
    provisioner = provisioner_factory.discover_provisioners(backend="fake", test_id=test_id)[0]

    db_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "scylla-db"]
    assert len(collector.db_cluster) == len(db_nodes)
    for collecting_node, v_m in zip(collector.db_cluster, db_nodes):
        assert collecting_node.name == v_m.name

    loader_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "loader"]
    assert len(collector.loader_set) == len(loader_nodes)
    for collecting_node, v_m in zip(collector.loader_set, loader_nodes):
        assert collecting_node.name == v_m.name

    monitor_nodes = [v_m for v_m in provisioner.list_instances() if v_m.tags.get("NodeType") == "monitor"]
    assert len(collector.monitor_set) == len(monitor_nodes)
    for collecting_node, v_m in zip(collector.monitor_set, monitor_nodes):
        assert collecting_node.name == v_m.name


def test_base_sct_log_collector_raises_when_no_local_files(tmp_path):
    """Test that BaseSCTLogCollector raises FileNotFoundError when no local files are found."""
    test_id = str(uuid.uuid4())
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    collector = BaseSCTLogCollector(
        nodes=[], test_id=test_id, storage_dir=str(storage_dir), params={"cluster_backend": "fake"}
    )

    # collect_logs should raise FileNotFoundError when no local files exist
    with pytest.raises(FileNotFoundError, match="No local files found for sct-runner-events"):
        collector.collect_logs(local_search_path=str(tmp_path))


def test_python_sct_log_collector_raises_when_no_local_files(tmp_path):
    """Test that PythonSCTLogCollector raises FileNotFoundError when no local files are found."""
    test_id = str(uuid.uuid4())
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    collector = PythonSCTLogCollector(
        nodes=[], test_id=test_id, storage_dir=str(storage_dir), params={"cluster_backend": "fake"}
    )

    # collect_logs should raise FileNotFoundError when no local files exist
    with pytest.raises(FileNotFoundError, match="No local files found for sct-runner-python-log"):
        collector.collect_logs(local_search_path=str(tmp_path))


def test_collector_tracks_critical_failures(test_id, tmp_path_factory, monkeypatch):
    """Test that Collector.run() tracks and returns error message for critical SCT log failures."""
    test_dir = tmp_path_factory.mktemp("log-collector-fail")

    # Create a collector instance
    collector = Collector(
        test_id=test_id, test_dir=test_dir, params={"cluster_backend": "fake", "use_cloud_manager": False}
    )

    # Mock get_running_cluster_sets to avoid needing real infrastructure
    def mock_get_running_cluster_sets(backend):
        collector.sct_set = []

    monkeypatch.setattr(collector, "get_running_cluster_sets", mock_get_running_cluster_sets)

    # Mock get_testrun_dir to return our temp directory
    def mock_get_testrun_dir(base_dir, test_id):
        return str(test_dir)

    monkeypatch.setattr(common, "get_testrun_dir", mock_get_testrun_dir)

    # The run() should return error message when SCT logs are missing
    results, error_msg = collector.run()
    assert error_msg is not None
    assert "Failed to collect critical SCT runner logs" in error_msg


def test_schema_log_collector_is_tracked_as_critical(tmp_path):
    """Test that SchemaLogCollector (subclass of BaseSCTLogCollector) is tracked as critical."""
    test_id = str(uuid.uuid4())
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    collector = SchemaLogCollector(
        nodes=[], test_id=test_id, storage_dir=str(storage_dir), params={"cluster_backend": "fake"}
    )

    # SchemaLogCollector should also raise FileNotFoundError when no local files exist
    # since it inherits from BaseSCTLogCollector
    with pytest.raises(FileNotFoundError, match="No local files found for schema-logs"):
        collector.collect_logs(local_search_path=str(tmp_path))


def test_failure_statistics_collector_does_not_raise_when_no_files(tmp_path):
    """Test that FailureStatisticsCollector does NOT raise exception when no files are found.

    Failure statistics are optional diagnostic files created only on test failures.
    This collector should not be treated as critical and should gracefully handle
    missing files by returning an empty list instead of raising an exception.
    """
    test_id = str(uuid.uuid4())
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir()

    collector = FailureStatisticsCollector(
        nodes=[], test_id=test_id, storage_dir=str(storage_dir), params={"cluster_backend": "fake"}
    )

    # collect_logs should return empty list when no files exist, NOT raise an exception
    result = collector.collect_logs(local_search_path=str(tmp_path))
    assert result == []


def test_get_baremetal_instances_by_testid(test_id, tmp_path_factory, baremetal_config):
    """Test that baremetal instances are correctly collected from config."""
    test_dir = tmp_path_factory.mktemp("log-collector-baremetal")

    mock_keystore = MagicMock()
    mock_keystore.get_baremetal_config.return_value = baremetal_config

    params = {
        "cluster_backend": "baremetal",
        "use_cloud_manager": False,
        "s3_baremetal_config": "test_baremetal_config",
        "user_credentials_path": "~/.ssh/test_key",
        "ip_ssh_connections": "public",
    }

    with patch("sdcm.logcollector.KeyStore", return_value=mock_keystore):
        collector = Collector(test_id=test_id, test_dir=test_dir, params=params)
        collector.get_baremetal_instances_by_testid()

    # Verify db_cluster nodes
    assert len(collector.db_cluster) == 3
    for idx, node in enumerate(collector.db_cluster):
        assert node.ssh_login_info["user"] == "scylla"
        assert node.ssh_login_info["hostname"] == f"10.0.0.{idx + 1}"
        assert node.tags["NodeType"] == "scylla-db"

    # Verify loader_set nodes
    assert len(collector.loader_set) == 2
    for idx, node in enumerate(collector.loader_set):
        assert node.ssh_login_info["user"] == "loader_user"
        assert node.ssh_login_info["hostname"] == f"10.0.1.{idx + 1}"
        assert node.tags["NodeType"] == "loader"

    # Verify monitor_set nodes
    assert len(collector.monitor_set) == 1
    assert collector.monitor_set[0].ssh_login_info["user"] == "monitor_user"
    assert collector.monitor_set[0].ssh_login_info["hostname"] == "10.0.2.1"
    assert collector.monitor_set[0].tags["NodeType"] == "monitor"


def test_get_baremetal_instances_uses_private_ip(test_id, tmp_path_factory, baremetal_config):
    """Test that baremetal uses private IPs when configured."""
    test_dir = tmp_path_factory.mktemp("log-collector-baremetal-private")

    mock_keystore = MagicMock()
    mock_keystore.get_baremetal_config.return_value = baremetal_config

    params = {
        "cluster_backend": "baremetal",
        "use_cloud_manager": False,
        "s3_baremetal_config": "test_baremetal_config",
        "user_credentials_path": "~/.ssh/test_key",
        "ip_ssh_connections": "private",
    }

    with patch("sdcm.logcollector.KeyStore", return_value=mock_keystore):
        collector = Collector(test_id=test_id, test_dir=test_dir, params=params)
        collector.get_baremetal_instances_by_testid()

    # Verify db_cluster nodes use private IPs
    assert len(collector.db_cluster) == 3
    for idx, node in enumerate(collector.db_cluster):
        assert node.ssh_login_info["hostname"] == f"192.168.1.{idx + 1}"


def test_get_baremetal_instances_no_config(test_id, tmp_path_factory):
    """Test graceful handling when s3_baremetal_config is not set."""
    test_dir = tmp_path_factory.mktemp("log-collector-baremetal-noconfig")

    params = {
        "cluster_backend": "baremetal",
        "use_cloud_manager": False,
        "s3_baremetal_config": None,
        "user_credentials_path": "~/.ssh/test_key",
    }

    collector = Collector(test_id=test_id, test_dir=test_dir, params=params)
    collector.get_baremetal_instances_by_testid()

    # Should not raise, but also should not populate any nodes
    assert len(collector.db_cluster) == 0
    assert len(collector.loader_set) == 0
    assert len(collector.monitor_set) == 0


def test_get_running_cluster_sets_baremetal(test_id, tmp_path_factory, baremetal_config):
    """Test that get_running_cluster_sets dispatches to baremetal correctly."""
    test_dir = tmp_path_factory.mktemp("log-collector-baremetal-dispatch")

    mock_keystore = MagicMock()
    mock_keystore.get_baremetal_config.return_value = baremetal_config

    params = {
        "cluster_backend": "baremetal",
        "use_cloud_manager": False,
        "s3_baremetal_config": "test_baremetal_config",
        "user_credentials_path": "~/.ssh/test_key",
    }

    with patch("sdcm.logcollector.KeyStore", return_value=mock_keystore):
        collector = Collector(test_id=test_id, test_dir=test_dir, params=params)
        collector.get_running_cluster_sets("baremetal")

    # Verify nodes were collected via the baremetal method
    assert len(collector.db_cluster) == 3
    assert len(collector.loader_set) == 2
    assert len(collector.monitor_set) == 1

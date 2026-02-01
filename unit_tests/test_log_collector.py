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
from unittest.mock import Mock

import pytest

from sdcm.logcollector import (
    Collector,
    BaseSCTLogCollector,
    PythonSCTLogCollector,
    SchemaLogCollector,
    FailureStatisticsCollector,
    PrometheusSnapshots,
    MonitoringStack,
    GrafanaScreenShot,
)
from sdcm.provision import provisioner_factory
from unit_tests.lib.fake_resources import prepare_fake_region
from sdcm.utils import common


@pytest.fixture(scope="session")
def test_id():
    return f"{str(uuid.uuid4())}"


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


def test_monitoring_entities_skip_for_docker_backend():
    """Test that monitoring entities skip collection for docker backend."""
    # Test with docker backend - should skip
    docker_params = {"cluster_backend": "docker"}

    prometheus_entity = PrometheusSnapshots(name="test_prometheus")
    prometheus_entity.set_params(docker_params)
    assert prometheus_entity.should_skip_for_backend() is True

    monitoring_entity = MonitoringStack(name="test_monitoring")
    monitoring_entity.set_params(docker_params)
    assert monitoring_entity.should_skip_for_backend() is True

    grafana_entity = GrafanaScreenShot(name="test_grafana")
    grafana_entity.set_params(docker_params)
    assert grafana_entity.should_skip_for_backend() is True


def test_monitoring_entities_do_not_skip_for_other_backends():
    """Test that monitoring entities do not skip collection for non-docker backends."""
    # Test with various non-docker backends - should not skip
    for backend in ["aws", "gce", "azure", "baremetal", "k8s-eks", "k8s-gke"]:
        backend_params = {"cluster_backend": backend}

        prometheus_entity = PrometheusSnapshots(name="test_prometheus")
        prometheus_entity.set_params(backend_params)
        assert prometheus_entity.should_skip_for_backend() is False, f"Should not skip for {backend}"

        monitoring_entity = MonitoringStack(name="test_monitoring")
        monitoring_entity.set_params(backend_params)
        assert monitoring_entity.should_skip_for_backend() is False, f"Should not skip for {backend}"

        grafana_entity = GrafanaScreenShot(name="test_grafana")
        grafana_entity.set_params(backend_params)
        assert grafana_entity.should_skip_for_backend() is False, f"Should not skip for {backend}"


def test_monitoring_entities_collect_returns_early_for_docker():
    """Test that collect methods return early for docker backend without attempting collection."""
    docker_params = {"cluster_backend": "docker"}
    mock_node = Mock()

    # PrometheusSnapshots should return None
    prometheus_entity = PrometheusSnapshots(name="test_prometheus")
    prometheus_entity.set_params(docker_params)
    result = prometheus_entity.collect(mock_node, "/tmp/test", None, None)
    assert result is None

    # MonitoringStack should return None
    monitoring_entity = MonitoringStack(name="test_monitoring")
    monitoring_entity.set_params(docker_params)
    result = monitoring_entity.collect(mock_node, "/tmp/test", None, None)
    assert result is None

    # GrafanaScreenShot should return empty list
    grafana_entity = GrafanaScreenShot(name="test_grafana")
    grafana_entity.set_params(docker_params)
    result = grafana_entity.collect(mock_node, "/tmp/test", None, None)
    assert result == []


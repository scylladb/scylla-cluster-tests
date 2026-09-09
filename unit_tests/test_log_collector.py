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

import logging
import uuid
from unittest.mock import patch, MagicMock

import pytest

from sdcm.logcollector import (
    Collector,
    BaseSCTLogCollector,
    PythonSCTLogCollector,
    SchemaLogCollector,
    RemoteDirArchiveLog,
    ScyllaLogCollector,
    LogCollector,
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


def test_hydra_watchdog_log_collected_from_result_dir(tmp_path):
    """hydra's transport watchdog appends its samples to the test's result dir (SCT-1044)."""
    result_dir = tmp_path / "20260923-165527-000000"
    result_dir.mkdir()
    (result_dir / "hydra-watchdog.log").write_text("=== 2026-09-23T17:30:32Z\nbuilder -> runner sockets:\n")
    (result_dir / "collected_logs").mkdir()
    (result_dir / "collected_logs" / "hydra-watchdog.log").write_text("already collected copy")
    local_dst = tmp_path / "dst"

    entity = next(e for e in BaseSCTLogCollector.log_entities if e.name == "hydra-watchdog.log")
    entity.collect(None, str(local_dst), local_search_path=str(result_dir))

    assert [p.name for p in local_dst.iterdir()] == ["hydra-watchdog.log"]
    assert (local_dst / "hydra-watchdog.log").read_text().startswith("=== 2026-09-23T17:30:32Z")


def _remote_dir_node(file_sizes="704512\n3145728\n", dir_exists=True, tar_exit_status=0):
    """A node whose remoter answers the commands the entity issues, by command shape."""
    node = MagicMock()
    node.name = "perf-collector-node-1"

    def sudo(cmd, **_):
        if cmd.startswith("find "):
            return MagicMock(ok=dir_exists, stdout=file_sizes if dir_exists else "")
        if cmd.startswith("tar "):
            return MagicMock(exit_status=tar_exit_status, stdout="")
        return MagicMock(ok=True, stdout="")

    node.remoter.sudo.side_effect = sudo
    return node


def test_remote_dir_archive_log_archives_and_receives(tmp_path, caplog):
    """The directory is tarred on the node under sudo and the archive is fetched."""
    node = _remote_dir_node()
    entity = RemoteDirArchiveLog(name="perf-data", remote_dir="/var/log/scylla-perf/")

    with (
        patch("sdcm.logcollector.check_archive", return_value=True) as mock_check,
        patch("sdcm.logcollector.LogCollector.receive_log") as mock_receive,
        caplog.at_level(logging.INFO, logger="sdcm.logcollector"),
    ):
        result = entity.collect(node=node, local_dst=str(tmp_path), remote_dst="/tmp/collected")

    assert result == str(tmp_path / "perf-data.tar.zst")
    # the size comes from the same root command that checks for content: no extra round-trip
    assert "(2 files, 3.7 MiB)" in caplog.text
    assert sum(call.args[0].startswith("find ") for call in node.remoter.sudo.call_args_list) == 1
    tar_cmd = [call.args[0] for call in node.remoter.sudo.call_args_list if call.args[0].startswith("tar ")][0]
    # -C the parent, so the archive keeps the directory itself as its single top-level entry
    assert "-cf '/tmp/collected/perf-data.tar.zst' -C '/var/log' 'scylla-perf'" in tar_cmd
    mock_check.assert_called_once_with(node.remoter, "/tmp/collected/perf-data.tar.zst")
    mock_receive.assert_called_once_with(
        node=node, remote_log_path="/tmp/collected/perf-data.tar.zst", local_dir=str(tmp_path), timeout=600
    )


@pytest.mark.parametrize(
    "dir_exists,file_sizes",
    (pytest.param(False, "", id="directory-missing"), pytest.param(True, "", id="directory-empty")),
)
def test_remote_dir_archive_log_skips_when_there_is_nothing_to_collect(tmp_path, dir_exists, file_sizes):
    """The package creates the directory, so an empty one is normal and not an error."""
    node = _remote_dir_node(file_sizes=file_sizes, dir_exists=dir_exists)
    entity = RemoteDirArchiveLog(name="perf-data", remote_dir="/var/log/scylla-perf")

    with patch("sdcm.logcollector.LogCollector.receive_log") as mock_receive:
        assert entity.collect(node=node, local_dst=str(tmp_path), remote_dst="/tmp/collected") is None

    assert not [call for call in node.remoter.sudo.call_args_list if call.args[0].startswith("tar ")]
    mock_receive.assert_not_called()


def test_remote_dir_archive_log_does_not_fetch_a_failed_archive(tmp_path):
    node = _remote_dir_node(tar_exit_status=2)
    entity = RemoteDirArchiveLog(name="perf-data", remote_dir="/var/log/scylla-perf")

    with patch("sdcm.logcollector.LogCollector.receive_log") as mock_receive:
        assert entity.collect(node=node, local_dst=str(tmp_path), remote_dst="/tmp/collected") is None

    mock_receive.assert_not_called()


def test_remote_dir_archive_log_does_not_fetch_a_corrupted_archive(tmp_path):
    node = _remote_dir_node()
    entity = RemoteDirArchiveLog(name="perf-data", remote_dir="/var/log/scylla-perf")

    with (
        patch("sdcm.logcollector.check_archive", return_value=False),
        patch("sdcm.logcollector.LogCollector.receive_log") as mock_receive,
    ):
        assert entity.collect(node=node, local_dst=str(tmp_path), remote_dst="/tmp/collected") is None

    mock_receive.assert_not_called()


def test_remote_dir_archive_log_needs_a_remote_storage_dir(tmp_path):
    """Without remote_dst there is nowhere on the node to put the archive."""
    node = _remote_dir_node()
    entity = RemoteDirArchiveLog(name="perf-data", remote_dir="/var/log/scylla-perf")

    assert entity.collect(node=node, local_dst=str(tmp_path), remote_dst=None) is None
    node.remoter.sudo.assert_not_called()


def test_scylla_log_collector_collects_the_perf_collector_recordings():
    """The perf recordings of scylla-perf-collector must stay part of the db-cluster logs."""
    entities = [entity for entity in ScyllaLogCollector.log_entities if isinstance(entity, RemoteDirArchiveLog)]
    assert [(entity.name, entity.remote_dir) for entity in entities] == [("perf-data", "/var/log/scylla-perf")]


def test_archive_log_remotely_defaults_to_the_source_dir_without_sudo():
    """The pre-existing callers pass a path they own, and must keep archiving in place."""
    node = MagicMock()
    node.remoter.run.return_value = MagicMock(ok=True, exit_status=0, stdout="")

    with patch("sdcm.logcollector.check_archive", return_value=True):
        archive = LogCollector.archive_log_remotely(node, "/home/ubuntu/snapshot", "prometheus_data")

    assert archive == "/home/ubuntu/prometheus_data.tar.zst"
    assert "-cf '/home/ubuntu/prometheus_data.tar.zst' -C '/home/ubuntu' 'snapshot'" in node.remoter.run.call_args[0][0]
    node.remoter.sudo.assert_not_called()


def test_archive_log_remotely_with_sudo_writes_to_the_given_destination():
    node = MagicMock()
    node.remoter.sudo.return_value = MagicMock(ok=True, exit_status=0, stdout="")

    with patch("sdcm.logcollector.check_archive", return_value=True):
        archive = LogCollector.archive_log_remotely(
            node, "/var/log/scylla-perf", "perf-data", archive_dst="/tmp/collected", use_sudo=True
        )

    assert archive == "/tmp/collected/perf-data.tar.zst"
    sudo_cmds = [call.args[0] for call in node.remoter.sudo.call_args_list]
    assert (
        "tar --zstd --warning=no-file-changed -cf '/tmp/collected/perf-data.tar.zst' -C '/var/log' 'scylla-perf'"
        in sudo_cmds
    )
    # the archive is verified and fetched as the login user, not as root
    assert "chmod a+r '/tmp/collected/perf-data.tar.zst'" in sudo_cmds
    node.remoter.run.assert_not_called()


def test_archive_log_remotely_warns_when_the_archive_cannot_be_made_readable(caplog):
    """A failed chmod would otherwise surface only as a misleading archive check failure."""
    node = MagicMock()
    node.remoter.sudo.side_effect = lambda cmd, **_: MagicMock(
        ok=not cmd.startswith("chmod "), exit_status=0, stdout=""
    )

    with patch("sdcm.logcollector.check_archive", return_value=True), caplog.at_level(logging.WARNING):
        LogCollector.archive_log_remotely(
            node, "/var/log/scylla-perf", "perf-data", archive_dst="/tmp/collected", use_sudo=True
        )

    assert "Unable to make `/tmp/collected/perf-data.tar.zst' readable by the login user" in caplog.text


@pytest.mark.parametrize(
    "exit_status,archived",
    (
        pytest.param(0, True, id="success"),
        # a collector that was not stopped (aborted run, collect-logs on a kept cluster) keeps
        # appending to its current recording, and that must not drop the finished ones with it
        pytest.param(1, True, id="file-changed-while-read"),
        pytest.param(2, False, id="fatal-error"),
    ),
)
def test_archive_log_remotely_tolerates_files_changing_while_read(exit_status, archived):
    node = MagicMock()
    node.remoter.sudo.return_value = MagicMock(ok=exit_status == 0, exit_status=exit_status, stdout="")

    with patch("sdcm.logcollector.check_archive", return_value=True) as mock_check:
        archive = LogCollector.archive_log_remotely(
            node, "/var/log/scylla-perf", "perf-data", archive_dst="/tmp/collected", use_sudo=True
        )

    assert archive == ("/tmp/collected/perf-data.tar.zst" if archived else None)
    assert mock_check.called == archived

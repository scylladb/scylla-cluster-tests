"""Tests for sdcm.nemesis.monkey.manager module."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import FilesNotCorrupted, UnsupportedNemesis
from sdcm.mgmt.common import ObjectStorageUploadMode, TaskStatus
from sdcm.nemesis.monkey.manager import (
    MgmtBackup,
    MgmtBackupSpecificKeyspaces,
    MgmtCorruptThenRepair,
    MgmtRepair,
    MgmtRestore,
    _delete_existing_backups,
    _get_manager_tool,
    _mgmt_backup,
    manager_backup,
)
from sdcm.nemesis.utils.common_ops import destroy_data_and_restart_scylla

_MODULE = "sdcm.nemesis.monkey.manager"
_COMMON_OPS_MODULE = "sdcm.nemesis.utils.common_ops"

pytestmark = pytest.mark.usefixtures("events")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with manager-nemesis-specific attributes."""
    base_runner.cluster.params = MagicMock()
    base_runner.cluster.params.get.return_value = False  # default: no manager configured
    base_runner.cluster.params.region_names = ["us-east-1"]
    base_runner.run_repair_manager = MagicMock()
    base_runner.get_all_sstables = MagicMock(return_value=[])
    base_runner.replace_full_file_name_to_prefix = MagicMock(return_value="")
    base_runner.tester.monitors = MagicMock()
    base_runner.tester.monitors.nodes = [MagicMock()]
    base_runner.tester.test_config = MagicMock()
    base_runner.tester.params = MagicMock(artifact_scylla_version="2025.3.0")
    mock_tester_obj = MagicMock()
    mock_tester_obj.params = base_runner.tester.params
    with (
        patch("sdcm.utils.issues.SkipPerIssues.get_issue_details", return_value=None),
        patch("sdcm.sct_events.group_common_events.TestConfig") as mock_test_config,
    ):
        mock_test_config.return_value.tester_obj.return_value = mock_tester_obj
        yield base_runner


# ---------------------------------------------------------------------------
# Monkey class flags
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "monkey_class, expected_flags",
    [
        pytest.param(
            MgmtBackup,
            {"manager_operation": True, "disruptive": False, "limited": True, "supports_high_disk_utilization": False},
            id="MgmtBackup",
        ),
        pytest.param(
            MgmtBackupSpecificKeyspaces,
            {"manager_operation": True, "disruptive": False, "limited": True, "supports_high_disk_utilization": False},
            id="MgmtBackupSpecificKeyspaces",
        ),
        pytest.param(
            MgmtRestore,
            {
                "manager_operation": True,
                "disruptive": True,
                "kubernetes": True,
                "xcloud": True,
                "limited": True,
                "supports_high_disk_utilization": False,
            },
            id="MgmtRestore",
        ),
        pytest.param(
            MgmtRepair,
            {"manager_operation": True, "disruptive": False, "kubernetes": True, "limited": True},
            id="MgmtRepair",
        ),
        pytest.param(
            MgmtCorruptThenRepair,
            {"manager_operation": True, "disruptive": True, "kubernetes": True},
            id="MgmtCorruptThenRepair",
        ),
    ],
)
def test_monkey_class_flags(monkey_class, expected_flags):
    """Verify each manager monkey class has correct boolean flags for nemesis selection."""
    for flag_name, expected_value in expected_flags.items():
        assert getattr(monkey_class, flag_name) == expected_value, (
            f"{monkey_class.__name__}.{flag_name} expected {expected_value}"
        )


# ---------------------------------------------------------------------------
# _get_manager_tool
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}.mgmt")
def test_get_manager_tool_delegates_to_mgmt(mock_mgmt):
    """Verify _get_manager_tool calls mgmt.get_scylla_manager_tool with the first monitor node."""
    tester = MagicMock()
    monitor_node = MagicMock()
    tester.monitors.nodes = [monitor_node]
    _get_manager_tool(tester)
    mock_mgmt.get_scylla_manager_tool.assert_called_once_with(manager_node=monitor_node)


# ---------------------------------------------------------------------------
# _delete_existing_backups
# ---------------------------------------------------------------------------


def test_delete_existing_backups_removes_actionable_tasks(runner):
    """Verify _delete_existing_backups deletes tasks with NEW/RUNNING/STARTING/ERROR status."""
    task_new = MagicMock(status=TaskStatus.NEW, id="task-1")
    task_running = MagicMock(status=TaskStatus.RUNNING, id="task-2")
    task_done = MagicMock(status=TaskStatus.DONE, id="task-3")
    task_error = MagicMock(status=TaskStatus.ERROR, id="task-4")

    mgr_cluster = MagicMock()
    mgr_cluster.backup_task_list = [task_new, task_running, task_done, task_error]

    _delete_existing_backups(runner, mgr_cluster)

    # task_done should NOT be deleted
    assert mgr_cluster.delete_task.call_count == 3
    deleted_tasks = [call.args[0] for call in mgr_cluster.delete_task.call_args_list]
    assert task_new in deleted_tasks
    assert task_running in deleted_tasks
    assert task_error in deleted_tasks
    assert task_done not in deleted_tasks


def test_delete_existing_backups_no_tasks(runner):
    """Verify _delete_existing_backups does nothing when task list is empty."""
    mgr_cluster = MagicMock()
    mgr_cluster.backup_task_list = []
    _delete_existing_backups(runner, mgr_cluster)
    mgr_cluster.delete_task.assert_not_called()


# ---------------------------------------------------------------------------
# _mgmt_backup — UnsupportedNemesis guards
# ---------------------------------------------------------------------------


def test_mgmt_backup_raises_when_no_manager_configured(runner):
    """Verify _mgmt_backup raises UnsupportedNemesis when neither use_mgmt nor use_cloud_manager is set."""
    runner.cluster.params.get.return_value = False
    with pytest.raises(UnsupportedNemesis, match="Scylla-manager configuration is not defined"):
        _mgmt_backup(runner, backup_specific_tables=False)


def test_mgmt_backup_raises_when_no_bucket_location(runner):
    """Verify _mgmt_backup raises UnsupportedNemesis when backup_bucket_location is not configured."""

    def params_get(key, *args, **kwargs):
        return {
            "use_mgmt": True,
            "use_cloud_manager": False,
            "backup_bucket_location": None,
        }.get(key, None)

    runner.cluster.params.get.side_effect = params_get
    with pytest.raises(UnsupportedNemesis, match="backup bucket location"):
        _mgmt_backup(runner, backup_specific_tables=False)


# ---------------------------------------------------------------------------
# manager_backup
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}._manager_backup_and_report")
def test_manager_backup_calls_report_and_deletes_snapshot(mock_report, runner):
    """Verify manager_backup calls _manager_backup_and_report and then deletes the snapshot."""
    mock_task = MagicMock()
    mock_report.return_value = mock_task

    manager_backup(runner, ObjectStorageUploadMode.RCLONE, "test_label")

    mock_report.assert_called_once()
    # Label should have a time postfix appended
    call_label = mock_report.call_args.args[2]
    assert call_label.startswith("test_label_")
    mock_task.delete_backup_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# UnsupportedNemesis guards — Monkey classes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "monkey_class",
    [
        pytest.param(MgmtBackup, id="MgmtBackup"),
        pytest.param(MgmtBackupSpecificKeyspaces, id="MgmtBackupSpecificKeyspaces"),
    ],
)
def test_backup_monkeys_raise_when_no_manager(runner, monkey_class):
    """Verify backup monkey classes raise UnsupportedNemesis when manager is not configured."""
    runner.cluster.params.get.return_value = False
    with pytest.raises(UnsupportedNemesis, match="Scylla-manager configuration is not defined"):
        monkey_class(runner).disrupt()


@pytest.mark.parametrize(
    "monkey_class",
    [
        pytest.param(MgmtRepair, id="MgmtRepair"),
        pytest.param(MgmtCorruptThenRepair, id="MgmtCorruptThenRepair"),
    ],
)
def test_repair_monkeys_raise_when_no_manager(runner, monkey_class):
    """Verify repair monkey classes raise UnsupportedNemesis when manager is not configured."""
    runner.cluster.params.get.return_value = False
    with pytest.raises(UnsupportedNemesis, match="Scylla-manager configuration is not defined"):
        monkey_class(runner).disrupt()


def test_restore_raises_when_no_manager(runner):
    """Verify MgmtRestore raises UnsupportedNemesis when manager is not configured."""

    def params_get(key, *args, **kwargs):
        return {"use_mgmt": False, "use_cloud_manager": False, "simulated_regions": 0}.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    runner.cluster.params.region_names = ["us-east-1"]
    with pytest.raises(UnsupportedNemesis, match="Scylla-manager configuration is not defined"):
        MgmtRestore(runner).disrupt()


def test_restore_raises_for_unsupported_backend(runner):
    """Verify MgmtRestore raises UnsupportedNemesis for non-AWS/EKS backends."""

    def params_get(key, *args, **kwargs):
        return {
            "use_mgmt": True,
            "use_cloud_manager": False,
            "cluster_backend": "gce",
            "simulated_regions": 0,
        }.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    runner.cluster.params.region_names = ["us-east-1"]
    with pytest.raises(UnsupportedNemesis, match="only supports 'AWS' and 'K8S-EKS'"):
        MgmtRestore(runner).disrupt()


# ---------------------------------------------------------------------------
# MgmtRepair — happy path
# ---------------------------------------------------------------------------


def test_mgmt_repair_calls_run_repair_manager(runner):
    """Verify MgmtRepair.disrupt() calls run_repair_manager when manager is configured."""

    def params_get(key, *args, **kwargs):
        return {"use_mgmt": True, "use_cloud_manager": False}.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    MgmtRepair(runner).disrupt()
    runner.run_repair_manager.assert_called_once()


# ---------------------------------------------------------------------------
# MgmtCorruptThenRepair — happy path
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}.destroy_data_and_restart_scylla")
def test_mgmt_corrupt_then_repair_calls_destroy_and_repair(mock_destroy, runner):
    """Verify MgmtCorruptThenRepair destroys data and then repairs."""

    def params_get(key, *args, **kwargs):
        return {"use_mgmt": True, "use_cloud_manager": False}.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    MgmtCorruptThenRepair(runner).disrupt()
    mock_destroy.assert_called_once_with(runner)
    runner.run_repair_manager.assert_called_once()


# ---------------------------------------------------------------------------
# destroy_data_and_restart_scylla (common_ops.py)
# ---------------------------------------------------------------------------


def test_destroy_data_raises_when_no_tables(runner):
    """Verify destroy_data_and_restart_scylla raises UnsupportedNemesis when no tables are found."""
    runner.cluster.get_non_system_ks_cf_list.return_value = []
    with pytest.raises(UnsupportedNemesis, match="Non-system keyspace and table are not found"):
        destroy_data_and_restart_scylla(runner)


def test_destroy_data_raises_when_no_sstables(runner):
    """Verify destroy_data_and_restart_scylla raises UnsupportedNemesis when no SStables are found."""
    runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.table1"]
    runner.get_all_sstables.return_value = []
    with pytest.raises(UnsupportedNemesis, match="SStables for destroy are not found"):
        destroy_data_and_restart_scylla(runner)
    # Scylla should still be restarted even when UnsupportedNemesis is raised from sstable lookup
    runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


@patch(f"{_COMMON_OPS_MODULE}.random")
@patch(f"{_COMMON_OPS_MODULE}.DbNodeLogger")
def test_destroy_data_happy_path(mock_db_logger, mock_random, runner):
    """Verify destroy_data_and_restart_scylla stops scylla, removes files, and restarts."""
    mock_db_logger.return_value.__enter__ = MagicMock()
    mock_db_logger.return_value.__exit__ = MagicMock(return_value=False)

    runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.table1"]
    sstables = ["/var/lib/scylla/data/ks1/table1/mc-1-big-Data.db"]
    runner.get_all_sstables.return_value = list(sstables)
    mock_random.choice.return_value = sstables[0]
    runner.replace_full_file_name_to_prefix.return_value = "/var/lib/scylla/data/ks1/table1/mc-1-*"
    runner.target_node.remoter.sudo.return_value = MagicMock(stderr="")

    destroy_data_and_restart_scylla(runner, sstables_to_destroy_perc=100)

    runner.target_node.stop_scylla_server.assert_called_once_with(verify_up=False, verify_down=True)
    runner.target_node.remoter.sudo.assert_called_once()
    runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


@patch(f"{_COMMON_OPS_MODULE}.random")
@patch(f"{_COMMON_OPS_MODULE}.DbNodeLogger")
def test_destroy_data_restarts_scylla_on_file_removal_error(mock_db_logger, mock_random, runner):
    """Verify Scylla is restarted even when file removal fails."""
    mock_db_logger.return_value.__enter__ = MagicMock()
    mock_db_logger.return_value.__exit__ = MagicMock(return_value=False)

    runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.table1"]
    sstables = ["/var/lib/scylla/data/ks1/table1/mc-1-big-Data.db"]
    runner.get_all_sstables.return_value = list(sstables)
    mock_random.choice.return_value = sstables[0]
    runner.replace_full_file_name_to_prefix.return_value = "/var/lib/scylla/data/ks1/table1/mc-1-*"
    runner.target_node.remoter.sudo.return_value = MagicMock(stderr="Permission denied")

    with pytest.raises(FilesNotCorrupted):
        destroy_data_and_restart_scylla(runner, sstables_to_destroy_perc=100)

    # Scylla must be restarted regardless of the error
    runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


# ---------------------------------------------------------------------------
# MgmtRestore — happy path
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}.get_persistent_snapshots")
@patch(f"{_MODULE}.get_schema_create_statements_from_snapshot")
@patch(f"{_MODULE}.get_dc_name_from_ks_statement")
def test_restore_happy_path_existing_keyspace(mock_get_dc, mock_get_schema, mock_get_snapshots, runner):
    """Verify MgmtRestore happy path: restore data into an already-existing keyspace, validate, and clean up."""

    def params_get(key, *args, **kwargs):
        return {
            "use_mgmt": True,
            "use_cloud_manager": False,
            "cluster_backend": "aws",
            "backup_bucket_backend": "s3",
            "simulated_regions": 0,
        }.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    runner.cluster.params.region_names = ["us-east-1"]
    runner.tester.test_duration = 2000
    runner.random.choice.side_effect = lambda seq: seq[0]

    # Mock df output for partition size calculation (200 GB)
    runner.cluster.data_nodes = [runner.target_node]
    runner.target_node.remoter.run.return_value = MagicMock(
        stdout="/dev/sda1 209715200 100000000 100000000 50% /var/lib/scylla"
    )
    runner.cluster.nodes = [runner.target_node]
    runner.target_node.is_enterprise = True

    # Mock persistent snapshots structure
    mock_get_snapshots.return_value = {
        "aws": {
            "bucket": "backup-bucket-{region}",
            "snapshots_sizes": {
                5: {
                    "expected_timeout": 3600,
                    "number_of_rows": 1000,
                    "snapshots": {
                        "us-east-1": {
                            "sm_20250101120000UTC": {
                                "keyspace_name": "restored_ks",
                                "cluster_id": "cluster-123",
                                "scylla_product": "enterprise",
                            }
                        }
                    },
                }
            },
            "confirmation_stress_template": "cassandra-stress read n={num_of_rows} -schema 'keyspace={keyspace_name}' -pop 'seq={sequence_start}..{sequence_end}'",
        }
    }

    # The keyspace already exists in the cluster — skip schema restoration
    runner.cluster.get_test_keyspaces.return_value = ["restored_ks"]

    # Mock manager cluster and restore task
    mgr_cluster = MagicMock()
    restore_task = MagicMock()
    restore_task.status = TaskStatus.DONE
    mgr_cluster.create_restore_task.return_value = restore_task
    runner.cluster.get_cluster_manager.return_value = mgr_cluster

    # Mock stress validation
    runner.tester.params.get.return_value = [1]  # n_loaders
    stress_thread = MagicMock()
    runner.tester.run_stress_thread.return_value = stress_thread
    runner.tester.verify_stress_thread.return_value = True

    # Execute
    MgmtRestore(runner).disrupt()

    # Verify restore task was created with correct params
    mgr_cluster.create_restore_task.assert_called_once_with(
        restore_data=True,
        location_list=["s3:backup-bucket-us-east-1"],
        snapshot_tag="sm_20250101120000UTC",
    )
    restore_task.wait_and_get_final_status.assert_called_once()

    # Verify data validation ran
    runner.tester.run_stress_thread.assert_called_once()
    runner.tester.verify_stress_thread.assert_called_once_with(stress_thread)

    # Verify cleanup: keyspace dropped
    runner.target_node.run_cqlsh.assert_called_once_with('DROP KEYSPACE IF EXISTS "restored_ks";')


@patch(f"{_MODULE}.suppress_expected_unavailability_errors")
@patch(f"{_MODULE}.get_persistent_snapshots")
@patch(f"{_MODULE}.get_schema_create_statements_from_snapshot")
@patch(f"{_MODULE}.get_dc_name_from_ks_statement")
def test_restore_happy_path_with_schema_restoration(
    mock_get_dc, mock_get_schema, mock_get_snapshots, mock_suppress, runner
):
    """Verify MgmtRestore restores schema when keyspace doesn't exist and DC names mismatch."""

    def params_get(key, *args, **kwargs):
        return {
            "use_mgmt": True,
            "use_cloud_manager": False,
            "cluster_backend": "aws",
            "backup_bucket_backend": "s3",
            "simulated_regions": 0,
        }.get(key, False)

    runner.cluster.params.get.side_effect = params_get
    runner.cluster.params.region_names = ["us-east-1"]
    runner.tester.test_duration = 2000
    runner.random.choice.side_effect = lambda seq: seq[0]

    # Mock df output
    runner.cluster.data_nodes = [runner.target_node]
    runner.target_node.remoter.run.return_value = MagicMock(
        stdout="/dev/sda1 209715200 100000000 100000000 50% /var/lib/scylla"
    )
    runner.cluster.nodes = [runner.target_node]
    runner.target_node.is_enterprise = True

    mock_get_snapshots.return_value = {
        "aws": {
            "bucket": "backup-bucket-{region}",
            "snapshots_sizes": {
                5: {
                    "expected_timeout": 3600,
                    "number_of_rows": 1000,
                    "snapshots": {
                        "us-east-1": {
                            "sm_20250101120000UTC": {
                                "keyspace_name": "new_ks",
                                "cluster_id": "cluster-123",
                                "scylla_product": "enterprise",
                            }
                        }
                    },
                }
            },
            "confirmation_stress_template": "cassandra-stress read n={num_of_rows} -schema 'keyspace={keyspace_name}'",
        }
    }

    # Keyspace does NOT exist yet — triggers schema restoration
    runner.cluster.get_test_keyspaces.return_value = ["other_ks"]

    # DC name mismatch — triggers manual CQL application
    runner.cluster.get_nodetool_status.return_value = {"dc_local": {}}
    mock_get_schema.return_value = (
        ["CREATE KEYSPACE new_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'dc_backup': 3}"],
        ["CREATE TABLE new_ks.t1 (id int PRIMARY KEY)"],
    )
    mock_get_dc.return_value = ("dc_backup",)

    # Mock suppress context manager
    mock_suppress.return_value.__enter__ = MagicMock()
    mock_suppress.return_value.__exit__ = MagicMock(return_value=False)

    # Mock manager cluster and restore task
    mgr_cluster = MagicMock()
    restore_task = MagicMock()
    restore_task.status = TaskStatus.DONE
    mgr_cluster.create_restore_task.return_value = restore_task
    runner.cluster.get_cluster_manager.return_value = mgr_cluster

    # Mock stress validation
    runner.tester.params.get.return_value = [1]
    stress_thread = MagicMock()
    runner.tester.run_stress_thread.return_value = stress_thread
    runner.tester.verify_stress_thread.return_value = True

    # Execute
    MgmtRestore(runner).disrupt()

    # Verify schema was applied manually with DC name replacement
    cqlsh_calls = runner.target_node.run_cqlsh.call_args_list
    # First calls: schema restoration (ks statement with DC replaced + table statement)
    assert any("dc_local" in str(call) for call in cqlsh_calls), "DC name should be replaced in CQL statements"
    assert any("CREATE TABLE" in str(call) for call in cqlsh_calls), "Table creation CQL should be applied"

    # Verify cluster was restarted after schema restoration
    runner.cluster.restart_scylla.assert_called_once()

    # Verify data restore task was created
    mgr_cluster.create_restore_task.assert_called_once_with(
        restore_data=True,
        location_list=["s3:backup-bucket-us-east-1"],
        snapshot_tag="sm_20250101120000UTC",
    )

    # Verify cleanup
    assert any("DROP KEYSPACE" in str(call) for call in cqlsh_calls)

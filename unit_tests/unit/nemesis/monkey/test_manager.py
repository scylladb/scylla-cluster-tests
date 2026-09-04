"""Tests for sdcm.nemesis.monkey.manager module."""

from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import FilesNotCorrupted, UnsupportedNemesis
from sdcm.mgmt.common import ObjectStorageUploadMode, TaskStatus
from sdcm.nemesis.monkey.manager import (
    MgmtCorruptThenRepair,
    MgmtRepair,
    MgmtRestore,
    delete_existing_backups,
    get_manager_tool,
    manager_backup,
    mgmt_backup,
)
from sdcm.nemesis.utils.common_ops import destroy_data_and_restart_scylla

_MODULE = "sdcm.nemesis.monkey.manager"
_COMMON_OPS_MODULE = "sdcm.nemesis.utils.common_ops"

pytestmark = pytest.mark.usefixtures("events")

_SNAPSHOT_TAG = "sm_20250101120000UTC"
_RESTORED_KS = "restored_ks"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _set_params(runner, **values):
    """Point ``runner.cluster.params.get`` at ``values``, honouring the caller's default."""
    runner.cluster.params.get.side_effect = lambda key, default=None, **kwargs: values.get(key, default)


def _snapshot_catalog():
    """A minimal ``get_persistent_snapshots()`` catalog: a single 5 GB AWS snapshot."""
    return {
        "aws": {
            "bucket": "backup-bucket-{region}",
            "snapshots_sizes": {
                5: {
                    "expected_timeout": 3600,
                    "number_of_rows": 1000,
                    "snapshots": {
                        "us-east-1": {
                            _SNAPSHOT_TAG: {
                                "keyspace_name": _RESTORED_KS,
                                "cluster_id": "cluster-123",
                                "scylla_product": "enterprise",
                            }
                        }
                    },
                }
            },
            "confirmation_stress_template": (
                "cassandra-stress read n={num_of_rows} -schema 'keyspace={keyspace_name}' "
                "-pop 'seq={sequence_start}..{sequence_end}'"
            ),
        }
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with manager-nemesis-specific attributes."""
    base_runner.cluster.params = MagicMock()
    base_runner.cluster.params.get.return_value = False
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


@pytest.fixture(autouse=True)
def _stub_common_ops():
    """Stub the ``common_ops`` internals: DbNodeLogger opens SSH sessions to every
    node, and the SStable pick has to be deterministic."""
    with patch(f"{_COMMON_OPS_MODULE}.DbNodeLogger"), patch(f"{_COMMON_OPS_MODULE}.random") as mock_random:
        mock_random.choice.side_effect = lambda seq: seq[0]
        yield


@pytest.fixture()
def destroy_node_data_runner(runner):
    """``runner`` wired for a successful ``destroy_data_and_restart_scylla`` run:
    one table holding one SStable that ``rm`` removes without complaining."""
    runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.table1"]
    runner.get_all_sstables.return_value = ["/var/lib/scylla/data/ks1/table1/mc-1-big-Data.db"]
    runner.replace_full_file_name_to_prefix.return_value = "/var/lib/scylla/data/ks1/table1/mc-1-*"
    runner.target_node.remoter.sudo.return_value = MagicMock(stderr="")
    return runner


@pytest.fixture()
def restore_snapshot_runner(runner):
    """``runner`` wired for a successful ``MgmtRestore`` into an existing keyspace.

    An AWS/S3 single-region cluster with a 200 GB ``/var/lib/scylla`` partition
    (so the 5 GB snapshot of ``_snapshot_catalog()`` fits), a Manager restore
    task that ends up DONE, and a single loader whose verification passes.  The
    snapshot keyspace already exists in the cluster, so the schema-restoration
    branch is not taken — ``restore_snapshot_with_schema_runner`` covers that one.
    """
    _set_params(runner, use_mgmt=True, cluster_backend="aws", backup_bucket_backend="s3")
    runner.tester.test_duration = 2000
    runner.cluster.nodes = runner.cluster.data_nodes = [runner.target_node]
    runner.target_node.is_enterprise = True
    runner.target_node.remoter.run.return_value = MagicMock(  # df -k output -> 200 GB partition
        stdout="/dev/sda1 209715200 100000000 100000000 50% /var/lib/scylla"
    )
    runner.cluster.get_test_keyspaces.return_value = [_RESTORED_KS]
    runner.cluster.get_cluster_manager.return_value.create_restore_task.return_value.status = TaskStatus.DONE
    runner.tester.params.get.return_value = [1]  # n_loaders
    runner.tester.verify_stress_thread.return_value = True
    with patch(f"{_MODULE}.get_persistent_snapshots", return_value=_snapshot_catalog()):
        yield runner


@pytest.fixture()
def restore_snapshot_with_schema_runner(restore_snapshot_runner):
    """``restore_snapshot_runner`` steered into the schema-restoration branch.

    The snapshot keyspace is absent from the cluster and the snapshot's DC name
    differs from the cluster's, so the schema is applied CQL statement by CQL
    statement with the DC name rewritten, rather than restored by the Manager.
    """
    restore_snapshot_runner.cluster.get_test_keyspaces.return_value = ["other_ks"]
    restore_snapshot_runner.cluster.get_nodetool_status.return_value = {"dc_local": {}}
    with (
        patch(f"{_MODULE}.suppress_expected_unavailability_errors", return_value=nullcontext()),
        patch(
            f"{_MODULE}.get_schema_create_statements_from_snapshot",
            return_value=(
                [f"CREATE KEYSPACE {_RESTORED_KS} WITH replication = {{'class': 'NTS', 'dc_backup': 3}}"],
                [f"CREATE TABLE {_RESTORED_KS}.t1 (id int PRIMARY KEY)"],
            ),
        ),
        patch(f"{_MODULE}.get_dc_name_from_ks_statement", return_value=("dc_backup",)),
    ):
        yield restore_snapshot_runner


# ---------------------------------------------------------------------------
# get_manager_tool
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}.mgmt")
def test_get_manager_tool_delegates_to_mgmt(mock_mgmt):
    """get_manager_tool() asks mgmt for a tool bound to the first monitor node."""
    tester = MagicMock()
    monitor_node = MagicMock()
    tester.monitors.nodes = [monitor_node]

    get_manager_tool(tester)

    mock_mgmt.get_scylla_manager_tool.assert_called_once_with(manager_node=monitor_node)


# ---------------------------------------------------------------------------
# delete_existing_backups
# ---------------------------------------------------------------------------


def test_delete_existing_backups_removes_actionable_tasks(runner):
    """Only NEW/RUNNING/STARTING/ERROR tasks are deleted — a DONE task is left alone."""
    actionable = (TaskStatus.NEW, TaskStatus.RUNNING, TaskStatus.STARTING, TaskStatus.ERROR)
    tasks = {status: MagicMock(status=status, id=f"task-{status}") for status in (*actionable, TaskStatus.DONE)}
    mgr_cluster = MagicMock(backup_task_list=list(tasks.values()))

    delete_existing_backups(runner, mgr_cluster)

    deleted = [call.args[0] for call in mgr_cluster.delete_task.call_args_list]
    assert deleted == [tasks[status] for status in actionable]


def test_delete_existing_backups_no_tasks(runner):
    """Nothing is deleted when the cluster has no backup tasks at all."""
    mgr_cluster = MagicMock(backup_task_list=[])

    delete_existing_backups(runner, mgr_cluster)

    mgr_cluster.delete_task.assert_not_called()


# ---------------------------------------------------------------------------
# mgmt_backup
# ---------------------------------------------------------------------------


def test_mgmt_backup_raises_when_no_bucket_location(runner):
    """mgmt_backup() still rejects a missing backup bucket location at run time."""
    _set_params(runner, use_mgmt=True)

    with pytest.raises(UnsupportedNemesis, match="backup bucket location"):
        mgmt_backup(runner, backup_specific_tables=False)


# ---------------------------------------------------------------------------
# manager_backup
# ---------------------------------------------------------------------------


@patch(f"{_MODULE}._manager_backup_and_report")
def test_manager_backup_calls_report_and_deletes_snapshot(mock_report, runner):
    """manager_backup() reports the backup under a time-stamped label, then deletes the snapshot."""
    manager_backup(runner, ObjectStorageUploadMode.RCLONE, "test_label")

    assert mock_report.call_args.args[2].startswith("test_label_")
    mock_report.return_value.delete_backup_snapshot.assert_called_once()


# ---------------------------------------------------------------------------
# MgmtRepair / MgmtCorruptThenRepair
# ---------------------------------------------------------------------------


def test_mgmt_repair_calls_run_repair_manager(runner):
    """MgmtRepair.disrupt() delegates straight to run_repair_manager()."""
    MgmtRepair(runner).disrupt()

    runner.run_repair_manager.assert_called_once()


@patch(f"{_MODULE}.destroy_data_and_restart_scylla")
def test_mgmt_corrupt_then_repair_calls_destroy_and_repair(mock_destroy, runner):
    """MgmtCorruptThenRepair.disrupt() destroys data first and repairs afterwards."""
    MgmtCorruptThenRepair(runner).disrupt()

    mock_destroy.assert_called_once_with(runner)
    runner.run_repair_manager.assert_called_once()


# ---------------------------------------------------------------------------
# destroy_data_and_restart_scylla (common_ops.py)
# ---------------------------------------------------------------------------


def test_destroy_data_raises_when_no_tables(destroy_node_data_runner):
    """No non-system table to corrupt — the nemesis cannot run."""
    destroy_node_data_runner.cluster.get_non_system_ks_cf_list.return_value = []

    with pytest.raises(UnsupportedNemesis, match="Non-system keyspace and table are not found"):
        destroy_data_and_restart_scylla(destroy_node_data_runner)


def test_destroy_data_raises_when_no_sstables(destroy_node_data_runner):
    """No SStable to corrupt — the nemesis cannot run, but Scylla is brought back up."""
    destroy_node_data_runner.get_all_sstables.return_value = []

    with pytest.raises(UnsupportedNemesis, match="SStables for destroy are not found"):
        destroy_data_and_restart_scylla(destroy_node_data_runner)

    destroy_node_data_runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


def test_destroy_data_happy_path(destroy_node_data_runner):
    """The happy path stops Scylla, removes the SStable group, and restarts Scylla."""
    destroy_data_and_restart_scylla(destroy_node_data_runner, sstables_to_destroy_perc=100)

    destroy_node_data_runner.target_node.stop_scylla_server.assert_called_once_with(verify_up=False, verify_down=True)
    destroy_node_data_runner.target_node.remoter.sudo.assert_called_once()
    destroy_node_data_runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


def test_destroy_data_restarts_scylla_on_file_removal_error(destroy_node_data_runner):
    """Scylla is restarted even when ``rm`` fails and FilesNotCorrupted is raised."""
    destroy_node_data_runner.target_node.remoter.sudo.return_value = MagicMock(stderr="Permission denied")

    with pytest.raises(FilesNotCorrupted):
        destroy_data_and_restart_scylla(destroy_node_data_runner, sstables_to_destroy_perc=100)

    destroy_node_data_runner.target_node.start_scylla_server.assert_called_once_with(verify_up=True, verify_down=False)


# ---------------------------------------------------------------------------
# MgmtRestore
# ---------------------------------------------------------------------------


def test_restore_raises_for_unsupported_backend(runner):
    """MgmtRestore.disrupt() still rejects backends other than AWS / K8S-EKS."""
    _set_params(runner, use_mgmt=True, cluster_backend="gce")

    with pytest.raises(UnsupportedNemesis, match="only supports 'AWS' and 'K8S-EKS'"):
        MgmtRestore(runner).disrupt()


def test_restore_happy_path_existing_keyspace(restore_snapshot_runner):
    """The keyspace already exists: restore the data, validate it, then drop the keyspace."""
    MgmtRestore(restore_snapshot_runner).disrupt()

    mgr_cluster = restore_snapshot_runner.cluster.get_cluster_manager.return_value
    mgr_cluster.create_restore_task.assert_called_once_with(
        restore_data=True, location_list=["s3:backup-bucket-us-east-1"], snapshot_tag=_SNAPSHOT_TAG
    )
    mgr_cluster.create_restore_task.return_value.wait_and_get_final_status.assert_called_once()
    restore_snapshot_runner.cluster.restart_scylla.assert_not_called()  # no schema restoration
    restore_snapshot_runner.tester.run_stress_thread.assert_called_once()
    restore_snapshot_runner.tester.verify_stress_thread.assert_called_once_with(
        restore_snapshot_runner.tester.run_stress_thread.return_value
    )
    restore_snapshot_runner.target_node.run_cqlsh.assert_called_once_with(f'DROP KEYSPACE IF EXISTS "{_RESTORED_KS}";')


def test_restore_happy_path_with_schema_restoration(restore_snapshot_with_schema_runner):
    """The keyspace is missing and the DC names mismatch: apply the schema CQL with the
    DC name rewritten, restart the cluster, then restore the data as usual."""
    MgmtRestore(restore_snapshot_with_schema_runner).disrupt()

    statements = [str(call) for call in restore_snapshot_with_schema_runner.target_node.run_cqlsh.call_args_list]
    assert any("dc_local" in stmt for stmt in statements), "DC name should be rewritten in the keyspace statement"
    assert any("CREATE TABLE" in stmt for stmt in statements), "Table creation CQL should be applied"
    assert any("DROP KEYSPACE" in stmt for stmt in statements), "Restored keyspace should be cleaned up"
    restore_snapshot_with_schema_runner.cluster.restart_scylla.assert_called_once()
    mgr_cluster = restore_snapshot_with_schema_runner.cluster.get_cluster_manager.return_value
    mgr_cluster.create_restore_task.assert_called_once_with(
        restore_data=True, location_list=["s3:backup-bucket-us-east-1"], snapshot_tag=_SNAPSHOT_TAG
    )

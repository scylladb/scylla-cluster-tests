"""
Module containing all Scylla Manager nemesis classes.

Includes backup (rclone / native), restore, repair-via-manager, and
corrupt-then-repair disruptions.  Module-level helper functions are shared
across the monkey classes defined here.
"""

import datetime
import logging
from datetime import timedelta

from sdcm import mgmt
from sdcm.exceptions import UnsupportedNemesis
from sdcm.mgmt.argus_report import report_manager_backup_results_to_argus
from sdcm.mgmt.backup import run_manager_backup
from sdcm.mgmt.common import TaskStatus, ObjectStorageUploadMode, get_persistent_snapshots
from sdcm.mgmt.helpers import get_dc_name_from_ks_statement, get_schema_create_statements_from_snapshot
from sdcm.nemesis import NemesisBaseClass, target_data_nodes
from sdcm.nemesis.utils.common_ops import destroy_data_and_restart_scylla
from sdcm.sct_events.group_common_events import (
    decorate_with_context_if_issues_open,
    ignore_take_snapshot_failing,
    suppress_expected_unavailability_errors,
)
from sdcm.utils.cql_utils import cql_unquote_if_needed
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.issues import SkipPerIssues

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------


def _get_manager_tool(tester):
    """Return a ``ManagerTool`` instance from the first monitor node."""
    return mgmt.get_scylla_manager_tool(manager_node=tester.monitors.nodes[0])


def _run_manager_backup(runner, mgr_cluster, object_storage_upload_mode: ObjectStorageUploadMode, timeout: int):
    """Execute a Manager backup, wrapped in an action-log scope.

    Args:
        runner: ``NemesisRunner`` instance.
        mgr_cluster: Manager cluster object.
        object_storage_upload_mode: Upload mode (RCLONE or NATIVE).
        timeout: Timeout in seconds.

    Returns:
        ``BackupTask`` representing the completed backup.
    """
    with runner.action_log_scope("Scylla Manager backup"):
        task = run_manager_backup(mgr_cluster, runner.tester.locations, object_storage_upload_mode, timeout)
    return task


def _manager_backup_and_report(runner, method: ObjectStorageUploadMode, label):
    """Run a backup using Scylla Manager and report the result to Argus.

    Args:
        runner: ``NemesisRunner`` instance.
        method: The transfer mode for object storage (e.g., RCLONE or NATIVE).
        label: A label for reporting.

    Returns:
        ``BackupTask`` object representing the backup operation.
    """
    timeout = int(timedelta(hours=14).total_seconds())
    manager_tool = _get_manager_tool(runner.tester)
    mgr_cluster = runner.tester.ensure_and_get_cluster(manager_tool)
    decorated = latency_calculator_decorator(legend="Scylla-Manager Backup", cycle_name=label)(_run_manager_backup)
    task = decorated(runner, mgr_cluster, method, timeout)
    report_manager_backup_results_to_argus(runner.tester.monitors, runner.tester.test_config, label, task, mgr_cluster)
    return task


def manager_backup(runner, object_storage_upload_mode: ObjectStorageUploadMode, label):
    """Perform a Manager backup as a nemesis.  Deletes the snapshot at end.

    This is the top-level entry-point used by ``ManagerRcloneBackup`` and
    ``ManagerNativeBackup`` runner classes in ``runners.py``.

    Args:
        runner: ``NemesisRunner`` instance.
        object_storage_upload_mode: The upload mode (e.g., RCLONE or NATIVE).
        label: Label for reporting to Argus.
    """
    time_postfix = datetime.datetime.now().strftime("_%m%d_%H%M")
    label_with_time = f"{label}{time_postfix}"
    task = _manager_backup_and_report(runner, object_storage_upload_mode, label_with_time)
    with runner.action_log_scope("Delete Manager backup snapshot"):
        task.delete_backup_snapshot()


def _delete_existing_backups(runner, mgr_cluster):
    """Delete any in-progress or errored backup tasks before starting a new one."""
    deleted_tasks = []
    existing_backup_tasks = mgr_cluster.backup_task_list
    for backup_task in existing_backup_tasks:
        if backup_task.status in [TaskStatus.NEW, TaskStatus.RUNNING, TaskStatus.STARTING, TaskStatus.ERROR]:
            deleted_tasks.append(backup_task.id)
            mgr_cluster.delete_task(backup_task)
    if deleted_tasks:
        runner.log.warning("Deleted the following backup tasks before the nemesis starts: %s", ", ".join(deleted_tasks))


@decorate_with_context_if_issues_open(
    ignore_take_snapshot_failing, issue_refs=["https://github.com/scylladb/scylla-manager/issues/3389"]
)
def _mgmt_backup(runner, backup_specific_tables):
    """Create and run a Manager backup task, optionally for specific keyspaces.

    Args:
        runner: ``NemesisRunner`` instance.
        backup_specific_tables: If ``True``, back up only test keyspaces.
    """
    if not runner.cluster.params.get("use_mgmt") and not runner.cluster.params.get("use_cloud_manager"):
        raise UnsupportedNemesis("Scylla-manager configuration is not defined!")
    mgr_cluster = runner.cluster.get_cluster_manager()
    if runner.cluster.params.get("use_cloud_manager"):
        auto_backup_task = mgr_cluster.backup_task_list[0]
        #  An example of the auto generated backup task of cloud manager is:
        #  │ backup/8e40b8b4-5394-42d3-9884-a4ce8ab69687
        #  │ --dc 'AWS_US_EAST_1' -L AWS_US_EAST_1:s3:scylla-cloud-backup-9952-10120-4q4w4d --retention 14
        #  --rate-limit AWS_US_EAST_1:100 --snapshot-parallel '<nil>' --upload-parallel '<nil>'
        #  │ 06 Jun 21 18:10:05 UTC (+1d)  │ NEW
        location = auto_backup_task.get_task_info_dict()["location"]
    else:
        if not runner.cluster.params.get("backup_bucket_location"):
            raise UnsupportedNemesis("backup bucket location configuration is not defined!")

        backup_bucket_backend = runner.cluster.params.get("backup_bucket_backend")
        region = next(iter(runner.cluster.params.region_names), "")
        backup_bucket_location = runner.cluster.params.get("backup_bucket_location")[0].format(region=region)
        location = f"{backup_bucket_backend}:{backup_bucket_location}"
    _delete_existing_backups(runner, mgr_cluster)
    if backup_specific_tables:
        non_test_keyspaces = [cql_unquote_if_needed(ks) for ks in runner.cluster.get_test_keyspaces()]
        runner.actions_log.info(f"Starting Scylla Manager backup task for keyspaces: {non_test_keyspaces}")
        mgr_task = mgr_cluster.create_backup_task(
            location_list=[
                location,
            ],
            keyspace_list=non_test_keyspaces,
        )
    else:
        runner.actions_log.info("Starting Scylla Manager backup task for all keyspaces")
        mgr_task = mgr_cluster.create_backup_task(
            location_list=[
                location,
            ]
        )

    assert mgr_task is not None, "Backup task wasn't created"

    status = mgr_task.wait_and_get_final_status(timeout=54000, step=5, only_final=True)
    runner.actions_log.info(f"Scylla Manager backup task finished with status: {status}")
    if status == TaskStatus.DONE:
        runner.log.info("Task: %s is done.", mgr_task.id)
    elif status in (TaskStatus.ERROR, TaskStatus.ERROR_FINAL):
        assert False, f"Backup task {mgr_task.id} failed"
    else:
        mgr_task.stop()
        assert False, f"Backup task {mgr_task.id} timed out - while on status {status}"


# ---------------------------------------------------------------------------
# Monkey classes — Scylla Manager nemesis
# ---------------------------------------------------------------------------


@target_data_nodes
class MgmtBackup(NemesisBaseClass):
    """Run a full Scylla Manager backup of all keyspaces."""

    manager_operation = True
    disruptive = False
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):
        _mgmt_backup(self.runner, backup_specific_tables=False)


@target_data_nodes
class MgmtBackupSpecificKeyspaces(NemesisBaseClass):
    """Run a Scylla Manager backup of test keyspaces only."""

    manager_operation = True
    disruptive = False
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):
        _mgmt_backup(self.runner, backup_specific_tables=True)


@target_data_nodes
class MgmtRestore(NemesisBaseClass):
    """Restore a persistent Manager snapshot and validate the data."""

    manager_operation = True
    disruptive = True
    kubernetes = True
    xcloud = True
    limited = True
    supports_high_disk_utilization = False  # Snapshot/Restore operations consume disk space

    def disrupt(self):  # noqa: PLR0914
        self._disrupt_mgmt_restore()

    def _disrupt_mgmt_restore(self):  # noqa: PLR0914
        runner = self.runner

        def get_total_scylla_partition_size():
            result = runner.cluster.data_nodes[0].remoter.run("df -k | grep /var/lib/scylla")  # Size in KB
            free_space_size = int(result.stdout.split()[1]) / 1024**2  # Converting to GB
            return free_space_size

        def choose_snapshot(snapshots_dict, region: str):
            snapshot_groups_by_size = snapshots_dict["snapshots_sizes"]
            total_partition_size = get_total_scylla_partition_size()
            all_snapshot_sizes = sorted(list(snapshot_groups_by_size.keys()), reverse=True)
            fitting_snapshot_sizes = [size for size in all_snapshot_sizes if total_partition_size / size >= 20]
            if runner.tester.test_duration < 1000:
                # Since verifying the restored data takes a long time, the nemesis limits the size of the restored
                # backup based on the test duration
                fitting_snapshot_sizes = [size for size in fitting_snapshot_sizes if size < 50]
            # The restore should not take more than 5% of the space total space in /var/lib/scylla
            assert fitting_snapshot_sizes, "There's not enough space for any snapshot restoration"

            chosen_snapshot_size = runner.random.choice(fitting_snapshot_sizes)
            all_snapshots_per_region = snapshot_groups_by_size[chosen_snapshot_size]["snapshots"][region]

            if runner.cluster.nodes[0].is_enterprise:
                snapshot_tag = runner.random.choice(list(all_snapshots_per_region.keys()))
            else:
                oss_snapshots = [
                    snapshot_key
                    for snapshot_key, snapshot_value in all_snapshots_per_region.items()
                    if snapshot_value["scylla_product"] == "oss"
                ]
                snapshot_tag = runner.random.choice(oss_snapshots)

            snapshot_info = all_snapshots_per_region[snapshot_tag]
            snapshot_info.update(
                {
                    "expected_timeout": snapshot_groups_by_size[chosen_snapshot_size]["expected_timeout"],
                    "number_of_rows": snapshot_groups_by_size[chosen_snapshot_size]["number_of_rows"],
                }
            )
            return snapshot_tag, snapshot_info

        def execute_data_validation_thread(command_template, keyspace_name, number_of_rows):
            stress_queue = []
            number_of_loaders = sum(runner.tester.params.get("n_loaders"))
            rows_per_loader = int(number_of_rows / number_of_loaders)
            for loader_index in range(number_of_loaders):
                stress_command = command_template.format(
                    num_of_rows=rows_per_loader,
                    keyspace_name=keyspace_name,
                    sequence_start=rows_per_loader * loader_index + 1,
                    sequence_end=rows_per_loader * (loader_index + 1),
                )
                read_thread = runner.tester.run_stress_thread(
                    stress_cmd=stress_command, round_robin=True, stop_test_on_failure=False
                )
                stress_queue.append(read_thread)
            return stress_queue

        def _restore_schema(locations: list, cluster_id: str, tag: str) -> None:
            """Introduced to cover two flows:

            - When a backup snapshot has different DC name than the current cluster's DC name -
            restore schema out of the Manager applying CQL statements saved in schema.json file

            - When backup snapshot has the same DC name as the current cluster's DC name -
            restore schema using the Manager (sctool restore --restore-schema ...)
            """
            ks_statements, other_statements = get_schema_create_statements_from_snapshot(
                bucket=locations[0].split(":")[-1],
                mgr_cluster_id=cluster_id,
                snapshot_tag=tag,
            )

            dc_under_test_name = next(iter(runner.cluster.get_nodetool_status()))
            # Test is supposed to work with single DC setups only, so we can take the first DC name
            dc_from_backup_name = get_dc_name_from_ks_statement(ks_statements[0])[0]
            runner.log.debug("DC name from backup: %s, DC name under test: %s", dc_from_backup_name, dc_under_test_name)

            if dc_under_test_name != dc_from_backup_name:
                runner.log.info(
                    "DC names mismatch - restoring the schema manually altering cql statements and "
                    "applying them one by one"
                )
                # Alter the dc_name in keyspace cql statements to match the current cluster's dc_name
                old_dc_block = f"'{dc_from_backup_name}':"  # Include quotes and colon to avoid unintended replacements
                new_dc_block = f"'{dc_under_test_name}':"
                ks_statements = [stmt.replace(old_dc_block, new_dc_block) for stmt in ks_statements]
                # Apply cql statements one by one to restore schema
                for cql_stmt in ks_statements + other_statements:
                    runner.target_node.run_cqlsh(cql_stmt)
            else:
                runner.log.info("Restoring the schema using the Scylla Manager")
                task = mgr_cluster.create_restore_task(restore_schema=True, location_list=locations, snapshot_tag=tag)
                task.wait_and_get_final_status(step=10, timeout=6 * 60)  # 6 giving minutes to restore the schema
                assert task.status == TaskStatus.DONE, f"Schema restoration failed: snapshot tag - {tag}"

        skip_issues = [
            "https://github.com/scylladb/scylla-manager/issues/3829",
            "https://github.com/scylladb/scylla-manager/issues/4049",
        ]
        is_multi_dc = (
            len(runner.cluster.params.region_names) > 1 or (runner.cluster.params.get("simulated_regions") or 0) > 1
        )
        if SkipPerIssues(skip_issues, params=runner.tester.params) and is_multi_dc:
            raise UnsupportedNemesis("MultiDC cluster configuration is not supported by this nemesis")

        if not (runner.cluster.params.get("use_mgmt") or runner.cluster.params.get("use_cloud_manager")):
            raise UnsupportedNemesis("Scylla-manager configuration is not defined!")
        if runner.cluster.params.get("cluster_backend") not in ("aws", "k8s-eks"):
            raise UnsupportedNemesis("The restore test only supports 'AWS' and 'K8S-EKS' backends.")

        mgr_cluster = runner.cluster.get_cluster_manager()
        cluster_backend = runner.cluster.params.get("cluster_backend")
        if cluster_backend == "k8s-eks":
            cluster_backend = "aws"

        persistent_manager_snapshots_dict = get_persistent_snapshots()
        region = next(iter(runner.cluster.params.region_names), "")
        target_bucket = persistent_manager_snapshots_dict[cluster_backend]["bucket"].format(region=region)
        chosen_snapshot_tag, chosen_snapshot_info = choose_snapshot(
            snapshots_dict=persistent_manager_snapshots_dict[cluster_backend], region=region
        )

        runner.log.info("Restoring the keyspace %s", chosen_snapshot_info["keyspace_name"])
        location_list = [f"{runner.cluster.params.get('backup_bucket_backend')}:{target_bucket}"]
        test_keyspaces = [cql_unquote_if_needed(keyspace) for keyspace in runner.cluster.get_test_keyspaces()]
        # Keyspace names that start with a digit are surrounded by quotation marks in the output of a describe query
        if chosen_snapshot_info["keyspace_name"] not in test_keyspaces:
            runner.log.info("Restoring the schema of the keyspace '%s'", chosen_snapshot_info["keyspace_name"])
            _restore_schema(
                locations=location_list,
                cluster_id=chosen_snapshot_info["cluster_id"],
                tag=chosen_snapshot_tag,
            )
            with suppress_expected_unavailability_errors():
                runner.cluster.restart_scylla()  # After schema restoration, you should restart the nodes

            # TODO: Bring it back after the implementation of https://github.com/scylladb/scylla-manager/issues/4049
            # which will unblock schema restore into a different DC. For now, we can restore schema only within one DC.
            # According to https://github.com/scylladb/scylla-manager/issues/4041#issuecomment-2565489699, the step
            # below is not needed if restoring the schema within one DC.
            #
            # self.tester.set_ks_strategy_to_network_and_rf_according_to_cluster(
            #    keyspace=chosen_snapshot_info["keyspace_name"], repair_after_alter=False)
        try:
            restore_task = mgr_cluster.create_restore_task(
                restore_data=True, location_list=location_list, snapshot_tag=chosen_snapshot_tag
            )
            restore_task.wait_and_get_final_status(step=30, timeout=chosen_snapshot_info["expected_timeout"])
            assert restore_task.status == TaskStatus.DONE, f"Data restoration of {chosen_snapshot_tag} has failed!"

            confirmation_stress_template = (persistent_manager_snapshots_dict)[cluster_backend][
                "confirmation_stress_template"
            ]
            stress_queue = execute_data_validation_thread(
                command_template=confirmation_stress_template,
                keyspace_name=chosen_snapshot_info["keyspace_name"],
                number_of_rows=chosen_snapshot_info["number_of_rows"],
            )

            for stress in stress_queue:
                is_passed = runner.tester.verify_stress_thread(stress)
                assert is_passed, (
                    "Data verification stress command, triggered by the 'mgmt_restore' nemesis, has failed"
                )
        finally:
            runner.log.info("Cleaning up restored keyspace '%s'", chosen_snapshot_info["keyspace_name"])
            drop_ks_stmt = f'DROP KEYSPACE IF EXISTS "{chosen_snapshot_info["keyspace_name"]}";'
            try:
                runner.target_node.run_cqlsh(drop_ks_stmt)
            except Exception as drop_err:  # noqa: BLE001
                runner.log.warning("Failed to drop restored keyspace: %s", drop_err)


@target_data_nodes
class MgmtRepair(NemesisBaseClass):
    """Run a repair via Scylla Manager CLI."""

    manager_operation = True
    disruptive = False
    kubernetes = True
    limited = True

    def disrupt(self):
        if not self.runner.cluster.params.get("use_mgmt") and not self.runner.cluster.params.get("use_cloud_manager"):
            raise UnsupportedNemesis("Scylla-manager configuration is not defined!")
        self.runner.log.info("MgmtRepair Nemesis begin")
        self.runner.run_repair_manager()
        self.runner.log.info("MgmtRepair Nemesis end")


@target_data_nodes
class MgmtCorruptThenRepair(NemesisBaseClass):
    """Corrupt data by destroying SStables, then repair via Scylla Manager."""

    manager_operation = True
    disruptive = True
    kubernetes = True

    def disrupt(self):
        if not self.runner.cluster.params.get("use_mgmt") and not self.runner.cluster.params.get("use_cloud_manager"):
            raise UnsupportedNemesis("Scylla-manager configuration is not defined!")
        destroy_data_and_restart_scylla(self.runner)
        self.runner.run_repair_manager()

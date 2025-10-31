#!/usr/bin/env python

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
# Copyright (c) 2016 ScyllaDB

import random
import threading
import time
from datetime import timedelta

from sdcm import mgmt
from sdcm.argus_results import (
    send_manager_benchmark_results_to_argus,
    send_manager_snapshot_details_to_argus,
    ManagerRestoreBenchmarkResult,
    ManagerOneOneRestoreBenchmarkResult,
)
from sdcm.mgmt import ScyllaManagerError, TaskStatus, HostStatus, HostSsl, HostRestStatus
from sdcm.mgmt.argus_report import report_to_argus, ManagerReportType
from sdcm.mgmt.cli import RestoreTask
from sdcm.mgmt.common import (
    reconfigure_scylla_manager,
    get_persistent_snapshots,
    get_backup_size,
    ObjectStorageUploadMode,
)
from sdcm.provision.helpers.certificate import TLSAssets
from sdcm.nemesis import MgmtRepair
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.alternator.table_setup import alternator_backuped_tables
from sdcm.utils.aws_utils import AwsIAM
from sdcm.utils.features import is_tablets_feature_enabled
from sdcm.utils.common import reach_enospc_on_node, clean_enospc_on_node
from sdcm.utils.time_utils import ExecutionTimer
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn, SnapshotData
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events.group_common_events import ignore_no_space_errors, ignore_stream_mutation_fragments_errors
from sdcm.utils.tablets.common import TabletsConfiguration


class ManagerRestoreTests(ManagerTestFunctionsMixIn):
    def test_restore_multiple_backup_snapshots(self):  # noqa: PLR0914
        mgr_cluster = self.db_cluster.get_cluster_manager()
        cluster_backend = self.params.get("cluster_backend")
        if cluster_backend != "aws":
            self.log.error("Test supports only AWS ATM")
            return
        persistent_manager_snapshots_dict = get_persistent_snapshots()
        region = next(iter(self.params.region_names), "")
        target_bucket = persistent_manager_snapshots_dict[cluster_backend]["bucket"].format(region=region)
        backup_bucket_backend = self.params.get("backup_bucket_backend")
        location_list = [f"{backup_bucket_backend}:{target_bucket}"]
        confirmation_stress_template = persistent_manager_snapshots_dict[cluster_backend][
            "confirmation_stress_template"
        ]
        read_stress_list = []
        snapshot_sizes = persistent_manager_snapshots_dict[cluster_backend]["snapshots_sizes"]
        for size in snapshot_sizes:
            number_of_rows = persistent_manager_snapshots_dict[cluster_backend]["snapshots_sizes"][size][
                "number_of_rows"
            ]
            expected_timeout = persistent_manager_snapshots_dict[cluster_backend]["snapshots_sizes"][size][
                "expected_timeout"
            ]
            snapshot_dict = persistent_manager_snapshots_dict[cluster_backend]["snapshots_sizes"][size]["snapshots"]
            snapshot_tag = random.choice(list(snapshot_dict.keys()))
            keyspace_name = snapshot_dict[snapshot_tag]["keyspace_name"]

            self.restore_backup_with_task(
                mgr_cluster=mgr_cluster,
                snapshot_tag=snapshot_tag,
                timeout=180,
                restore_schema=True,
                location_list=location_list,
            )
            self.restore_backup_with_task(
                mgr_cluster=mgr_cluster,
                snapshot_tag=snapshot_tag,
                timeout=expected_timeout,
                restore_data=True,
                location_list=location_list,
            )
            stress_command = confirmation_stress_template.format(
                num_of_rows=number_of_rows, keyspace_name=keyspace_name, sequence_start=1, sequence_end=number_of_rows
            )
            read_stress_list.append(stress_command)
        for stress in read_stress_list:
            read_thread = self.run_stress_thread(stress_cmd=stress, round_robin=False)
            self.verify_stress_thread(read_thread)

    def test_restore_backup_with_task(self, ks_names: list = None):
        self.log.info("starting test_restore_backup_with_task")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        if not ks_names:
            ks_names = ["keyspace1"]
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, keyspace_list=ks_names)
        backup_task_status = backup_task.wait_and_get_final_status(timeout=1500)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        soft_timeout = 36 * 60
        hard_timeout = 50 * 60
        with adaptive_timeout(Operations.MGMT_REPAIR, self.db_cluster.data_nodes[0], timeout=soft_timeout):
            self.verify_backup_success(
                mgr_cluster=mgr_cluster,
                backup_task=backup_task,
                ks_names=ks_names,
                restore_data_with_task=True,
                timeout=hard_timeout,
            )
        self.run_verification_read_stress(ks_names)
        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_restore_backup_with_task")

    def test_restore_alternator_backup_with_task(self, delete_tables: list = None):
        self.log.info("starting test_restore_alternator_backup_with_task")
        mgr_cluster = self.db_cluster.get_cluster_manager(
            alternator_credentials=self.alternator.get_credentials(node=self.db_cluster.nodes[0])
        )
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations)
        backup_task_status = backup_task.wait_and_get_final_status(timeout=1500)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        soft_timeout = 36 * 60
        hard_timeout = 50 * 60
        with adaptive_timeout(Operations.MGMT_REPAIR, self.db_cluster.data_nodes[0], timeout=soft_timeout):
            self.verify_alternator_backup_success(
                mgr_cluster=mgr_cluster, backup_task=backup_task, delete_tables=delete_tables, timeout=hard_timeout
            )
        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_restore_alternator_backup_with_task")


class ManagerBackupTests(ManagerRestoreTests):
    def test_basic_backup(self, ks_names: list = None):
        self.log.info("starting test_basic_backup")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations)
        backup_task_status = backup_task.wait_and_get_final_status(timeout=1500)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        # Do restore with a task for multiDC clusters, otherwise the test will take a long time
        restore_with_task = True if self.db_node.test_config.MULTI_REGION else False
        self.verify_backup_success(
            mgr_cluster=mgr_cluster,
            backup_task=backup_task,
            ks_names=ks_names,
            restore_data_with_task=restore_with_task,
        )
        self.run_verification_read_stress(ks_names)
        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_basic_backup")

    def test_backup_multiple_ks_tables(self):
        self.log.info("starting test_backup_multiple_ks_tables")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        tables = self.create_ks_and_tables(10, 100)
        self.log.debug("tables list = {}".format(tables))
        # TODO: insert data to those tables
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations)
        backup_task_status = backup_task.wait_and_get_final_status(timeout=1500)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
        self.log.info("finishing test_backup_multiple_ks_tables")

    def test_backup_location_with_path(self):
        self.log.info("starting test_backup_location_with_path")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        try:
            mgr_cluster.create_backup_task(location_list=[f"{location}/path_testing/" for location in self.locations])
        except ScyllaManagerError as error:
            self.log.info("Expected to fail - error: {}".format(error))
        self.log.info("finishing test_backup_location_with_path")

    def test_backup_rate_limit(self):
        self.log.info("starting test_backup_rate_limit")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        rate_limit_list = [f"{dc}:{random.randint(15, 25)}" for dc in self.get_all_dcs_names()]
        self.log.info("rate limit will be {}".format(rate_limit_list))
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=rate_limit_list)
        task_status = backup_task.wait_and_get_final_status(timeout=18000)
        assert task_status == TaskStatus.DONE, (
            f"Task {backup_task.id} did not end successfully:\n{backup_task.detailed_progress}"
        )
        self.log.info("backup task finished with status {}".format(task_status))
        # TODO: verify that the rate limit is as set in the cmd
        self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
        self.log.info("finishing test_backup_rate_limit")

    def test_backup_purge_removes_orphan_files(self):
        """
        The test stops a backup task mid-upload, so that orphan files will remain in the destination bucket.
        Afterwards, the test reruns the backup task from scratch (with the --no-continue flag, so it's practically
        a new task) and after the task concludes (successfully) the test makes sure the manager has deleted the
        previously mentioned orphan files from the bucket.
        """
        self.log.info("starting test_backup_purge_removes_orphan_files")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        snapshot_file_list_pre_test = self.get_all_snapshot_files(cluster_id=mgr_cluster.id)

        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, retention=1)
        backup_task.wait_for_uploading_stage(step=5)
        backup_task.stop()
        snapshot_file_list_post_task_stopping = self.get_all_snapshot_files(cluster_id=mgr_cluster.id)
        orphan_files_pre_rerun = snapshot_file_list_post_task_stopping.difference(snapshot_file_list_pre_test)
        assert orphan_files_pre_rerun, "SCT could not create orphan snapshots by stopping a backup task"

        # So that the files' names will be different form the previous ones,
        # and they won't simply replace the previous files in the bucket
        for node in self.db_cluster.nodes:
            node.run_nodetool("compact")

        backup_task.start(continue_task=False)
        backup_task.wait_and_get_final_status(step=10)
        snapshot_file_list_post_purge = self.get_all_snapshot_files(cluster_id=mgr_cluster.id)
        orphan_files_post_rerun = snapshot_file_list_post_purge.intersection(orphan_files_pre_rerun)
        assert not orphan_files_post_rerun, "orphan files were not deleted!"

        self.log.info("finishing test_backup_purge_removes_orphan_files")

    def test_enospc_during_backup(self):
        self.log.info("starting test_enospc_during_backup")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        # deleting previous snapshots so that the current backup will last longer
        previous_backup_tasks = mgr_cluster.backup_task_list
        for backup_task in previous_backup_tasks:
            backup_task.delete_backup_snapshot()

        target_node = self.db_cluster.nodes[1]

        with ignore_no_space_errors(node=target_node):
            try:
                backup_task = mgr_cluster.create_backup_task(location_list=self.locations)
                backup_task.wait_for_uploading_stage()
                backup_task.stop()

                reach_enospc_on_node(target_node=target_node)

                backup_task.start()

                backup_task.wait_and_get_final_status()
                assert backup_task.status == TaskStatus.DONE, (
                    "The backup failed to run on a node with no free space,"
                    " while it should have had the room for snapshots due "
                    "to the previous run"
                )

            finally:
                clean_enospc_on_node(target_node=target_node, sleep_time=30)
        self.log.info("finishing test_enospc_during_backup")

    def test_enospc_before_restore(self):
        if is_tablets_feature_enabled(self.db_cluster.nodes[0]):
            # TODO: Get back to this restriction after https://github.com/scylladb/scylla-manager/issues/4275 resolution
            self.log.info(
                "Skipping test_enospc_before_restore due to enabled tablets. "
                "For details https://github.com/scylladb/scylla-manager/issues/4276"
            )
            return

        self.log.info("starting test_enospc_before_restore")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, keyspace_list=["keyspace1"])
        backup_task_status = backup_task.wait_and_get_final_status(timeout=1500)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        target_node = self.db_cluster.nodes[1]
        with ignore_no_space_errors(node=target_node), ignore_stream_mutation_fragments_errors():
            try:
                reach_enospc_on_node(target_node=target_node)

                snapshot_tag = backup_task.get_snapshot_tag()
                restore_task = mgr_cluster.create_restore_task(
                    restore_data=True, location_list=self.locations, snapshot_tag=snapshot_tag
                )
                final_status = restore_task.wait_and_get_final_status(step=30)

                assert final_status == TaskStatus.ERROR, (
                    f"The restore task is supposed to fail, since node {target_node} lacks the disk space to download"
                    f"the snapshot files"
                )
            finally:
                clean_enospc_on_node(target_node=target_node, sleep_time=30)
        self.log.info("finishing test_enospc_before_restore")

    def test_backup_feature(self):
        self.generate_load_and_wait_for_results()
        with self.subTest("Backup Multiple KS' and Tables"):
            self.test_backup_multiple_ks_tables()
        with self.subTest("Backup to Location with path"):
            self.test_backup_location_with_path()
        with self.subTest("Test Backup Rate Limit"):
            self.test_backup_rate_limit()
        with self.subTest("Test Backup Purge Removes Orphans Files"):
            self.test_backup_purge_removes_orphan_files()
        with self.subTest("Test restore a backup with restore task"):
            self.test_restore_backup_with_task()
        with self.subTest("Test Backup end of space"):  # Preferably at the end
            self.test_enospc_during_backup()
        with self.subTest("Test Restore end of space"):
            self.test_enospc_before_restore()

    def test_alternator_backup_feature(self):
        test_table_config = self.params.get("alternator_test_table") or {}
        features = {
            "lsi": test_table_config.get("lsi_name", None),
            "gsi": test_table_config.get("gsi_name", None),
            "tags": test_table_config.get("tags", None),
        }
        target_node = self.db_cluster.nodes[0]
        with alternator_backuped_tables(target_node, self.alternator, params=self.params, **features) as tables:
            self.alternator.verify_tables_features(node=target_node, tables=tables, **features)
            self.generate_load_and_wait_for_results()
            with self.subTest("Test restore alternator backup with restore task"):
                self.test_restore_alternator_backup_with_task(delete_tables=tables.keys())
                self.alternator.verify_tables_features(
                    node=target_node,
                    tables=tables,
                    wait_for_item_count=test_table_config.get("items", None),
                    **features,
                )
                self.run_verification_read_stress()

    def test_no_delta_backup_at_disabled_compaction(self):
        """The purpose of test is to check that delta backup (no changes to DB between backups) takes time -> 0.

        Important test precondition is to disable compaction on all nodes in the cluster.
        Otherwise, new set of SSTables is created what ends up in the situation that almost no deduplication is applied.

        For more details https://github.com/scylladb/scylla-manager/issues/3936#issuecomment-2277611709
        """
        self.log.info("starting test_consecutive_backups")

        self.log.info("Run write stress")
        self.run_prepare_write_cmd()

        self.log.info("Disable compaction for every node in the cluster")
        self.disable_compaction()

        self.log.info("Prepare Manager")
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)

        self.log.info("Run backup #1")
        backup_task_1 = mgr_cluster.create_backup_task(location_list=self.locations)
        backup_task_1_status = backup_task_1.wait_and_get_final_status(timeout=3600)
        assert backup_task_1_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_1_status} instead of {TaskStatus.DONE}"
        )
        self.log.info(f"Backup task #1 duration - {backup_task_1.duration}")

        self.log.info("Run backup #2")
        backup_task_2 = mgr_cluster.create_backup_task(location_list=self.locations)
        backup_task_2_status = backup_task_2.wait_and_get_final_status(timeout=60)
        assert backup_task_2_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_2_status} instead of {TaskStatus.DONE}"
        )
        self.log.info(f"Backup task #2 duration - {backup_task_2.duration}")

        assert backup_task_2.duration < timedelta(seconds=15), "No-delta backup took more than 15 seconds"

        self.log.info("Verify restore from backup #2")
        self.verify_backup_success(
            mgr_cluster=mgr_cluster, backup_task=backup_task_2, restore_data_with_task=True, timeout=3600
        )

        self.log.info("Run verification read stress")
        self.run_verification_read_stress()

        self.log.info("finishing test_consecutive_backups")


class ManagerRepairTests(ManagerTestFunctionsMixIn):
    LOCALSTRATEGY_KEYSPACE_NAME = "localstrategy_keyspace"
    NETWORKSTRATEGY_KEYSPACE_NAME = "networkstrategy_keyspace"

    def _test_intensity_and_parallel(self, fault_multiple_nodes):
        keyspace_to_be_repaired = "keyspace2"
        InfoEvent(message="starting test_intensity_and_parallel").publish()
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(
            name=self.CLUSTER_NAME + "_intensity_and_parallel",
            db_cluster=self.db_cluster,
            auth_token=self.monitors.mgmt_auth_token,
        )

        InfoEvent(message="Starting faulty load (to be repaired)").publish()
        self.create_missing_rows_in_cluster(
            create_missing_rows_in_multiple_nodes=fault_multiple_nodes,
            keyspace_to_be_repaired=keyspace_to_be_repaired,
            total_num_of_rows=29296872,
        )

        InfoEvent(message="Starting a repair with no intensity").publish()
        base_repair_task = mgr_cluster.create_repair_task(keyspace="keyspace*")
        base_repair_task.wait_and_get_final_status(step=30)
        assert base_repair_task.status == TaskStatus.DONE, "The base repair task did not end in the expected time"
        InfoEvent(message=f"The base repair, with no intensity argument, took {base_repair_task.duration}").publish()

        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_to_be_repaired}")

        arg_list = [
            {"intensity": 0.5},
            {"intensity": 0.25},
            {"intensity": 0.0001},
            {"intensity": 2},
            {"intensity": 4},
            {"parallel": 1},
            {"parallel": 2},
            {"intensity": 2, "parallel": 1},
            {"intensity": 100},
            {"intensity": 0},
        ]

        for arg_dict in arg_list:
            InfoEvent(message="Starting faulty load (to be repaired)").publish()
            self.create_missing_rows_in_cluster(
                create_missing_rows_in_multiple_nodes=fault_multiple_nodes,
                keyspace_to_be_repaired=keyspace_to_be_repaired,
                total_num_of_rows=29296872,
            )

            InfoEvent(message=f"Starting a repair with {arg_dict}").publish()
            repair_task = mgr_cluster.create_repair_task(**arg_dict, keyspace="keyspace*")
            repair_task.wait_and_get_final_status(step=30)
            InfoEvent(message=f"repair with {arg_dict} took {repair_task.duration}").publish()

            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
                session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_to_be_repaired}")
        InfoEvent(message="finishing test_intensity_and_parallel").publish()

    def test_repair_intensity_feature(self, fault_multiple_nodes):
        InfoEvent(message="Starting C-S write load").publish()
        self.run_prepare_write_cmd()
        InfoEvent(message="Flushing").publish()
        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")
        InfoEvent(message="Waiting for compactions to end").publish()
        self.wait_no_compactions_running(n=30, sleep_time=30)
        InfoEvent(message="Starting C-S read load").publish()
        stress_read_thread = self.generate_background_read_load()
        time.sleep(600)  # So we will see the base load of the cluster
        InfoEvent(message="Sleep ended - Starting tests").publish()
        with self.subTest("test_intensity_and_parallel"):
            self._test_intensity_and_parallel(fault_multiple_nodes=fault_multiple_nodes)
        load_results = stress_read_thread.get_results()
        self.log.info("load={}".format(load_results))

    def test_repair_multiple_keyspace_types(self):
        self.log.info("starting test_repair_multiple_keyspace_types")
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.db_cluster.get_cluster_manager()

        rf = self.get_rf_based_on_nodes_number() if self.db_node.test_config.MULTI_REGION else 3
        self.create_keyspace_and_basic_table(self.NETWORKSTRATEGY_KEYSPACE_NAME, replication_factor=rf)

        self.create_keyspace_and_basic_table(self.LOCALSTRATEGY_KEYSPACE_NAME, replication_factor=0)
        repair_task = mgr_cluster.create_repair_task()
        task_final_status = repair_task.wait_and_get_final_status(timeout=7200)
        assert task_final_status == TaskStatus.DONE, "Task: {} final status is: {}.".format(
            repair_task.id, str(repair_task.status)
        )
        self.log.info("Task: {} is done.".format(repair_task.id))
        self.log.debug("sctool version is : {}".format(manager_tool.sctool.version))

        expected_keyspaces_to_be_repaired = ["system_distributed", self.NETWORKSTRATEGY_KEYSPACE_NAME]
        if not self.db_cluster.nodes[0].raft.is_consistent_topology_changes_enabled:
            expected_keyspaces_to_be_repaired.append("system_auth")
        self.log.debug("Keyspaces expected to be repaired: {}".format(expected_keyspaces_to_be_repaired))
        per_keyspace_progress = repair_task.per_keyspace_progress
        self.log.info("Looking in the repair output for all of the required keyspaces")
        for keyspace_name in expected_keyspaces_to_be_repaired:
            keyspace_repair_percentage = per_keyspace_progress.get(keyspace_name, None)
            assert keyspace_repair_percentage is not None, "The keyspace {} was not included in the repair!".format(
                keyspace_name
            )

            assert keyspace_repair_percentage == 100, "The repair of the keyspace {} stopped at {}%".format(
                keyspace_name, keyspace_repair_percentage
            )

        localstrategy_keyspace_percentage = per_keyspace_progress.get(self.LOCALSTRATEGY_KEYSPACE_NAME, None)
        assert localstrategy_keyspace_percentage is None, (
            "The keyspace with the replication strategy of localstrategy was included in repair, when it shouldn't"
        )
        self.log.info("the sctool repair command was completed successfully")

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_repair_multiple_keyspace_types")

    def test_repair_intensity_feature_on_multiple_node(self):
        self.test_repair_intensity_feature(fault_multiple_nodes=True)

    def test_repair_intensity_feature_on_single_node(self):
        self.test_repair_intensity_feature(fault_multiple_nodes=False)

    def test_repair_control(self):
        InfoEvent(message="Starting C-S write load").publish()
        self.run_prepare_write_cmd()
        InfoEvent(message="Flushing").publish()
        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")
        InfoEvent(message="Waiting for compactions to end").publish()
        self.wait_no_compactions_running(n=90, sleep_time=30)
        InfoEvent(message="Starting C-S read load").publish()
        stress_read_thread = self.generate_background_read_load()
        time.sleep(600)  # So we will see the base load of the cluster
        InfoEvent(message="Sleep ended - Starting tests").publish()
        self.create_repair_and_alter_it_with_repair_control()
        load_results = stress_read_thread.get_results()
        self.log.info("load={}".format(load_results))


class ManagerCRUDTests(ManagerTestFunctionsMixIn):
    def test_cluster_crud(self):
        """
        Test steps:
        1) add a cluster to manager.
        2) update the cluster attributes in manager: name/host
        3) delete the cluster from manager and re-add again.
        """
        self.log.info("starting test_mgmt_cluster_crud")
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.db_cluster.get_cluster_manager()
        # Test cluster attributes
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name + "_renamed", "Cluster name wasn't changed after update command"
        mgr_cluster.delete()
        mgr_cluster = manager_tool.add_cluster(
            self.CLUSTER_NAME, db_cluster=self.db_cluster, auth_token=self.monitors.mgmt_auth_token
        )

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_mgmt_cluster_crud")


class ManagerHealthCheckTests(ManagerTestFunctionsMixIn):
    def test_cluster_healthcheck(self):
        self.log.info("starting test_mgmt_cluster_healthcheck")
        mgr_cluster = self.db_cluster.get_cluster_manager()
        other_host, other_host_ip = [
            host_data
            for host_data in self.get_cluster_hosts_with_ips()
            if host_data[1] != self.get_cluster_hosts_ip()[0]
        ][0]
        sleep = 40
        self.log.debug("Sleep {} seconds, waiting for health-check task to run by schedule on first time".format(sleep))
        time.sleep(sleep)
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.status == HostStatus.UP, "Not all hosts status is 'UP'"
            assert host_health.rest_status == HostRestStatus.UP, "Not all hosts REST status is 'UP'"
        # Check for sctool status change after scylla-server down
        other_host.stop_scylla_server()
        self.log.debug("Health-check next run is: {}".format(healthcheck_task.next_run))
        self.log.debug("Sleep {} seconds, waiting for health-check task to run after node down".format(sleep))
        time.sleep(sleep)
        dict_host_health = mgr_cluster.get_hosts_health()
        assert dict_host_health[other_host_ip].status == HostStatus.DOWN, "Host: {} status is not 'DOWN'".format(
            other_host_ip
        )
        assert dict_host_health[other_host_ip].rest_status == HostRestStatus.DOWN, (
            "Host: {} REST status is not 'DOWN'".format(other_host_ip)
        )
        other_host.start_scylla_server()

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_mgmt_cluster_healthcheck")

    def test_healthcheck_change_max_timeout(self):
        """
        New in manager 2.6
        'max_timeout' is new parameter in the scylla manager yaml. It decides the maximum
        amount of time the manager healthcheck function will wait for a ping response before
        it will announce the node as timed out.

        The test sets the timeout as such that the latency of the nodes that exist in the
        local region (us-east-1) will not be longer than the set timeout, and therefore
        will appear as UP in the healthcheck, while the latency of the node that is the
        distant region (us-west-2) will be longer than the set timeout, and therefore the
        healthcheck will report it as TIMEOUT.

        The test makes sure that the healthcheck reports those statuses correctly.
        """
        self.log.info("starting test_healthcheck_change_max_timeout")

        nodes_num_per_dc = 3
        nodes_from_local_dc = self.db_cluster.nodes[:nodes_num_per_dc]
        nodes_from_distant_dc = self.db_cluster.nodes[nodes_num_per_dc:]
        manager_node = self.monitors.nodes[0]
        mgr_cluster = self.db_cluster.get_cluster_manager()
        try:
            reconfigure_scylla_manager(
                manager_node=manager_node, logger=self.log, values_to_update=[{"healthcheck": {"max_timeout": "20ms"}}]
            )
            sleep = 40
            self.log.debug("Sleep %s seconds, waiting for health-check task to rerun", sleep)
            time.sleep(sleep)
            dict_host_health = mgr_cluster.get_hosts_health()
            for node in nodes_from_distant_dc:
                assert dict_host_health[node.ip_address].status == HostStatus.TIMEOUT, (
                    f'After setting "max_timeout" to a value shorter than the latency of the distant dc nodes, '
                    f"the healthcheck status of {node.ip_address} was not {HostStatus.TIMEOUT} as expected, but "
                    f"instead it was {dict_host_health[node.ip_address].status}"
                )
            for node in nodes_from_local_dc:
                assert dict_host_health[node.ip_address].status == HostStatus.UP, (
                    f'After setting "max_timeout" to a value longer than the latency of the local dc nodes, '
                    f"the healthcheck status of {node.ip_address} is not {HostStatus.UP} as expected, but "
                    f"instead it was {dict_host_health[node.ip_address].status}"
                )
        finally:
            reconfigure_scylla_manager(manager_node=manager_node, logger=self.log, values_to_remove=["healthcheck"])
            mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_healthcheck_change_max_timeout")


class ManagerEncryptionTests(ManagerTestFunctionsMixIn):
    def _disable_client_encryption(self) -> None:
        for node in self.db_cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yml:
                scylla_yml.client_encryption_options.enabled = False
            node.restart_scylla()

    def test_client_encryption(self):
        self.log.info("starting test_client_encryption")

        self.log.info("ENABLED client encryption checks")
        if not self.db_cluster.nodes[0].is_client_encrypt:
            self.db_cluster.enable_client_encrypt()

        manager_node = self.monitors.nodes[0]

        self.log.info("Create and send client TLS certificate/key to the manager node")
        manager_node.create_node_certificate(
            cert_file=manager_node.ssl_conf_dir / TLSAssets.CLIENT_CERT,
            cert_key=manager_node.ssl_conf_dir / TLSAssets.CLIENT_KEY,
        )
        manager_node.remoter.run(f"mkdir -p {mgmt.cli.SSL_CONF_DIR}")
        manager_node.remoter.send_files(src=str(manager_node.ssl_conf_dir) + "/", dst=str(mgmt.cli.SSL_CONF_DIR))

        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True, client_encrypt=True)

        healthcheck_task = mgr_cluster.get_healthcheck_task()
        healthcheck_task.wait_for_status(list_status=[TaskStatus.DONE], step=5, timeout=240)
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))

        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.ON, "Not all hosts ssl is 'ON'"
            assert host_health.status == HostStatus.UP, "Not all hosts status is 'UP'"

        self.log.info("DISABLED client encryption checks")
        self._disable_client_encryption()
        # SM caches scylla nodes configuration and the healthcheck svc is independent on the cache updates.
        # Cache is being updated periodically, every 1 minute following the manager config for SCT.
        # We need to wait until SM is aware about the configuration change.
        sleep_time = 90
        self.log.debug("Sleep %s seconds, waiting for SM is aware about the configuration change", sleep_time)
        time.sleep(sleep_time)

        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.OFF, "Not all hosts ssl is 'OFF'"

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info("finishing test_client_encryption")


class ManagerSuspendTests(ManagerTestFunctionsMixIn):
    def _test_suspend_and_resume_task_template(self, task_type):
        # task types: backup/repair
        self.log.info("starting test_suspend_and_resume_{}".format(task_type))
        # re-add the cluster to make the backup task run from scratch, otherwise it may be very fast and
        # the test is not able to catch the required statuses
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)
        if task_type == "backup":
            suspendable_task = mgr_cluster.create_backup_task(location_list=self.locations)
        elif task_type == "repair":
            # Set intensity and parallel to 1 to make repair task run longer to be able to catch RUNNING state
            suspendable_task = mgr_cluster.create_repair_task(intensity=1, parallel=1)
        else:
            raise ValueError(f"Not familiar with task type: {task_type}")
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=5), (
            f"task {suspendable_task.id} failed to reach status {TaskStatus.RUNNING}"
        )
        with mgr_cluster.suspend_manager_then_resume(start_tasks=True):
            assert suspendable_task.wait_for_status(list_status=[TaskStatus.STOPPED], timeout=300, step=10), (
                f"task {suspendable_task.id} failed to reach status {TaskStatus.STOPPED}"
            )
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1200, step=10), (
            f"task {suspendable_task.id} failed to reach status {TaskStatus.DONE}"
        )
        self.log.info("finishing test_suspend_and_resume_{}".format(task_type))

    def _test_suspend_with_on_resume_start_tasks_flag_template(self, wait_for_duration):
        suspension_duration = 75
        test_name_filler = "after_duration_passed" if wait_for_duration else "before_duration_passed"
        self.log.info("starting test_suspend_with_on_resume_start_tasks_flag_{}".format(test_name_filler))
        # re-add the cluster to make the backup task run from scratch, otherwise it may run very fast and
        # the test won't be able to catch the required statuses
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)
        task_type = random.choice(["backup", "repair"])
        if task_type == "backup":
            suspendable_task = mgr_cluster.create_backup_task(location_list=self.locations)
        else:
            suspendable_task = mgr_cluster.create_repair_task()
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=5), (
            f"task {suspendable_task.id} failed to reach status {TaskStatus.RUNNING}"
        )
        with mgr_cluster.suspend_manager_then_resume(
            start_tasks=False, start_tasks_in_advance=True, duration=f"{suspension_duration}s"
        ):
            assert suspendable_task.wait_for_status(list_status=[TaskStatus.STOPPED], timeout=60, step=2), (
                f"task {suspendable_task.id} failed to reach status {TaskStatus.STOPPED}"
            )
            if wait_for_duration:  # Whether waiting for the duration time to pass or not
                time.sleep(suspension_duration + 5)
        if wait_for_duration:
            assert suspendable_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1200, step=10), (
                f"After the cluster was resumed (while resuming AFTER the suspend duration has passed),"
                f" task {suspendable_task.id} failed to reach status "
                f"{TaskStatus.DONE}, but instead stayed in {suspendable_task.status}"
            )
        else:
            assert suspendable_task.status == TaskStatus.STOPPED, (
                "After the cluster was resumed (while resuming BEFORE the suspend duration "
                f"has passed), task {suspendable_task.id} failed to stay in status STOPPED"
            )
            time.sleep(suspension_duration + 5)
            assert suspendable_task.status == TaskStatus.STOPPED, (
                "After the cluster was resumed (while resuming BEFORE the suspend duration "
                f"has passed), task {suspendable_task.id} failed to stay in status STOPPED after suspension time ended"
            )
        self.log.info("finishing test_suspend_with_on_resume_start_tasks_flag_{}".format(test_name_filler))

    def _test_suspend_and_resume_without_starting_tasks(self):
        self.log.info("starting test_suspend_and_resume_without_starting_tasks")
        # re-add the cluster to make the backup task run from scratch, otherwise it may be very fast and
        # the test is not able to catch the required statuses
        mgr_cluster = self.db_cluster.get_cluster_manager(force_add=True)
        suspendable_task = mgr_cluster.create_backup_task(location_list=self.locations)
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=5), (
            f"task {suspendable_task.id} failed to reach status {TaskStatus.RUNNING}"
        )
        with mgr_cluster.suspend_manager_then_resume(start_tasks=False):
            mgr_cluster.suspend()
            assert suspendable_task.wait_for_status(list_status=[TaskStatus.STOPPED], timeout=300, step=10), (
                f"task {suspendable_task.id} failed to reach status {TaskStatus.STOPPED}"
            )
            mgr_cluster.resume(start_tasks=False)
            self.log.info("Waiting a little time to make sure the task isn't started")
            time.sleep(60)
            current_task_status = suspendable_task.status
            assert current_task_status == TaskStatus.STOPPED, (
                f'Task {current_task_status} did not remain in "{TaskStatus.STOPPED}" status, but instead '
                f'reached "{current_task_status}" status'
            )
        self.log.info("finishing test_suspend_and_resume_without_starting_tasks")

    def test_suspend_and_resume(self):
        with self.subTest("Suspend and resume backup task"):
            self._test_suspend_and_resume_task_template(task_type="backup")
        with self.subTest("Suspend and resume repair task"):
            self._test_suspend_and_resume_task_template(task_type="repair")
        with self.subTest("Suspend and resume without starting task"):
            self._test_suspend_and_resume_without_starting_tasks()
        with self.subTest("Suspend with on resume start tasks flag after duration has passed"):
            self._test_suspend_with_on_resume_start_tasks_flag_template(wait_for_duration=True)
        with self.subTest("Suspend with on resume start tasks flag before duration has passed"):
            self._test_suspend_with_on_resume_start_tasks_flag_template(wait_for_duration=False)


class ManagerHelperTests(ManagerTestFunctionsMixIn):
    def _unlock_cloud_key(self) -> str | None:
        """The operation is required to make the particular Cloud cluster key reusable.
        For that, the key should be unlocked from the original cluster.
        """
        self.log.info("Unlock the EaR key used by cluster to reuse it while restoring to a new cluster (1-1 restore)")
        ear_key = self.db_cluster.get_ear_key()

        if not ear_key:
            self.log.warning("No EaR key found, skipping unlock")
            return None

        self.db_cluster.unlock_ear_key()
        return ear_key.get("keyid")

    def test_prepare_backup_snapshot(self):  # pylint: disable=too-many-locals  # noqa: PLR0914
        """Test prepares backup snapshot for its future use in nemesis or restore benchmarks

        Steps:
        1. Populate the cluster with data.
           - C-S write cmd is based on `confirmation_stress_template` template in manager_persistent_snapshots.yaml
           - Backup size should be specified in Jenkins job passing `mgmt_prepare_snapshot_size` parameter
        2. Run backup and wait for it to finish.
        3. Log snapshot details into console.
        """
        is_cloud_manager = self.params.get("use_cloud_manager")

        self.log.info("Initialize Scylla Manager")
        mgr_cluster = self.db_cluster.get_cluster_manager()

        self.log.info("Define backup location")
        if is_cloud_manager:
            # Extract location from an automatically scheduled backup task
            auto_backup_task = mgr_cluster.backup_task_list[0]
            location_list = [auto_backup_task.get_task_info_dict()["location"]]

            self.log.info("Delete scheduled backup task to not interfere")
            mgr_cluster.delete_task(auto_backup_task)
        else:
            location_list = self.locations

        self.log.info("Populate the cluster with data")
        backup_size = self.params.get("mgmt_prepare_snapshot_size")  # in Gb
        assert backup_size and backup_size >= 1, "Backup size must be at least 1Gb"

        ks_name, cs_write_cmds = self.build_snapshot_preparer_cs_write_cmd(backup_size)
        self.run_and_verify_stress_in_threads(cs_cmds=cs_write_cmds, stop_on_failure=True)

        self.log.info("Run backup and wait for it to finish")
        backup_task = mgr_cluster.create_backup_task(location_list=location_list, rate_limit_list=["0"])
        backup_task_status = backup_task.wait_and_get_final_status(timeout=200000)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )

        if is_cloud_manager:
            self.log.info("Copy bucket with snapshot since the original bucket is deleted together with cluster")
            # can be several locations for multiDC cluster, for example,
            # 'AWS_EU_SOUTH_1:s3:scylla-cloud-backup-170-176-15c7bm,AWS_EU_WEST_1:s3:scylla-cloud-backup-170-175-9dy2w4'
            location_list = location_list[0].split(",")
            for location in location_list:
                # from AWS_US_EAST_1:s3:scylla-cloud-backup-8072-7216-v5dn53' to scylla-cloud-backup-8072-7216-v5dn53
                original_bucket_name = location.split(":")[-1].strip("'")
                bucket_name = original_bucket_name + "-manager-tests"
                region = self.get_region_from_bucket_location(location)
                self.copy_backup_snapshot_bucket(source=original_bucket_name, destination=bucket_name, region=region)

        if is_cloud_manager:
            cluster_id = self.db_cluster.cloud_cluster_id
            key_id = self._unlock_cloud_key()
            manager_cluster_id = self.db_cluster.get_manager_cluster_id()
        else:
            cluster_id = mgr_cluster.id
            key_id = "N/A"
            manager_cluster_id = "N/A"

        self.log.info("Send snapshot details to Argus")
        snapshot_details = {
            "tag": backup_task.get_snapshot_tag(),
            "size": backup_size,
            "locations": ",".join(location_list),
            "ks_name": ks_name,
            "scylla_version": self.params.get_version_based_on_conf()[0],
            "cluster_id": cluster_id,
            "ear_key_id": key_id,
            "manager_cluster_id": manager_cluster_id,
        }
        send_manager_snapshot_details_to_argus(
            argus_client=self.test_config.argus_client(),
            snapshot_details=snapshot_details,
        )


class ManagerSanityTests(
    ManagerBackupTests,
    ManagerRestoreTests,
    ManagerRepairTests,
    ManagerCRUDTests,
    ManagerHealthCheckTests,
    ManagerSuspendTests,
    ManagerEncryptionTests,
):
    def test_manager_sanity(self, prepared_ks: bool = False, ks_names: list = None):
        """
        Test steps:
        1) Run the repair test.
        2) Run test_mgmt_cluster test.
        3) test_mgmt_cluster_healthcheck
        4) test_client_encryption
        """
        if not prepared_ks:
            self.generate_load_and_wait_for_results()
        with self.subTest("Basic Backup Test"):
            self.test_basic_backup(ks_names=ks_names)
        with self.subTest("Restore Backup Test"):
            self.test_restore_backup_with_task(ks_names=ks_names)
        with self.subTest("Repair Multiple Keyspace Types"):
            self.test_repair_multiple_keyspace_types()
        with self.subTest("Mgmt Cluster CRUD"):
            self.test_cluster_crud()
        with self.subTest("Mgmt cluster Health Check"):
            self.test_cluster_healthcheck()
        # test_healthcheck_change_max_timeout requires a multi dc run
        if self.db_cluster.nodes[0].test_config.MULTI_REGION:
            with self.subTest("Basic test healthcheck change max timeout"):
                self.test_healthcheck_change_max_timeout()
        with self.subTest("Basic test suspend and resume"):
            self.test_suspend_and_resume()
        with self.subTest("Client Encryption"):
            # Since this test activates encryption, it has to be the last test in the sanity
            self.test_client_encryption()

    def test_manager_sanity_vnodes_tablets_cluster(self):
        """
        Test steps:
        1) Create tablets keyspace and propagate some data.
        2) Create vnodes keyspace and propagate some data.
        3) Run sanity test (test_manager_sanity).
        """
        self.log.info("starting test_manager_sanity_vnodes_tablets_cluster")

        ks_config = [("tablets_keyspace", True), ("vnodes_keyspace", False)]
        ks_names = [i[0] for i in ks_config]
        for ks_name, tablets_enabled in ks_config:
            tablets_config = TabletsConfiguration(enabled=tablets_enabled)
            self.create_keyspace(ks_name, replication_factor=3, tablets_config=tablets_config)
            self.generate_load_and_wait_for_results(keyspace_name=ks_name)

        self.test_manager_sanity(prepared_ks=True, ks_names=ks_names)

        self.log.info("finishing test_manager_sanity_vnodes_tablets_cluster")


class ManagerRollbackTests(ManagerTestFunctionsMixIn):
    def test_mgmt_repair_nemesis(self):
        """
        Test steps:
        1) Run cassandra stress on cluster.
        2) Add cluster to Manager and run full repair via Nemesis
        """
        self.generate_load_and_wait_for_results()
        self.log.debug("test_mgmt_cli: initialize MgmtRepair nemesis")
        mgmt_nemesis = MgmtRepair(tester_obj=self, termination_event=self.db_cluster.nemesis_termination_event)
        mgmt_nemesis.disrupt()

    def test_manager_upgrade(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run manager upgrade to new version of yaml: 'scylla_mgmt_upgrade_to_repo'. (the 'from' version is: 'scylla_mgmt_address').
        """
        self.log.info("starting test_manager_upgrade")
        scylla_mgmt_upgrade_to_repo = self.params.get("scylla_mgmt_upgrade_to_repo")
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        selected_host = self.get_cluster_hosts_ip()[0]
        cluster_name = "mgr_cluster1"
        mgr_cluster = manager_tool.get_cluster(cluster_name=cluster_name) or manager_tool.add_cluster(
            name=cluster_name, host=selected_host, auth_token=self.monitors.mgmt_auth_token
        )
        self.log.info("Running some stress and repair before upgrade")
        self.test_mgmt_repair_nemesis()

        repair_task_list = mgr_cluster.repair_task_list

        manager_from_version = manager_tool.sctool.version
        manager_tool.upgrade(scylla_mgmt_upgrade_to_repo=scylla_mgmt_upgrade_to_repo)

        assert manager_from_version[0] != manager_tool.sctool.version[0], "Manager version not changed after upgrade."
        # verify all repair tasks exist
        for repair_task in repair_task_list:
            self.log.debug("{} status: {}".format(repair_task.id, repair_task.status))

        self.log.info("Running a new repair task after upgrade")
        repair_task = mgr_cluster.create_repair_task()
        self.log.debug("{} status: {}".format(repair_task.id, repair_task.status))
        self.log.info("finishing test_manager_upgrade")

    def test_manager_rollback_upgrade(self):
        """
        Test steps:
        1) Run Upgrade test: scylla_mgmt_address --> scylla_mgmt_upgrade_to_repo
        2) Run manager downgrade to pre-upgrade version as in yaml: 'scylla_mgmt_address'.
        """
        self.log.info("starting test_manager_rollback_upgrade")
        self.test_manager_upgrade()
        scylla_mgmt_address = self.params.get("scylla_mgmt_address")
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        manager_from_version = manager_tool.sctool.version
        manager_tool.rollback_upgrade(scylla_mgmt_address=scylla_mgmt_address)
        assert manager_from_version[0] != manager_tool.sctool.version[0], "Manager version not changed after rollback."
        self.log.info("finishing test_manager_rollback_upgrade")


class ManagerInstallationTests(ManagerTestFunctionsMixIn):
    def test_manager_installed_and_functional(self):
        """Verify that the Manager is installed and functional.

        The test is intended for execution on non-main OS distributions (for example, debian10)
        where the main goal is to execute installation test.
        The rest of the checks are executed on Ubuntu distribution in test_manager_sanity.
        """
        self.log.info("starting test_manager_installed_and_functional")

        manager_node = self.monitors.nodes[0]
        scylla_node = self.db_cluster.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)

        # Check scylla-manager and scylla-manager-agent are up and running (/ping endpoint)
        manager_node.is_manager_server_up()
        scylla_node.is_manager_agent_up()

        # Check the sctool version method is callable
        self.log.info("Got Manager's version: %s", manager_tool.sctool.version)

        # Add cluster and verify hosts health
        mgr_cluster = self.db_cluster.get_cluster_manager()
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.status == HostStatus.UP, "Host status is not 'UP'"
            assert host_health.rest_status == HostRestStatus.UP, "Host REST status is not 'UP'"

        self.log.info("finishing test_manager_installed_and_functional")


class ManagerRestoreBenchmarkTests(ManagerTestFunctionsMixIn):
    def tearDown(self):
        """Unlock EaR key used by Cloud cluster to reuse it in the future
        Otherwise, if not unlocked, the key will be deleted together with the cluster
        """
        if self.params.get("use_cloud_manager"):
            self.db_cluster.unlock_ear_key()
        super().tearDown()

    def get_restore_extra_parameters(self) -> str:
        extra_params = self.params.get("mgmt_restore_extra_params")
        return extra_params if extra_params else None

    def _send_restore_results_to_argus(
        self, task: RestoreTask, manager_version_timestamp: int, dataset_label: str = None
    ):
        total_restore_time = int(task.duration.total_seconds())
        repair_time = int(task.post_restore_repair_duration.total_seconds())
        results = {
            "restore time": (total_restore_time - repair_time),
            "repair time": repair_time,
            "total": total_restore_time,
        }
        download_bw, load_and_stream_bw = task.download_bw, task.load_and_stream_bw
        if download_bw:
            results["download bandwidth"] = download_bw
        if load_and_stream_bw:
            results["l&s bandwidth"] = load_and_stream_bw
        result_table = ManagerRestoreBenchmarkResult(sut_timestamp=manager_version_timestamp)
        send_manager_benchmark_results_to_argus(
            argus_client=self.test_config.argus_client(),
            result=results,
            result_table=result_table,
            row_name=dataset_label,
        )

    def _adjust_aws_restore_policy(self, snapshot: SnapshotData, cluster_id: str) -> None:
        assert self.params.get("use_cloud_manager"), "Should be applied to Cloud-managed clusters only"

        iam_client = AwsIAM()
        policies = iam_client.get_policy_by_name_prefix(f"s3-scylla-cloud-backup-{cluster_id}")
        for policy_arn in policies:
            for location in snapshot.locations:
                iam_client.add_resource_to_iam_policy(
                    policy_arn=policy_arn,
                    resource_to_add=f"arn:aws:s3:::{location.split(':')[-1]}",
                )

    def test_backup_and_restore_only_data(self, manager_backup_restore_method: ObjectStorageUploadMode):
        """The test is extensively used for restore benchmarking purposes and consists of the following steps:
        1. Populate the cluster with data (currently operates with datasets of 500GB, 1TB, 2TB, 5TB);
        2. Run the backup task and wait for its completion;
        3. Truncate the tables in the cluster;
        4. Run the restore task (custom batch_size and parallel params can be set in the pipeline)
           and wait for its completion;
        5. Run the verification read stress to ensure the data is restored correctly.
        """
        self.run_prepare_write_cmd()

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.db_cluster.get_cluster_manager()

        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])
        backup_task_status = backup_task.wait_and_get_final_status(timeout=200000)
        assert backup_task_status == TaskStatus.DONE, (
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        )
        InfoEvent(message=f"The backup task has ended successfully. Backup run time: {backup_task.duration}").publish()
        self.manager_test_metrics.backup_time = backup_task.duration

        ks_number = self.params.get("keyspace_num") or 1
        ks_names = self.get_keyspace_name(ks_number=ks_number)
        for ks_name in ks_names:
            self.db_cluster.nodes[0].run_cqlsh(f"TRUNCATE {ks_name}.standard1")

        extra_params = self.get_restore_extra_parameters()
        task = self.restore_backup_with_task(
            mgr_cluster=mgr_cluster,
            snapshot_tag=backup_task.get_snapshot_tag(),
            timeout=110000,
            restore_data=True,
            extra_params=extra_params,
            manager_backup_restore_method=manager_backup_restore_method,
        )
        self.manager_test_metrics.restore_time = task.duration

        manager_version_timestamp = manager_tool.sctool.client_version_timestamp
        self._send_restore_results_to_argus(task, manager_version_timestamp)

        self.run_verification_read_stress()

    def test_restore_from_precreated_backup(
        self,
        snapshot_name: str,
        manager_backup_restore_method: ObjectStorageUploadMode = None,
        restore_outside_manager: bool = False,
    ):
        """The test restores the schema and data from a pre-created backup and runs the verification read stress.
        1. Define the backup to restore from
        2. Run restore schema to empty cluster
        3. Run restore data
        4. Run verification read stress

        Args:
            snapshot_name: The name of the snapshot to restore from.
                           All snapshots are defined in the 'defaults/manager_restore_benchmark_snapshots.yaml'
            restore_outside_manager: set True to restore outside of Manager via nodetool refresh
        """
        self.log.info("Initialize Scylla Manager")
        mgr_cluster = self.db_cluster.get_cluster_manager()

        self.log.info("Define snapshot details and location")
        snapshot_data = self.get_snapshot_data(snapshot_name)
        locations = snapshot_data.locations

        if self.params.get("use_cloud_manager"):
            self.log.info("Delete scheduled backup task to not interfere")
            auto_backup_task = mgr_cluster.backup_task_list[0]
            mgr_cluster.delete_task(auto_backup_task)

            self.log.info("Adjust restore cluster backup policy")
            if self.params.get("cluster_backend") == "aws":
                self._adjust_aws_restore_policy(snapshot_data, cluster_id=self.db_cluster.cloud_cluster_id)

            self.log.info("Grant admin permissions to scylla_manager user")
            self.db_cluster.nodes[0].run_cqlsh(cmd="grant scylla_admin to scylla_manager")

        self.log.info("Restoring the schema")
        self.restore_backup_with_task(
            mgr_cluster=mgr_cluster,
            snapshot_tag=snapshot_data.tag,
            timeout=600,
            restore_schema=True,
            location_list=locations,
        )

        if restore_outside_manager:
            self.log.info("Restoring the data outside the Manager")
            with ExecutionTimer() as timer:
                self.restore_backup_without_manager(
                    mgr_cluster=mgr_cluster,
                    snapshot_tag=snapshot_data.tag,
                    ks_tables_list=snapshot_data.ks_tables_map,
                    location=locations[0],
                    precreated_backup=True,
                )
            restore_time = timer.duration
        else:
            self.log.info("Restoring the data")
            extra_params = self.get_restore_extra_parameters()
            task = self.restore_backup_with_task(
                mgr_cluster=mgr_cluster,
                snapshot_tag=snapshot_data.tag,
                restore_data=True,
                timeout=snapshot_data.exp_timeout,
                location_list=locations,
                extra_params=extra_params,
                manager_backup_restore_method=manager_backup_restore_method,
            )
            restore_time = task.duration
            manager_version_timestamp = mgr_cluster.sctool.client_version_timestamp
            self._send_restore_results_to_argus(task, manager_version_timestamp, dataset_label=snapshot_name)

        self.manager_test_metrics.restore_time = restore_time

        if not (self.params.get("mgmt_skip_post_restore_stress_read") or snapshot_data.prohibit_verification_read):
            self.log.info("Running verification read stress")
            cs_verify_cmds = self.build_cs_read_cmd_from_snapshot_details(snapshot_data)
            self.run_and_verify_stress_in_threads(cs_cmds=cs_verify_cmds)
        else:
            self.log.info("Skipping verification read stress because of the test or snapshot configuration")

    def test_restore_benchmark(self):
        """Benchmark restore operation using configured method.

        The test suggests two flows - populate the cluster with data, create the backup, and then restore it or
        restore from a pre-created backup.
        """
        if manager_backup_restore_method := self.params.get("manager_backup_restore_method"):
            manager_backup_restore_method = ObjectStorageUploadMode(manager_backup_restore_method)
        if reuse_snapshot_name := self.params.get("mgmt_reuse_backup_snapshot_name"):
            self.log.info("Executing test_restore_from_precreated_backup...")
            self.test_restore_from_precreated_backup(
                reuse_snapshot_name, manager_backup_restore_method=manager_backup_restore_method
            )
        else:
            self.log.info("Executing test_backup_and_restore_only_data...")
            self.test_backup_and_restore_only_data(manager_backup_restore_method=manager_backup_restore_method)

    def test_restore_data_without_manager(self):
        """The test restores the schema and data from a pre-created backup.
        The distinctive feature is that data restore is performed outside the Manager via nodetool refresh.
        Nodetool refresh cmd can be run with extra flags: --load-and-stream and --primary-replica-only.
        These extra flags can be set in `mgmt_nodetool_refresh_flags` variable.

        The motivation of having such a test is to check L&S efficiency when doing the restore of the full cluster in
        comparison with the same test but with restore executed via Manager.
        """
        snapshot_name = self.params.get("mgmt_reuse_backup_snapshot_name")
        assert snapshot_name, (
            "The test requires a pre-created snapshot to restore from. "
            "Please provide the 'mgmt_reuse_backup_snapshot_name' parameter."
        )

        self.test_restore_from_precreated_backup(snapshot_name, restore_outside_manager=True)


class ManagerOneToOneRestore(ManagerTestFunctionsMixIn):
    """The class contains tests and test methods for one-to-one restore functionality.
    In current shape, 1-1 restore is supposed to be used for Scylla Cloud clusters only.
    So, the test is not applicable for on-prem clusters used for regular Manager SCT tests.
    """

    AWS_CLOUD_PROVIDER_ID = 1
    GCP_CLOUD_PROVIDER_ID = 2

    def setUp(self):
        super().setUp()
        if not self.params.get("use_cloud_manager"):
            raise ValueError("The test is applicable only for Scylla Cloud clusters")

    def tearDown(self):
        """Unlock EaR key used by Cloud cluster to reuse it in the future
        Otherwise, if not unlocked, the key will be deleted together with the cluster
        """
        self.db_cluster.unlock_ear_key(ignore_status=True)
        super().tearDown()

    def _send_one_one_restore_results_to_argus(self, bootstrap_duration: int, restore_duration: int) -> None:
        results = {
            "bootstrap time": bootstrap_duration,
            "restore time": restore_duration,
            "total": bootstrap_duration + restore_duration,
        }
        result_table = ManagerOneOneRestoreBenchmarkResult()
        send_manager_benchmark_results_to_argus(
            argus_client=self.test_config.argus_client(), result=results, result_table=result_table
        )

    def _define_cloud_provider_id(self) -> int:
        cluster_backend = self.params.get("cluster_backend")
        if cluster_backend == "aws":
            return self.AWS_CLOUD_PROVIDER_ID
        elif cluster_backend == "gce":
            return self.GCP_CLOUD_PROVIDER_ID
        else:
            raise ValueError("Unsupported cloud provider")

    def test_one_to_one_restore(self):
        self.log.info("Get snapshot details")
        snapshot_name = self.params.get("mgmt_reuse_backup_snapshot_name")
        assert snapshot_name, (
            "The test requires a pre-created snapshot to restore from. "
            "Please, provide the 'mgmt_reuse_backup_snapshot_name' parameter."
        )
        snapshot_data = self.get_snapshot_data(snapshot_name)

        self.log.info("Initialize Scylla Manager")
        mgr_cluster = self.db_cluster.get_cluster_manager()

        self.log.info("Delete scheduled backup task to not interfere")
        auto_backup_task = mgr_cluster.backup_task_list[0]
        mgr_cluster.delete_task(auto_backup_task)

        self.log.info("Run 1-1 restore")
        with ExecutionTimer() as timer:
            self.db_cluster.run_one_to_one_restore(
                sm_cluster_id=snapshot_data.one_one_restore_params["sm_cluster_id"],
                buckets=",".join(snapshot_data.locations),
                snapshot_tag=snapshot_data.tag,
                account_credential_id=snapshot_data.one_one_restore_params["account_credential_id"],
                provider_id=self._define_cloud_provider_id(),
            )
        restore_duration = int(timer.duration.total_seconds())
        self.log.debug(f"1-1 restore took {restore_duration} seconds")

        self.log.info("Report results to Argus")
        self._send_one_one_restore_results_to_argus(
            bootstrap_duration=int(self.params.get("one_one_restore_cluster_bootstrap_duration")),
            restore_duration=restore_duration,
        )

        if not (self.params.get("mgmt_skip_post_restore_stress_read") or snapshot_data.prohibit_verification_read):
            self.log.info("Running verification read stress")
            cs_verify_cmds = self.build_cs_read_cmd_from_snapshot_details(snapshot_data)
            self.run_and_verify_stress_in_threads(cs_cmds=cs_verify_cmds)
        else:
            self.log.info(
                f"Skipping verification read stress because of the test or snapshot configuration,"
                f" mgmt_skip_post_restore_stress_read: {self.params.get('mgmt_skip_post_restore_stress_read')}, snapshot prohibit_verification_read: {snapshot_data.prohibit_verification_read}"
            )


class ManagerBackupRestoreConcurrentTests(ManagerTestFunctionsMixIn):
    def manager_backup_and_report(self, mgr_cluster, label: str, timeout: int = 7200):
        InfoEvent(message="Starting `rclone` based backup").publish()
        task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])

        backup_status = task.wait_and_get_final_status(timeout=timeout)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"

        backup_report = {
            "Size": get_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        report_to_argus(self.monitors, self.test_config, ManagerReportType.BACKUP, backup_report, label)
        return task

    def run_read_stress_and_report(self, label):
        stress_queue = []

        for command in self.params.get("stress_read_cmd"):
            stress_queue.append(self.run_stress_thread(command, round_robin=True, stop_test_on_failure=False))

        with ExecutionTimer() as stress_timer:
            for stress in stress_queue:
                assert self.verify_stress_thread(stress), "Read stress command"
        InfoEvent(message=f"Read stress duration: {stress_timer.duration}s.").publish()

        read_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
        }
        report_to_argus(self.monitors, self.test_config, ManagerReportType.READ, read_stress_report, label)

    def test_backup_benchmark(self):
        self.log.info("Executing test_backup_restore_benchmark...")

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.log.info("Disable clusterwide compaction")
        # Disable keyspace autocompaction cluster-wide since we dont want it to interfere with our restore timing
        self.disable_compaction()

        mgr_cluster = self.db_cluster.get_cluster_manager()

        self.log.info("Create and report backup time")
        backup_task = self.manager_backup_and_report(mgr_cluster, "Backup")

        self.log.info("Remove backup")
        backup_task.delete_backup_snapshot()

        self.log.info("Run read test")
        self.run_read_stress_and_report("Read stress")

        self.log.info("Create and report backup time during read stress")

        backup_thread = threading.Thread(
            target=self.manager_backup_and_report,
            kwargs={"mgr_cluster": mgr_cluster, "label": "Backup during read stress"},
        )

        read_stress_thread = threading.Thread(
            target=self.run_read_stress_and_report, kwargs={"label": "Read stress during backup"}
        )
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

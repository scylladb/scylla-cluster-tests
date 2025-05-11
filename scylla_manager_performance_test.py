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
# Copyright (c) 2025 ScyllaDB
# pylint: disable=too-many-lines
import re
import threading
import uuid
from datetime import timedelta
from enum import Enum
from functools import partial

from docker.errors import InvalidArgument

from argus.client.generic_result import Status
from mgmt_cli_test import ManagerTestFunctionsMixIn
from sdcm import mgmt
from sdcm.argus_results import send_manager_benchmark_results_to_argus, ManagerBackupReadResult, \
    ManagerBackupBenchmarkResult, submit_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.mgmt import TaskStatus
from sdcm.mgmt.cli import RestoreTask, BackupTask, ManagerCluster
from sdcm.mgmt.common import get_backup_size
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.cluster_tools import clear_snapshot_nodes
from sdcm.utils.common import ParallelObject, format_size
from sdcm.utils.compaction_ops import CompactionOps
from sdcm.utils.decorators import latency_calculator_decorator
from sdcm.utils.time_utils import ExecutionTimer


class ManagerReportType(Enum):
    READ = 1
    BACKUP = 2


class ManagerRestoreBenchmarkTests(ManagerTestFunctionsMixIn):

    def get_restore_extra_parameters(self) -> str:
        extra_params = self.params.get('mgmt_restore_extra_params')
        return extra_params if extra_params else None

    def _send_restore_results_to_argus(self, task: RestoreTask, manager_version_timestamp: int,
                                       dataset_label: str = None):
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
        send_manager_benchmark_results_to_argus(
            argus_client=self.test_config.argus_client(),
            result=results,
            sut_timestamp=manager_version_timestamp,
            row_name=dataset_label,
        )

    def test_backup_and_restore_only_data(self):
        """The test is extensively used for restore benchmarking purposes and consists of the following steps:
        1. Populate the cluster with data (currently operates with datasets of 500GB, 1TB, 2TB, 5TB);
        2. Run the backup task and wait for its completion;
        3. Truncate the tables in the cluster;
        4. Run the restore task (custom batch_size and parallel params can be set in the pipeline)
           and wait for its completion;
        5. Run the verification read stress to ensure the data is restored correctly.
        """
        self.run_prepare_write_cmd()

        compaction_ops = CompactionOps(cluster=self.db_cluster)
        #  Disable keyspace autocompaction cluster-wide since we dont want it to interfere with our restore timing
        for node in self.db_cluster.nodes:
            compaction_ops.disable_autocompaction_on_ks_cf(node=node)

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)

        backup_task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])
        backup_task_status = backup_task.wait_and_get_final_status(timeout=200000)
        assert backup_task_status == TaskStatus.DONE, \
            f"Backup task ended in {backup_task_status} instead of {TaskStatus.DONE}"
        InfoEvent(message=f'The backup task has ended successfully. Backup run time: {backup_task.duration}').publish()
        self.manager_test_metrics.backup_time = backup_task.duration

        ks_number = self.params.get('keyspace_num') or 1
        ks_names = self.get_keyspace_name(ks_number=ks_number)
        for ks_name in ks_names:
            self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE {ks_name}.standard1')

        extra_params = self.get_restore_extra_parameters()
        task = self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=backup_task.get_snapshot_tag(),
                                             timeout=110000, restore_data=True, extra_params=extra_params)
        self.manager_test_metrics.restore_time = task.duration

        manager_version_timestamp = manager_tool.sctool.client_version_timestamp
        self._send_restore_results_to_argus(task, manager_version_timestamp)

        self.run_verification_read_stress()

    def test_restore_from_precreated_backup(self, snapshot_name: str, restore_outside_manager: bool = False):
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
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)

        snapshot_data = self.get_snapshot_data(snapshot_name)

        self.log.info("Restoring the schema")
        location = [f"s3:{snapshot_data.bucket}"]
        self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_data.tag, timeout=600,
                                      restore_schema=True, location_list=location)
        for ks_name in snapshot_data.keyspaces:
            self.set_ks_strategy_to_network_and_rf_according_to_cluster(keyspace=ks_name, repair_after_alter=False)

        compaction_ops = CompactionOps(cluster=self.db_cluster)
        # Disable keyspace autocompaction cluster-wide since we dont want it to interfere with our restore timing
        for node in self.db_cluster.nodes:
            compaction_ops.disable_autocompaction_on_ks_cf(node=node)

        if restore_outside_manager:
            self.log.info("Restoring the data outside the Manager")
            with ExecutionTimer() as timer:
                self.restore_backup_without_manager(
                    mgr_cluster=mgr_cluster,
                    snapshot_tag=snapshot_data.tag,
                    ks_tables_list=snapshot_data.ks_tables_map,
                    location=location[0],
                    precreated_backup=True,
                )
            restore_time = timer.duration
        else:
            self.log.info("Restoring the data with Manager task")
            extra_params = self.get_restore_extra_parameters()
            task = self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_data.tag,
                                                 timeout=snapshot_data.exp_timeout, restore_data=True,
                                                 location_list=location, extra_params=extra_params)
            restore_time = task.duration
            manager_version_timestamp = manager_tool.sctool.client_version_timestamp
            self._send_restore_results_to_argus(task, manager_version_timestamp, dataset_label=snapshot_name)

        self.manager_test_metrics.restore_time = restore_time

        if not (self.params.get('mgmt_skip_post_restore_stress_read') or snapshot_data.prohibit_verification_read):
            self.log.info("Running verification read stress")
            cs_verify_cmds = self.build_cs_read_cmd_from_snapshot_details(snapshot_data)
            self.run_and_verify_stress_in_threads(cs_cmds=cs_verify_cmds)
        else:
            self.log.info("Skipping verification read stress because of the test or snapshot configuration")

    def test_restore_benchmark(self):
        """Benchmark restore operation.

        The test suggests two flows - populate the cluster with data, create the backup, and then restore it or
        restore from a pre-created backup.
        """
        if reuse_snapshot_name := self.params.get('mgmt_reuse_backup_snapshot_name'):
            self.log.info("Executing test_restore_from_precreated_backup...")
            self.test_restore_from_precreated_backup(reuse_snapshot_name)
        else:
            self.log.info("Executing test_backup_and_restore_only_data...")
            self.test_backup_and_restore_only_data()

    def test_restore_data_without_manager(self):
        """The test restores the schema and data from a pre-created backup.
        The distinctive feature is that data restore is performed outside the Manager via nodetool refresh.
        Nodetool refresh cmd can be run with extra flags: --load-and-stream and --primary-replica-only.
        These extra flags can be set in `mgmt_nodetool_refresh_flags` variable.

        The motivation of having such a test is to check L&S efficiency when doing the restore of the full cluster in
        comparison with the same test but with restore executed via Manager.
        """
        snapshot_name = self.params.get('mgmt_reuse_backup_snapshot_name')
        assert snapshot_name, ("The test requires a pre-created snapshot to restore from. "
                               "Please provide the 'mgmt_reuse_backup_snapshot_name' parameter.")

        self.test_restore_from_precreated_backup(snapshot_name, restore_outside_manager=True)


class ManagerBackupRestoreConcurrentTests(ManagerTestFunctionsMixIn):
    keyspace = 'keyspace1'
    table = 'standard1'
    snapshot_list_lock = threading.Lock()
    base_prefix = ""

    def report_to_argus(self, report_type: ManagerReportType, data: dict, label: str):
        timestamp = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0]).sctool.client_version_timestamp
        if report_type == ManagerReportType.READ:
            table = ManagerBackupReadResult(sut_timestamp=timestamp)
        elif report_type == ManagerReportType.BACKUP:
            table = ManagerBackupBenchmarkResult(sut_timestamp=timestamp)
        else:
            raise InvalidArgument("Unknown report type")

        for key, value in data.items():
            table.add_result(column=key, value=value, row=label, status=Status.UNSET)
        submit_results_to_argus(self.test_config.argus_client(), table)

    def report_manager_backup_results_to_argus(self, label: str, task: BackupTask, mgr_cluster: ManagerCluster) -> None:
        report = {
            "Size": get_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, report, label)

    def _manager_backup(self, mgr_cluster, timeout: int = 7200):
        InfoEvent(message='Starting `rclone` based backup').publish()
        task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])
        backup_status = task.wait_and_get_final_status(timeout=timeout)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"
        return task

    def manager_backup_and_report(self, mgr_cluster, label: str, delete_snapshot: bool = False, timeout: int = 7200):
        task = self._manager_backup(mgr_cluster=mgr_cluster, timeout=timeout)
        self.report_manager_backup_results_to_argus(label=label, task=task, mgr_cluster=mgr_cluster)
        if delete_snapshot:
            self.log.info("Delete Manager backup snapshot")
            task.delete_backup_snapshot()
        return task

    def native_backup_and_report(self, label):
        InfoEvent(message='Starting Native based backup').publish()
        self.snapshot_ids = {}
        self.node_backup_size = {}
        self.base_prefix = f"{self.keyspace}/{self.table}/{str(uuid.uuid4())}"
        self.s3_endpoint_name = self._get_object_storage_endpoint()["name"]
        self.backup_bucket_location = self.params.get('backup_bucket_location')
        self.log.info(
            f"Starting Native based backup [{self.base_prefix}] using endpoint {self.s3_endpoint_name} to bucket {self.backup_bucket_location}")
        with ExecutionTimer() as backup_timer:
            backup_jobs = [partial(self.native_backup, scylla_node=node, )
                           for node in self.db_cluster.data_nodes]
            ParallelObject(objects=backup_jobs, timeout=36000).call_objects()

        backup_report = {
            "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Time": int(backup_timer.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)
        clear_snapshot_nodes(cluster=self.db_cluster)

    def run_read_stress_and_report(self, label):
        stress_queue = []

        for command in self.params.get('stress_read_cmd'):
            stress_queue.append(self.run_stress_thread(command, round_robin=True, stop_test_on_failure=False))

        with ExecutionTimer() as stress_timer:
            for stress in stress_queue:
                assert self.verify_stress_thread(stress), "Read stress command"
        InfoEvent(message=f'Read stress duration: {stress_timer.duration}s.').publish()

        read_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.READ, read_stress_report, label)

    def run_stress_and_report(self, legend: str):
        """
        Run all read and write stress in parallel.
        Wait for all to finish.
        Report its perf stats to Argus.
        Args:
            legend: a label for Argus reporting
        """
        stress_write_cmd = self.params.get('stress_cmd')
        stress_read_cmd = self.params.get('stress_read_cmd')

        decorated_run_stress_write = latency_calculator_decorator(
            legend=f"{legend} - Write:", cycle_name=legend, workload="write"
        )(self._run_stress_batch)

        decorated_run_stress_read = latency_calculator_decorator(
            legend=f"{legend} - Read:", cycle_name=legend, workload="read"
        )(self._run_stress_batch)

        InfoEvent(message='Read and Write stress start').publish()
        stress_jobs = [
            partial(decorated_run_stress_write, cmds=stress_write_cmd),
            partial(decorated_run_stress_read, cmds=stress_read_cmd)
        ]
        ParallelObject(objects=stress_jobs, timeout=self.benchmark_timeout).call_objects()
        InfoEvent(message='Read and Write stress finished').publish()

    def test_backup_benchmark(self):
        self.log.info("Executing test_backup_restore_benchmark...")

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.log.info("Disable clusterwide compaction")
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        #  Disable keyspace autocompaction cluster-wide since we dont want it to interfere with our restore timing
        for node in self.db_cluster.nodes:
            compaction_ops.disable_autocompaction_on_ks_cf(node=node)

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)

        self.log.info("Create and report backup time")
        backup_task = self.manager_backup_and_report(mgr_cluster, "Backup")

        self.log.info("Remove backup")
        backup_task.delete_backup_snapshot()

        self.log.info("Run read test")
        self.run_read_stress_and_report("Read stress")

        self.log.info("Create and report backup time during read stress")

        backup_thread = threading.Thread(target=self.manager_backup_and_report,
                                         kwargs={"mgr_cluster": mgr_cluster, "label": "Backup during read stress"})

        read_stress_thread = threading.Thread(target=self.run_read_stress_and_report,
                                              kwargs={"label": "Read stress during backup"})
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

    def _run_stress_batch(self, cmds):
        stress_queue = []
        InfoEvent(message=f'Run stress batch of {len(cmds)} commands').publish()
        params = {'stress_cmd': cmds, 'round_robin': True}
        self._run_all_stress_cmds(stress_queue, params)
        for stress in stress_queue:
            self.verify_stress_thread(thread_pool=stress)
        results = []
        for stress in stress_queue:
            results.extend(self.get_stress_results(queue=stress, store_results=False))
            self.log.debug("One c-s command results: %s", results[-1])
        return results, stress_queue

    def native_backup(self, scylla_node: BaseNode):
        result = scylla_node.run_nodetool('snapshot')
        snapshot_name = re.findall(r'(\d+)', result.stdout.split("snapshot name")[1])[0]
        with self.snapshot_list_lock:
            self.snapshot_ids[scylla_node.uuid] = snapshot_name
            backup_size_res = scylla_node.remoter.sudo(
                f"du -sb /var/lib/scylla/data/{self.keyspace}/{self.table}-*/snapshots/{snapshot_name}/")
            if backup_size_res.stdout:
                self.node_backup_size[scylla_node.uuid] = int(
                    backup_size_res.stdout[:backup_size_res.stdout.find("\t")])

        backup_res = scylla_node.run_nodetool(
            f"backup --endpoint {self.s3_endpoint_name} --bucket {self.backup_bucket_location}  --prefix {self.base_prefix}/{snapshot_name} --keyspace {self.keyspace} --table {self.table} --snapshot {snapshot_name}")
        if backup_res is not None and backup_res.exit_status != 0:
            raise Exception(f"Backup failed: {backup_res.stdout}")

    def _get_object_storage_endpoint(self) -> dict | None:
        if append_scylla_yaml := self.params.get('append_scylla_yaml'):
            if endpoints := append_scylla_yaml.get("object_storage_endpoints", {}):
                return endpoints[0]
        return None

    def test_native_backup_benchmark(self):
        self.log.info("Executing test_native_backup_benchmark...")
        if not self._get_object_storage_endpoint():
            raise Exception("object_storage_endpoints are not defined in Yaml configuration, skipping test")
        self.benchmark_timeout = int(timedelta(hours=14).total_seconds())
        InfoEvent(message='Pre-load dataset').publish()
        self.run_prepare_write_cmd()

        # Run a major compaction before perf measurements
        self._align_cluster_data_state(self.keyspace, self.table)

        InfoEvent(message='Start Read and Write stress baseline').publish()
        self.run_stress_and_report(legend="Baseline stress")

        # Cleanup the extra stress
        self._align_cluster_data_state(self.keyspace, self.table)

        # Backup baseline (rClone)
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)
        self.log.info("Create and report backup baseline time")
        self.manager_backup_and_report(mgr_cluster=mgr_cluster, label="rClone Backup baseline", delete_snapshot=True,
                                       timeout=self.benchmark_timeout)

        backup_and_stress_jobs = [
            partial(self.manager_backup_and_report, mgr_cluster,
                    "rClone backup with stress", True, self.benchmark_timeout),
            partial(self.run_stress_and_report, legend="stress with rClone backup")
        ]

        ParallelObject(objects=backup_and_stress_jobs, timeout=self.benchmark_timeout).call_objects()

        # Cleanup the extra stress
        self._align_cluster_data_state(self.keyspace, self.table)

        # Backup native
        self.native_backup_and_report(label="Native Backup baseline")
        self.run_fstrim_on_all_db_nodes()

        # Backup native with read and write stress
        backup_and_stress_jobs = [
            partial(self.native_backup_and_report, label="Native backup with stress"),
            partial(self.run_stress_and_report, legend="stress with Native backup")
        ]
        ParallelObject(objects=backup_and_stress_jobs, timeout=self.benchmark_timeout).call_objects()

import json
import os
import re
import threading
import uuid
from enum import Enum

from argus.client.generic_result import Status
from mgmt_cli_test import ManagerTestFunctionsMixIn
from sdcm import mgmt
from sdcm.argus_results import ManagerBackupReadResult, ManagerBackupBenchmarkResult, submit_results_to_argus
from sdcm.cluster import BaseNode
from sdcm.mgmt import TaskStatus
from sdcm.remote import shell_script_cmd
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.node import build_node_api_command, RequestMethods
from sdcm.utils.time_utils import ExecutionTimer


class ManagerReportType(Enum):
    READ = 1
    BACKUP = 2


def parse_backup_size(mgr_cluster, task_id):
    res = mgr_cluster.sctool.run(cmd=f"progress {task_id} -c {mgr_cluster.id}",
                                 parse_table_res=False)
    match = re.search(r".+100% │ (.*?) │ ", res.stdout, re.MULTILINE)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Backup size not found in the output in {res.stdout}")


def format_size(size_in_bytes):
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB']:
        if size_in_bytes < 1024:
            return f"{size_in_bytes:.2f} {unit}"
        size_in_bytes /= 1024

def prepare_server_for_s3(node):

    # prepare everything required:
    # - pkcs11
    # - add object_storage.yaml to scylla.yaml
    # - populate object_storage.yaml with data
    node.remoter.sudo(shell_script_cmd("""\
    apt install p11-kit p11-kit-modules
    mkdir /usr/lib64/pkcs11
    ln -s /usr/lib/x86_64-linux-gnu/pkcs11/p11-kit-trust.so /usr/lib64/pkcs11/p11-kit-trust.so
    echo 'object_storage_config_file: /etc/scylla/object_storage.yaml\n' >> /etc/scylla/scylla.yaml
    echo 'endpoints:\n  - name: s3.us-east-1.amazonaws.com\n    port: 443\n    https: true\n    aws_region: us-east-1\n    iam_role_arn: arn:aws:iam::797456418907:instance-profile/qa-scylla-manager-backup-instance-profile\n' > /etc/scylla/object_storage.yaml
        """))

    # restart the node
    node.restart_scylla_server()


class ManagerBackupRestoreConcurrentTests(ManagerTestFunctionsMixIn):
    snapshot_list_lock = threading.Lock()
    snapshot_ids = {}
    node_sstables = {}
    node_backup_size = {}
    base_prefix = ""

    def report_to_argus(self, report_type: ManagerReportType, data: dict, label: str):
        if report_type == ManagerReportType.READ:
            table = ManagerBackupReadResult(sut_timestamp=mgmt.get_scylla_manager_tool(
                manager_node=self.monitors.nodes[0]).sctool.client_version_timestamp)
        elif report_type == ManagerReportType.BACKUP:
            table = ManagerBackupBenchmarkResult(sut_timestamp=mgmt.get_scylla_manager_tool(
                manager_node=self.monitors.nodes[0]).sctool.client_version_timestamp)
        else:
            raise ValueError("Unknown report type")

        for key, value in data.items():
            table.add_result(column=key, value=value, row=label, status=Status.UNSET)
        submit_results_to_argus(self.test_config.argus_client(), table)

    def create_backup_and_report(self, mgr_cluster, label: str):
        InfoEvent(message='Starting `rclone` based backup').publish()
        task = mgr_cluster.create_backup_task(location_list=self.locations, rate_limit_list=["0"])

        backup_status = task.wait_and_get_final_status(timeout=72000)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"

        backup_report = {
            "Size": parse_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)
        return task

    def create_restore_and_report(self, mgr_cluster, snapshot_tag, label: str):
        self.set_balancing(False)

        task = self.restore_backup_with_task(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tag,
                                             timeout=72000, restore_data=True)

        backup_status = task.wait_and_get_final_status(timeout=72000)
        assert backup_status == TaskStatus.DONE, "Backup upload has failed!"

        backup_report = {
            "Size": parse_backup_size(mgr_cluster, task.id),
            "Time": int(task.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)
        self.set_balancing(True)
        return task

    def run_read_stress_and_report(self, label):
        stress_queue = {"read": [], "write": []}

        for command in self.params.get('stress_cmd'):
            if " write " in command:
                stress_queue["write"].append(self.run_stress_thread(command))
            elif " read " in command:
                stress_queue["read"].append(self.run_stress_thread(command))
            else:
                raise ValueError("Unknown stress command")

        def get_stress_averages(queue):
            averages = {'op rate': 0.0, 'partition rate': 0.0, 'row rate': 0.0, 'latency 99th percentile': 0.0}
            num_results = 0
            for stress in queue:
                results = self.get_stress_results(queue=stress)
                num_results += len(results)
                for result in results:
                    for key in averages:
                        averages[key] += float(result[key])
            stats = {key: averages[key] / num_results for key in averages}
            return stats

        with ExecutionTimer() as stress_timer:
            read_stats = get_stress_averages(stress_queue["read"])
            write_stats = get_stress_averages(stress_queue["write"])

        InfoEvent(message=f'Read stress duration: {stress_timer.duration}s.').publish()

        read_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
            "op rate": read_stats['op rate'],
            "partition rate": read_stats['partition rate'],
            "row rate": read_stats['row rate'],
            "latency 99th percentile": read_stats['latency 99th percentile'],
        }
        self.report_to_argus(ManagerReportType.READ, read_stress_report, "Read stress: " + label)

        write_stress_report = {
            "read time": int(stress_timer.duration.total_seconds()),
            "op rate": write_stats['op rate'],
            "partition rate": write_stats['partition rate'],
            "row rate": write_stats['row rate'],
            "latency 99th percentile": write_stats['latency 99th percentile'],
        }
        self.report_to_argus(ManagerReportType.READ, write_stress_report, "Write stress: " + label)

    def backup(self, scylla_node: BaseNode):
        scylla_node.run_nodetool("flush")
        result = scylla_node.run_nodetool('snapshot')
        snapshot_name = re.findall(r'(\d+)', result.stdout.split("snapshot name")[1])[0]
        with self.snapshot_list_lock:
            self.snapshot_ids[scylla_node.uuid] = snapshot_name
            manifest_path = scylla_node.remoter.sudo(
                f"find /var/lib/scylla/data/keyspace1/standard1-*/snapshots/{snapshot_name}/ -name manifest.json",
                ignore_status=True, verbose=True)
            manifest_content = scylla_node.remoter.sudo(f"cat {manifest_path.stdout}", ignore_status=True,
                                                        verbose=True).stdout

            parsed_data = json.loads(manifest_content)
            file_list = parsed_data['files']
            self.node_sstables[scylla_node.uuid] = [file_name.replace("-Data.db", "-TOC.txt") for file_name in
                                                    file_list]

            backup_size_res = scylla_node.remoter.sudo(
                f"du -sb /var/lib/scylla/data/keyspace1/standard1-*/snapshots/{snapshot_name}/")
            if backup_size_res.stdout:
                self.node_backup_size[scylla_node.uuid] = int(
                    backup_size_res.stdout[:backup_size_res.stdout.find("\t")])

        backup_res = scylla_node.run_nodetool(
            f"backup --endpoint s3.us-east-1.amazonaws.com --bucket manager-backup-tests-us-east-1  --prefix {self.base_prefix}/{snapshot_name} --keyspace keyspace1 --table standard1 --snapshot {snapshot_name}")
        if backup_res is not None and backup_res.exit_status != 0:
            raise Exception(f"Backup failed: {backup_res.stdout}")

    def backup_and_report(self, label):
        self.snapshot_ids = {}
        self.node_sstables = {}
        self.node_backup_size = {}
        self.base_prefix = f"standard1/keyspace1/{str(uuid.uuid4())}"
        backup_threads = []
        with ExecutionTimer() as backup_timer:
            for node in self.db_cluster.nodes:
                thread = threading.Thread(target=self.backup, args=(node,))
                backup_threads.append(thread)
                thread.start()
            for thread in backup_threads:
                thread.join()
        backup_report = {
            "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Time": int(backup_timer.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, backup_report, label)

    def restore(self, scylla_node: BaseNode):
        sstables_list = []
        for toc in self.node_sstables[scylla_node.uuid]:
            sstables_list.append(os.path.basename(toc))

        chunks = []
        chunk = ""
        for toc in sstables_list:
            if len(chunk) + len(toc) > 1024 * 1024:
                chunks.append(chunk)
                chunk = toc
            else:
                chunk += " " + toc
        if chunk:
            chunks.append(chunk)

        for chunk in chunks:
            res = scylla_node.run_nodetool(
                f"restore --endpoint s3.us-east-1.amazonaws.com --bucket manager-backup-tests-us-east-1 --scope node --prefix {self.base_prefix}/{self.snapshot_ids[scylla_node.uuid]} --keyspace keyspace1 --table standard1 {chunk}")
            if res is not None and res.exit_status != 0:
                raise Exception(f"Restore failed: {res.stdout}")
        res = scylla_node.run_nodetool("repair keyspace1 standard1")
        if res is not None and res.exit_status != 0:
            raise Exception(f"Repair failed: {res.stdout}")

    def restore_and_report(self, label):
        self.set_balancing(False)
        restore_threads = []
        with ExecutionTimer() as restore_timer:
            for node in self.db_cluster.nodes:
                thread = threading.Thread(target=self.restore, args=(node,))
                restore_threads.append(thread)
                thread.start()
            for thread in restore_threads:
                thread.join()
        restore_report = {
            "Size": format_size(sum(self.node_backup_size.values()) / len(self.node_backup_size.values())),
            "Time": int(restore_timer.duration.total_seconds()),
        }
        self.report_to_argus(ManagerReportType.BACKUP, restore_report, label)
        self.set_balancing(True)

    def set_balancing(self, balancing: bool):
        for node in self.db_cluster.nodes:
            balancing_cmd = build_node_api_command(
                f'/storage_service/tablets/balancing?enabled={"true" if balancing else "false"}', RequestMethods.POST)
            node.remoter.run(balancing_cmd)

    # actual tests
    def test_just_native_backup_restore(self):
        self.log.info("Executing test_backup_restore_benchmark...")

        for node in self.db_cluster.nodes:
            prepare_server_for_s3(node)

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.log.info("Create and report backup time")

        self.backup_and_report("Native backup")

        self.db_cluster.nodes[0].run_cqlsh('TRUNCATE keyspace1.standard1')
        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'disabled'};")

        self.restore_and_report("Native restore")

    def test_just_backup_restore(self):

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)
        backup_task = self.create_backup_and_report(mgr_cluster, "`rclone` based backup")

        self.db_cluster.nodes[0].run_cqlsh('TRUNCATE keyspace1.standard1')

        self.set_balancing(False)

        self.create_restore_and_report(mgr_cluster, backup_task.get_snapshot_tag(), "`rclone` based restore")

    def test_rclone_backup_restore(self):

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()
        self.set_balancing(False)
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = self.ensure_and_get_cluster(manager_tool)
        backup_task = self.create_backup_and_report(mgr_cluster, "`rclone` based backup")

        self.run_read_stress_and_report(" w/o concurrent backup")

        backup_thread = threading.Thread(target=self.create_backup_and_report,
                                         kwargs={"mgr_cluster": mgr_cluster,
                                                 "label": "`rclone` based backup during R/W stress"})

        read_stress_thread = threading.Thread(target=self.run_read_stress_and_report,
                                              kwargs={"label": " with concurrent `rclone` based backup"})
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

        self.db_cluster.nodes[0].run_cqlsh('TRUNCATE keyspace1.standard1')

        self.create_restore_and_report(mgr_cluster, backup_task.get_snapshot_tag(), "`rclone` based restore")

    def test_native_backup_restore(self):
        for node in self.db_cluster.nodes:
            prepare_server_for_s3(node)

        self.log.info("Write data to table")
        self.run_prepare_write_cmd()

        self.backup_and_report("Native backup")
        self.run_read_stress_and_report(" w/o concurrent native backup")

        self.node_backup_size = {}
        backup_thread = threading.Thread(target=self.backup_and_report,
                                         kwargs={"label": "Native backup during R/W stress"})

        read_stress_thread = threading.Thread(target=self.run_read_stress_and_report,
                                              kwargs={"label": " with concurrent native backup"})
        backup_thread.start()
        read_stress_thread.start()

        backup_thread.join()
        read_stress_thread.join()

        self.db_cluster.nodes[0].run_cqlsh('TRUNCATE keyspace1.standard1')
        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'disabled'};")

        self.restore_and_report("Native restore")

        self.db_cluster.nodes[0].run_cqlsh("ALTER TABLE keyspace1.standard1 WITH tombstone_gc = {'mode': 'repair'};")

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
# Copyright (c) 2021 ScyllaDB

from contextlib import nullcontext
from datetime import datetime

from sdcm import mgmt
from sdcm.mgmt import TaskStatus
from sdcm.mgmt.cli import ManagerTask
from sdcm.mgmt.common import (
    TaskRunDetails,
    create_cron_list_from_timedelta,
    get_manager_repo,
    get_task_run_details,
)
from sdcm.mgmt.operations import ManagerTestFunctionsMixIn
from sdcm.sct_events.group_common_events import ignore_aborted_snapshot_upload_storage_io_errors
from sdcm.tester import ClusterTester


class ManagerUpgradeTest(ManagerTestFunctionsMixIn, ClusterTester):
    """Test Scylla Manager upgrade scenarios with backward compatibility validation.

    This test suite validates that Scylla Manager can be upgraded from one version to another
    while maintaining full backward compatibility with existing operations, tasks, and data.

    Test Coverage:
        - Manager server and agent component upgrades
        - Cluster registration persistence across upgrades
        - Manager YAML configuration preservation (HTTP port, Prometheus port)
        - Task state preservation (repair and backup tasks)
        - Continuing stopped/paused tasks after upgrade
        - Restoring backups created with older Manager versions
        - Purging backups with retention policies across versions
        - Backup file integrity validation across versions
    """

    def get_email_data(self) -> dict:
        self.log.info("Prepare data for email")
        email_data = self._get_common_email_data()
        email_data.update(
            {
                "manager_server_repo": self.params.get("scylla_mgmt_address"),
                "manager_agent_repo": (
                    self.params.get("scylla_mgmt_agent_address") or self.params.get("scylla_mgmt_address")
                ),
                "target_manager_server_repo": self.params.get("target_scylla_mgmt_server_address"),
                "target_manager_agent_repo": self.params.get("target_scylla_mgmt_agent_address"),
            }
        )
        return email_data

    def _create_simple_table(self, table_name: str, keyspace_name: str = "ks1") -> None:
        with self.db_cluster.cql_connection_patient(self.db_node) as session:
            session.execute(
                f"""CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                    WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}}"""
            )
            session.execute(
                f"""CREATE TABLE IF NOT EXISTS {keyspace_name}.{table_name} (
                        key varchar PRIMARY KEY,
                        c1 text,
                        c2 text
                )"""
            )

    def _write_multiple_rows(self, table_name: str, key_range: tuple, keyspace_name: str = "ks1") -> None:
        with self.db_cluster.cql_connection_patient(self.db_node) as session:
            for num in range(*key_range):
                session.execute(
                    f"""INSERT INTO {keyspace_name}.{table_name} (key, c1, c2)
                        VALUES ('k_{num}', 'v_{num}', 'v_{num}')"""
                )

    @staticmethod
    def validate_pre_upgrade_task_details(task: ManagerTask, previous_task_details: TaskRunDetails) -> None:
        """Compares task details captured before upgrade with current details after upgrade.

        Args:
            task: The manager task object to validate
            previous_task_details: TaskRunDetails object captured before upgrade
        """
        current_task_details = get_task_run_details(task, wait=False)

        mismatches = []
        for detail_name, previous_value in previous_task_details.model_dump().items():
            current_value = getattr(current_task_details, detail_name)

            if isinstance(current_value, datetime):
                # Allow 60 second delta for datetime comparisons due to possible calculation imprecision
                delta_seconds = abs((current_value - previous_value).total_seconds())
                if delta_seconds > 60:
                    mismatches.append((detail_name, previous_value, current_value))
            elif current_value != previous_value:
                mismatches.append((detail_name, previous_value, current_value))

        if mismatches:
            error_details = "\n".join(f"{name}: from {prev} to {curr}" for name, prev, curr in mismatches)
            raise AssertionError(f"Task details of {task.id} changed:\n{error_details}")

    def test_upgrade(self) -> None:  # noqa: PLR0914
        target_upgrade_server_version = self.params.get("target_scylla_mgmt_server_address")
        target_upgrade_agent_version = self.params.get("target_scylla_mgmt_agent_address")
        target_manager_version = self.params.get("target_manager_version")
        if not target_upgrade_server_version:
            target_upgrade_server_version = get_manager_repo(target_manager_version, distro=self.manager_node.distro)
        if not target_upgrade_agent_version:
            target_upgrade_agent_version = get_manager_repo(target_manager_version, distro=self.db_node.distro)

        new_manager_http_port = 12345
        with self.manager_node.remote_manager_yaml() as scylla_manager_yaml:
            node_ip = scylla_manager_yaml["http"].split(":", maxsplit=1)[0]
            scylla_manager_yaml["http"] = f"{node_ip}:{new_manager_http_port}"
            scylla_manager_yaml["prometheus"] = f"{node_ip}:{self.params.get('manager_prometheus_port')}"
            pre_upgrade_manager_yaml = dict(scylla_manager_yaml)  # Create a copy for post-upgrade validation
            self.log.info("The new Scylla Manager yaml configuration: %s", pre_upgrade_manager_yaml)
        self.manager_node.restart_manager_server(port=new_manager_http_port)

        mgr_cluster = self.db_cluster.get_cluster_manager()
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.manager_node)
        current_manager_version = manager_tool.sctool.version

        with self.subTest("Generating load"):
            self.generate_load_and_wait_for_results()

        with self.subTest("Creating reoccurring backup and repair tasks"):
            repair_task = mgr_cluster.create_repair_task(cron=create_cron_list_from_timedelta(minutes=2))
            repair_task_current_details = get_task_run_details(repair_task)

            backup_task = mgr_cluster.create_backup_task(
                cron=create_cron_list_from_timedelta(minutes=2),
                location_list=self.locations,
                keyspace_list=["keyspace1"],
                method=self.backup_method,
            )
            backup_task_current_details = get_task_run_details(backup_task)
            backup_task_snapshot = backup_task.get_snapshot_tag()
            pre_upgrade_backup_task_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)

        with self.subTest("Creating a simple backup with the intention of purging it"):
            self._create_simple_table(table_name="cf1")
            self._write_multiple_rows(table_name="cf1", key_range=(1, 11))
            self._create_simple_table(table_name="cf2")
            self._write_multiple_rows(table_name="cf2", key_range=(1, 11))

            rerunning_backup_task = mgr_cluster.create_backup_task(
                location_list=self.locations,
                keyspace_list=["ks1"],
                retention=2,
                method=self.backup_method,
            )
            rerunning_backup_task.wait_and_get_final_status(timeout=300, step=20)
            assert rerunning_backup_task.status == TaskStatus.DONE, (
                f"Unknown failure in task {rerunning_backup_task.id}"
            )

        with self.subTest("Creating a backup task and stopping it"):
            self.generate_load_and_wait_for_results(keyspace_name="keyspace2")
            stopped_backup_task = mgr_cluster.create_backup_task(
                cron=create_cron_list_from_timedelta(minutes=1),
                location_list=self.locations,
                keyspace_list=["keyspace2"],
                method=self.backup_method,
            )
            stopped_backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=180, step=2)
            ctx = (
                ignore_aborted_snapshot_upload_storage_io_errors()
                if self.params.get("manager_backup_restore_method") == "native"
                else nullcontext()
            )
            with ctx:
                stopped_backup_task.stop()

        with self.subTest("Running Manager upgrade"):
            self.upgrade_scylla_manager(
                pre_upgrade_manager_version=current_manager_version,
                target_upgrade_server_version=target_upgrade_server_version,
                target_upgrade_agent_version=target_upgrade_agent_version,
            )

        with self.subTest("Make sure that the cluster is still added to the Manager"):
            assert manager_tool.get_cluster(cluster_name=self.db_cluster.scylla_manager_cluster_name)

        with self.subTest("Validating that Manager yaml configuration is preserved after upgrade"):
            with self.manager_node.remote_manager_yaml() as scylla_manager_yaml:
                post_upgrade_manager_yaml = dict(scylla_manager_yaml)
                assert pre_upgrade_manager_yaml == post_upgrade_manager_yaml, (
                    f"Manager yaml configuration changed after upgrade:\n"
                    f"pre-upgrade: {pre_upgrade_manager_yaml}\n"
                    f"post-upgrade: {post_upgrade_manager_yaml}"
                )

        with self.subTest("Checking that the details of the repair that was created before the upgrade didn't change"):
            self.validate_pre_upgrade_task_details(task=repair_task, previous_task_details=repair_task_current_details)

        with self.subTest("Checking that the details of the backup that was created before the upgrade didn't change"):
            self.validate_pre_upgrade_task_details(task=backup_task, previous_task_details=backup_task_current_details)

        with self.subTest("Continuing a older version stopped backup task with newer version Manager"):
            stopped_backup_task.start()
            stopped_backup_task.wait_and_get_final_status(timeout=1200, step=20)
            assert stopped_backup_task.status == TaskStatus.DONE, (
                f"Task {stopped_backup_task.id} failed to continue after Manager upgrade"
            )

        with self.subTest("Restoring an older version backup task with newer version of Manager"):
            self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
            self.run_verification_read_stress()

        with self.subTest("Restoring an older version backup task with newer version of Manager, using a restore task"):
            self.verify_backup_success(
                mgr_cluster=mgr_cluster, backup_task=backup_task, restore_data_with_task=True, timeout=600
            )
            self.run_verification_read_stress()

        with self.subTest(
            "Executing the 'backup list' and 'backup files' commands on a older version backup"
            " with newer version of Manager"
        ):
            current_backup_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)
            assert pre_upgrade_backup_task_files == current_backup_files, (
                f"Backup task of the task {backup_task.id} is not identical after the Manager upgrade:"
                f"\nbefore the upgrade:\n{pre_upgrade_backup_task_files}\nafter the upgrade:\n{current_backup_files}"
            )
            mgr_cluster.sctool.run(cmd=f"backup list -c {mgr_cluster.id}", is_verify_errorless_result=True)

        with self.subTest("Purging a older version backup"):
            table_ks_name = "ks1"
            table_to_delete = "cf1"

            self.log.debug("Dropping one table")
            with self.db_cluster.cql_connection_patient(self.db_node) as session:
                session.execute(f"DROP TABLE {table_ks_name}.{table_to_delete} ;")

            for i in range(2, 4):
                self.log.debug("Rerunning the backup task for the %s time", i)
                rerunning_backup_task.start(continue_task=False)
                rerunning_backup_task.wait_and_get_final_status(step=5)
                assert rerunning_backup_task.status == TaskStatus.DONE, (
                    f"Backup {rerunning_backup_task.id} that was rerun again from the start has failed to reach "
                    f"status DONE within expected time limit"
                )
            per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(
                snapshot_tag=rerunning_backup_task.get_snapshot_tag()
            )
            for node in self.db_cluster.nodes:
                node_id = node.host_id
                assert table_to_delete not in per_node_backup_file_paths[node_id][table_ks_name], (
                    "The missing table is still in s3, even though it should have been purged"
                )

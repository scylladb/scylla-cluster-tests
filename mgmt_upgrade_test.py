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

import logging
from time import sleep
from datetime import datetime, timedelta

from sdcm.tester import ClusterTester
from sdcm.mgmt import get_scylla_manager_tool, TaskStatus
from sdcm.mgmt.cli import RepairTask
from sdcm.mgmt.common import get_manager_repo_from_defaults, create_cron_list_from_timedelta
from mgmt_cli_test import BackupFunctionsMixIn


LOGGER = logging.getLogger(__name__)


class ManagerUpgradeTest(BackupFunctionsMixIn, ClusterTester):
    CLUSTER_NAME = "cluster_under_test"

    def create_simple_table(self, table_name, keyspace_name="ks1"):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"""CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': '3'}};""")
            session.execute(f"""CREATE table IF NOT EXISTS {keyspace_name}.{table_name} (
                                key varchar PRIMARY KEY,
                                c1 text,
                                c2 text);""")

    def write_multiple_rows(self, table_name, key_range, keyspace_name="ks1"):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for num in range(*key_range):
                session.execute(f"insert into {keyspace_name}.{table_name} (key, c1, c2) "
                                f"VALUES ('k_{num}', 'v_{num}', 'v_{num}');")

    def _create_and_add_cluster(self):
        manager_node = self.monitors.nodes[0]
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        self.update_all_agent_config_files()
        current_manager_version = manager_tool.sctool.version
        mgr_cluster = manager_tool.add_cluster(name=ManagerUpgradeTest.CLUSTER_NAME, db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        return mgr_cluster, current_manager_version

    def test_upgrade(self):  # pylint: disable=too-many-locals,too-many-statements
        manager_node = self.monitors.nodes[0]

        target_upgrade_server_version = self.params.get('target_scylla_mgmt_server_address')
        target_upgrade_agent_version = self.params.get('target_scylla_mgmt_agent_address')
        target_manager_version = self.params.get('target_manager_version')
        if not target_upgrade_server_version:
            target_upgrade_server_version = get_manager_repo_from_defaults(target_manager_version,
                                                                           distro=manager_node.distro)
        if not target_upgrade_agent_version:
            target_upgrade_agent_version = get_manager_repo_from_defaults(target_manager_version,
                                                                          distro=self.db_cluster.nodes[0].distro)

        new_manager_http_port = 12345
        with manager_node.remote_manager_yaml() as scylla_manager_yaml:
            node_ip = scylla_manager_yaml["http"].split(":", maxsplit=1)[0]
            scylla_manager_yaml["http"] = f"{node_ip}:{new_manager_http_port}"
            scylla_manager_yaml["prometheus"] = f"{node_ip}:{self.params.get('manager_prometheus_port')}"
            LOGGER.info("The new Scylla Manager is:\n{}".format(scylla_manager_yaml))
        manager_node.restart_manager_server(port=new_manager_http_port)
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        manager_tool.add_cluster(name="cluster_under_test", db_cluster=self.db_cluster,
                                 auth_token=self.monitors.mgmt_auth_token)
        current_manager_version = manager_tool.sctool.version

        LOGGER.debug("Generating load")
        self.generate_load_and_wait_for_results()

        mgr_cluster = manager_tool.get_cluster(cluster_name="cluster_under_test")

        with self.subTest("Creating reoccurring backup and repair tasks"):

            repair_task = mgr_cluster.create_repair_task(cron=create_cron_list_from_timedelta(minutes=2))
            repair_task_current_details = wait_until_task_finishes_return_details(repair_task)

            backup_task = mgr_cluster.create_backup_task(cron=create_cron_list_from_timedelta(minutes=2),
                                                         location_list=self.locations, keyspace_list=["keyspace1"])
            backup_task_current_details = wait_until_task_finishes_return_details(backup_task)
            backup_task_snapshot = backup_task.get_snapshot_tag()
            pre_upgrade_backup_task_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)

        with self.subTest("Creating a simple backup with the intention of purging it"):
            self.create_simple_table(table_name="cf1")
            self.write_multiple_rows(table_name="cf1", key_range=(1, 11))
            self.create_simple_table(table_name="cf2")
            self.write_multiple_rows(table_name="cf2", key_range=(1, 11))

            rerunning_backup_task = \
                mgr_cluster.create_backup_task(location_list=self.locations, keyspace_list=["ks1"], retention=2)
            rerunning_backup_task.wait_and_get_final_status(timeout=300, step=20)
            assert rerunning_backup_task.status == TaskStatus.DONE, \
                f"Unknown failure in task {rerunning_backup_task.id}"

        with self.subTest("Creating a backup task and stopping it"):
            self.generate_load_and_wait_for_results(keyspace_name="keyspace2")
            legacy_args = "--force" if manager_tool.sctool.client_version.startswith("2.1") else None
            pausable_backup_task = mgr_cluster.create_backup_task(
                cron=create_cron_list_from_timedelta(minutes=1),
                location_list=self.locations,
                keyspace_list=["keyspace2"],
                legacy_args=legacy_args,
            )
            pausable_backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=180, step=2)
            pausable_backup_task.stop()

        upgrade_scylla_manager(pre_upgrade_manager_version=current_manager_version,
                               target_upgrade_server_version=target_upgrade_server_version,
                               target_upgrade_agent_version=target_upgrade_agent_version,
                               manager_node=manager_node,
                               db_cluster=self.db_cluster)

        LOGGER.debug("Checking that the previously created tasks' details have not changed")
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        # make sure that the cluster is still added to the manager
        manager_tool.get_cluster(cluster_name="cluster_under_test")

        with self.subTest("Checking that the details of the repair that was created before the upgrade didn't change"):
            validate_previous_task_details(task=repair_task, previous_task_details=repair_task_current_details)

        with self.subTest("Checking that the details of the backup that was created before the upgrade didn't change"):
            validate_previous_task_details(task=backup_task, previous_task_details=backup_task_current_details)

        with self.subTest("Continuing a older version stopped backup task with newer version manager"):
            pausable_backup_task.start()
            pausable_backup_task.wait_and_get_final_status(timeout=1200, step=20)
            assert pausable_backup_task.status == TaskStatus.DONE, \
                f"task {pausable_backup_task.id} failed to continue after manager upgrade"

        with self.subTest("Restoring an older version backup task with newer version manager"):
            self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
            self.run_verification_read_stress()

        with self.subTest("Restoring an older version backup task with newer version manager, using a restore task"):
            self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task,
                                       restore_data_with_task=True, timeout=600)
            self.run_verification_read_stress()

        with self.subTest("Executing the 'backup list' and 'backup files' commands on a older version backup"
                          " with newer version manager"):
            current_backup_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)
            assert pre_upgrade_backup_task_files == current_backup_files, \
                f"Backup task of the task {backup_task.id} is not identical after the manager upgrade:" \
                f"\nbefore the upgrade:\n{pre_upgrade_backup_task_files}\nafter the upgrade:\n{current_backup_files}"
            mgr_cluster.sctool.run(cmd=f" backup list -c {mgr_cluster.id}", is_verify_errorless_result=True)

        with self.subTest("purging a older version backup"):
            # Dropping one table
            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
                session.execute("DROP TABLE ks1.cf1 ;")

            for i in range(2, 4):
                LOGGER.debug("rerunning the backup task for the %s time", i)
                rerunning_backup_task.start(continue_task=False)
                rerunning_backup_task.wait_and_get_final_status(step=5)
                assert rerunning_backup_task.status == TaskStatus.DONE, \
                    f"backup {rerunning_backup_task.id} that was rerun again from the start has failed to reach " \
                    f"status DONE within expected time limit"
            per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(
                snapshot_tag=rerunning_backup_task.get_snapshot_tag())
            for node in self.db_cluster.nodes:
                node_id = node.host_id
                # making sure that the files of the missing table isn't in s3
                assert "cf1" not in per_node_backup_file_paths[node_id]["ks1"], \
                    "The missing table is still in s3, even though it should have been purged"

    def update_all_agent_config_files(self):
        region_name = self.params.get("backup_bucket_region") or self.params.get("region_name").split()[0]
        for node in self.db_cluster.nodes:
            node.update_manager_agent_config(region=region_name)
        sleep(60)

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"manager_server_repo": self.params.get("scylla_mgmt_address"),
                           "manager_agent_repo": (self.params.get("scylla_mgmt_agent_address") or
                                                  self.params.get("scylla_mgmt_address")),
                           "target_manager_server_repo": self.params.get('target_scylla_mgmt_server_address'),
                           "target_manager_agent_repo": self.params.get('target_scylla_mgmt_agent_address')})

        return email_data


def wait_until_task_finishes_return_details(task, wait=True, timeout=1000, step=10):
    if wait:
        task.wait_and_get_final_status(timeout=timeout, step=step)
    task_history = task.history
    latest_run_id = task.latest_run_id
    start_time = task.sctool.get_table_value(parsed_table=task_history, column_name="start time",
                                             identifier=latest_run_id)
    duration = task.sctool.get_table_value(parsed_table=task_history, column_name="duration",
                                           identifier=latest_run_id)
    next_run_time = task.next_run
    if next_run_time.startswith("in "):
        next_run_time = time_diff_string_to_datetime(time_diff_string=next_run_time)  # in 23h59m49s
    else:
        next_run_time = time_string_to_datetime(next_run_time)  # 14 Jun 23 15:41:00 UTC
    if task.sctool.is_v3_cli:
        end_time = time_diff_string_to_datetime(time_diff_string=duration, base_time_string=start_time)
    else:
        end_time_string = task.sctool.get_table_value(parsed_table=task_history, column_name="end time",
                                                      identifier=latest_run_id)
        end_time = time_string_to_datetime(end_time_string)
    task_details = {"next run": next_run_time,
                    "latest run id": latest_run_id,
                    "start time": start_time,
                    "end time": end_time,
                    "duration": duration}
    return task_details


def time_diff_string_to_datetime(time_diff_string, base_time_string=None):
    if " " in time_diff_string:
        time_diff_string = time_diff_string[time_diff_string.find(" ") + 1:]
    days, hours, minutes, seconds = 0, 0, 0, 0
    if "d" in time_diff_string:
        days = int(time_diff_string[:time_diff_string.find("d")])
        time_diff_string = time_diff_string[time_diff_string.find("d") + 1:]
    if "h" in time_diff_string:
        hours = int(time_diff_string[:time_diff_string.find("h")])
        time_diff_string = time_diff_string[time_diff_string.find("h") + 1:]
    if "m" in time_diff_string:
        minutes = int(time_diff_string[:time_diff_string.find("m")])
        time_diff_string = time_diff_string[time_diff_string.find("m") + 1:]
    if "s" in time_diff_string:
        seconds = int(time_diff_string[:time_diff_string.find("s")])
    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if base_time_string is None:
        base_time = datetime.utcnow()
    else:
        base_time = datetime.strptime(base_time_string, "%d %b %y %H:%M:%S %Z")
    return base_time + delta


def time_string_to_datetime(time_string):
    if " (" in time_string:
        time_string = time_string[:time_string.find(" (")]
    datetime_object = datetime.strptime(time_string, "%d %b %y %H:%M:%S %Z")
    return datetime_object


def _create_mismatched_details_error_message(previous_task_details, current_task_details, mismatched_details_name_list):
    error_message_format = "\n{}: from {} to {}"
    complete_error_description = ""
    for name in mismatched_details_name_list:
        complete_error_description += error_message_format.format(name,
                                                                  previous_task_details[name],
                                                                  current_task_details[name])
    return complete_error_description


def validate_previous_task_details(task, previous_task_details):
    """
    Compares the details of the task next run and history to the previously extracted next run and history
    """
    mismatched_details_name_list = []
    current_task_details = wait_until_task_finishes_return_details(task, wait=False)
    for detail_name, current_value in current_task_details.items():
        if isinstance(current_value, datetime):
            delta = current_value - previous_task_details[detail_name]
            # I check that the time delta is smaller than 60 seconds since we calculate the next run time on our own,
            # and as a result it could be a BIT imprecise
            if abs(delta.total_seconds()) > 60:
                mismatched_details_name_list.append(detail_name)
        else:
            if current_value != previous_task_details[detail_name]:
                mismatched_details_name_list.append(detail_name)
    complete_error_description = _create_mismatched_details_error_message(previous_task_details,
                                                                          current_task_details,
                                                                          mismatched_details_name_list)
    assert not mismatched_details_name_list, f"Task details of {task.id} changed:{complete_error_description}"


def upgrade_scylla_manager(
        pre_upgrade_manager_version,
        target_upgrade_server_version,
        target_upgrade_agent_version,
        manager_node,
        db_cluster):
    LOGGER.debug("Stopping manager server")
    if manager_node.is_docker():
        manager_node.remoter.sudo('supervisorctl stop scylla-manager')
    else:
        manager_node.remoter.sudo("systemctl stop scylla-manager")

    LOGGER.debug("Stopping manager agents")
    for node in db_cluster.nodes:
        node.remoter.sudo("systemctl stop scylla-manager-agent")

    LOGGER.debug("Upgrading manager server")
    manager_node.upgrade_mgmt(target_upgrade_server_version, start_manager_after_upgrade=False)

    LOGGER.debug("Upgrading and starting manager agents")
    for node in db_cluster.nodes:
        node.upgrade_manager_agent(target_upgrade_agent_version)

    LOGGER.debug("Starting manager server")
    if manager_node.is_docker():
        manager_node.remoter.sudo('supervisorctl start scylla-manager')
    else:
        manager_node.remoter.sudo("systemctl start scylla-manager")
    time_to_sleep = 30
    LOGGER.debug("Sleep %s seconds, waiting for manager service ready to respond", time_to_sleep)
    sleep(time_to_sleep)

    LOGGER.debug("Comparing the new manager versions")
    manager_tool = get_scylla_manager_tool(manager_node=manager_node)
    new_manager_version = manager_tool.sctool.version
    assert new_manager_version != pre_upgrade_manager_version, "Manager failed to upgrade - " \
                                                               "previous and new versions are the same. Test failed!"


def _create_stopped_repair_task_with_manual_argument(mgr_cluster, arg_string):
    res = mgr_cluster.sctool.run(cmd=f"repair -c {mgr_cluster.id} {arg_string}",
                                     parse_table_res=False)
    assert not res.stderr, f"Task creation failed: {res.stderr}"
    task_id = res.stdout.split('\n')[0]
    repair_task = RepairTask(task_id=task_id, cluster_id=mgr_cluster.id,
                             manager_node=mgr_cluster.manager_node)
    repair_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=500, step=10)
    repair_task.stop()
    return repair_task

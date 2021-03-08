import logging
from time import sleep

from sdcm.tester import ClusterTester
from sdcm.mgmt import get_scylla_manager_tool, TaskStatus
from sdcm.mgmt.cli import update_config_file, SCYLLA_MANAGER_AGENT_YAML_PATH, RepairTask
from mgmt_cli_test import BackupFunctionsMixIn


LOGGER = logging.getLogger(__name__)


class ManagerUpgradeTest(BackupFunctionsMixIn, ClusterTester):
    DESTINATION = '/tmp/backup'
    CLUSTER_NAME = "cluster_under_test"

    def create_simple_table(self, table_name, keyspace_name="ks1"):
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"""CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}};""")
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
        current_manager_version = manager_tool.version
        mgr_cluster = manager_tool.add_cluster(name=ManagerUpgradeTest.CLUSTER_NAME, db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        return mgr_cluster, current_manager_version

    def test_upgrade(self):  # pylint: disable=too-many-locals,too-many-statements
        target_upgrade_server_version = self.params.get('target_scylla_mgmt_server_repo')
        target_upgrade_agent_version = self.params.get('target_scylla_mgmt_agent_repo')
        manager_node = self.monitors.nodes[0]

        new_manager_http_port = 12345
        with manager_node.remote_manager_yaml() as scylla_manager_yaml:
            node_ip = scylla_manager_yaml["http"].split(":", maxsplit=1)[0]
            scylla_manager_yaml["http"] = f"{node_ip}:{new_manager_http_port}"
            scylla_manager_yaml["prometheus"] = f"{node_ip}:{self.params['manager_prometheus_port']}"
            LOGGER.info("The new Scylla Manager is:\n{}".format(scylla_manager_yaml))
        manager_node.remoter.sudo("systemctl restart scylla-manager")
        manager_node.wait_manager_server_up(port=new_manager_http_port)
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        manager_tool.add_cluster(name="cluster_under_test", db_cluster=self.db_cluster,
                                 auth_token=self.monitors.mgmt_auth_token)
        current_manager_version = manager_tool.version

        LOGGER.debug("Generating load")
        self.generate_load_and_wait_for_results()

        mgr_cluster = manager_tool.get_cluster(cluster_name="cluster_under_test")

        with self.subTest("Creating reoccurring backup and repair tasks"):

            repair_task = mgr_cluster.create_repair_task(interval="1d")
            repair_task_current_details = wait_until_task_finishes_return_details(repair_task)

            if not self.is_cred_file_configured:
                self.update_config_file()
            location_list = [self.bucket_name, ]
            backup_task = mgr_cluster.create_backup_task(interval="1d", location_list=location_list,
                                                         keyspace_list=["keyspace1"])
            backup_task_current_details = wait_until_task_finishes_return_details(backup_task)
            backup_task_snapshot = backup_task.get_snapshot_tag()
            pre_upgrade_backup_task_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)

        with self.subTest("Creating a backup task and stopping it"):
            legacy_args = "--force" if manager_tool.client_version.startswith("2.1") else None
            pausable_backup_task = mgr_cluster.create_backup_task(interval="1d", location_list=location_list,
                                                                  keyspace_list=["system_*"], legacy_args=legacy_args)
            pausable_backup_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=180, step=2)
            pausable_backup_task.stop()

        with self.subTest("Creating a simple backup with the intention of purging it"):
            self.create_simple_table(table_name="cf1")
            self.write_multiple_rows(table_name="cf1", key_range=(1, 11))
            self.create_simple_table(table_name="cf2")
            self.write_multiple_rows(table_name="cf2", key_range=(1, 11))

            if not self.is_cred_file_configured:
                self.update_config_file()
            location_list = [self.bucket_name, ]
            rerunning_backup_task = mgr_cluster.create_backup_task(location_list=location_list, keyspace_list=["ks1"],
                                                                   retention=2)
            rerunning_backup_task.wait_and_get_final_status(timeout=300, step=20)
            assert rerunning_backup_task.status == TaskStatus.DONE, \
                f"Unknown failure in task {rerunning_backup_task.id}"

        upgrade_scylla_manager(pre_upgrade_manager_version=current_manager_version,
                               target_upgrade_server_version=target_upgrade_server_version,
                               target_upgrade_agent_version=target_upgrade_agent_version,
                               manager_node=manager_node,
                               db_cluster=self.db_cluster)

        LOGGER.debug("Checking that the previously created tasks' details have not changed")
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        # make sure that the cluster is still added to the manager
        manager_tool.get_cluster(cluster_name="cluster_under_test")
        validate_previous_task_details(task=repair_task, previous_task_details=repair_task_current_details)
        validate_previous_task_details(task=backup_task, previous_task_details=backup_task_current_details)

        with self.subTest("Restoring a 2.0 backup task with 2.1 manager"):
            self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)

        with self.subTest("Continuing a 2.0 stopped backup task with 2.1 manager"):
            pausable_backup_task.start()
            pausable_backup_task.wait_and_get_final_status(timeout=1200, step=20)
            assert pausable_backup_task.status == TaskStatus.DONE, \
                f"task {pausable_backup_task.id} failed to continue after manager upgrade"

        with self.subTest("Executing the 'backup list' and 'backup files' commands on a 2.0 backup with 2.1 manager"):
            current_backup_files = mgr_cluster.get_backup_files_dict(backup_task_snapshot)
            assert pre_upgrade_backup_task_files == current_backup_files,\
                f"Backup task of the task {backup_task.id} is not identical after the manager upgrade:" \
                f"\nbefore the upgrade:\n{pre_upgrade_backup_task_files}\nafter the upgrade:\n{current_backup_files}"
            mgr_cluster.sctool.run(cmd=f" backup list -c {mgr_cluster.id}", is_verify_errorless_result=True)

        with self.subTest("purging a 2.0 backup"):
            # Dropping one table
            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
                session.execute("DROP TABLE ks1.cf1 ;")

            for i in range(2, 4):
                LOGGER.debug(f"rerunning the backup task for the {i} time")
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
        region_name = self.params.get('region_name').split()[0]
        for node in self.db_cluster.nodes:
            update_config_file(node=node, region=region_name, config_file=SCYLLA_MANAGER_AGENT_YAML_PATH)
        sleep(60)

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"manager_server_repo": self.params.get("scylla_mgmt_repo"),
                           "manager_agent_repo": (self.params.get("scylla_mgmt_agent_repo") or
                                                  self.params.get("scylla_mgmt_repo")),
                           "target_manager_server_repo": self.params.get('target_scylla_mgmt_server_repo'),
                           "target_manager_agent_repo": self.params.get('target_scylla_mgmt_agent_repo')})

        return email_data


def wait_until_task_finishes_return_details(task, wait=True, timeout=1000, step=10):
    if wait:
        task.wait_and_get_final_status(timeout=timeout, step=step)
    task_history = task.history
    latest_run_id = task.latest_run_id
    task_details = {"next run": task.next_run,
                    "latest run id": latest_run_id,
                    "start time": task.sctool.get_table_value(parsed_table=task_history, column_name="start time",
                                                              identifier=latest_run_id),
                    "end time": task.sctool.get_table_value(parsed_table=task_history, column_name="end time",
                                                            identifier=latest_run_id),
                    "duration": task.sctool.get_table_value(parsed_table=task_history, column_name="duration",
                                                            identifier=latest_run_id)}
    return task_details


def validate_previous_task_details(task, previous_task_details):
    """
    Compares the details of the task next run and history to the previously extracted next run and history
    """
    current_task_details = wait_until_task_finishes_return_details(task, wait=False)
    for detail_name in current_task_details:
        assert current_task_details[detail_name] == previous_task_details[detail_name],\
            f"previous task {detail_name} is not identical to the current history"


def upgrade_scylla_manager(pre_upgrade_manager_version, target_upgrade_server_version, target_upgrade_agent_version,
                           manager_node, db_cluster):
    LOGGER.debug("Stopping manager server")
    if manager_node.is_docker():
        manager_node.remoter.run('sudo supervisorctl stop scylla-manager')
    else:
        manager_node.remoter.run("sudo systemctl stop scylla-manager")

    LOGGER.debug("Stopping manager agents")
    for node in db_cluster.nodes:
        node.remoter.run("sudo systemctl stop scylla-manager-agent")

    LOGGER.debug("Upgrading manager server")
    manager_node.upgrade_mgmt(target_upgrade_server_version, start_manager_after_upgrade=False)

    LOGGER.debug("Upgrading and starting manager agents")
    for node in db_cluster.nodes:
        node.upgrade_manager_agent(target_upgrade_agent_version)

    LOGGER.debug("Starting manager server")
    if manager_node.is_docker():
        manager_node.remoter.run('sudo supervisorctl start scylla-manager')
    else:
        manager_node.remoter.run("sudo systemctl start scylla-manager")
    time_to_sleep = 30
    LOGGER.debug('Sleep {} seconds, waiting for manager service ready to respond'.format(time_to_sleep))
    sleep(time_to_sleep)

    LOGGER.debug("Comparing the new manager versions")
    manager_tool = get_scylla_manager_tool(manager_node=manager_node)
    new_manager_version = manager_tool.version
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

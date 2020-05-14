import logging
from time import sleep

from sdcm.tester import ClusterTester
from sdcm.mgmt import get_scylla_manager_tool, update_config_file


LOGGER = logging.getLogger(__name__)


class ManagerUpgradeTest(ClusterTester):
    def test_upgrade(self):
        target_upgrade_version = self.params.get('new_scylla_mgmt_repo')
        manager_node = self.monitors.nodes[0]
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        manager_tool.add_cluster(name="cluster_under_test", db_cluster=self.db_cluster,
                                 auth_token=self.monitors.mgmt_auth_token)
        current_manager_version = manager_tool.version

        LOGGER.debug("Creating reoccurring backup and repair tasks")

        mgr_cluster = manager_tool.get_cluster(cluster_name="cluster_under_test")
        repair_task = mgr_cluster.create_repair_task(interval="1d")
        repair_task_current_details = wait_until_task_finishes_return_details(repair_task)

        self.update_all_agent_config_files()
        bucket_name = self.params.get('backup_bucket_location').split()[0]
        location_list = [f's3:{bucket_name}']
        backup_task = mgr_cluster.create_backup_task(interval="1d", location_list=location_list,
                                                     keyspace_list=["system_auth"])
        backup_task_current_details = wait_until_task_finishes_return_details(backup_task)

        upgrade_scylla_manager(pre_upgrade_manager_version=current_manager_version,
                               target_upgrade_version=target_upgrade_version,
                               manager_node=manager_node,
                               db_cluster=self.db_cluster)

        LOGGER.debug("Checking that the previously created tasks' details have not changed")
        manager_tool = get_scylla_manager_tool(manager_node=manager_node)
        # make sure that the cluster is still added to the manager
        manager_tool.get_cluster(cluster_name="cluster_under_test")
        validate_previous_task_details(task=repair_task, previous_task_details=repair_task_current_details)
        validate_previous_task_details(task=backup_task, previous_task_details=backup_task_current_details)

    def update_all_agent_config_files(self):
        config_file = '/etc/scylla-manager-agent/scylla-manager-agent.yaml'
        region_name = self.params.get('region_name').split()[0]
        for node in self.db_cluster.nodes:
            update_config_file(node=node, region=region_name, config_file=config_file)
        sleep(60)


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


def upgrade_scylla_manager(pre_upgrade_manager_version, target_upgrade_version, manager_node, db_cluster):
    LOGGER.debug("Stopping manager server")
    if manager_node.is_docker():
        manager_node.remoter.run('sudo supervisorctl stop scylla-manager')
    else:
        manager_node.remoter.run("sudo systemctl stop scylla-manager")

    LOGGER.debug("Stopping manager agents")
    for node in db_cluster.nodes:
        node.remoter.run("sudo systemctl stop scylla-manager-agent")

    LOGGER.debug("Upgrading manager server")
    manager_node.upgrade_mgmt(target_upgrade_version, start_manager_after_upgrade=False)

    LOGGER.debug("Upgrading and starting manager agents")
    for node in db_cluster.nodes:
        node.upgrade_manager_agent(target_upgrade_version)

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

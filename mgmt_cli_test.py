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

import os
import time
from random import randint
from invoke import exceptions

from sdcm import mgmt
from sdcm.mgmt import HostStatus, HostSsl, HostRestStatus, TaskStatus, ScyllaManagerError
from sdcm.nemesis import MgmtRepair
from sdcm.tester import ClusterTester
from sdcm.cluster import Setup


class MgmtCliTest(ClusterTester):
    """
    Test Scylla Manager operations on Scylla cluster.

    :avocado: enable
    """
    CLUSTER_NAME = "mgr_cluster1"
    LOCALSTRATEGY_KEYSPACE_NAME = "localstrategy_keyspace"
    SIMPLESTRATEGY_KEYSPACE_NAME = "simplestrategy_keyspace"

    def test_mgmt_repair_nemesis(self):
        """

            Test steps:
            1) Run cassandra stress on cluster.
            2) Add cluster to Manager and run full repair via Nemesis
        """
        self._generate_load()
        self.log.debug("test_mgmt_cli: initialize MgmtRepair nemesis")
        mgmt_nemesis = MgmtRepair(tester_obj=self, termination_event=self.db_cluster.termination_event)
        mgmt_nemesis.disrupt()

    def test_mgmt_cluster_crud(self):
        """

        Test steps:
        1) add a cluster to manager.
        2) update the cluster attributes in manager: name/host/ssh-user
        3) delete the cluster from manager and re-add again.
        """

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        hosts = self._get_cluster_hosts_ip()
        selected_host = hosts[0]
        cluster_name = 'mgr_cluster_crud'
        mgr_cluster = manager_tool.add_cluster(name=cluster_name, host=selected_host,
                                               auth_token=self.monitors.mgmt_auth_token)

        # Test cluster attributes
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"

        mgr_cluster.delete()
        manager_tool.add_cluster(name=cluster_name, host=selected_host, auth_token=self.monitors.mgmt_auth_token)

    def _get_cluster_hosts_ip(self):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips()]

    def _get_cluster_hosts_with_ips(self):
        ip_addr_attr = 'public_ip_address' if self.params.get('cluster_backend') != 'gce' and \
            Setup.INTRA_NODE_COMM_PUBLIC else 'private_ip_address'
        return [[node, getattr(node, ip_addr_attr)] for node in self.db_cluster.nodes]

    def get_all_dcs_names(self):
        dcs_names = set()
        for node in self.db_cluster.nodes:
            data_center = self.db_cluster.get_nodetool_info(node)['Data Center']
            dcs_names.add(data_center)
            node.region = data_center
        return dcs_names

    def _create_keyspace_and_basic_table(self, keyspace_name, strategy, table_name="example_table"):
        self.log.info("creating keyspace {}".format(keyspace_name))
        keyspace_existence = self.create_keyspace(keyspace_name, 1, strategy)
        assert keyspace_existence, "keyspace creation failed"

        # Keyspaces without tables won't appear in the repair, so the must have one
        self.log.info("createing the table {} in the keyspace {}".format(table_name, keyspace_name))

        self.create_table(table_name, keyspace_name=keyspace_name)

    def run_cmd_with_retry(self, cmd, node, retries=10):
        for _ in range(retries):
            try:
                node.remoter.run(cmd)
            except exceptions.UnexpectedExit as ex:
                self.log.debug(f'cmd {cmd} failed with error {ex}, will retry')
            else:
                break

    def download_file_from_backup_repo(self, local, bucket, cluster_id):
        self.log.info('Will download files for each db machine')
        cmd = f'''sudo yum install -y epel-release
                  sudo yum install -y python-pip
                  sudo yum remove -y epel-release
                  sudo pip install awscli
                  sudo pip install boto3
                  mkdir -p {local}'''
        for node in self.db_cluster.nodes:
            self.run_cmd_with_retry(cmd=cmd, node=node)
            nodetool_info = self.db_cluster.get_nodetool_info(node)
            node_id = nodetool_info['ID']
            # FIXME: it will work only with 1 single bucket
            datacenter_name = nodetool_info['Data Center']
            source_path = f's3://{bucket}/backup/sst/cluster/{cluster_id}/dc/{datacenter_name}/node/{node_id}/keyspace/'
            download_cmd = f'aws s3 cp {source_path} {local} --recursive'
            node.remoter.run(download_cmd)

    def test_manager_sanity(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run test_mgmt_cluster test.
        3) test_mgmt_cluster_healthcheck
        4) test_client_encryption
        :return:
        """
        self.test_basic_backup()
        self.test_repair_multiple_keyspace_types()
        self.test_mgmt_cluster_crud()
        self.test_mgmt_cluster_healthcheck()
        self.test_client_encryption()

    def test_backup_feature(self):
        with self.subTest('Backup Multiple KS\' and Tables'):
            self.test_backup_multiple_ks_tables()
        with self.subTest('Backup to Location with path'):
            self.test_backup_location_with_path()
        with self.subTest('Test Backup Rate Limit'):
            self.test_backup_rate_limit()

    def update_cred_file(self):
        # FIXME: add to the nodes not in the same region as the bucket the bucket's region
        # this is a temporary fix, after https://github.com/scylladb/mermaid/issues/1456 is fixed, this is not necessary
        cred_file = '/etc/scylla-manager-agent/scylla-manager-agent.yaml'
        region = 'us-east-1'
        for node in self.db_cluster.nodes:
            node.remoter.run(f'sudo chmod o+w {cred_file}')
            node.remoter.run(f"sudo echo -e \"s3:\n   region: {region}\" >> {cred_file}")
            node.remoter.run('sudo systemctl restart scylla-manager-agent')

    def create_ks_and_tables(self, num_ks, num_table):
        table_name = []
        for keyspace in range(num_ks):
            self.create_keyspace(f'ks00{keyspace}', 1)
            for table in range(num_table):
                self.create_table(f'table00{table}', keyspace_name=f'ks00{keyspace}')
                table_name.append(f'ks00{keyspace}.table00{table}')
                # FIXME: improve the structure + data insertion
                # can use this function to populate tables better?
                # self.populate_data_parallel()
        return table_name

    # pylint: disable=too-many-arguments
    def verify_backup_success(self, bucket, cluster_id, keyspace_name='keyspace1', table_name='standard1',
                              local='/tmp/backup', truncate=True):
        # FIXME: it will only work with 1 single bucket
        self.download_file_from_backup_repo(local=local, bucket=bucket, cluster_id=cluster_id)
        node = self.db_cluster.nodes[0]
        path_to_tmp = f'{local}/{keyspace_name}/table/{table_name}'
        table_id = node.remoter.run(f'ls {path_to_tmp}').stdout.strip()
        path_to_upload = f'/var/lib/scylla/data/{keyspace_name}/{table_name}-{table_id}/upload'
        node.remoter.run(f'sudo cp {path_to_tmp}/{table_id}/* {path_to_upload}/.')
        node.remoter.run(f'sudo chown scylla:scylla -Rf {path_to_upload}')
        if truncate:
            node.run_cqlsh(f'TRUNCATE {keyspace_name}.{table_name}')
        node.run_nodetool(sub_cmd='refresh', args=f'-- {keyspace_name} {table_name}')
        limit = 10
        after_restore = node.run_cqlsh(f'SELECT * FROM {keyspace_name}.{table_name} LIMIT {limit}').stdout
        assert len(after_restore.strip().splitlines()[2:-2]) == limit

    def test_basic_backup(self):
        self.log.info('starting test_basic_backup')
        self.update_cred_file()
        location_list = ['s3:manager-backup-tests-us']
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_basic', db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self._generate_load()
        backup_task = mgr_cluster.create_backup_task({'location': location_list})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE])
        self.verify_backup_success(bucket=location_list[0].split(':')[1], cluster_id=backup_task.cluster_id)
        self.log.info('finishing test_basic_backup')

    def test_backup_multiple_ks_tables(self):
        self.log.info('starting test_backup_multiple_ks_tables')
        self.update_cred_file()
        location_list = ['s3:manager-backup-tests-us']
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_multiple-ks', db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        tables = self.create_ks_and_tables(10, 100)
        self.log.debug(f'tables list = {tables}')
        # TODO: insert data to those tables
        backup_task = mgr_cluster.create_backup_task({'location': location_list})
        backup_task.wait_for_status(list_status=[TaskStatus.DONE])
        self.verify_backup_success(bucket=location_list[0].split(':')[1], cluster_id=backup_task.cluster_id)
        self.log.info('finishing test_backup_multiple_ks_tables')

    def test_backup_location_with_path(self):
        self.log.info('starting test_backup_location_with_path')
        self.update_cred_file()
        location_list = ['s3:manager-backup-tests-us/path_testing/']
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_bucket_with_path',
                                               db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self._generate_load()
        try:
            mgr_cluster.create_backup_task({'location': location_list})
        except ScyllaManagerError as error:
            self.log.info(f'Expected to fail - error: {error}')
        self.log.info('finishing test_backup_location_with_path')

    def test_backup_rate_limit(self):
        self.log.info('starting test_backup_rate_limit')
        self.update_cred_file()
        location_list = ['s3:manager-backup-tests-us']
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_rate_limit', db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self._generate_load()
        rate_limit = ','.join([f'{dc}:{randint(1, 10)}' for dc in self.get_all_dcs_names()])
        self.log.info(f'rate limit will be {rate_limit}')
        backup_task = mgr_cluster.create_backup_task({'location': location_list, 'rate-limit': rate_limit})
        task_status = backup_task.wait_and_get_final_status()
        self.log.info(f'backup task finished with status {task_status}')
        self.verify_backup_success(bucket=location_list[0].split(':')[1], cluster_id=backup_task.cluster_id)
        self.log.info('finishing test_backup_rate_limit')

    def test_client_encryption(self):
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME+"_encryption", db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self._generate_load()
        repair_task = mgr_cluster.create_repair_task()
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.OFF, "Not all hosts ssl is 'OFF'"
        self.db_cluster.enable_client_encrypt()
        mgr_cluster.update(client_encrypt=True)
        repair_task.start(use_continue=True)
        sleep = 40
        self.log.debug('Sleep {} seconds, waiting for health-check task to run by schedule on first time'.format(sleep))
        time.sleep(sleep)

        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.ON, "Not all hosts ssl is 'ON'"
            assert host_health.status == HostStatus.UP, "Not all hosts status is 'UP'"

    def test_mgmt_cluster_healthcheck(self):

        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        selected_host_ip = self._get_cluster_hosts_ip()[0]
        cluster_name = 'mgr_cluster1'
        mgr_cluster = manager_tool.get_cluster(cluster_name=cluster_name) or manager_tool.add_cluster(
            name=cluster_name, db_cluster=self.db_cluster, auth_token=self.monitors.mgmt_auth_token)
        other_host, other_host_ip = [
            host_data for host_data in self._get_cluster_hosts_with_ips() if host_data[1] != selected_host_ip][0]

        sleep = 40
        self.log.debug('Sleep {} seconds, waiting for health-check task to run by schedule on first time'.format(sleep))
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
        self.log.debug('Sleep {} seconds, waiting for health-check task to run after node down'.format(sleep))
        time.sleep(sleep)

        dict_host_health = mgr_cluster.get_hosts_health()
        assert dict_host_health[other_host_ip].status == HostStatus.DOWN, "Host: {} status is not 'DOWN'".format(
            other_host_ip)
        assert dict_host_health[other_host_ip].rest_status == HostRestStatus.DOWN, "Host: {} REST status is not 'DOWN'".format(
            other_host_ip)

        other_host.start_scylla_server()

    def test_ssh_setup_script(self):
        new_user = "qa_user"
        new_user_identity_file = os.path.join(mgmt.MANAGER_IDENTITY_FILE_DIR, new_user)+".pem"
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        selected_host_ip = self._get_cluster_hosts_ip()[0]
        res_ssh_setup, _ssh = manager_tool.scylla_mgr_ssh_setup(
            node_ip=selected_host_ip, single_node=True, create_user=new_user)
        self.log.debug('res_ssh_setup: {}'.format(res_ssh_setup))
        new_user_login_message = "This account is currently not available"
        # sudo ssh -i /root/.ssh/qa_user.pem -q -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -L 59164:0.0.0.0:10000 qa_user@54.163.180.81
        new_user_login_cmd = "sudo ssh -i {} -q -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -L 59164:0.0.0.0:10000 {}@{}".format(
            new_user_identity_file, new_user, selected_host_ip)
        self.log.debug("new_user_login_cmd command is: {}".format(new_user_login_cmd))
        res_new_user_login_cmd = manager_tool.manager_node.remoter.run(new_user_login_cmd, ignore_status=True)
        self.log.debug("res_new_user_login_cmd is: {}".format(res_new_user_login_cmd))
        assert new_user_login_message in res_new_user_login_cmd.stdout, "unexpected login-returned-message: {} . (expected: {}) ".format(
            res_new_user_login_cmd.stdout, new_user_login_message)

        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME+"_ssh_setup", host=selected_host_ip,
                                               single_node=True, auth_token=self.monitors.mgmt_auth_token)
        # self.log.debug('mgr_cluster: {}'.format(mgr_cluster))
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            self.log.debug("host_health is: {}".format(host_health))

    def test_manager_upgrade(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run manager upgrade to new version of yaml: 'scylla_mgmt_upgrade_to_repo'. (the 'from' version is: 'scylla_mgmt_repo').
        """
        scylla_mgmt_upgrade_to_repo = self.params.get('scylla_mgmt_upgrade_to_repo')
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        selected_host = self._get_cluster_hosts_ip()[0]
        cluster_name = 'mgr_cluster1'
        mgr_cluster = manager_tool.get_cluster(cluster_name=cluster_name) or \
            manager_tool.add_cluster(name=cluster_name, host=selected_host,
                                     auth_token=self.monitors.mgmt_auth_token)
        self.log.info('Running some stress and repair before upgrade')
        self.test_mgmt_repair_nemesis()

        repair_task_list = mgr_cluster.repair_task_list

        manager_from_version = manager_tool.version
        manager_tool.upgrade(scylla_mgmt_upgrade_to_repo=scylla_mgmt_upgrade_to_repo)

        assert manager_from_version[0] != manager_tool.version[0], "Manager version not changed after upgrade."
        # verify all repair tasks exist
        for repair_task in repair_task_list:
            self.log.debug("{} status: {}".format(repair_task.id, repair_task.status))

        self.log.info('Running a new repair task after upgrade')
        repair_task = mgr_cluster.create_repair_task()
        self.log.debug("{} status: {}".format(repair_task.id, repair_task.status))

    def test_manager_rollback_upgrade(self):
        """
        Test steps:
        1) Run Upgrade test: scylla_mgmt_repo --> scylla_mgmt_upgrade_to_repo
        2) Run manager downgrade to pre-upgrade version as in yaml: 'scylla_mgmt_repo'.
        """
        self.test_manager_upgrade()
        scylla_mgmt_repo = self.params.get('scylla_mgmt_repo')
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        manager_from_version = manager_tool.version
        manager_tool.rollback_upgrade(scylla_mgmt_repo=scylla_mgmt_repo)
        assert manager_from_version[0] != manager_tool.version[0], "Manager version not changed after rollback."

    def _generate_load(self):
        self.log.info('Starting c-s write workload for 1m')
        stress_cmd = self.params.get('stress_cmd')
        self.run_stress_thread(stress_cmd=stress_cmd, duration=5)
        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)

    def test_repair_multiple_keyspace_types(self):  # pylint: disable=invalid-name
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        hosts = self._get_cluster_hosts_ip()
        selected_host = hosts[0]
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, host=selected_host,
                                        auth_token=self.monitors.mgmt_auth_token)
        self._create_keyspace_and_basic_table(self.SIMPLESTRATEGY_KEYSPACE_NAME, "SimpleStrategy")
        self._create_keyspace_and_basic_table(self.LOCALSTRATEGY_KEYSPACE_NAME, "LocalStrategy")

        repair_task = mgr_cluster.create_repair_task()
        task_final_status = repair_task.wait_and_get_final_status(timeout=7200)
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(repair_task.id,
                                                                                            str(repair_task.status))
        self.log.info('Task: {} is done.'.format(repair_task.id))
        self.log.debug("sctool version is : {}".format(manager_tool.version))

        expected_keyspaces_to_be_repaired = ["system_auth", "system_distributed", "system_traces",  # pylint: disable=invalid-name
                                             self.SIMPLESTRATEGY_KEYSPACE_NAME]
        repair_progress_table = repair_task.detailed_progress
        self.log.info("Looking in the repair output for all of the required keyspaces")
        for keyspace_name in expected_keyspaces_to_be_repaired:
            keyspace_repair_percentage = self._keyspace_value_in_progress_table(
                repair_task, repair_progress_table, keyspace_name)
            assert keyspace_repair_percentage is not None, \
                "The keyspace {} was not included in the repair!".format(keyspace_name)
            assert keyspace_repair_percentage == 100, \
                "The repair of the keyspace {} stopped at {}%".format(
                    keyspace_name, keyspace_repair_percentage)

        localstrategy_keyspace_percentage = self._keyspace_value_in_progress_table(   # pylint: disable=invalid-name
            repair_task, repair_progress_table, self.LOCALSTRATEGY_KEYSPACE_NAME)
        assert localstrategy_keyspace_percentage is None, \
            "The keyspace with the replication strategy of localstrategy was included in repair, when it shouldn't"
        self.log.info("the sctool repair commend was completed successfully")

    @staticmethod
    def _keyspace_value_in_progress_table(repair_task, repair_progress_table, keyspace_name):
        try:
            table_repair_progress = repair_task.sctool.get_table_value(repair_progress_table, keyspace_name)
            table_repair_percentage = float(table_repair_progress.replace('%', ''))
            return table_repair_percentage
        except ScyllaManagerError as err:
            assert "not found in" in str(err), "Unexpected error: {}".format(str(err))
            return None

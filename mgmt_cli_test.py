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
import re
import time
from avocado import main

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
        mgr_cluster = manager_tool.add_cluster(name=cluster_name, host=selected_host)

        # Test cluster attributes
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"

        origin_ssh_user = mgr_cluster.ssh_user
        origin_rsa_id = mgr_cluster.ssh_identity_file
        new_ssh_user = "centos"
        new_rsa_id = '/tmp/scylla-test'

        mgr_cluster.update(ssh_user=new_ssh_user, ssh_identity_file=new_rsa_id)
        assert mgr_cluster.ssh_user == new_ssh_user, "Cluster ssh-user wasn't changed after update command"

        mgr_cluster.update(ssh_user=origin_ssh_user, ssh_identity_file=origin_rsa_id)
        mgr_cluster.delete()
        mgr_cluster2 = manager_tool.add_cluster(name=cluster_name, host=selected_host)

    def _get_cluster_hosts_ip(self):
        return [node_data[1] for node_data in self._get_cluster_hosts_with_ips()]

    def _get_cluster_hosts_with_ips(self):
        ip_addr_attr = 'public_ip_address' if self.params.get('cluster_backend') != 'gce' and \
            Setup.MULTI_REGION else 'private_ip_address'
        return [[node, getattr(node, ip_addr_attr)] for node in self.db_cluster.nodes]

    def _create_keyspace_and_basic_table(self, keyspace_name, strategy, table_name="example_table"):
        self.log.info("creating keyspace {}".format(keyspace_name))
        keyspace_existence = self.create_keyspace(keyspace_name, 1, strategy)
        assert keyspace_existence, "keyspace creation failed"

        # Keyspaces without tables won't appear in the repair, so the must have one
        self.log.info("createing the table {} in the keyspace {}".format(table_name, keyspace_name))
        with self.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute("use {}".format(keyspace_name))
            result = self.create_cf(session, table_name)
            if result:
                self.log.info(result.response_future)

    def test_manager_sanity(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run test_mgmt_cluster test.
        3) test_mgmt_cluster_healthcheck
        4) test_client_encryption
        :return:
        """
        self.test_repair_multiple_keyspace_types()
        self.test_mgmt_cluster_crud()
        self.test_mgmt_cluster_healthcheck()
        self.test_client_encryption()

    def test_client_encryption(self):
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME+"_encryption", db_cluster=self.db_cluster)
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
        mgr_cluster = manager_tool.get_cluster(cluster_name=cluster_name) or manager_tool.add_cluster(name=cluster_name, db_cluster=self.db_cluster)
        other_host, other_host_ip = [host_data for host_data in self._get_cluster_hosts_with_ips() if host_data[1] != selected_host_ip][0]

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
        assert dict_host_health[other_host_ip].status == HostStatus.DOWN, "Host: {} status is not 'DOWN'".format(other_host_ip)
        assert dict_host_health[other_host_ip].rest_status == HostRestStatus.DOWN, "Host: {} REST status is not 'DOWN'".format(other_host_ip)

        other_host.start_scylla_server()

    def test_ssh_setup_script(self):
        new_user = "qa_user"
        new_user_identity_file = os.path.join(mgmt.MANAGER_IDENTITY_FILE_DIR, new_user)+".pem"
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        selected_host_ip = self._get_cluster_hosts_ip()[0]
        res_ssh_setup, _ssh = manager_tool.scylla_mgr_ssh_setup(node_ip=selected_host_ip, single_node=True, create_user=new_user)
        self.log.debug('res_ssh_setup: {}'.format(res_ssh_setup))
        new_user_login_message = "This account is currently not available"
        # sudo ssh -i /root/.ssh/qa_user.pem -q -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -L 59164:0.0.0.0:10000 qa_user@54.163.180.81
        new_user_login_cmd = "sudo ssh -i {} -q -o BatchMode=yes -o ConnectTimeout=5 -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -L 59164:0.0.0.0:10000 {}@{}".format(new_user_identity_file, new_user, selected_host_ip)
        self.log.debug("new_user_login_cmd command is: {}".format(new_user_login_cmd))
        res_new_user_login_cmd = manager_tool.manager_node.remoter.run(new_user_login_cmd, ignore_status=True)
        self.log.debug("res_new_user_login_cmd is: {}".format(res_new_user_login_cmd))
        assert new_user_login_message in res_new_user_login_cmd.stdout, "unexpected login-returned-message: {} . (expected: {}) ".format(res_new_user_login_cmd.stdout, new_user_login_message)

        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME+"_ssh_setup", host=selected_host_ip, single_node=True)
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
        mgr_cluster = manager_tool.get_cluster(cluster_name=cluster_name) or manager_tool.add_cluster(name=cluster_name,
                                                                                                      host=selected_host)
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
        stress_cmd_queue = self.run_stress_thread(stress_cmd=stress_cmd, duration=5)
        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)

    def test_repair_multiple_keyspace_types(self):
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        hosts = self._get_cluster_hosts_ip()
        selected_host = hosts[0]
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, host=selected_host)
        self._create_keyspace_and_basic_table(self.SIMPLESTRATEGY_KEYSPACE_NAME, "SimpleStrategy")
        self._create_keyspace_and_basic_table(self.LOCALSTRATEGY_KEYSPACE_NAME, "LocalStrategy")

        repair_task = mgr_cluster.create_repair_task()
        task_final_status = repair_task.wait_and_get_final_status(timeout=7200)
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(repair_task.id,
                                                                                            str(repair_task.status))
        self.log.info('Task: {} is done.'.format(repair_task.id))
        self.log.debug("sctool version is : {}".format(manager_tool.version))

        expected_keyspaces_to_be_repaired = ["system_auth", "system_distributed", "system_traces",
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

        localstrategy_keyspace_percentage = self._keyspace_value_in_progress_table(
            repair_task, repair_progress_table, self.LOCALSTRATEGY_KEYSPACE_NAME)
        assert localstrategy_keyspace_percentage is None, \
            "The keyspace with the replication strategy of localstrategy was included in repair, when it shouldn't"
        self.log.info("the sctool repair commend was completed successfully")

    def _keyspace_value_in_progress_table(self, repair_task, repair_progress_table, keyspace_name):
        try:
            table_repair_progress = repair_task.sctool.get_table_value(repair_progress_table, keyspace_name)
            table_repair_percentage = float(table_repair_progress.replace('%', ''))
            return table_repair_percentage
        except ScyllaManagerError as err:
            assert "not found in" in err.message, "Unexpected error: {}".format(err.message)
            return None


if __name__ == '__main__':
    main()

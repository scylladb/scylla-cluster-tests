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
import re
from random import randint

from invoke import exceptions
from pkg_resources import parse_version

from sdcm import mgmt
from sdcm.sct_events.group_common_events import ignore_no_space_errors
from sdcm.mgmt import ScyllaManagerError, TaskStatus, HostStatus, HostSsl, HostRestStatus
from sdcm.mgmt.cli import ScyllaManagerTool, SCYLLA_MANAGER_AGENT_YAML_PATH, update_config_file
from sdcm.nemesis import MgmtRepair
from sdcm.sct_events.system import InfoEvent
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.utils.common import reach_enospc_on_node, clean_enospc_on_node
from sdcm.tester import ClusterTester


class BackupFunctionsMixIn:
    is_cred_file_configured = False
    region = None
    bucket_name = None
    DESTINATION = '/tmp/backup'

    @staticmethod
    def download_files_from_s3(node, destination, file_list):
        download_cmd = 'aws s3 cp {} {}'
        for file_path in file_list:
            node.remoter.run(download_cmd.format(file_path, destination))

    @staticmethod
    def download_files_from_gs(node, destination, file_list):
        download_cmd = 'gsutil cp {} {}'
        for file_path in file_list:
            file_path = file_path.replace('gcs://', 'gs://')
            node.remoter.run(download_cmd.format(file_path, destination))

    def run_cmd_with_retry(self, cmd, node, retries=10):
        for _ in range(retries):
            try:
                node.remoter.run(cmd)
            except exceptions.UnexpectedExit as ex:
                self.log.debug(f'cmd {cmd} failed with error {ex}, will retry')
            else:
                break

    def install_awscli_dependencies(self, node, destination):
        cmd = f'''sudo yum install -y epel-release
                  sudo yum install -y python-pip
                  sudo yum remove -y epel-release
                  sudo pip install awscli==1.18.140
                  mkdir -p {destination}'''
        self.run_cmd_with_retry(cmd=cmd, node=node)

    def install_gsutil_dependencies(self, node, destination):
        cmd = f'''curl https://sdk.cloud.google.com > install.sh
                  bash install.sh --disable-prompts
                  mkdir -p {destination}'''
        self.run_cmd_with_retry(cmd=cmd, node=node)

    def restore_backup(self, mgr_cluster, snapshot_tag, keyspace_and_table_list):
        # pylint: disable=too-many-locals
        if self.params.get('cluster_backend') == 'aws':
            install_dependencies = self.install_awscli_dependencies
            download_files = self.download_files_from_s3
        elif self.params.get('cluster_backend') == 'gce':
            install_dependencies = self.install_gsutil_dependencies
            download_files = self.download_files_from_gs
        else:
            raise ValueError(f'"{self.params.get("cluster_backend")}" not supported')

        per_node_backup_file_paths = mgr_cluster.get_backup_files_dict(snapshot_tag)
        for node in self.db_cluster.nodes:
            install_dependencies(node=node, destination=self.DESTINATION)
            node_data_path = '/var/lib/scylla/data'
            node_id = node.host_id
            for keyspace, tables in keyspace_and_table_list.items():
                keyspace_path = os.path.join(node_data_path, keyspace)
                for table in tables:
                    node.remoter.run(f'mkdir -p {os.path.join(self.DESTINATION, table)}')
                    download_files(node=node, destination=os.path.join(self.DESTINATION, table, ''),
                                   file_list=per_node_backup_file_paths[node_id][keyspace][table])
                    file_details_lst = per_node_backup_file_paths[node_id][keyspace][table][0].split('/')
                    table_id = file_details_lst[file_details_lst.index(table) + 1]
                    table_upload_path = os.path.join(keyspace_path, table + '-' + table_id, 'upload')
                    node.remoter.run(f'sudo cp {os.path.join(self.DESTINATION, table, "*")} {table_upload_path}/.')
                    node.remoter.run(f'sudo chown scylla:scylla -Rf {table_upload_path}')
                    node.run_nodetool(f"refresh -- {keyspace} {table}")

    def restore_backup_from_backup_task(self, mgr_cluster, backup_task, keyspace_and_table_list):
        snapshot_tag = backup_task.get_snapshot_tag()
        self.restore_backup(mgr_cluster=mgr_cluster, snapshot_tag=snapshot_tag,
                            keyspace_and_table_list=keyspace_and_table_list)

    # pylint: disable=too-many-arguments
    def verify_backup_success(self, mgr_cluster, backup_task, keyspace_name='keyspace1', tables_names=None,
                              truncate=True):
        if tables_names is None:
            tables_names = ['standard1']
        per_keyspace_tables_dict = {keyspace_name: tables_names}
        if truncate:
            for table_name in tables_names:
                self.log.info(f'running truncate on {keyspace_name}.{table_name}')
                self.db_cluster.nodes[0].run_cqlsh(f'TRUNCATE {keyspace_name}.{table_name}')
        self.restore_backup_from_backup_task(mgr_cluster=mgr_cluster, backup_task=backup_task,
                                             keyspace_and_table_list=per_keyspace_tables_dict)

    def _generate_load(self):
        self.log.info('Starting c-s write workload for 1m')
        stress_cmd = self.params.get('stress_cmd')
        stress_thread = self.run_stress_thread(stress_cmd=stress_cmd, duration=5)
        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)
        return stress_thread

    def generate_load_and_wait_for_results(self):
        load_thread = self._generate_load()
        load_results = load_thread.get_results()
        self.log.info(f'load={load_results}')

    def _parse_stress_cmd(self, stress_cmd, params):
        # Due to an issue with scylla & cassandra-stress - we need to create the counter table manually
        if 'counter_' in stress_cmd:
            self._create_counter_table()

        if 'compression' in stress_cmd:
            if 'keyspace_name' not in params:
                compression_prefix = re.search('compression=(.*)Compressor', stress_cmd).group(1)
                keyspace_name = "keyspace_{}".format(compression_prefix.lower())
                params.update({'keyspace_name': keyspace_name})

        return params

    @staticmethod
    def _get_keyspace_name(ks_number, keyspace_pref='keyspace'):
        return '{}{}'.format(keyspace_pref, ks_number)

    def _run_all_stress_cmds(self, stress_queue, params):
        stress_cmds = params['stress_cmd']
        if not isinstance(stress_cmds, list):
            stress_cmds = [stress_cmds]
        # In some cases we want the same stress_cmd to run several times (can be used with round_robin or not).
        stress_multiplier = self.params.get('stress_multiplier')
        if stress_multiplier > 1:
            stress_cmds *= stress_multiplier

        for stress_cmd in stress_cmds:
            params.update({'stress_cmd': stress_cmd})
            self._parse_stress_cmd(stress_cmd, params)

            # Run all stress commands
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            if stress_cmd.startswith('scylla-bench'):
                stress_queue.append(self.run_stress_thread_bench(stress_cmd=stress_cmd,
                                                                 stats_aggregate_cmds=False,
                                                                 round_robin=self.params.get('round_robin')))
            else:
                stress_queue.append(self.run_stress_thread(**params))

    def run_prepare_write_cmd(self):
        # In some cases (like many keyspaces), we want to create the schema (all keyspaces & tables) before the load
        # starts - due to the heavy load, the schema propogation can take long time and c-s fails.
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = self.params.get('keyspace_num')
        write_queue = list()

        # When the load is too heavy for one loader when using MULTI-KEYSPACES, the load is spreaded evenly across
        # the loaders (round_robin).
        if keyspace_num > 1 and self.params.get('round_robin'):
            self.log.debug("Using round_robin for multiple Keyspaces...")
            for i in range(1, keyspace_num + 1):
                keyspace_name = self._get_keyspace_name(i)
                self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                               'keyspace_name': keyspace_name,
                                                               'round_robin': True})
        # Not using round_robin and all keyspaces will run on all loaders
        else:
            self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                           'keyspace_num': keyspace_num,
                                                           'round_robin': self.params.get('round_robin')})

        for stress in write_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

    def update_config_file(self):
        # FIXME: add to the nodes not in the same region as the bucket the bucket's region
        # this is a temporary fix, after https://github.com/scylladb/mermaid/issues/1456 is fixed, this is not necessary
        if self.params.get('cluster_backend') == 'aws':
            self.region = self.params.get('region_name').split()
            self.bucket_name = f"s3:{self.params.get('backup_bucket_location').split()[0]}"
            for node in self.db_cluster.nodes:
                update_config_file(node=node, region=self.region[0], config_file=SCYLLA_MANAGER_AGENT_YAML_PATH)
        elif self.params.get('cluster_backend') == 'gce':
            self.region = self.params.get('gce_datacenter')
            self.bucket_name = f"gcs:{self.params.get('backup_bucket_location')}"
        self.is_cred_file_configured = True

    def generate_background_read_load(self):
        self.log.info('Starting c-s read')
        stress_cmd = self.params.get('stress_read_cmd')
        number_of_nodes = self.params.get("n_db_nodes")
        number_of_loaders = self.params.get("n_loaders")

        scylla_version = self.db_cluster.nodes[0].scylla_version
        if parse_version(scylla_version).release[0] == 2019:
            # Making sure scylla version is 2019.1.x
            throttle_per_node = 10666
        else:
            throttle_per_node = 14666

        throttle_per_loader = int(throttle_per_node * number_of_nodes / number_of_loaders)
        stress_cmd = stress_cmd.replace("<THROTTLE_PLACE_HOLDER>", str(throttle_per_loader))
        stress_thread = self.run_stress_thread(stress_cmd=stress_cmd)
        self.log.info('Sleeping for 15s to let cassandra-stress run...')
        time.sleep(15)
        return stress_thread


class NoFilesFoundToDestroy(Exception):
    pass


class NoKeyspaceFound(Exception):
    pass


class FilesNotCorrupted(Exception):
    pass


# pylint: disable=too-many-public-methods
class MgmtCliTest(BackupFunctionsMixIn, ClusterTester):
    """
    Test Scylla Manager operations on Scylla cluster.
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
        self.generate_load_and_wait_for_results()
        self.log.debug("test_mgmt_cli: initialize MgmtRepair nemesis")
        mgmt_nemesis = MgmtRepair(tester_obj=self, termination_event=self.db_cluster.nemesis_termination_event)
        mgmt_nemesis.disrupt()

    def test_mgmt_cluster_crud(self):
        """
        Test steps:
        1) add a cluster to manager.
        2) update the cluster attributes in manager: name/host/ssh-user
        3) delete the cluster from manager and re-add again.
        """
        self.log.info('starting test_mgmt_cluster_crud')
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        # Test cluster attributes
        cluster_orig_name = mgr_cluster.name
        mgr_cluster.update(name="{}_renamed".format(cluster_orig_name))
        assert mgr_cluster.name == cluster_orig_name+"_renamed", "Cluster name wasn't changed after update command"
        mgr_cluster.delete()
        mgr_cluster = manager_tool.add_cluster(self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info('finishing test_mgmt_cluster_crud')

    def get_cluster_hosts_ip(self):
        return ScyllaManagerTool.get_cluster_hosts_ip(self.db_cluster)

    def get_cluster_hosts_with_ips(self):
        return ScyllaManagerTool.get_cluster_hosts_with_ips(self.db_cluster)

    def get_all_dcs_names(self):
        dcs_names = set()
        for node in self.db_cluster.nodes:
            data_center = self.db_cluster.get_nodetool_info(node)['Data Center']
            dcs_names.add(data_center)
        return dcs_names

    def _create_keyspace_and_basic_table(self, keyspace_name, strategy, table_name="example_table", rf=1):
        self.log.info("creating keyspace {}".format(keyspace_name))
        keyspace_existence = self.create_keyspace(keyspace_name, rf, strategy)
        assert keyspace_existence, "keyspace creation failed"
        # Keyspaces without tables won't appear in the repair, so the must have one
        self.log.info("creating the table {} in the keyspace {}".format(table_name, keyspace_name))
        self.create_table(table_name, keyspace_name=keyspace_name)

    def test_manager_sanity(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run test_mgmt_cluster test.
        3) test_mgmt_cluster_healthcheck
        4) test_client_encryption
        :return:
        """
        with self.subTest('Basic Backup Test'):
            self.test_basic_backup()
        with self.subTest('Repair Multiple Keyspace Types'):
            self.test_repair_multiple_keyspace_types()
        with self.subTest('Mgmt Cluster CRUD'):
            self.test_mgmt_cluster_crud()
        with self.subTest('Mgmt cluster Health Check'):
            self.test_mgmt_cluster_healthcheck()
        self.test_suspend_and_resume()
        with self.subTest('Client Encryption'):
            # Since this test activates encryption, it has to be the last test in the sanity
            self.test_client_encryption()

    def test_repair_intensity_feature_on_multiple_node(self):
        self._repair_intensity_feature(fault_multiple_nodes=True)

    def test_repair_intensity_feature_on_single_node(self):
        self._repair_intensity_feature(fault_multiple_nodes=False)

    def test_repair_control(self):
        InfoEvent(message="Starting C-S write load")
        self.run_prepare_write_cmd()
        InfoEvent(message="Flushing")
        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")
        InfoEvent(message="Waiting for compactions to end")
        self.wait_no_compactions_running(n=90, sleep_time=30)
        InfoEvent(message="Starting C-S read load")
        stress_read_thread = self.generate_background_read_load()
        time.sleep(600)  # So we will see the base load of the cluster
        InfoEvent(message="Sleep ended - Starting tests")
        self._create_repair_and_alter_it_with_repair_control()
        load_results = stress_read_thread.get_results()
        self.log.info(f'load={load_results}')

    def _create_repair_and_alter_it_with_repair_control(self):
        keyspace_to_be_repaired = "keyspace2"
        if not self.is_cred_file_configured:
            self.update_config_file()
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_repair_control',
                                               db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        # writing 292968720 rows, equal to the amount of data written in the prepare (around 100gb per node),
        # to create a large data fault and therefore a longer running repair
        self.create_missing_rows_in_cluster(create_missing_rows_in_multiple_nodes=True,
                                            keyspace_to_be_repaired=keyspace_to_be_repaired,
                                            total_num_of_rows=292968720)
        arg_list = [{"intensity": .0001},
                    {"intensity": 0},
                    {"parallel": 1},
                    {"intensity": 2, "parallel": 1}]

        InfoEvent(message="Repair started")
        repair_task = mgr_cluster.create_repair_task(keyspace="keyspace2")
        next_percentage_block = 20
        repair_task.wait_for_percentage(next_percentage_block)
        for args in arg_list:
            next_percentage_block += 20
            InfoEvent(message=f"Changing repair args to: {args}")
            mgr_cluster.control_repair(**args)
            repair_task.wait_for_percentage(next_percentage_block)
        repair_task.wait_and_get_final_status(step=30)
        InfoEvent(message="Repair ended")

    def _repair_intensity_feature(self, fault_multiple_nodes):
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
        with self.subTest('test_intensity_and_parallel'):
            self.test_intensity_and_parallel(fault_multiple_nodes=fault_multiple_nodes)
        load_results = stress_read_thread.get_results()
        self.log.info(f'load={load_results}')

    def get_email_data(self):
        self.log.info("Prepare data for email")

        email_data = self._get_common_email_data()
        email_data.update({"manager_server_repo": self.params.get("scylla_mgmt_repo"),
                           "manager_agent_repo": self.params.get("scylla_mgmt_agent_repo") or
                           self.params.get("scylla_mgmt_repo"), })

        return email_data

    def test_backup_feature(self):
        with self.subTest('Backup Multiple KS\' and Tables'):
            self.test_backup_multiple_ks_tables()
        with self.subTest('Backup to Location with path'):
            self.test_backup_location_with_path()
        with self.subTest('Test Backup Rate Limit'):
            self.test_backup_rate_limit()
        with self.subTest('Test Backup end of space'):  # Preferably at the end
            self.test_enospc_during_backup()

    def create_ks_and_tables(self, num_ks, num_table):
        # FIXME: beforehand we better change to have RF=1 to avoid restoring content while restoring replica of data
        table_name = []
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            for keyspace in range(num_ks):
                session.execute(f"CREATE KEYSPACE IF NOT EXISTS ks00{keyspace} "
                                "WITH replication={'class':'SimpleStrategy', 'replication_factor':1}")
                for table in range(num_table):
                    session.execute(f'CREATE COLUMNFAMILY IF NOT EXISTS ks00{keyspace}.table00{table} '
                                    '(key varchar, c varchar, v varchar, PRIMARY KEY(key, c))')
                    table_name.append(f'ks00{keyspace}.table00{table}')
                    # FIXME: improve the structure + data insertion
                    # can use this function to populate tables better?
                    # self.populate_data_parallel()
        return table_name

    def test_basic_backup(self):
        self.log.info('starting test_basic_backup')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [self.bucket_name, ]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        self.generate_load_and_wait_for_results()
        backup_task = mgr_cluster.create_backup_task(location_list=location_list)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE])
        self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info('finishing test_basic_backup')

    def test_backup_multiple_ks_tables(self):
        self.log.info('starting test_backup_multiple_ks_tables')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [self.bucket_name, ]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_multiple-ks', db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        tables = self.create_ks_and_tables(10, 100)
        self.generate_load_and_wait_for_results()
        self.log.debug(f'tables list = {tables}')
        # TODO: insert data to those tables
        backup_task = mgr_cluster.create_backup_task(location_list=location_list)
        backup_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=10800)
        self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
        self.log.info('finishing test_backup_multiple_ks_tables')

    def test_backup_location_with_path(self):
        self.log.info('starting test_backup_location_with_path')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [f'{self.bucket_name}/path_testing/']
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_bucket_with_path',
                                               db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self.generate_load_and_wait_for_results()
        try:
            mgr_cluster.create_backup_task(location_list=location_list)
        except ScyllaManagerError as error:
            self.log.info(f'Expected to fail - error: {error}')
        self.log.info('finishing test_backup_location_with_path')

    def test_backup_rate_limit(self):
        self.log.info('starting test_backup_rate_limit')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [self.bucket_name, ]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(name=self.CLUSTER_NAME + '_rate_limit', db_cluster=self.db_cluster,
                                               auth_token=self.monitors.mgmt_auth_token)
        self.generate_load_and_wait_for_results()
        rate_limit_list = [f'{dc}:{randint(1, 10)}' for dc in self.get_all_dcs_names()]
        self.log.info(f'rate limit will be {rate_limit_list}')
        backup_task = mgr_cluster.create_backup_task(location_list=location_list, rate_limit_list=rate_limit_list)
        task_status = backup_task.wait_and_get_final_status()
        self.log.info(f'backup task finished with status {task_status}')
        # TODO: verify that the rate limit is as set in the cmd
        self.verify_backup_success(mgr_cluster=mgr_cluster, backup_task=backup_task)
        self.log.info('finishing test_backup_rate_limit')

    def test_client_encryption(self):
        self.log.info('starting test_client_encryption')
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        self.generate_load_and_wait_for_results()
        repair_task = mgr_cluster.create_repair_task(fail_fast=True)
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.OFF, "Not all hosts ssl is 'OFF'"

        with DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR, line="failed to do checksum for"), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line="failed to do checksum for"), \
                DbEventsFilter(db_event=DatabaseLogEvent.DATABASE_ERROR, line="Reactor stalled"), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line="failed to repair"), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line="repair id "), \
                DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line="get_repair_meta: repair_meta_id"):

            self.db_cluster.enable_client_encrypt()

            repair_task.wait_for_status(list_status=[TaskStatus.ERROR, TaskStatus.ERROR_FINAL], step=5, timeout=240)

        mgr_cluster.update(client_encrypt=True)
        repair_task.start()
        sleep = 40
        self.log.debug('Sleep {} seconds, waiting for health-check task to run by schedule on first time'.format(sleep))
        time.sleep(sleep)
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            assert host_health.ssl == HostSsl.ON, "Not all hosts ssl is 'ON'"
            assert host_health.status == HostStatus.UP, "Not all hosts status is 'UP'"

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info('finishing test_client_encryption')

    def test_mgmt_cluster_healthcheck(self):
        self.log.info('starting test_mgmt_cluster_healthcheck')
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        other_host, other_host_ip = [
            host_data for host_data in self.get_cluster_hosts_with_ips() if
            host_data[1] != self.get_cluster_hosts_ip()[0]][0]
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

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info('finishing test_mgmt_cluster_healthcheck')

    def test_ssh_setup_script(self):
        self.log.info('starting test_ssh_setup_script')
        new_user = "qa_user"
        new_user_identity_file = os.path.join(mgmt.cli.MANAGER_IDENTITY_FILE_DIR, new_user)+".pem"
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        selected_host_ip = self.get_cluster_hosts_ip()[0]
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

        mgr_cluster = manager_tool.add_cluster(  # pylint: disable=unexpected-keyword-arg
            name=self.CLUSTER_NAME+"_ssh_setup", host=selected_host_ip,
            single_node=True, auth_token=self.monitors.mgmt_auth_token)
        # self.log.debug('mgr_cluster: {}'.format(mgr_cluster))
        healthcheck_task = mgr_cluster.get_healthcheck_task()
        self.log.debug("Health-check task history is: {}".format(healthcheck_task.history))
        dict_host_health = mgr_cluster.get_hosts_health()
        for host_health in dict_host_health.values():
            self.log.debug("host_health is: {}".format(host_health))
        self.log.info('finishing test_ssh_setup_script')

    def test_manager_upgrade(self):
        """
        Test steps:
        1) Run the repair test.
        2) Run manager upgrade to new version of yaml: 'scylla_mgmt_upgrade_to_repo'. (the 'from' version is: 'scylla_mgmt_repo').
        """
        self.log.info('starting test_manager_upgrade')
        scylla_mgmt_upgrade_to_repo = self.params.get('scylla_mgmt_upgrade_to_repo')
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        selected_host = self.get_cluster_hosts_ip()[0]
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
        self.log.info('finishing test_manager_upgrade')

    def test_manager_rollback_upgrade(self):
        """
        Test steps:
        1) Run Upgrade test: scylla_mgmt_repo --> scylla_mgmt_upgrade_to_repo
        2) Run manager downgrade to pre-upgrade version as in yaml: 'scylla_mgmt_repo'.
        """
        self.log.info('starting test_manager_rollback_upgrade')
        self.test_manager_upgrade()
        scylla_mgmt_repo = self.params.get('scylla_mgmt_repo')
        manager_node = self.monitors.nodes[0]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=manager_node)
        manager_from_version = manager_tool.version
        manager_tool.rollback_upgrade(scylla_mgmt_repo=scylla_mgmt_repo)
        assert manager_from_version[0] != manager_tool.version[0], "Manager version not changed after rollback."
        self.log.info('finishing test_manager_rollback_upgrade')

    def test_repair_multiple_keyspace_types(self):  # pylint: disable=invalid-name
        self.log.info('starting test_repair_multiple_keyspace_types')
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        self._create_keyspace_and_basic_table(self.SIMPLESTRATEGY_KEYSPACE_NAME, "SimpleStrategy", rf=2)
        self._create_keyspace_and_basic_table(self.LOCALSTRATEGY_KEYSPACE_NAME, "LocalStrategy")
        repair_task = mgr_cluster.create_repair_task()
        task_final_status = repair_task.wait_and_get_final_status(timeout=7200)
        assert task_final_status == TaskStatus.DONE, 'Task: {} final status is: {}.'.format(repair_task.id,
                                                                                            str(repair_task.status))
        self.log.info('Task: {} is done.'.format(repair_task.id))
        self.log.debug("sctool version is : {}".format(manager_tool.version))

        expected_keyspaces_to_be_repaired = ["system_auth", "system_distributed", "system_traces",  # pylint: disable=invalid-name
                                             self.SIMPLESTRATEGY_KEYSPACE_NAME]
        per_keyspace_progress = repair_task.per_keyspace_progress
        self.log.info("Looking in the repair output for all of the required keyspaces")
        for keyspace_name in expected_keyspaces_to_be_repaired:
            keyspace_repair_percentage = per_keyspace_progress.get(keyspace_name, None)
            assert keyspace_repair_percentage is not None, \
                "The keyspace {} was not included in the repair!".format(keyspace_name)
            assert keyspace_repair_percentage == 100, \
                "The repair of the keyspace {} stopped at {}%".format(
                    keyspace_name, keyspace_repair_percentage)

        localstrategy_keyspace_percentage = per_keyspace_progress.get(self.LOCALSTRATEGY_KEYSPACE_NAME, None)
        assert localstrategy_keyspace_percentage is None, \
            "The keyspace with the replication strategy of localstrategy was included in repair, when it shouldn't"
        self.log.info("the sctool repair command was completed successfully")

        mgr_cluster.delete()  # remove cluster at the end of the test
        self.log.info('finishing test_repair_multiple_keyspace_types')

    def test_enospc_during_backup(self):
        self.log.info('starting test_enospc_during_backup')
        if not self.is_cred_file_configured:
            self.update_config_file()
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        hosts = self.get_cluster_hosts_ip()
        location_list = [self.bucket_name, ]
        selected_host = hosts[0]
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, host=selected_host,
                                        auth_token=self.monitors.mgmt_auth_token)

        target_node = self.db_cluster.nodes[1]

        self.generate_load_and_wait_for_results()
        has_enospc_been_reached = False
        with ignore_no_space_errors(node=target_node):
            try:
                backup_task = mgr_cluster.create_backup_task(location_list=location_list)
                backup_task.wait_for_uploading_stage()
                backup_task.stop()

                reach_enospc_on_node(target_node=target_node)
                has_enospc_been_reached = True

                backup_task.start()

                backup_task.wait_and_get_final_status()
                assert backup_task.status == TaskStatus.DONE, "The backup failed to run on a node with no free space," \
                                                              " while it should have had the room for snapshots due " \
                                                              "to the previous run"

            finally:
                if has_enospc_been_reached:
                    clean_enospc_on_node(target_node=target_node, sleep_time=30)

    def _delete_keyspace_directory(self, db_node, keyspace_name):
        # Stop scylla service before deleting sstables to avoid partial deletion of files that are under compaction
        db_node.stop_scylla_server(verify_up=False, verify_down=True)

        try:
            directoy_path = f"/var/lib/scylla/data/{keyspace_name}"
            directory_size_result = db_node.remoter.sudo(f"du -h --max-depth=0 {directoy_path}")
            result = db_node.remoter.sudo(f'rm -rf {directoy_path}')
            if result.stderr:
                raise FilesNotCorrupted('Files were not corrupted. CorruptThenRepair nemesis can\'t be run. '
                                        'Error: {}'.format(result))
            if directory_size_result.stdout:
                directory_size = directory_size_result.stdout[:directory_size_result.stdout.find("\t")]
                self.log.debug(f"Removed the directory of keyspace {keyspace_name} from node "
                               f"{db_node}\nThe size of the directory is {directory_size}")

        finally:
            db_node.start_scylla_server(verify_up=True, verify_down=False)

    def _insert_data_while_excluding_each_node(self, total_num_of_rows, keyspace_name="keyspace2"):
        """
        The function split the number of rows to the number of nodes (minus 1) and in loop does the following:
        shuts down one node, insert one part of the rows and starts the node again.
        As a result, each node that was shut down will have missing rows and will require repair.
        """
        num_of_nodes = self.params.get("n_db_nodes")
        num_of_rows_per_insertion = int(total_num_of_rows / (num_of_nodes - 1))
        stress_command_template = "cassandra-stress write cl=QUORUM n={} -schema 'keyspace={} replication(factor=3)'" \
                                  " -col 'size=FIXED(1024) n=FIXED(1)' -pop seq={}..{} -port jmx=6868 -mode cql3" \
                                  " native -rate threads=200 -log interval=5"
        start_of_range = 1
        # We can't shut down node 1 since it's the default contact point of the stress command, and we have no way
        # of changing that. As such, we skip it.
        for node in self.db_cluster.nodes[1:]:
            self.log.info(f"inserting {num_of_rows_per_insertion} rows to every node except {node.name}")
            end_of_range = start_of_range + num_of_rows_per_insertion - 1
            node.stop_scylla_server(verify_up=False, verify_down=True)
            stress_thread = self.run_stress_thread(stress_cmd=stress_command_template.format(num_of_rows_per_insertion,
                                                                                             keyspace_name,
                                                                                             start_of_range,
                                                                                             end_of_range))
            time.sleep(15)
            self.log.info(f'load={stress_thread.get_results()}')
            node.start_scylla_server(verify_up=True, verify_down=False)
            start_of_range = end_of_range + 1
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"ALTER TABLE {keyspace_name}.standard1 WITH read_repair_chance = 0.0")

        for node in self.db_cluster.nodes:
            node.run_nodetool("flush")

    def create_missing_rows_in_cluster(self, create_missing_rows_in_multiple_nodes, total_num_of_rows,
                                       keyspace_to_be_repaired=None):
        if create_missing_rows_in_multiple_nodes:
            self._insert_data_while_excluding_each_node(total_num_of_rows=total_num_of_rows,
                                                        keyspace_name=keyspace_to_be_repaired)
            self.wait_no_compactions_running(n=40, sleep_time=10)
        else:
            target_node = self.db_cluster.nodes[2]
            self._delete_keyspace_directory(db_node=target_node, keyspace_name="keyspace1")

    def test_intensity_and_parallel(self, fault_multiple_nodes):
        keyspace_to_be_repaired = "keyspace2"
        InfoEvent(message='starting test_intensity_and_parallel').publish()
        if not self.is_cred_file_configured:
            self.update_config_file()
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.add_cluster(
            name=self.CLUSTER_NAME + '_intensity_and_parallel',
            db_cluster=self.db_cluster,
            auth_token=self.monitors.mgmt_auth_token,
        )

        InfoEvent(message="Starting faulty load (to be repaired)").publish()
        self.create_missing_rows_in_cluster(create_missing_rows_in_multiple_nodes=fault_multiple_nodes,
                                            keyspace_to_be_repaired=keyspace_to_be_repaired,
                                            total_num_of_rows=29296872)

        InfoEvent(message="Starting a repair with no intensity").publish()
        base_repair_task = mgr_cluster.create_repair_task(keyspace="keyspace*")
        base_repair_task.wait_and_get_final_status(step=30)
        assert base_repair_task.status == TaskStatus.DONE, "The base repair task did not end in the expected time"
        InfoEvent(message=f"The base repair, with no intensity argument, took {base_repair_task.duration}").publish()

        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_to_be_repaired}")

        arg_list = [{"intensity": .5},
                    {"intensity": .25},
                    {"intensity": .0001},
                    {"intensity": 2},
                    {"intensity": 4},
                    {"parallel": 1},
                    {"parallel": 2},
                    {"intensity": 2, "parallel": 1},
                    {"intensity": 100},
                    {"intensity": 0}]

        for arg_dict in arg_list:
            InfoEvent(message="Starting faulty load (to be repaired)").publish()
            self.create_missing_rows_in_cluster(create_missing_rows_in_multiple_nodes=fault_multiple_nodes,
                                                keyspace_to_be_repaired=keyspace_to_be_repaired,
                                                total_num_of_rows=29296872)

            InfoEvent(message=f"Starting a repair with {arg_dict}").publish()
            repair_task = mgr_cluster.create_repair_task(**arg_dict, keyspace="keyspace*")
            repair_task.wait_and_get_final_status(step=30)
            InfoEvent(message=f"repair with {arg_dict} took {repair_task.duration}").publish()

            with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
                session.execute(f"DROP KEYSPACE IF EXISTS {keyspace_to_be_repaired}")
        InfoEvent(message='finishing test_intensity_and_parallel').publish()

    def test_suspend_and_resume(self):
        self.generate_load_and_wait_for_results()
        with self.subTest('Suspend and resume backup task'):
            self._suspend_and_resume_task_template(task_type="backup")
        with self.subTest('Suspend and resume repair task'):
            self._suspend_and_resume_task_template(task_type="repair")
        with self.subTest('Suspend and resume without starting task'):
            self.test_suspend_and_resume_without_starting_tasks()

    def _suspend_and_resume_task_template(self, task_type):
        # task types: backup/repair
        self.log.info(f'starting test_suspend_and_resume_{task_type}')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [self.bucket_name, ]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        if task_type == "backup":
            suspendable_task = mgr_cluster.create_backup_task(location_list=location_list)
        elif task_type == "repair":
            suspendable_task = mgr_cluster.create_repair_task()
        else:
            raise ValueError(f"Not familiar with task type: {task_type}")
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=5), \
            f"task {suspendable_task.id} failed to reach status {TaskStatus.RUNNING}"
        mgr_cluster.suspend()
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.STOPPED], timeout=300, step=10), \
            f"task {suspendable_task.id} failed to reach status {TaskStatus.STOPPED}"
        mgr_cluster.resume(start_tasks=True)
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.DONE], timeout=1200, step=10), \
            f"task {suspendable_task.id} failed to reach status {TaskStatus.DONE}"
        self.log.info(f'finishing test_suspend_and_resume_{task_type}')

    def test_suspend_and_resume_without_starting_tasks(self):
        self.log.info(f'starting test_suspend_and_resume_without_starting_tasks')
        if not self.is_cred_file_configured:
            self.update_config_file()
        location_list = [self.bucket_name, ]
        manager_tool = mgmt.get_scylla_manager_tool(manager_node=self.monitors.nodes[0])
        mgr_cluster = manager_tool.get_cluster(cluster_name=self.CLUSTER_NAME) \
            or manager_tool.add_cluster(name=self.CLUSTER_NAME, db_cluster=self.db_cluster,
                                        auth_token=self.monitors.mgmt_auth_token)
        suspendable_task = mgr_cluster.create_backup_task(location_list=location_list)
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.RUNNING], timeout=300, step=5), \
            f"task {suspendable_task.id} failed to reach status {TaskStatus.RUNNING}"
        mgr_cluster.suspend()
        assert suspendable_task.wait_for_status(list_status=[TaskStatus.STOPPED], timeout=300, step=10), \
            f"task {suspendable_task.id} failed to reach status {TaskStatus.STOPPED}"
        mgr_cluster.resume(start_tasks=False)
        self.log.info("Waiting a little time to make sure the task isn't started")
        time.sleep(60)
        current_task_status = suspendable_task.status
        assert current_task_status == TaskStatus.STOPPED, \
            f'Task {current_task_status} did not remain in "{TaskStatus.STOPPED}" status, but instead ' \
            f'reached "{current_task_status}" status'
        self.log.info(f'finishing test_suspend_and_resume_without_starting_tasks')

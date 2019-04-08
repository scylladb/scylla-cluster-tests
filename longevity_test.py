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
import yaml
from avocado import main

from sdcm.tester import ClusterTester


class LongevityTest(ClusterTester):
    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    def _run_all_stress_cmds(self, stress_queue, params):
        stress_cmds = params['stress_cmd']
        if not isinstance(stress_cmds, list):
            stress_cmds = [stress_cmds]
        # In some cases we want the same stress_cmd to run several times (can be used with round_robin or not).
        stress_multiplier = self.params.get('stress_multiplier', default=1)
        if stress_multiplier > 1:
            stress_cmds *= stress_multiplier

        for stress_cmd in stress_cmds:
            params.update({'stress_cmd': stress_cmd})
            self._parse_stress_cmd(stress_cmd, params)

            # Run all stress commands
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            if stress_cmd.startswith('cassandra-stress'):
                stress_queue.append(self.run_stress_thread(**params))
            else:
                stress_queue.append(self.run_stress_thread_bench(stress_cmd=stress_cmd, stats_aggregate_cmds=False))
            time.sleep(10)

            # Remove "user profile" param for the next command
            if 'profile' in params:
                del params['profile']

            if 'keyspace_name' in params:
                del params['keyspace_name']

        return stress_queue

    def _parse_stress_cmd(self, stress_cmd, params):
        # Due to an issue with scylla & cassandra-stress - we need to create the counter table manually
        if 'counter_' in stress_cmd:
            self._create_counter_table()

        # When using cassandra-stress with "user profile" the profile yaml should be provided
        if 'profile' in stress_cmd:
            if 'profile' not in params:
                cs_profile = re.search('profile=(.*)yaml', stress_cmd).group(1) + 'yaml'
                cs_profile = os.path.join(os.path.dirname(__file__), 'data_dir', os.path.basename(cs_profile))
                with open(cs_profile, 'r') as yaml_stream:
                    profile = yaml.safe_load(yaml_stream)
                    keyspace_name = profile['keyspace']
                params.update({'profile': cs_profile, 'keyspace_name': keyspace_name})

        if 'compression' in stress_cmd:
            if 'keyspace_name' not in params:
                keyspace_name = "keyspace_{}".format(re.search('compression=(.*)Compressor', stress_cmd).group(1))
                params.update({'keyspace_name': keyspace_name})

        return params

    default_params = {'timeout': 650000}

    @staticmethod
    def _get_keyspace_name(ks_number, keyspace_pref='keyspace'):
        return '{}{}'.format(keyspace_pref, ks_number)

    def _get_fullscan_params(self):
        params = {}
        fullscan = self.params.get('run_fullscan', default=None)
        if fullscan:
            fullscan = fullscan.split(',')
            params['ks.cf'] = fullscan[0].strip()
            params['interval'] = int(fullscan[1].strip())
            self.log.info('Fullscan target: {} Fullscan interval: {}'.format(params['ks.cf'],
                                                                             params['interval']))
        return params

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    tester_obj=self)
        stress_queue = list()
        write_queue = list()
        verify_queue = list()

        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd', default=None)
        keyspace_num = self.params.get('keyspace_num', default=1)
        pre_create_schema = self.params.get('pre_create_schema', default=False)

        if prepare_write_cmd:
            # In some cases (like many keyspaces), we want to create the schema (all keyspaces & tables) before the load
            # starts - due to the heavy load, the schema propogation can take long time and c-s fails.
            if pre_create_schema:
                self._pre_create_schema(keyspace_num, scylla_encryption_options=self.params.get('scylla_encryption_options', None))
            # When the load is too heavy for one lader when using MULTI-KEYSPACES, the load is spreaded evenly across
            # the loaders (round_robin).
            if keyspace_num > 1 and self.params.get('round_robin', default='false').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num + 1):
                    keyspace_name = self._get_keyspace_name(i)
                    self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                                   'keyspace_name': keyspace_name,
                                                                   'round_robin': True})
            # Not using round_robin and all keyspaces will run on all loaders
            else:
                self._run_all_stress_cmds(write_queue, params={'stress_cmd': prepare_write_cmd,
                                                               'keyspace_num': keyspace_num})

            # In some cases we don't want the nemesis to run during the "prepare" stage in order to be 100% sure that
            # all keys were written succesfully
            if self.params.get('nemesis_during_prepare', default='true').lower() == 'true':
                # Wait for some data (according to the param in the yal) to be populated, for multi keyspace need to
                # pay attention to the fact it checks only on keyspace1
                self.db_cluster.wait_total_space_used_per_node(keyspace=None)
                self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

            # Wait on the queue till all threads come back.
            # todo: we need to improve this part for some cases that threads are being killed and we don't catch it.
            for stress in write_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

            # Run nodetool flush on all nodes to make sure nothing left in memory
            # I decided to comment this out for now, when we found the data corruption bug, we wanted to be on the safe
            # side, but I don't think we should continue with this approach.
            # If we decided to add this back in the future, we need to wrap it with try-except because it can run
            # in parallel to nemesis and it will fail on one of the nodes.
            # self._flush_all_nodes()

            # In case we would like to verify all keys were written successfully before we start other stress / nemesis
            prepare_verify_cmd = self.params.get('prepare_verify_cmd', default=None)
            if prepare_verify_cmd:
                self._run_all_stress_cmds(verify_queue, params={'stress_cmd': prepare_verify_cmd,
                                                                'keyspace_num': keyspace_num})

                for stress in verify_queue:
                    self.verify_stress_thread(cs_thread_pool=stress)

        # Collect data about partitions and their rows amount
        validate_partitions = self.params.get('validate_partitions', default=None)
        table_name, primary_key_column, partitions_dict_before = '', '', {}
        if validate_partitions:
            table_name = self.params.get('table_name', default=None)
            primary_key_column = self.params.get('primary_key_column', default=None)
            self.log.debug('Save partitons info before reads')
            partitions_dict_before = self.collect_partitions_info(table_name=table_name,
                                                                  primary_key_column=primary_key_column,
                                                                  save_into_file_name='partitions_rows_before.log')

        stress_cmd = self.params.get('stress_cmd', default=None)
        if stress_cmd:
            # Stress: Same as in prepare_write - allow the load to be spread across all loaders when using multi ks
            if keyspace_num > 1 and self.params.get('round_robin', default='false').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num + 1):
                    keyspace_name = self._get_keyspace_name(i)
                    params = {'keyspace_name': keyspace_name, 'round_robin': True, 'stress_cmd': stress_cmd}

                    self._run_all_stress_cmds(stress_queue, params)

            # The old method when we run all stress_cmds for all keyspace on the same loader
            else:
                params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_cmd}
                self._run_all_stress_cmds(stress_queue, params)

        customer_profiles = self.params.get('cs_user_profiles')
        if customer_profiles:
            cs_duration = self.params.get('cs_duration', default='50m')
            for cs_profile in customer_profiles.split():
                assert os.path.exists(cs_profile), 'File not found: {}'.format(cs_profile)
                self.log.debug('Run stress test with user profile {}, duration {}'.format(cs_profile, cs_duration))
                profile_dst = os.path.join('/tmp', os.path.basename(cs_profile))
                with open(cs_profile) as pconf:
                    cont = pconf.readlines()
                    for cmd in [line.lstrip('#').strip() for line in cont if line.find('cassandra-stress') > 0]:
                        stress_cmd = (cmd.format(profile_dst, cs_duration))
                        params = {'stress_cmd': stress_cmd, 'profile': cs_profile}
                        self.log.debug('Stress cmd: {}'.format(stress_cmd))
                        self._run_all_stress_cmds(stress_queue, params)

        fullscan = self._get_fullscan_params()
        if fullscan:
            self.log.info('Fullscan target: {} Fullscan interval: {}'.format(fullscan['ks.cf'],
                                                                             fullscan['interval']))
            self.run_fullscan_thread(ks_cf=fullscan['ks.cf'], interval=fullscan['interval'])

        # Check if we shall wait for total_used_space or if nemesis wasn't started
        if not prepare_write_cmd or self.params.get('nemesis_during_prepare', default='true').lower() == 'false':
            self.db_cluster.wait_total_space_used_per_node(keyspace=None)
            self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        stress_read_cmd = self.params.get('stress_read_cmd', default=None)
        if stress_read_cmd:
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_read_cmd}
            self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(cs_thread_pool=stress)

        if (stress_read_cmd or stress_cmd) and validate_partitions:
            self.log.debug('Save partitons info after reads')
            partitions_dict_after = self.collect_partitions_info(table_name=table_name,
                                                                 primary_key_column=primary_key_column,
                                                                 save_into_file_name='partitions_rows_after.log')
            self.assertEqual(partitions_dict_before, partitions_dict_after,
                             msg='Row amount in partitions is not same before and after running of nemesis')

    def test_batch_custom_time(self):
        """
        The test runs like test_custom_time but desgined for running multiple stress commands in batches.
        It take the keyspace_num and calculates the number of batches to run based on batch_size.
        For every batch, it runs the stress and verify them and only then moves to the next batch.

        Test assumes:
        - pre_create_schema (The test pre-creating the schema for all batches)
        - round_robin
        - No nemesis during prepare

        :param keyspace_num: Number of keyspaces to be batched (in future it can be enahnced with number of tables).
        :param batch_size: Number of stress commands to run together in a batch.
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(), tester_obj=self)

        total_stress = self.params.get('keyspace_num')  # In future it may be 1 keyspace but multiple tables in it.
        batch_size = self.params.get('batch_size', default=1)

        prepare_write_cmd = self.params.get('prepare_write_cmd', default=None)
        if prepare_write_cmd:
            self._run_stress_in_batches(total_stress=total_stress, batch_size=batch_size,
                                        stress_cmd=prepare_write_cmd)

        self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        stress_cmd = self.params.get('stress_cmd', default=None)
        self._run_stress_in_batches(total_stress=batch_size, batch_size=batch_size,
                                    stress_cmd=stress_cmd)

    def _run_stress_in_batches(self, total_stress, batch_size, stress_cmd):
        stress_queue = list()
        self._pre_create_schema(keyspace_num=total_stress)

        num_of_batches = int(total_stress / batch_size)
        for batch in xrange(0, num_of_batches):
            for i in xrange(1 + batch * batch_size, (batch + 1) * batch_size + 1):
                keyspace_name = self._get_keyspace_name(i)
                self._run_all_stress_cmds(stress_queue, params={'stress_cmd': stress_cmd,
                                                                'keyspace_name': keyspace_name, 'round_robin': True})
            for stress in stress_queue:
                self.verify_stress_thread(cs_thread_pool=stress)

    def _create_counter_table(self):
        """
        workaround for the issue https://github.com/scylladb/scylla-tools-java/issues/32
        remove when resolved
        """
        node = self.db_cluster.nodes[0]
        with self.cql_connection_patient(node) as session:
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS keyspace1
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;
            """)
            session.execute("""
                CREATE TABLE IF NOT EXISTS keyspace1.counter1 (
                    key blob PRIMARY KEY,
                    "C0" counter,
                    "C1" counter,
                    "C2" counter,
                    "C3" counter,
                    "C4" counter
                ) WITH COMPACT STORAGE
                    AND bloom_filter_fp_chance = 0.01
                    AND caching = '{"keys":"ALL","rows_per_partition":"ALL"}'
                    AND comment = ''
                    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
                    AND compression = {}
                    AND dclocal_read_repair_chance = 0.1
                    AND default_time_to_live = 0
                    AND gc_grace_seconds = 864000
                    AND max_index_interval = 2048
                    AND memtable_flush_period_in_ms = 0
                    AND min_index_interval = 128
                    AND read_repair_chance = 0.0
                    AND speculative_retry = '99.0PERCENTILE';
            """)

    def _pre_create_schema(self, keyspace_num=1, in_memory=False, scylla_encryption_options=None):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """
        node = self.db_cluster.nodes[0]
        with self.cql_connection_patient(node) as session:

            self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))

            for i in xrange(1, keyspace_num+1):
                keyspace_name = 'keyspace{}'.format(i)
                self.create_ks(session, keyspace_name, rf=3)
                self.log.debug('{} Created'.format(keyspace_name))
                self.create_cf(session,  'standard1', key_type='blob', read_repair=0.0, compact_storage=True,
                               columns={'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob', '"C4"': 'blob'},
                               in_memory=in_memory, scylla_encryption_options=scylla_encryption_options)

    def _flush_all_nodes(self):
        """
        This function will connect all db nodes in the cluster and run "nodetool flush" command.
        :return:
        """
        for node in self.db_cluster.nodes:
            node.remoter.run('sudo nodetool flush')


if __name__ == '__main__':
    main()

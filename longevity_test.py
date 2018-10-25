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

from sdcm.tester import ClusterTester


class LongevityTest(ClusterTester):
    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """

    def _run_all_stress_cmds(self, stress_queue, params):
        stress_cmds = self.params.get('stress_cmd')
        # In some cases we want the same stress_cmd to run several times (can be used with round_robin or not).
        stress_multiplier = self.params.get('stress_multiplier', default=1)
        if stress_multiplier > 1:
            stress_cmds *= stress_multiplier

        for stress_cmd in stress_cmds:
            params.update({'stress_cmd': stress_cmd})
            self._parse_stress_cmd(stress_cmd, params)

            # Run all stress commands
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(**params))
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
            cs_profile = re.search('profile=(.*)yaml', stress_cmd).group(1) + 'yaml'
            cs_profile = os.path.join(os.path.dirname(__file__), 'data_dir', os.path.basename(cs_profile))
            params.update({'profile': cs_profile})

        if 'compression' in stress_cmd:
            if 'keyspace_name' not in params:
                    keyspace_name = "keyspace_{}".format(re.search('compression=(.*)Compressor', stress_cmd).group(1))
                    params.update({'keyspace_name': keyspace_name})

        return params

    default_params = {'timeout': 650000}

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors,
                                    db_stats=self.get_stats_obj())
        stress_queue = list()
        write_queue = list()
        verify_queue = list()

        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = self.params.get('keyspace_num', default=1)
        pre_create_schema = self.params.get('pre_create_schema', default=False)

        if prepare_write_cmd:
            # In some cases (like many keyspaces), we want to create the schema (all keyspaces & tables) before the load
            # starts - due to the heavy load, the schema propogation can take long time and c-s fails.
            if pre_create_schema:
                self._pre_create_schema()
            # When the load is too heavy for one lader when using MULTI-KEYSPACES, the load is spreaded evenly across
            # the loaders (round_robin).
            if keyspace_num > 1 and self.params.get('round_robin', default='').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num + 1):
                    keyspace_name = 'keyspace{}'.format(i)
                    write_queue.append(self.run_stress_thread(stress_cmd=prepare_write_cmd,
                                                              keyspace_name=keyspace_name, round_robin=True))
                    time.sleep(2)
            # Not using round_robin and all keyspaces will run on all loaders
            else:
                write_queue.append(self.run_stress_thread(stress_cmd=prepare_write_cmd, keyspace_num=keyspace_num))
            # Wait for some data (according to the param in the yal) to be populated, for multi keyspace need to
            # pay attention to the fact it checks only on keyspace1
            self.db_cluster.wait_total_space_used_per_node()

            # In some cases we don't want the nemesis to run during the "prepare" stage in order to be 100% sure that
            # all keys were written succesfully
            if self.params.get('nemesis_during_prepare', default='true').lower() == 'true':
                self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

            # Wait on the queue till all threads come back.
            # todo: we need to improve this part for some cases that threads are being killed and we don't catch it.
            for stress in write_queue:
                self.verify_stress_thread(queue=stress)

            # Run nodetool flush on all nodes to make sure nothing left in memory
            self._flush_all_nodes()

            # In case we would like to verify all keys were written successfully before we start other stress / nemesis
            prepare_verify_cmd = self.params.get('prepare_verify_cmd', default=None)
            if prepare_verify_cmd:
                verify_queue.append(self.run_stress_thread(stress_cmd=prepare_verify_cmd, keyspace_num=keyspace_num))

                for stress in verify_queue:
                    self.verify_stress_thread(queue=stress)

        # Stress: Same as in prepare_write - allow the load to be spread across all loaders when using MULTI-KEYSPACES
        if keyspace_num > 1 and self.params.get('round_robin', default='').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num + 1):
                    keyspace_name = 'keyspace{}'.format(i)
                    params = {'keyspace_name': keyspace_name, 'round_robin': True}

                    self._run_all_stress_cmds(stress_queue, params)

        # The old method when we run all stress_cmds for all keyspace on the same loader
        else:
                params = {'keyspace_num': keyspace_num}
                self._run_all_stress_cmds(stress_queue, params)

        # Check if we shall wait for total_used_space or if nemesis wasn't started
        if not prepare_write_cmd or self.params.get('nemesis_during_prepare', default='true').lower() == 'false':
            self.db_cluster.wait_total_space_used_per_node()
            self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        # The below sleep is a temporary HACK to wait some time before we start reading until more data will be written.
        # It wasn't necessary in the past because we had the wait_total_space_used, however now we have more keyspaces
        # and tables while wait_total_space_used is checking only keyspace1.
        # Todo: refactor wait_total_space_used to consider all keyspaces/tables in our stress list.

        time.sleep(600)

        stress_read_cmd = self.params.get('stress_read_cmd', default=None)
        if stress_read_cmd:
            for stress_cmd in stress_read_cmd:
                self.log.debug('stress read cmd: {}'.format(stress_cmd))
                if 'compression' in stress_cmd:
                    keyspace_name = "keyspace_{}".format(re.search('compression=(.*)Compressor', stress_cmd).group(1))
                    stress_queue.append(self.run_stress_thread(stress_cmd=stress_cmd, keyspace_name=keyspace_name))
                else:
                    stress_queue.append(self.run_stress_thread(stress_cmd=stress_cmd))

        for stress in stress_queue:
            self.verify_stress_thread(queue=stress)

    def _create_counter_table(self):
        """
        workaround for the issue https://github.com/scylladb/scylla-tools-java/issues/32
        remove when resolved
        """
        node = self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)
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

    def _pre_create_schema(self, in_memory=False):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """
        node = self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)

        keyspace_num = self.params.get('keyspace_num', default=1)
        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))

        for i in xrange(1, keyspace_num+1):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_ks(session, keyspace_name, rf=3)
            self.log.debug('{} Created'.format(keyspace_name))
            self.create_cf(session,  'standard1', key_type='blob', read_repair=0.0, compact_storage=True,
                           columns={'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob', '"C4"': 'blob'},
                           in_memory=in_memory)

    def _flush_all_nodes(self):
        """
        This function will connect all db nodes in the cluster and run "nodetool flush" command.
        :return:
        """
        for node in self.db_cluster.nodes:
            node.remoter.run('sudo nodetool flush')

if __name__ == '__main__':
    main()

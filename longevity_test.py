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

    default_params = {'timeout': 650000}

    def test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
        self.db_cluster.add_nemesis(nemesis=self.get_nemesis_class(),
                                    loaders=self.loaders,
                                    monitoring_set=self.monitors,
                                    test_index=self.test_index,
                                    test_type=self.test_type,
                                    test_id=self.test_id)
        stress_queue = list()
        write_queue = list()

        # prepare write workload
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        keyspace_num = self.params.get('keyspace_num', default=1)
        pre_create_schema = self.params.get('pre_create_schema', default=False)

        if prepare_write_cmd:
            # If the test load is too heavy for one lader (e.g. many keyspaces), the load should be splitted evenly
            # across the loaders (round_robin).
            if pre_create_schema:
                self._pre_create_schema()
            if keyspace_num > 1 and self.params.get('round_robin', default='').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num):
                    keyspace_name = 'keyspace{}'.format(i)
                    write_queue.append(self.run_stress_thread(stress_cmd=prepare_write_cmd, keyspace_name=keyspace_name))
                    time.sleep(5)
            else:
                write_queue.append(self.run_stress_thread(stress_cmd=prepare_write_cmd, keyspace_num=keyspace_num))
            self.db_cluster.wait_total_space_used_per_node()
            self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))
            for stress in write_queue:
                self.verify_stress_thread(queue=stress)

        stress_cmds = self.params.get('stress_cmd')
        stress_multiplier = self.params.get('stress_multiplier', default=1)
        if stress_multiplier > 1:
            stress_cmds *= stress_multiplier
        for stress_cmd in stress_cmds:
            params = {'stress_cmd': stress_cmd, 'keyspace_num': keyspace_num}
            if 'counter_' in stress_cmd:
                self._create_counter_table()
            if 'profile' in stress_cmd:
                cs_profile = re.search('profile=(.*)yaml', stress_cmd).group(1) + 'yaml'
                cs_profile = os.path.join(os.path.dirname(__file__), 'data_dir', os.path.basename(cs_profile))
                params.update({'profile': cs_profile})
            self.log.debug('stress cmd: {}'.format(stress_cmd))
            stress_queue.append(self.run_stress_thread(**params))
            if 'profile' in params:
                del params['profile']
        if not prepare_write_cmd:
            self.db_cluster.wait_total_space_used_per_node()
            self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        stress_read_cmd = self.params.get('stress_read_cmd', default=None)
        if stress_read_cmd:
            for stress_cmd in stress_read_cmd:
                self.log.debug('stress read cmd: {}'.format(stress_cmd))
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

    def _pre_create_schema(self):
        """
        For cases we are testing many keyspaces and tables, It's a possibility that we will do it better and faster than
        cassandra-stress.
        """
        node = self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)

        keyspace_num = self.params.get('keyspace_num', default=1)
        self.log.debug('Pre Creating Schema for c-s with {} keyspaces'.format(keyspace_num))

        for i in xrange(1, keyspace_num):
            keyspace_name = 'keyspace{}'.format(i)
            self.create_ks(session, keyspace_name, rf=3)
            self.log.debug('{} Created'.format(keyspace_name))
            self.create_cf(session,  'standard1', key_type='blob', read_repair=0.0, compact_storage=True,
                           columns={'"C0"': 'blob', '"C1"': 'blob', '"C2"': 'blob', '"C3"': 'blob', '"C4"': 'blob'})


if __name__ == '__main__':
    main()

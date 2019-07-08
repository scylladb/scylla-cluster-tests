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
# Copyright (c) 2019 ScyllaDB

import random
import string
import time

from sdcm.tester import ClusterTester

# performance_regression_row_level_repair_test.py
from sdcm.utils import measure_time


class PerformanceRegressionRowLevelRepairTest(ClusterTester):
    """
    Test Scylla performance regression with cassandra-stress.

    """

    KEYSPACE_NAME = "ks"
    TABLE_NAME = "cf"
    INT_COLUMNS = 20  # TODO: 99
    PARTITIONS = 30  # TODO: 100
    BIG_PARTITION_IDX = PARTITIONS + 1
    BIG_PARTITION_ROWS = 150  # TODO: 100000
    ROWS_IN_PARTITION = 15  # TODO: 30

    def __init__(self, *args, **kwargs):
        super(PerformanceRegressionRowLevelRepairTest, self).__init__(*args, **kwargs)

    # row-level-repair Test - measuring node-repair-time
    def test_row_level_repair(self):
        """
        Test steps:

        1. TODO: docstring
        """


        # Util functions ===============================================================================================

        def _get_cql_session(node=None):
            node = node or self.db_cluster.nodes[0]
            session = self.cql_connection_patient(node)
            return session

        def _get_cql_session_and_use_keyspace(node=None, keyspace=self.KEYSPACE_NAME):
            session = _get_cql_session(node=node)
            session.execute("USE {}".format(keyspace))
            return session

        def _create_update_command(column_expr, pk, ck, table_name=self.TABLE_NAME):
            cql_update_cmd = 'update {table_name} set {column_expr} where pk={pk} and ck={ck}'.format(**locals())
            self.log.debug("Generated CQL update command of: {}".format(cql_update_cmd))
            return cql_update_cmd

        def _update_table(table_name=self.TABLE_NAME, keyspace=self.KEYSPACE_NAME):
            self.log.debug('Update table')
            session = _get_cql_session_and_use_keyspace(keyspace=keyspace)

            num_of_updates = 2  # TODO: 50
            num_of_total_updates = num_of_updates * 2  # updating both a big partition and the largest partition.
            stmts = []
            self.log.debug(
                "Going to generate {} CQL updates, {} for big partition and for largest partition each".format(
                    num_of_total_updates, num_of_updates))
            for _ in range(num_of_updates):
                # Update/delete int columns to a random big partition
                column = random.randint(1, self.INT_COLUMNS - 1)
                column_name = 'c{}'.format(column)
                new_value = random.choice(['NULL', random.randint(0, 500000)])
                column_expr = '{} = {}'.format(column_name, new_value)
                stmts.append(_create_update_command(column_expr=column_expr,
                                                    pk=random.randint(1, self.PARTITIONS),
                                                    ck=random.randint(1, self.BIG_PARTITION_ROWS),
                                                    table_name=table_name))

                # Update/delete row inside the largest partition
                stmts.append(_create_update_command(column_expr=column_expr,
                                                    pk=self.BIG_PARTITION_IDX, ck=random.randint(1, self.BIG_PARTITION_ROWS),
                                                    table_name=table_name))

            for stmt in stmts:
                session.execute(stmt)

        def _pre_create_schema(table_name=self.TABLE_NAME, keyspace=self.KEYSPACE_NAME, int_columns=self.INT_COLUMNS):
            self.log.debug('Create schema')
            session = _get_cql_session()

            stmt = "CREATE KEYSPACE IF NOT EXISTS {}".format(
                keyspace) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
            session.execute(stmt)
            session.execute("USE {}".format(keyspace))
            stmt = 'create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, ' \
                   'PRIMARY KEY(pk, ck))'.format(table_name, ', '.join('c%d int' % i for i in xrange(1, int_columns)))
            session.execute(stmt)

        def _pre_fill_schema(table_name=self.TABLE_NAME, partitions=self.PARTITIONS, rows_in_partition=self.ROWS_IN_PARTITION):

            self.log.debug('Prefill schema')
            session = _get_cql_session_and_use_keyspace()

            # Prefill
            self.log.info('Create {} partitions with {} rows'.format(partitions, rows_in_partition))
            for i in xrange(1, partitions + 1):
                for k in xrange(1, rows_in_partition + 1):
                    str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                    stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                           '[{ilist}, {klist}], ' \
                           '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                           columns=', '.join(
                                                                               'c%d' % l for l in
                                                                               xrange(1, self.INT_COLUMNS)),
                                                                           int_values=', '.join(
                                                                               '%d' % l for l in
                                                                               xrange(1, self.INT_COLUMNS)),
                                                                           ilist=i, klist=k, open='{\'',
                                                                           set_value=str, close='\'}',
                                                                           map_value='{%d: \'%s\'}' % (k, str)
                                                                           )
                    session.execute(stmt)

            # Pre-fill the largest partition
            big_partition = partitions + 1
            big_partition_rows = self.BIG_PARTITION_ROWS  # TODO: 100000
            total_rows = partitions * rows_in_partition + big_partition_rows
            self.log.info('Create partition where pk = {} with {} rows'.format(big_partition, big_partition_rows))
            for k in xrange(1, big_partition_rows + 1):
                str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                       '[{ilist}, {klist}], ' \
                       '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                       columns=', '.join(
                                                                           'c%d' % l for l in xrange(1, self.INT_COLUMNS)),
                                                                       int_values=', '.join(
                                                                           '%d' % l for l in xrange(1, self.INT_COLUMNS)),
                                                                       ilist=big_partition, klist=k, open='{\'',
                                                                       set_value=str, close='\'}',
                                                                       map_value='{%d: \'%s\'}' % (k, str)
                                                                       )
                session.execute(stmt)

        @measure_time
        def _run_repair(node):
            self.log.info('Running nodetool repair on {}'.format(node.name))
            repair_cmd = 'nodetool -h localhost repair'
            result = node.run_nodetool(repair_cmd)

            return result

        # Util functions ===============================================================================================

        dict_specific_tested_stats = {'repair_runtime': -1}
        self.create_test_stats(specific_tested_stats=dict_specific_tested_stats)

        _pre_create_schema()
        _pre_fill_schema()

        self.log.info('Starting c-s/s-b write workload')
        prepare_write_cmd = self.params.get('prepare_write_cmd')
        prepare_cmd_queue = self.run_stress_thread(stress_cmd=prepare_write_cmd, duration=5)

        node2 = self.db_cluster.nodes[1]
        self.log.info('Stopping node-2 ({}) before updating cluster data'.format(node2.name))
        node2.stop_scylla_server()

        self.log.info('Updating cluster data when node2 ({}) is down'.format(node2.name))
        _update_table()

        self.log.info('Starting node-2 ({}) after updated cluster data'.format(node2.name))
        node2.start_scylla_server()

        self.log.info('Run Repair on node: {}'.format(node2.name))
        repair_time, res = _run_repair(node=node2)
        self.log.info('Repair time on node: {} is: {}'.format(node2.name, repair_time))

        dict_specific_tested_stats['repair_runtime'] = repair_time
        self.update_test_details(scylla_conf=True, dict_specific_tested_stats=dict_specific_tested_stats)

        self.check_specific_regression(dict_specific_tested_stats=dict_specific_tested_stats)


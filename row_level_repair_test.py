import random
import string
from multiprocessing import process

from longevity_test import LongevityTest

KEYSPACE_NAME = "ks"
TABLE_NAME = "cf"
INT_COLUMNS = 9 # TODO: 99
PARTITIONS = 100  # TODO: 100
BIG_PARTITION_IDX = PARTITIONS + 1
BIG_PARTITION_ROWS = 100  # TODO: 100000
ROWS_IN_PARTITION = 30

class RowLevelRepair(LongevityTest):

    """
    :avocado: enable
    """

    def __init__(self, *args, **kwargs):
        super(RowLevelRepair, self).__init__(*args, **kwargs)

    def _run_nodetool(self, cmd, node):
        try:
            result = node.remoter.run(cmd)
            self.log.debug("Command '%s' duration -> %s s", result.command,
                           result.duration)
            return result
        except process.CmdError, details:
            err = ("nodetool command '%s' failed on node %s: %s" %
                   (cmd, node, details.result))
            self.error_list.append(err)
            self.log.error(err)
            return None
        except Exception:
            err = 'Unexpected exception running nodetool'
            self.error_list.append(err)
            self.log.error(err, exc_info=True)
            return None

    def test_large_partitions_repair_performance(self):
        self._pre_create_schema2()
        self._pre_fill_schema2()

        self.log.info('Starting c-s/s-b write workload')
        stress_cmd = self.params.get('prepare_write_cmd')
        stress_cmd_queue = self.run_stress_thread(stress_cmd=stress_cmd, duration=5)

        node2 = self.db_cluster.nodes[1]
        self.log.info('Stopping node-2 ({}) before updating cluster data'.format(node2.name))
        node2.stop_scylla_server()

        self.log.info('Updating cluster data when node2 ({}) is down'.format(node2.name))
        # TODO: update some rows inside large partitions
        self._update_table()

        self.log.info('Starting node-2 ({}) after updated cluster data'.format(node2.name))
        node2.start_scylla_server()

        self.log.info('Running nodetool repair on node-2 ({})'.format(node2.name))
        repair_cmd = 'nodetool -h localhost repair'
        result = self._run_nodetool(cmd=repair_cmd, node=node2)
        self.log.debug(result)

        # self.test_custom_time()
        # self._test_custom_time()



    def _test_custom_time(self):
        """
        Run cassandra-stress with params defined in data_dir/scylla.yaml
        """
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
                self._pre_create_schema()
            # When the load is too heavy for one lader when using MULTI-KEYSPACES, the load is spreaded evenly across
            # the loaders (round_robin).
            if keyspace_num > 1 and self.params.get('round_robin', default='false').lower() == 'true':
                self.log.debug("Using round_robin for multiple Keyspaces...")
                for i in xrange(1, keyspace_num + 1):
                    keyspace_name = self._get_keyspace_name(i)
                    self._run_all_stress_cmds(write_queue, params={'stress_cmd':prepare_write_cmd,
                                                                   'keyspace_name': keyspace_name,
                                                                   'round_robin': True})
            # Not using round_robin and all keyspaces will run on all loaders
            else:
                self._run_all_stress_cmds(write_queue, params={'stress_cmd':prepare_write_cmd,
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
                self.verify_stress_thread(queue=stress)

            # Run nodetool flush on all nodes to make sure nothing left in memory
            self._flush_all_nodes()

            # In case we would like to verify all keys were written successfully before we start other stress / nemesis
            prepare_verify_cmd = self.params.get('prepare_verify_cmd', default=None)
            if prepare_verify_cmd:
                self._run_all_stress_cmds(verify_queue, params={'stress_cmd': prepare_verify_cmd,
                                                                'keyspace_num': keyspace_num})

                for stress in verify_queue:
                    self.verify_stress_thread(queue=stress)

        stress_cmd = self.params.get('stress_cmd', default=None)
        if stress_cmd:
            # Stress: Same as in prepare_write - allow the load to be spread across all loaders when using MULTI-KEYSPACES
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

        # Check if we shall wait for total_used_space or if nemesis wasn't started
        if not prepare_write_cmd or self.params.get('nemesis_during_prepare', default='true').lower() == 'false':
            self.db_cluster.wait_total_space_used_per_node(keyspace=None)
            self.db_cluster.start_nemesis(interval=self.params.get('nemesis_interval'))

        stress_read_cmd = self.params.get('stress_read_cmd', default=None)
        if stress_read_cmd:
            params = {'keyspace_num': keyspace_num, 'stress_cmd': stress_read_cmd}
            self._run_all_stress_cmds(stress_queue, params)

        for stress in stress_queue:
            self.verify_stress_thread(queue=stress)

    def _pre_create_schema(self, in_memory=False):
        node = self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)
        session.execute("""
                CREATE KEYSPACE IF NOT EXISTS scylla_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
        """)
        session.execute("""
                CREATE TABLE IF NOT EXISTS scylla_bench.test (
                pk bigint,
                ck bigint,
                v blob,
                PRIMARY KEY (pk, ck)
            ) WITH CLUSTERING ORDER BY (ck ASC)
                AND bloom_filter_fp_chance = 0.01
                AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
                AND comment = ''
                AND compression = {}
                AND crc_check_chance = 1.0
                AND dclocal_read_repair_chance = 0.0
                AND default_time_to_live = 0
                AND gc_grace_seconds = 864000 
                AND max_index_interval = 2048
                AND memtable_flush_period_in_ms = 0
                AND min_index_interval = 128
                AND read_repair_chance = 0.0
                AND speculative_retry = 'NONE';
                        """)

    def _create_update_command(self, column_expr, pk, ck, table_name=TABLE_NAME):
        cql_update_cmd = 'update {table_name} set {column_expr} where pk={pk} and ck={ck}'.format(**locals())
        self.log.debug("Generated CQL update command of: {}".format(cql_update_cmd))
        return cql_update_cmd

    def _pre_create_schema2(self, table_name=TABLE_NAME, keyspace=KEYSPACE_NAME):

        self.log.debug('Create schema')
        session = self._get_cql_session_and_use_keyspace(keyspace=keyspace)

        INT_COLUMNS = 99
        stmt = "CREATE KEYSPACE IF NOT EXISTS {}".format(keyspace) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}"
        session.execute(stmt)
        session.execute("USE {}".format(keyspace))
        stmt = 'create table {} (pk int, ck int, {}, clist list<int>, cset set<text>, cmap map<int, text>, ' \
               'PRIMARY KEY(pk, ck))'.format(table_name, ', '.join('c%d int' % i for i in xrange(1, INT_COLUMNS)))
        session.execute(stmt)

    def _update_table(self, table_name=TABLE_NAME, keyspace=KEYSPACE_NAME):
        self.log.debug('Update table')
        session = self._get_cql_session_and_use_keyspace(keyspace=keyspace)

        num_of_updates = 20  # TODO: 50
        num_of_total_updates = num_of_updates * 2  # updating both a big partition and the largest partition.
        stmts = []
        self.log.debug("Going to generate {} CQL updates, {} for big partition and for largest partition each".format(
            num_of_total_updates, num_of_updates))
        for _ in range(num_of_updates):
            # Update/delete int columns to a random big partition
            column = random.randint(1, INT_COLUMNS - 1)
            column_name = 'c{}'.format(column)
            new_value = random.choice(['NULL', random.randint(0, 500000)])
            column_expr = '{} = {}'.format(column_name, new_value)
            stmts.append(self._create_update_command(column_expr=column_expr,
                                               pk=random.randint(1, PARTITIONS), ck=random.randint(1, ROWS_IN_PARTITION)))

            # Update/delete row inside the largest partition
            stmts.append(self._create_update_command(column_expr=column_expr,
                                               pk=BIG_PARTITION_IDX, ck=random.randint(1, BIG_PARTITION_ROWS)))

        for stmt in stmts:
            session.execute(stmt)

    def _get_cql_session_and_use_keyspace(self, node=None, keyspace=KEYSPACE_NAME):
        node = node or self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)
        session.execute("USE {}".format(keyspace))
        return session

    def _pre_fill_schema2(self, table_name=TABLE_NAME):

        self.log.debug('Prefill schema')
        session = self._get_cql_session_and_use_keyspace()

        # Prefill
        partitions = PARTITIONS  # TODO: 100
        rows_in_partition = ROWS_IN_PARTITION  # TODO: 1000
        self.log.debug('Create {} partitions with {} rows'.format(partitions, rows_in_partition))
        for i in xrange(1, partitions + 1):
            for k in xrange(1, rows_in_partition + 1):
                str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
                stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                       '[{ilist}, {klist}], ' \
                       '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                       columns=', '.join(
                                                                           'c%d' % l for l in xrange(1, INT_COLUMNS)),
                                                                       int_values=', '.join(
                                                                           '%d' % l for l in xrange(1, INT_COLUMNS)),
                                                                       ilist=i, klist=k, open='{\'',
                                                                       set_value=str, close='\'}',
                                                                       map_value='{%d: \'%s\'}' % (k, str)
                                                                       )
                session.execute(stmt)

        # Pre-fill the largest partition
        big_partition = partitions + 1
        big_partition_rows = BIG_PARTITION_ROWS  # TODO: 100000
        total_rows = partitions * rows_in_partition + big_partition_rows
        self.log.debug('Create partition where pk = {} with {} rows'.format(big_partition, big_partition_rows))
        for k in xrange(1, big_partition_rows + 1):
            str = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
            stmt = 'insert into {table_name} (pk, ck, {columns}, clist, cset, cmap) values ({ilist}, {klist}, {int_values}, ' \
                   '[{ilist}, {klist}], ' \
                   '{open}{set_value}{close}, {map_value})'.format(table_name=table_name,
                                                                   columns=', '.join(
                                                                       'c%d' % l for l in xrange(1, INT_COLUMNS)),
                                                                   int_values=', '.join(
                                                                       '%d' % l for l in xrange(1, INT_COLUMNS)),
                                                                   ilist=big_partition, klist=k, open='{\'',
                                                                   set_value=str, close='\'}',
                                                                   map_value='{%d: \'%s\'}' % (k, str)
                                                                   )
            session.execute(stmt)


# prepare_write_cmd:  ["scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=10 -clustering-row-count=10000000 -clustering-row-size=5120 -concurrency=200 -rows-per-request=10",
#                      "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=10 -clustering-row-count=10000000 -clustering-row-size=5120 -rows-per-request=10 -concurrency=200 -max-rate=32000 -duration=10080m"
# ]
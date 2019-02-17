import random
import string
import time
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

        @measureTime # TODO: a temporary decorator to be replaced by infra decorator integrated with Elastic-search
        def _run_repair():
            self.log.info('Running nodetool repair on node-2 ({})'.format(node2.name))
            repair_cmd = 'nodetool -h localhost repair'
            result = self._run_nodetool(cmd=repair_cmd, node=node2)
            # the most accurate value is received in result.duration
            # sample result is:
            # Exit status: 0
            # Duration: 12.290612936
            # Stdout:
            # [2019-02-17 21:29:52,701] Starting repair command #1, repairing 1 ranges for keyspace system_traces (parallelism=SEQUENTIAL, full=true)
            # [2019-02-17 21:29:58,797] Repair session 1
            # [2019-02-17 21:29:58,797] Repair session 1 finished
            # [2019-02-17 21:29:58,834] Starting repair command #2, repairing 1 ranges for keyspace ks (parallelism=SEQUENTIAL, full=true)
            # [2019-02-17 21:30:01,950] Repair session 2
            # [2019-02-17 21:30:01,950] Repair session 2 finished
            # [2019-02-17 21:30:01,993] Starting repair command #3, repairing 1 ranges for keyspace system_auth (parallelism=SEQUENTIAL, full=true)
            # [2019-02-17 21:30:02,104] Repair session 3
            # [2019-02-17 21:30:02,105] Repair session 3 finished

        repair_time = _run_repair()
        self.log.debug("Repair time: {}".format(repair_time))

    # def _pre_create_schema(self, in_memory=False):
    #     node = self.db_cluster.nodes[0]
    #     session = self.cql_connection_patient(node)
    #     session.execute("""
    #             CREATE KEYSPACE IF NOT EXISTS scylla_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
    #     """)
    #     session.execute("""
    #             CREATE TABLE IF NOT EXISTS scylla_bench.test (
    #             pk bigint,
    #             ck bigint,
    #             v blob,
    #             PRIMARY KEY (pk, ck)
    #         ) WITH CLUSTERING ORDER BY (ck ASC)
    #             AND bloom_filter_fp_chance = 0.01
    #             AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    #             AND comment = ''
    #             AND compression = {}
    #             AND crc_check_chance = 1.0
    #             AND dclocal_read_repair_chance = 0.0
    #             AND default_time_to_live = 0
    #             AND gc_grace_seconds = 864000
    #             AND max_index_interval = 2048
    #             AND memtable_flush_period_in_ms = 0
    #             AND min_index_interval = 128
    #             AND read_repair_chance = 0.0
    #             AND speculative_retry = 'NONE';
    #                     """)

    def _create_update_command(self, column_expr, pk, ck, table_name=TABLE_NAME):
        cql_update_cmd = 'update {table_name} set {column_expr} where pk={pk} and ck={ck}'.format(**locals())
        self.log.debug("Generated CQL update command of: {}".format(cql_update_cmd))
        return cql_update_cmd

    def _pre_create_schema2(self, table_name=TABLE_NAME, keyspace=KEYSPACE_NAME):

        self.log.debug('Create schema')
        session = self._get_cql_session()

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

    def _get_cql_session(self, node=None):
        node = node or self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)
        return session

    def _get_cql_session_and_use_keyspace(self, node=None, keyspace=KEYSPACE_NAME):
        session = self._get_cql_session(node=node)
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



def measureTime(func, *args, **kwargs):
    def wrapped():
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        return end - start
    return wrapped

from longevity_test import LongevityTest


class LargePartitionLongevetyTest(LongevityTest):

    """
    :avocado: enable
    """

    def __init__(self, *args, **kwargs):
        super(LargePartitionLongevetyTest, self).__init__(*args, **kwargs)

    def test_large_partition_longevity(self):
        self._pre_create_schema()
        self.test_custom_time()

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


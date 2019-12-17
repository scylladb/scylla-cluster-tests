from longevity_test import LongevityTest
from test_lib.compaction import CompactionStrategy


class LargePartitionLongevityTest(LongevityTest):

    def test_large_partition_longevity(self):
        compaction_strategy = self.params.get('compaction_strategy', default=CompactionStrategy.SIZE_TIERED.value)
        self.pre_create_large_partitions_schema(compaction_strategy=compaction_strategy)
        self.test_custom_time()

    def pre_create_large_partitions_schema(self, compaction_strategy=CompactionStrategy.SIZE_TIERED.value):
        node = self.db_cluster.nodes[0]
        compaction_strategy_option = "AND compaction = {{'class': '{}'}};".format(compaction_strategy)
        create_table_query = """
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
                    AND speculative_retry = 'NONE'
                    """+compaction_strategy_option

        with self.cql_connection_patient(node) as session:
            # pylint: disable=no-member
            session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS scylla_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}
            """)
            session.execute(create_table_query)

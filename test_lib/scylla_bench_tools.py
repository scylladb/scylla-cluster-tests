import logging

LOGGER = logging.getLogger(__name__)


def create_scylla_bench_table_query(compaction_strategy=None):
    """

    :return: cql create table query for scylla-bench
    """

    compaction_strategy_option = (
        "AND compaction = {{'class': '{}'}};".format(compaction_strategy) if compaction_strategy else ""
    )
    scylla_bench_table_query = (
        """
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
                    """
        + compaction_strategy_option
    )
    return scylla_bench_table_query

import logging
import random
from enum import Enum

import yaml

LOGGER = logging.getLogger(__name__)


class CompactionStrategy(Enum):
    LEVELED = "LeveledCompactionStrategy"
    SIZE_TIERED = "SizeTieredCompactionStrategy"
    TIME_WINDOW = "TimeWindowCompactionStrategy"
    INCREMENTAL = "IncrementalCompactionStrategy"
    IN_MEMORY = "InMemoryCompactionStrategy"

    @classmethod
    def from_str(cls, output_str):
        try:
            return CompactionStrategy[CompactionStrategy(output_str).name]
        except AttributeError as attr_err:
            err_msg = "Could not recognize compaction strategy value: {} - {}".format(output_str, attr_err)
            raise ValueError(err_msg) from attr_err


def get_compaction_strategy(node, keyspace, table):
    """Get a given table compaction strategy

    Arguments:
        node {str} -- ip of db_node
        keyspace
        table
    """
    list_tables_compaction = node.run_cqlsh('SELECT keyspace_name, table_name, compaction FROM system_schema.tables',
                                            split=True)
    compaction = 'N/A'
    for row in list_tables_compaction:
        if '|' not in row:
            continue
        list_stripped_values = [val.strip() for val in row.split('|')]
        if list_stripped_values[0] == keyspace and list_stripped_values[1] == table:
            dict_compaction_values = yaml.safe_load(list_stripped_values[2])
            compaction = CompactionStrategy.from_str(output_str=dict_compaction_values['class'])
            break

    LOGGER.debug("Query result for {}.{} compaction is: {}".format(keyspace, table, compaction))
    return compaction


def get_compaction_random_additional_params():
    """

    :return: list_additional_params
    """
    bucket_high = round(random.uniform(1.1, 1.8), 2)
    bucket_low = round(random.uniform(0.3, 0.8), 2)
    min_sstable_size = random.randint(10, 500)
    min_threshold = random.randint(2, 10)
    max_threshold = random.randint(4, 64)
    sstable_size_in_mb = random.randint(50, 2000)
    list_additional_params = [{'bucket_high': bucket_high}, {'bucket_low': bucket_low},
                              {'min_sstable_size': min_sstable_size}, {'min_threshold': min_threshold},
                              {'max_threshold': max_threshold}, {'sstable_size_in_mb': sstable_size_in_mb}]

    return list_additional_params

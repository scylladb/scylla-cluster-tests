import logging
import random
from dataclasses import dataclass, fields
from enum import Enum
from typing import Optional

import yaml

from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class GcMode(Enum):
    REPAIR = "repair"
    DISABLED = "disabled"
    TIMEOUT = "timeout"
    IMMEDIATE = "immediate"


class CompactionStrategy(Enum):
    LEVELED = "LeveledCompactionStrategy"
    SIZE_TIERED = "SizeTieredCompactionStrategy"
    TIME_WINDOW = "TimeWindowCompactionStrategy"
    INCREMENTAL = "IncrementalCompactionStrategy"

    @classmethod
    def from_str(cls, output_str):
        try:
            return CompactionStrategy[CompactionStrategy(output_str).name]
        except AttributeError as attr_err:
            err_msg = "Could not recognize compaction strategy value: {} - {}".format(output_str, attr_err)
            raise ValueError(err_msg) from attr_err


@dataclass
class TimeWindowCompactionProperties:
    class_name: str = ''
    compaction_window_unit: Optional[str] = None
    compaction_window_size: Optional[int] = None
    expired_sstable_check_frequency_seconds: Optional[int] = None
    min_threshold: Optional[int] = None
    max_threshold: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict, **kwargs):
        field_names = [field.name for field in fields(cls)]
        return cls(**{key: val for key, val in data.items() if key in field_names}, **kwargs)


def get_gc_mode(node: BaseNode, keyspace: str, table: str) -> str | GcMode:
    """Get a given table GC mode

    :Arguments:
        node
        keyspace
        table
    """
    table_gc_mode_result = node.run_cqlsh(
        f"SELECT extensions FROM system_schema.tables where keyspace_name = '{keyspace}' and table_name = '{table}'",
        split=True)
    LOGGER.debug("Query result for %s.%s GC mode is: %s", keyspace, table, table_gc_mode_result)
    gc_mode = 'N/A'
    if table_gc_mode_result and len(table_gc_mode_result) >= 4:
        extensions_value = table_gc_mode_result[3]
        # TODO: A temporary workaround until 5.0 query-table-extensions issue is fixed:
        # https://github.com/scylladb/scylla/issues/10309
        if '6d6f646506000000' in extensions_value:
            return GcMode.REPAIR
        elif '6d6f646507000000' in extensions_value:
            return GcMode.TIMEOUT
        elif '6d6f646509000000' in extensions_value:
            return GcMode.IMMEDIATE
        elif '6d6f646508000000' in extensions_value:
            return GcMode.DISABLED

    LOGGER.debug("Query result for %s.%s GC mode is: %s", keyspace, table, gc_mode)
    return gc_mode


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


def get_table_compaction_info(keyspace: str, table: str, session: object):

    query = f"SELECT compaction FROM system_schema.tables WHERE keyspace_name = '{keyspace}' AND table_name = '{table}';"
    result = session.execute(query).one()
    LOGGER.debug(f"Query result for {keyspace}.{table} compaction is: {result}")

    if result and result.compaction:
        compaction_dict = result.compaction
        compaction_properties = TimeWindowCompactionProperties.from_dict(data=compaction_dict)
    else:
        compaction_properties = TimeWindowCompactionProperties(class_name='')

    return compaction_properties


def get_compaction_random_additional_params(strategy: CompactionStrategy):
    """

    :return: list_additional_params
    """
    bucket_high = round(random.uniform(1.1, 1.8), 2)
    bucket_low = round(random.uniform(0.3, 0.8), 2)
    min_sstable_size = random.randint(10, 500)
    min_threshold = random.randint(2, 10)
    max_threshold = random.randint(4, 64)
    sstable_size_in_mb = random.randint(50, 2000)

    # doc : https://github.com/scylladb/scylladb/blob/d543b96d1837c73f2ded42d4c5c8de17005c2f36/docs/cql/compaction.rst
    list_additional_params_options = {
        CompactionStrategy.LEVELED: [{'sstable_size_in_mb': sstable_size_in_mb}],
        CompactionStrategy.SIZE_TIERED:
            [{'bucket_high': bucket_high}, {'bucket_low': bucket_low}, {'min_sstable_size': min_sstable_size},
             {'min_threshold': min_threshold}, {'max_threshold': max_threshold}],
        CompactionStrategy.TIME_WINDOW: [{'min_threshold': min_threshold}, {'max_threshold': max_threshold}],
        CompactionStrategy.INCREMENTAL:
            [{'bucket_high': bucket_high}, {'bucket_low': bucket_low}, {'min_sstable_size': min_sstable_size},
             {'min_threshold': min_threshold}, {'max_threshold': max_threshold},
             {'sstable_size_in_mb': sstable_size_in_mb}]
    }
    return list_additional_params_options[strategy]


def calculate_allowed_twcs_ttl(
    compaction_properties: TimeWindowCompactionProperties,
    default_min_ttl: int = 864000,  # 10 days
    global_max_ttl: int = 4_300_000,  # 49 days
):
    # TODO retrieve twcs_max_window_count from scylla
    compaction_window_size = int(compaction_properties.compaction_window_size or 1)
    compaction_window_unit = (compaction_properties.compaction_window_unit or 'DAYS').upper()
    LOGGER.debug(f'Compaction window size: {compaction_window_size}, Unit: {compaction_window_unit}')

    unit_multipliers = {
        'MINUTES': 60,
        'HOURS': 3600,
        'DAYS': 86400,
        'WEEKS': 604800,
    }
    multiplier = unit_multipliers.get(compaction_window_unit, 86400)
    window_size_in_seconds = compaction_window_size * multiplier

    # twcs_max_window_count default value is 50;
    # opensource.docs.scylladb.com/stable/reference/configuration-parameters.html
    twcs_max_window_count = 50

    twcs_limit = twcs_max_window_count * window_size_in_seconds
    safe_max_ttl = min(global_max_ttl, twcs_limit)
    safe_min_ttl = max(1, min(default_min_ttl, safe_max_ttl))

    safe_max_ttl = max(1, safe_max_ttl)
    if safe_min_ttl > safe_max_ttl:
        return 1

    return random.randint(safe_min_ttl, safe_max_ttl)

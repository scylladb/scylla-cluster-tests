"""
Module containing all ModifyTable* nemesis classes.

Each class modifies a specific table property. The shared logic is captured in
the abstract base class ``ModifyTableBaseMonkey``.
"""

import abc
import logging
import time
from typing import Any, Dict

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.common import generate_random_string
from test_lib.compaction import (
    CompactionStrategy,
    calculate_allowed_twcs_ttl,
    get_compaction_strategy,
    get_table_compaction_info,
)

LOGGER = logging.getLogger(__name__)


class ModifyTableBaseMonkey(NemesisBaseClass, abc.ABC):
    """
    Abstract base class for all ModifyTable nemesis operations.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.random = self.runner.random

    def modify_table_property(
        self,
        name: str,
        val: Any,
        filter_out_table_with_counter=False,
        keyspace_table: str | None = None,
    ) -> None:
        """
        Helper method to modify a table property using CQL.
        This is the core of all ModifyTable* disruptions.
        It takes care of picking a random non-system table if one is not specified, and then executes the appropriate CQL command to modify the property.
        This keeps the low-level CQL execution in one place while exposing it through a simple interface to all the different disruptions that modify table properties.
        """
        disruption_name = "".join([p.strip().capitalize() for p in name.split("_")])
        InfoEvent("ModifyTableProperties%s %s" % (disruption_name, self.runner.target_node)).publish()

        if not keyspace_table:
            ks_cfs = self.cluster.get_non_system_ks_cf_list(
                db_node=self.runner.target_node,
                filter_out_table_with_counter=filter_out_table_with_counter,
                filter_out_mv=True,
            )  # not allowed to modify MV

            keyspace_table = self.random.choice(ks_cfs) if ks_cfs else ks_cfs

        if not keyspace_table:
            raise UnsupportedNemesis(
                "Non-system keyspace and table are not found. ModifyTableProperties nemesis can't be run"
            )

        cmd = "ALTER TABLE {keyspace_table} WITH {name} = {val};".format(
            keyspace_table=keyspace_table, name=name, val=val
        )
        self.runner.actions_log.info(f"Modify table property on {keyspace_table}: {name} = {val}")
        with self.runner.cluster.cql_connection_patient(self.runner.target_node) as session:
            session.execute(cmd)


# ---------------------------------------------------------------------------
# Concrete nemesis classes
# ---------------------------------------------------------------------------


class ModifyTableCommentMonkey(ModifyTableBaseMonkey):
    """Adds a random comment to a test table."""

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="comment", val=f"'{generate_random_string(24)}'")


class ModifyTableGcGraceTimeMonkey(ModifyTableBaseMonkey):
    """Modifies ``gc_grace_seconds`` to a random value.

    The number of seconds after data is marked with a tombstone (deletion
    marker) before it is eligible for garbage-collection.
    default: ``gc_grace_seconds = 864000``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="gc_grace_seconds", val=self.random.randint(216000, 864000))


class ModifyTableCachingMonkey(ModifyTableBaseMonkey):
    """Modifies the caching property of a test table.

    Caching optimizes the use of cache memory by a table without manual tuning.
    default: ``caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        prop_val = dict(
            keys=self.random.choice(["NONE", "ALL"]),
            rows_per_partition=self.random.choice(["NONE", "ALL", self.random.randint(1, 10000)]),
        )
        self.modify_table_property(name="caching", val=str(prop_val))


class ModifyTableBloomFilterFpChanceMonkey(ModifyTableBaseMonkey):
    """Modifies the Bloom filter false-positive probability.

    The Bloom filter sets the false-positive probability for SSTable Bloom
    filters.  Lower values result in larger Bloom filters that use more memory.
    default: ``bloom_filter_fp_chance = 0.01``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        # minimum value cannot be 0, as that would require "infinite" memory
        # the actual minimum value is 6.71e-05, as declared in `min_supported_bloom_filter_fp_chance()`
        self.modify_table_property(name="bloom_filter_fp_chance", val=self.random.uniform(6.71e-05, 0.5))


class ModifyTableCompactionMonkey(ModifyTableBaseMonkey):
    """Modifies the compaction strategy of a test table.

    Picks a random compaction strategy (SizeTiered, Leveled, or TimeWindow)
    with random parameters within valid ranges.
    default: ``compaction = {'class': 'SizeTieredCompactionStrategy', ...}``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        strategies = [
            lambda: {
                "class": "SizeTieredCompactionStrategy",
                "bucket_high": self.random.uniform(1.2, 2.0),
                "bucket_low": self.random.uniform(0.3, 0.7),
                "min_sstable_size": self.random.randint(10, 100),
                "min_threshold": self.random.randint(2, 6),
                "max_threshold": self.random.randint(10, 32),
            },
            lambda: {
                "class": "LeveledCompactionStrategy",
                "sstable_size_in_mb": self.random.randint(100, 200),
            },
            lambda: {
                "class": "TimeWindowCompactionStrategy",
                "compaction_window_unit": "DAYS",
                "compaction_window_size": self.random.randint(1, 7),
                "expired_sstable_check_frequency_seconds": self.random.randint(300, 1200),
                "min_threshold": self.random.randint(2, 6),
                "max_threshold": self.random.randint(10, 32),
            },
        ]

        # Pick a random strategy and get its properties.
        prop_val = self.random.choice(strategies)()

        if prop_val["class"] == "TimeWindowCompactionStrategy":
            # Max allowed TTL - 49 days (4300000) (to be compatible with default TWCS settings)
            self.modify_table_property(
                name="default_time_to_live", val=str(4300000), filter_out_table_with_counter=True
            )

        self.modify_table_property(name="compaction", val=str(prop_val))


class ModifyTableCompressionMonkey(ModifyTableBaseMonkey):
    """Modifies the compression algorithm of a test table.

    Valid algorithms: LZ4Compressor, SnappyCompressor, DeflateCompressor,
    ZstdCompressor (or no compression).
    default: ``compression = {}``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        algos = (
            "",  # no compression
            "LZ4Compressor",
            "SnappyCompressor",
            "DeflateCompressor",
            "ZstdCompressor",
        )
        algo = self.random.choice(algos)
        prop_val = {"sstable_compression": algo}
        if algo:
            prop_val["chunk_length_kb"] = self.random.choice(["4K", "64KB", "128KB"])
            prop_val["crc_check_chance"] = self.random.random()
        self.modify_table_property(name="compression", val=str(prop_val))


class ModifyTableCrcCheckChanceMonkey(ModifyTableBaseMonkey):
    """Modifies ``crc_check_chance``.

    Not implemented in ScyllaDB — value is ignored, testing only for
    backwards compatibility.
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="crc_check_chance", val=self.random.random())


class ModifyTableDclocalReadRepairChanceMonkey(ModifyTableBaseMonkey):
    """Modifies ``dclocal_read_repair_chance``.

    The probability that a successful read operation triggers a read repair,
    limited to replicas in the same DC as the coordinator.
    default: ``dclocal_read_repair_chance = 0.1``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="dclocal_read_repair_chance", val=self.random.choice([0, 0.2, 0.5, 0.9]))


class ModifyTableDefaultTimeToLiveMonkey(ModifyTableBaseMonkey):
    """Modifies ``default_time_to_live`` for a test table.

    The value of this property is a number of seconds.  When the table TTL
    is exceeded, Cassandra tombstones the table.
    default: ``default_time_to_live = 0``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        # Select table without columns with "counter" type — issue #1037
        # max allowed TTL - 49 days (4300000) (to be compatible with default TWCS settings)

        default_min_ttl = 864000  # 10 days in seconds
        default_max_ttl = 4300000

        runner = self.runner
        ks_cfs = runner.cluster.get_non_system_ks_cf_list(
            db_node=runner.target_node, filter_out_table_with_counter=True, filter_out_mv=True
        )

        if not ks_cfs:
            raise UnsupportedNemesis("No non-system user tables found")

        keyspace_table = self.random.choice(ks_cfs) if ks_cfs else ks_cfs
        keyspace, table = keyspace_table.split(".")
        compaction_strategy = get_compaction_strategy(node=runner.target_node, keyspace=keyspace, table=table)

        if compaction_strategy == CompactionStrategy.TIME_WINDOW:
            with runner.cluster.cql_connection_patient(runner.target_node) as session:
                LOGGER.debug("Getting data from Scylla node: %s, table: %s", runner.target_node, keyspace_table)
                compaction_properties = get_table_compaction_info(keyspace=keyspace, table=table, session=session)
            ttl_to_set = calculate_allowed_twcs_ttl(compaction_properties, default_min_ttl, default_max_ttl)
        else:
            ttl_to_set = default_max_ttl

        InfoEvent(f"New default time to live to be set: {ttl_to_set}, for table: {keyspace_table}").publish()
        self.modify_table_property(
            name="default_time_to_live",
            val=ttl_to_set,
            filter_out_table_with_counter=True,
            keyspace_table=keyspace_table,
        )


class ModifyTableMaxIndexIntervalMonkey(ModifyTableBaseMonkey):
    """Modifies ``max_index_interval``.

    If the total memory usage of all index summaries reaches this value,
    Cassandra decreases the index summaries for the coldest SSTables.
    default: ``max_index_interval = 2048``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="max_index_interval", val=self.random.choice([1024, 4096, 8192]))


class ModifyTableMinIndexIntervalMonkey(ModifyTableBaseMonkey):
    """Modifies ``min_index_interval``.

    The minimum gap between index entries in the index summary.
    default: ``min_index_interval = 128``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="min_index_interval", val=self.random.choice([128, 256, 512]))


class ModifyTableMemtableFlushPeriodInMsMonkey(ModifyTableBaseMonkey):
    """Modifies ``memtable_flush_period_in_ms``.

    The number of milliseconds before Cassandra flushes memtables
    associated with this table.
    default: ``memtable_flush_period_in_ms = 0``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(
            name="memtable_flush_period_in_ms", val=self.random.choice([0, self.random.randint(60000, 200000)])
        )


class ModifyTableReadRepairChanceMonkey(ModifyTableBaseMonkey):
    """Modifies ``read_repair_chance``.

    The probability that a successful read operation will trigger a read
    repair (not limited to replicas in the same DC).
    default: ``read_repair_chance = 0.0``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        self.modify_table_property(name="read_repair_chance", val=self.random.choice([0, 0.2, 0.5, 0.9]))


class ModifyTableSpeculativeRetryMonkey(ModifyTableBaseMonkey):
    """Modifies the ``speculative_retry`` property.

    Configures rapid read protection.  Options: ALWAYS, Xpercentile, Nms,
    NONE.
    default: ``speculative_retry = '99.0PERCENTILE'``
    """

    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True

    def disrupt(self):
        options = (
            "'ALWAYS'",
            "'%spercentile'" % self.random.randint(95, 99),
            "'%sms'" % self.random.randint(300, 1000),
        )
        self.modify_table_property(name="speculative_retry", val=self.random.choice(options))


class ModifyTableTwcsWindowSizeMonkey(ModifyTableBaseMonkey):
    """Changes window size for tables with TWCS.

    After window size of TWCS changed, tables should be reshaped.
    Process should not bring write amplification if size of sstables
    in timewindow differs significantly.
    """

    kubernetes = True
    xcloud = False
    limited = True
    schema_changes = True
    free_tier_set = True
    modify_table = True
    disruptive = True

    def set_new_twcs_settings(self, settings: Dict[str, Any]) -> Dict[str, Any]:
        """Adjust window unit and size with random increments in acceptable borders,
        ensuring the final TTL does not exceed 4,300,000 seconds (~49 days).
        """
        self.runner.log.debug("Initial TWCS settings are: %s", settings)
        MAX_TTL = 4_300_000  # ~49 days in seconds
        expected_sstable_number = 35

        compaction = settings["compaction"]
        current_unit = compaction.get("compaction_window_unit", "DAYS")
        current_size = int(compaction.get("compaction_window_size", 1))

        random_increments = {
            "MINUTES": (10, 90),
            "HOURS": (2, 24),
            "DAYS": (1, 10),
        }

        inc_min, inc_max = random_increments.get(current_unit, (1, 5))
        increment = self.random.randint(inc_min, inc_max)
        current_size += increment

        unit_multipliers = {
            "DAYS": 24 * 3600,
            "HOURS": 3600,
            "MINUTES": 60,
        }

        multiplier = unit_multipliers.get(current_unit, unit_multipliers["DAYS"])
        proposed_ttl = current_size * multiplier * expected_sstable_number

        if proposed_ttl > MAX_TTL:
            current_size = MAX_TTL // (multiplier * expected_sstable_number)
            current_size = max(current_size, 1)

            proposed_ttl = current_size * multiplier * expected_sstable_number

        settings["gc"] = proposed_ttl // 2
        settings["dttl"] = proposed_ttl
        settings["compaction"]["compaction_window_unit"] = current_unit
        settings["compaction"]["compaction_window_size"] = current_size

        self.runner.log.debug("New TWCS settings are: %s", settings)
        return settings

    def disrupt(self):
        runner = self.runner

        all_ks_cs_with_twcs = runner.cluster.get_all_tables_with_twcs(runner.target_node)
        runner.log.debug("All tables with TWCS %s", all_ks_cs_with_twcs)

        if not all_ks_cs_with_twcs:
            raise UnsupportedNemesis("No table found with TWCS")

        target_ks_cs_with_settings = self.random.choice(all_ks_cs_with_twcs)

        ks_cs_settings = self.set_new_twcs_settings(target_ks_cs_with_settings)
        keyspace, table = ks_cs_settings["name"].split(".")

        num_sstables_before_change = len(runner.target_node.get_list_of_sstables(keyspace, table, suffix="-Data.db"))

        runner.log.debug("New TWCS settings: %s", str(ks_cs_settings))
        self.modify_table_property(
            name="compaction", val=ks_cs_settings["compaction"], keyspace_table=ks_cs_settings["name"]
        )
        self.modify_table_property(
            name="default_time_to_live", val=ks_cs_settings["dttl"], keyspace_table=ks_cs_settings["name"]
        )
        self.modify_table_property(
            name="gc_grace_seconds", val=ks_cs_settings["gc"], keyspace_table=ks_cs_settings["name"]
        )

        with runner.action_log_scope("Waiting for schema agreement"):
            runner.cluster.wait_for_schema_agreement()
        # wait timeout equal 2% of test duration for generating sstables with timewindow settings
        sleep_timeout = int(0.02 * runner.tester.params["test_duration"])
        time.sleep(sleep_timeout)

        with runner.action_log_scope(f"Stopping Scylla on {runner.target_node.name}"):
            runner.target_node.stop_scylla()

        reshape_twcs_records = runner.target_node.follow_system_log(
            patterns=[
                "need reshape. Starting reshape process",
                "Reshaping",
                f"Reshape {ks_cs_settings['name']} .* Reshaped",
            ]
        )
        with runner.action_log_scope(f"Starting Scylla on {runner.target_node.name}"):
            runner.target_node.start_scylla()

        reshape_twcs_records = list(reshape_twcs_records)
        if not reshape_twcs_records:
            runner.log.warning(
                "Log message with sstables for reshape was not found. Autocompaction already"
                "compact sstables by timewindows"
            )
            runner.actions_log.info("TWCS reshape was not needed")
        runner.actions_log.info("TWCS reshape completed")
        runner.log.debug("Reshape log %s", reshape_twcs_records)

        num_sstables_after_change = len(runner.target_node.get_list_of_sstables(keyspace, table, suffix="-Data.db"))

        runner.log.info(
            "Number of sstables before: %s and after %s change twcs settings",
            num_sstables_before_change,
            num_sstables_after_change,
        )
        if num_sstables_before_change > num_sstables_after_change:
            runner.log.error("Number of sstables after change settings larger than before")
        # run major compaction on all nodes
        # to reshape sstables on other nodes
        for node in runner.cluster.data_nodes:
            num_sstables_before_change = len(node.get_list_of_sstables(keyspace, table, suffix="-Data.db"))
            with runner.action_log_scope(f"Run {keyspace}.{table} major compaction on {node.name}"):
                node.run_nodetool("compact", args=f"{keyspace} {table}")
            num_sstables_after_change = len(node.get_list_of_sstables(keyspace, table, suffix="-Data.db"))
            runner.log.info(
                "Number of sstables before: %s and after %s change twcs settings on node: %s",
                num_sstables_before_change,
                num_sstables_after_change,
                node.name,
            )
            if num_sstables_before_change > num_sstables_after_change:
                runner.log.error("Number of sstables after change settings larger than before")

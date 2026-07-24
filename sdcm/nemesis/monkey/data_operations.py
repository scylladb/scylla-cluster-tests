"""
Data manipulation nemesis: truncate, delete partitions/rows, add/drop columns.

Extracted from NemesisRunner as part of SCT-209.
"""

import logging
import math
import string
import time
from collections import defaultdict
from functools import partial

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra.query import SimpleStatement

from sdcm.cluster import HOUR_IN_SEC
from sdcm.exceptions import PartitionNotFound, TimestampNotFound, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.nemesis.utils.common import nemesis_stress_failure_handler, prepare_test_table
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.common import get_db_tables
from sdcm.utils.issues import SkipPerIssues
from sdcm.utils.version_utils import scylla_versions
from test_lib.cql_types import CQLTypeBuilder

LOGGER = logging.getLogger(__name__)


def get_all_tables_with_no_compact_storage(cluster, target_node, tables_to_skip=None):
    """
    Return all non-system tables without COMPACT STORAGE.

    Args:
        cluster: cluster object with cql_connection_patient
        target_node: node to query
        tables_to_skip: dict of keyspaces/tables to exclude.
            {'ks': 'table'} - skip ks.table
            {'ks': '*'} - skip all tables from keyspace "ks"
            {'*': 'table1,table2'} - skip table1 and table2 from any keyspace

    Returns:
        Dict with keyspace names as keys and list of table names as values.
    """
    keyspaces = []
    output = {}
    if tables_to_skip is None:
        tables_to_skip = {}
    to_be_skipped_default = tables_to_skip.get("*", "").split(",")
    with cluster.cql_connection_patient(target_node) as session:
        query_result = session.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        for result_rows in query_result:
            keyspaces.extend([row.lower() for row in result_rows if not row.lower().startswith(("system", "audit"))])
        for ks in keyspaces:
            to_be_skipped = tables_to_skip.get(ks, None)
            if to_be_skipped is None:
                to_be_skipped = to_be_skipped_default
            elif to_be_skipped == "*":
                continue
            elif to_be_skipped == "":
                to_be_skipped = []
            else:
                to_be_skipped = to_be_skipped.split(",") + to_be_skipped_default
            tables = get_db_tables(keyspace_name=ks, node=target_node, with_compact_storage=False)
            if to_be_skipped:
                tables = [table for table in tables if table not in to_be_skipped]
            if not tables:
                continue
            output[ks] = tables
    return output


def verify_initial_inputs_for_delete_nemesis(cluster, tester):
    """Verify preconditions for delete nemesis operations."""
    test_keyspaces = cluster.get_test_keyspaces()

    if "scylla_bench" not in test_keyspaces:
        raise UnsupportedNemesis("This nemesis can run on scylla_bench test only")

    if not (tester.partitions_attrs and tester.partitions_attrs.max_partitions_in_test_table):
        raise UnsupportedNemesis(
            'This nemesis expects "max_partitions_in_test_table" sub-parameter of data_validation to be set'
        )


def get_random_timestamp_from_partition(
    cluster, target_node, ks_cf, pkey, partition_percentage=0.25, log_prefix=""
) -> tuple[int, int]:
    """
    Get a write timestamp from a "pivot" row inside a single partition.

    Args:
        partition_percentage: controls where the "pivot" is (default 25% of partition size)

    Returns:
        Tuple of (timestamp, clustering_key_value)
    """
    with cluster.cql_connection_patient(node=target_node) as session:
        count_result = session.execute(SimpleStatement(f"select count(ck) from {ks_cf} where pk = {pkey}")).one()
        if not count_result or count_result.system_count_ck is None:
            message = f"Unable to count rows in partition (pk = {pkey})"
            LOGGER.error(message)
            raise PartitionNotFound(message)
        number_of_rows = count_result.system_count_ck
        if number_of_rows == 0:
            message = f"Partition (pk = {pkey}) is empty"
            LOGGER.error(message)
            raise PartitionNotFound(message)
        fetch_limit = max(math.ceil(number_of_rows * partition_percentage), 11)
        LOGGER.debug(
            "[%s_using_timestamp] Partition size: %s, fetching up to %s",
            log_prefix,
            number_of_rows,
            fetch_limit,
        )
        partition = session.execute(
            SimpleStatement(f"select pk, ck from {ks_cf} where pk = {pkey} limit {fetch_limit}")
        ).all()
        if not partition:
            message = (
                f"No rows found in partition (pk = {pkey}) after counting {number_of_rows} rows. "
                "The partition may have been deleted."
            )
            LOGGER.error(message)
            raise PartitionNotFound(message)
        delete_mark = partition[-1].ck
        timestamp_result = session.execute(
            SimpleStatement(f"select writetime(v) from {ks_cf} where pk = {pkey} and ck = {delete_mark}")
        ).one()
        if not timestamp_result or timestamp_result.writetime_v is None:
            message = f"Unable to get writetime for row (pk = {pkey}, ck = {delete_mark})"
            LOGGER.error(message)
            raise TimestampNotFound(message)
        timestamp = timestamp_result.writetime_v

        return timestamp, delete_mark


def run_deletions(cluster, target_node, queries, ks_cf):
    """Execute a list of delete CQL queries and flush."""
    with cluster.cql_connection_patient(target_node, connect_timeout=300) as session:
        for cmd in queries:
            LOGGER.debug("delete query: %s", cmd)
            session.execute(SimpleStatement(cmd, consistency_level=ConsistencyLevel.QUORUM), timeout=3600)

    target_node.run_nodetool("flush", args=ks_cf.replace(".", " "))


# ---------------------------------------------------------------------------
# Monkey classes
# ---------------------------------------------------------------------------


class TruncateMonkey(NemesisBaseClass):
    """Truncate a test table after flushing."""

    disruptive = False
    kubernetes = True
    xcloud = True
    limited = True
    free_tier_set = True

    TRUNCATE_TIMEOUT = 600

    @scylla_versions(("5.2.rc0", None), ("2023.1.rc0", None))
    def truncate_cmd_timeout_suffix(self, truncate_timeout):
        return f" USING TIMEOUT {int(truncate_timeout)}s"

    @scylla_versions((None, "5.1"), (None, "2022.2"))
    def truncate_cmd_timeout_suffix(self, truncate_timeout):
        return ""

    def disrupt(self):
        keyspace_truncate = "ks_truncate"
        table = "standard1"
        ks_cf = f"{keyspace_truncate}.{table}"

        self.runner.actions_log.info(f"Preparing test table for truncate nemesis: {ks_cf}")
        stress_handler = partial(nemesis_stress_failure_handler, self.__class__.__name__)
        prepare_test_table(
            self.runner.tester,
            self.runner.cluster,
            self.runner.target_node,
            ks=keyspace_truncate,
            stress_failure_handler=stress_handler,
        )

        with adaptive_timeout(Operations.FLUSH, self.runner.target_node, timeout=HOUR_IN_SEC * 2):
            self.runner.target_node.run_nodetool("flush")

        truncate_cmd_timeout_suffix = self.truncate_cmd_timeout_suffix(self.TRUNCATE_TIMEOUT)
        with self.runner.action_log_scope(f"Truncate {ks_cf} table using cqlsh"):
            self.runner.target_node.run_cqlsh(
                cmd=f"TRUNCATE {keyspace_truncate}.{table}{truncate_cmd_timeout_suffix}", timeout=self.TRUNCATE_TIMEOUT
            )


class TruncateLargeParititionMonkey(TruncateMonkey):
    """
    Truncate a large-partition table created by scylla-bench.

    Covers compaction improvement for increased frequency of abort checking during truncate.
    """

    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True
    limited = False

    def disrupt(self):
        if SkipPerIssues(
            issues="https://github.com/scylladb/scylladb/issues/20356", params=self.runner.tester.params
        ) and self.runner.tester.params.get("use_zero_nodes"):
            raise UnsupportedNemesis("Unsupported nemesis due to scylladb/scylladb#20356")

        ks_name = "ks_truncate_large_partition"
        table = "test_table"
        ks_cf = f"{ks_name}.{table}"
        stress_cmd = (
            "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=10 "
            + "-clustering-row-count=5555 -clustering-row-size=uniform:10..20 -concurrency=10 "
            + "-connection-count=10 -consistency-level=quorum -rows-per-request=10 -timeout=60s "
            + f"-keyspace {ks_name} -table {table}"
        )
        self.runner.actions_log.info(f"Preparing test table for truncate with scylla-bench: {ks_cf}")
        stress_handler = partial(nemesis_stress_failure_handler, self.__class__.__name__)
        bench_thread = self.runner.tester.run_stress_thread(stress_cmd=stress_cmd, stop_test_on_failure=False)
        self.runner.tester.verify_stress_thread(bench_thread, error_handler=stress_handler)

        with adaptive_timeout(Operations.FLUSH, self.runner.target_node, timeout=HOUR_IN_SEC * 2):
            self.runner.target_node.run_nodetool("flush")

        truncate_cmd_timeout_suffix = self.truncate_cmd_timeout_suffix(self.TRUNCATE_TIMEOUT)
        with self.runner.action_log_scope(f"Truncate {ks_cf} table using cqlsh"):
            self.runner.target_node.run_cqlsh(
                cmd=f"TRUNCATE {ks_name}.{table}{truncate_cmd_timeout_suffix}", timeout=self.TRUNCATE_TIMEOUT
            )


class DeleteMonkeyBase(NemesisBaseClass):
    """Base class for delete nemesis with shared partition selection and deletion logic."""

    disruptive = False
    kubernetes = True
    xcloud = True
    free_tier_set = True
    delete_rows = True

    KS_CF = "scylla_bench.test"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.random = self.runner.random

    @property
    def partition_deletion_divisor(self) -> int:
        """
        Compute how aggressively to delete partitions.

        If background load includes scylla-bench writes, partitions are being rewritten
        so we can afford to delete more (divisor=3). Otherwise use conservative divisor=5.
        """
        if stress_cmds := self.runner.cluster.params.get("stress_cmd"):
            for stress_cmd in stress_cmds:
                parts = stress_cmd.split()
                if "scylla-bench" in parts and "-mode=write" in parts:
                    return 3
        return 5

    def choose_partitions_for_delete(
        self, partitions_amount, ks_cf, with_clustering_key_data=False, exclude_partitions=None
    ):
        """
        Choose random partitions for deletion from a table.

        Returns:
            defaultdict mapping partition key to [min_ck, max_ck] if with_clustering_key_data else empty list.
        """
        if not exclude_partitions:
            exclude_partitions = []

        partitions_attrs = self.runner.tester.partitions_attrs
        if partitions_attrs.partition_range_with_data_validation:
            partitions_amount = min(partitions_amount, partitions_attrs.non_validated_partitions)
            available_partitions_for_deletion = list(
                range(partitions_attrs.partition_end_range + 1, partitions_attrs.max_partitions_in_test_table)
            )
        else:
            available_partitions_for_deletion = list(range(partitions_attrs.max_partitions_in_test_table))
        LOGGER.debug("Partitions amount for delete : %s", partitions_amount)

        partitions_for_delete = defaultdict(list)
        with self.runner.cluster.cql_connection_patient(self.runner.target_node, connect_timeout=300) as session:
            session.default_consistency_level = ConsistencyLevel.ONE

            while available_partitions_for_deletion and len(partitions_for_delete) < partitions_amount:
                random_index = self.random.randint(0, len(available_partitions_for_deletion) - 1)
                partition_key = available_partitions_for_deletion.pop(random_index)

                if exclude_partitions and partition_key in exclude_partitions:
                    continue

                cmd = f"select ck from {ks_cf} where pk={partition_key} order by ck desc limit 1"
                try:
                    result = session.execute(SimpleStatement(cmd, fetch_size=1), timeout=300)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error(str(exc))
                    continue

                if not result:
                    continue

                first_row = result.one()
                if not first_row or first_row.ck is None:
                    continue

                if not with_clustering_key_data:
                    partitions_for_delete[partition_key] = []
                    continue

                partitions_for_delete[partition_key].extend([0, first_row.ck])

                if None in partitions_for_delete[partition_key]:
                    partitions_for_delete.pop(partition_key)

        LOGGER.debug("Partitions for delete: %s", partitions_for_delete)
        return partitions_for_delete


class DeleteByPartitionsMonkey(DeleteMonkeyBase):
    """Delete 10 full partitions from a table with large partitions."""

    def disrupt(self):
        verify_initial_inputs_for_delete_nemesis(self.runner.cluster, self.runner.tester)

        partitions_for_delete = self.choose_partitions_for_delete(partitions_amount=10, ks_cf=self.KS_CF)

        if not partitions_for_delete:
            raise UnsupportedNemesis("Not found partitions for delete. Nemesis can not be run")

        self.runner.actions_log.info(
            f"Delete partitions in {self.KS_CF} table. Partitions count: {len(partitions_for_delete)}"
        )
        queries = []
        for partition_key in partitions_for_delete.keys():
            queries.append(f"delete from {self.KS_CF} where pk = {partition_key}")

        run_deletions(self.runner.cluster, self.runner.target_node, queries=queries, ks_cf=self.KS_CF)


class DeleteOverlappingRowRangesMonkey(DeleteMonkeyBase):
    """Delete several overlapping row ranges in a table with large partitions."""

    def disrupt(self):
        verify_initial_inputs_for_delete_nemesis(self.runner.cluster, self.runner.tester)

        partitions_for_delete = self.choose_partitions_for_delete(
            partitions_amount=self.runner.tester.partitions_attrs.non_validated_partitions
            // self.partition_deletion_divisor,
            ks_cf=self.KS_CF,
            with_clustering_key_data=True,
        )
        if not partitions_for_delete:
            LOGGER.error("No partitions for delete found!")
            raise UnsupportedNemesis("DeleteOverlappingRowRangesMonkey: No partitions for delete found!")

        self.runner.actions_log.info(
            f"Delete random row ranges in few partitions in {self.KS_CF} table. "
            f"Partitions count: {len(partitions_for_delete)}"
        )
        queries = []
        for pkey, ckey in partitions_for_delete.items():
            for _ in range(self.random.randint(3, 20)):
                min_ck = self.random.randint(0, ckey[1])
                max_ck = self.random.randint(min_ck, ckey[1])
                queries.append(f"delete from {self.KS_CF} where pk = {pkey} and ck > {min_ck} and ck < {max_ck}")
        run_deletions(self.runner.cluster, self.runner.target_node, queries=queries, ks_cf=self.KS_CF)


class DeleteByRowsRangeMonkey(DeleteMonkeyBase):
    """Delete row ranges: first half-partition or timestamp-based, then range in remaining partitions."""

    def delete_half_partition(self, ks_cf):
        """Delete half of each selected partition by clustering key range."""
        LOGGER.debug("Delete by range - half of partition")

        partitions_amount = self.runner.tester.partitions_attrs.non_validated_partitions // 2
        LOGGER.debug("delete_half_partition.partitions_amount: %s", partitions_amount)
        partitions_for_delete = self.choose_partitions_for_delete(
            partitions_amount=partitions_amount, ks_cf=ks_cf, with_clustering_key_data=True
        )
        if not partitions_for_delete:
            raise UnsupportedNemesis("Not found partitions for delete. Nemesis can not be run")

        self.runner.actions_log.info(f"Deleting half ({len(partitions_for_delete)}) of partitions on {ks_cf} table")
        queries = []
        for pkey, ckey in partitions_for_delete.items():
            queries.append(f"delete from {ks_cf} where pk = {pkey} and ck > {int(ckey[1] / 2)}")
        run_deletions(self.runner.cluster, self.runner.target_node, queries=queries, ks_cf=ks_cf)
        return partitions_for_delete

    def delete_by_range_using_timestamp(self, ks_cf, log_prefix=""):
        """Delete partitions using USING TIMESTAMP clause."""
        LOGGER.debug("Delete by range - using timestamp")

        partitions_for_delete = self.choose_partitions_for_delete(
            partitions_amount=self.runner.tester.partitions_attrs.non_validated_partitions
            // self.partition_deletion_divisor,
            ks_cf=ks_cf,
            with_clustering_key_data=False,
        )
        if not partitions_for_delete:
            message = "Unable to find partitions to delete"
            LOGGER.error(message)
            raise PartitionNotFound(message)

        queries = []
        verification_queries = []
        partition_percentage = self.random.randint(25, 75) / 100
        self.runner.actions_log.info(
            f"Deleting partitions using timestamp in {ks_cf} table."
            f" Partitions count: {len(partitions_for_delete)}, "
            f"partitions percentage: {partition_percentage}"
        )
        for pkey, _ in partitions_for_delete.items():
            LOGGER.debug("Using USING TIMESTAMP clause in the deletion for this partition: %s", pkey)
            timestamp, clustering_key = get_random_timestamp_from_partition(
                self.runner.cluster,
                self.runner.target_node,
                ks_cf=ks_cf,
                pkey=pkey,
                partition_percentage=partition_percentage,
                log_prefix=log_prefix,
            )
            queries.append(f"delete from {ks_cf} using timestamp {timestamp} where pk = {pkey}")
            verification_queries.append([pkey, clustering_key, timestamp])

        run_deletions(self.runner.cluster, self.runner.target_node, queries=queries, ks_cf=ks_cf)
        self.verify_using_timestamp_deletions(ks_cf=ks_cf, verification_queries=verification_queries)

        return partitions_for_delete

    def verify_using_timestamp_deletions(self, ks_cf, verification_queries):
        """Verify that rows deleted via USING TIMESTAMP are actually gone."""
        mv_not_configured = False
        mv_table_name = ".".join([ks_cf.split(sep=".")[0], "view_test"])
        with self.runner.cluster.cql_connection_patient(self.runner.target_node, connect_timeout=300) as session:
            for pk, ck, ts in verification_queries:
                result = session.execute(
                    SimpleStatement(f"SELECT pk, ck, writetime(v) FROM {ks_cf} WHERE pk = {pk} AND ck = {ck}")
                ).one()
                if result and result.writetime_v == ts:
                    raise RuntimeError(
                        f"USING TIMESTAMP: deletion failed for ({pk}, {ck}), row still exists, timestamp used: {ts}"
                    )
                if not mv_not_configured:
                    try:
                        result = session.execute(
                            SimpleStatement(
                                f"SELECT pk, ck, writetime(v) FROM {mv_table_name} WHERE pk = {pk} AND ck = {ck}"
                            )
                        ).one()
                        if result and result.writetime_v == ts:
                            raise RuntimeError(
                                f"USING TIMESTAMP: deletion failed for ({pk}, {ck}), row still exists in MV, timestamp used: {ts}"
                            )
                    except InvalidRequest:
                        mv_not_configured = True

    def delete_range_in_few_partitions(self, ks_cf, partitions_for_exclude_dict):
        """Delete the same clustering key range across multiple partitions."""
        LOGGER.debug("Delete same range in the few partitions")

        partitions_for_exclude = list(partitions_for_exclude_dict.keys())
        partitions_for_delete = self.choose_partitions_for_delete(
            partitions_amount=self.runner.tester.partitions_attrs.non_validated_partitions
            // self.partition_deletion_divisor,
            ks_cf=ks_cf,
            with_clustering_key_data=True,
            exclude_partitions=partitions_for_exclude,
        )
        if not partitions_for_delete:
            raise UnsupportedNemesis("No partitions for deletion found. Cannot execute a range deletion.")

        min_clustering_key = max([v[0] for v in partitions_for_delete.values()])
        max_clustering_key = min([v[1] for v in partitions_for_delete.values()])
        clustering_keys = []
        if max_clustering_key > min_clustering_key:
            third_ck = int((max_clustering_key - min_clustering_key) / 3)
            clustering_keys = range(min_clustering_key + third_ck, max_clustering_key - third_ck)

        if not clustering_keys:
            clustering_keys = range(min_clustering_key, max_clustering_key)

        self.runner.actions_log.info(
            f"Delete same range in the few partitions in {ks_cf} table. Partitions count: {len(partitions_for_delete)}"
        )
        queries = []
        for pkey in partitions_for_delete.keys():
            queries.append(
                f"delete from {ks_cf} where pk = {pkey} and ck >= {clustering_keys[0]} and ck <= {clustering_keys[-1]}"
            )

        run_deletions(self.runner.cluster, self.runner.target_node, queries=queries, ks_cf=ks_cf)

        return list(partitions_for_delete.keys()) + partitions_for_exclude

    def disrupt(self):
        verify_initial_inputs_for_delete_nemesis(self.runner.cluster, self.runner.tester)

        ks_cf = self.KS_CF

        # Step 1: delete_half_partition or delete_by_range_using_timestamp
        if self.random.random() > 0.5:
            partitions_for_exclude = self.delete_half_partition(ks_cf)
        else:
            partitions_for_exclude = self.delete_by_range_using_timestamp(ks_cf, log_prefix="delete_by_rows_range")
        # Step 2: delete_range_in_few_partitions
        self.delete_range_in_few_partitions(ks_cf, partitions_for_exclude)


class AddDropColumnMonkey(NemesisBaseClass):
    """
    Add and drop columns on a non-compact-storage table in cycles.

    State is accumulated across disrupt() calls to track which columns have been added,
    ensuring only self-added columns are dropped.
    """

    disruptive = False
    networking = False
    kubernetes = True
    xcloud = True
    limited = True
    schema_changes = True
    free_tier_set = True

    MAX_PER_DROP = 5
    MAX_PER_ADD = 5
    MAX_COLUMN_NAME_SIZE = 10
    MAX_COLUMNS = 200
    CYCLE_DURATION = 600
    TABLES_TO_IGNORE = {
        "alternator_usertable": "*",
        "ks_truncate": "counter1",
        "keyspace1": "counter1",
        "cqlstress_lwt_example": "*",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.random = self.runner.random
        self.target_table = []
        self.columns_info = {}

    def disrupt(self):
        LOGGER.debug("AddDropColumnMonkey: Started")
        self.target_table = self.get_target_table(self.target_table)
        if self.target_table is None:
            raise UnsupportedNemesis("AddDropColumnMonkey: can't find table to run on")
        InfoEvent(f"AddDropColumnMonkey table {'.'.join(self.target_table)}").publish()
        self.run_in_cycle()

    def get_target_table(self, stored_target_table):
        """Find or reuse a table that supports ADD/DROP column."""
        current_tables = get_all_tables_with_no_compact_storage(
            self.runner.cluster, self.runner.target_node, self.TABLES_TO_IGNORE
        )
        if stored_target_table:
            if stored_target_table[1] in current_tables.get(stored_target_table[0], []):
                return stored_target_table
        if not current_tables:
            return None
        ks_name = next(iter(current_tables.keys()))
        table_name = current_tables[ks_name][0]
        return [ks_name, table_name]

    def get_added_columns_info(self, target_table):
        """Get or create the tracking dict for columns added to a specific table."""
        ks = self.columns_info.get(target_table[0], None)
        if ks is None:
            output = {"column_names": {}, "column_types": {}}
            self.columns_info[target_table[0]] = {target_table[1]: output}
            return output
        table = ks.get(target_table[1], None)
        if table is not None:
            return table
        ks[target_table[1]] = output = {"column_names": {}, "column_types": {}}
        return output

    def generate_columns_to_drop(self, added_columns_info):
        """Choose random columns to drop from previously added ones."""
        drop = []
        columns_to_drop = min(len(added_columns_info["column_names"]) + 1, self.MAX_PER_DROP + 1)
        if columns_to_drop > 1:
            columns_to_drop = self.random.randrange(1, columns_to_drop)
        for _ in range(columns_to_drop):
            choice = [n for n in added_columns_info["column_names"] if n not in drop]
            if choice:
                column_name = self.random.choice(choice)
                drop.append(column_name)
        return drop

    def generate_columns_to_add(self, added_columns_info):
        """Generate random columns to add."""
        add = []
        columns_to_add = min(
            self.MAX_COLUMNS - len(added_columns_info["column_names"]),
            self.MAX_PER_ADD,
        )
        if columns_to_add > 1:
            columns_to_add = self.random.randrange(1, columns_to_add)
        for _ in range(columns_to_add):
            new_column_name = self.random_column_name(
                added_columns_info["column_names"].keys(), self.MAX_COLUMN_NAME_SIZE
            )
            new_column_type = CQLTypeBuilder.get_random(
                added_columns_info["column_types"], allow_levels=10, avoid_types=["counter"], forget_on_exhaust=True
            )
            if new_column_type is None:
                continue
            add.append([new_column_name, new_column_type])
        return add

    def random_column_name(self, avoid_names=None, max_name_size=5):
        """Generate a random column name that doesn't collide with existing ones."""
        if avoid_names is None:
            avoid_names = []
        while True:
            column_name = "".join(self.random.choices(string.ascii_lowercase, k=max_name_size))
            if column_name not in avoid_names:
                break
        return column_name

    def run_cql_query(self, cmd, ks, consistency_level=ConsistencyLevel.ALL):
        """Execute a CQL query, returning True on success, False on failure."""
        try:
            with self.runner.cluster.cql_connection_patient(self.runner.target_node, keyspace=ks) as session:
                session.default_consistency_level = consistency_level
                session.execute(cmd)
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug(
                "Add/Remove Column Nemesis: CQL query '%s' execution has failed with error '%s'", cmd, str(exc)
            )
            self.runner.actions_log.info(f"Failed to add/drop column: {exc!s}")
            return False
        return True

    def add_drop_column(self, drop=True, add=True):
        """Execute one cycle of add/drop column operations."""
        self.target_table = self.get_target_table(self.target_table)
        if self.target_table is None:
            return
        added_columns_info = self.get_added_columns_info(self.target_table)
        if not added_columns_info["column_names"]:
            drop = False
        if drop:
            drop = self.generate_columns_to_drop(added_columns_info)
        if add:
            add = self.generate_columns_to_add(added_columns_info)
        if not add and not drop:
            return
        ks_cf = f"{self.target_table[0]}.{self.target_table[1]}"
        if drop:
            self.runner.actions_log.info(f"Dropping {len(drop)} columns from {ks_cf} table")
            cmd = f"ALTER TABLE {self.target_table[1]} DROP ( {', '.join(drop)} );"
            if self.run_cql_query(cmd, self.target_table[0]):
                for column_name in drop:
                    del added_columns_info["column_names"][column_name]
        if add:
            self.runner.actions_log.info(f"Adding {len(add)} columns to {ks_cf} table")
            cmd = (
                f"ALTER TABLE {self.target_table[1]} ADD ( {', '.join(['%s %s' % (col[0], col[1]) for col in add])} );"
            )
            if self.run_cql_query(cmd, self.target_table[0]):
                for column_name, column_type in add:
                    added_columns_info["column_names"][column_name] = column_type
                    column_type.remember_variant(added_columns_info["column_types"])

    def run_in_cycle(self):
        """Run add/drop column operations in a time-boxed loop."""
        start_time = time.time()
        end_time = start_time + self.CYCLE_DURATION
        while time.time() < end_time:
            self.add_drop_column()

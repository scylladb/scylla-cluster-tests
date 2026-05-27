"""Tests for sdcm.nemesis.monkey.data_operations module.

Tests focus on verifying correct CQL commands, nodetool calls, and state management.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.data_operations import (
    AddDropColumnMonkey,
    DeleteByPartitionsMonkey,
    DeleteByRowsRangeMonkey,
    DeleteOverlappingRowRangesMonkey,
    TruncateLargeParititionMonkey,
    TruncateMonkey,
    verify_initial_inputs_for_delete_nemesis,
)
from sdcm.nemesis.utils.common import prepare_test_table

MODULE = "sdcm.nemesis.monkey.data_operations"

pytestmark = pytest.mark.usefixtures("events")

STANDARD1_STRESS_CMD = (
    "cassandra-stress write n=400000 cl=QUORUM -mode native cql3 "
    "-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' -log interval=5"
)


@pytest.fixture()
def runner(base_runner):
    """Runner with params suitable for data operation tests."""
    base_runner.cluster.params = {"stress_cmd": ["scylla-bench -mode=write -workload=sequential"]}
    base_runner.cluster.get_test_keyspaces.return_value = ["scylla_bench"]
    base_runner.tester.partitions_attrs = MagicMock()
    base_runner.tester.partitions_attrs.max_partitions_in_test_table = 1000
    base_runner.tester.partitions_attrs.non_validated_partitions = 500
    base_runner.tester.partitions_attrs.partition_range_with_data_validation = False
    # Reset CQL session to plain MagicMock (TestRunner sets a restrictive side_effect)
    base_runner.cluster.cql_session.execute.side_effect = None
    return base_runner


@pytest.fixture(autouse=True)
def mock_adaptive_timeout():
    """Patch adaptive_timeout for all tests so nodetool/cqlsh calls go through."""
    with patch(f"{MODULE}.adaptive_timeout") as mock_timeout:
        mock_timeout.return_value.__enter__ = MagicMock()
        mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
        yield mock_timeout


@pytest.fixture(autouse=True)
def mock_prepare_test_table():
    """Patch prepare_test_table to avoid running real stress."""
    with patch(f"{MODULE}.prepare_test_table") as mock:
        yield mock


@pytest.fixture()
def common_prepare_test_table_mocks():
    tester = MagicMock()
    tester.reliable_replication_factor = 3
    cluster = MagicMock()
    cluster.get_non_system_ks_cf_list.return_value = []
    cluster.get_test_keyspaces.return_value = []
    target_node = MagicMock()
    return tester, cluster, target_node


@pytest.fixture()
def runner_v54(runner):
    """Runner configured with Scylla 5.4 (supports USING TIMEOUT)."""
    runner.cluster.params = {"scylla_version": "5.4.0", "stress_cmd": ["scylla-bench -mode=write"]}
    runner.cluster.nodes = [runner.target_node]
    runner.target_node.scylla_version = "5.4.0"
    return runner


@pytest.fixture()
def runner_v51(runner):
    """Runner configured with Scylla 5.1 (no USING TIMEOUT)."""
    runner.cluster.params = {"scylla_version": "5.1.0", "stress_cmd": ["scylla-bench -mode=write"]}
    runner.cluster.nodes = [runner.target_node]
    runner.target_node.scylla_version = "5.1.0"
    return runner


@pytest.fixture()
def truncate_monkey(runner_v54):
    return TruncateMonkey(runner_v54)


@pytest.fixture()
def truncate_large_monkey(runner_v54):
    params = MagicMock()
    params.get.return_value = False
    params.__getitem__ = MagicMock(return_value=False)
    runner_v54.tester.params = params
    runner_v54.tester.run_stress_thread.return_value = MagicMock()
    return TruncateLargeParititionMonkey(runner_v54)


@pytest.fixture()
def delete_partitions_monkey(runner):
    return DeleteByPartitionsMonkey(runner)


@pytest.fixture()
def delete_overlapping_monkey(runner):
    return DeleteOverlappingRowRangesMonkey(runner)


@pytest.fixture()
def delete_rows_range_monkey(runner):
    return DeleteByRowsRangeMonkey(runner)


@pytest.fixture()
def add_drop_column_monkey(runner):
    """AddDropColumnMonkey with single-iteration cycle."""
    monkey = AddDropColumnMonkey(runner)
    # Override run_in_cycle to execute exactly one add_drop_column call
    monkey.run_in_cycle = monkey.add_drop_column
    return monkey


@pytest.fixture()
def mock_verify_inputs():
    with patch(f"{MODULE}.verify_initial_inputs_for_delete_nemesis") as mock:
        yield mock


@pytest.fixture()
def mock_run_deletions():
    with patch(f"{MODULE}.run_deletions") as mock:
        yield mock


@pytest.fixture()
def mock_get_tables():
    with patch(f"{MODULE}.get_all_tables_with_no_compact_storage") as mock:
        yield mock


@pytest.fixture(autouse=True)
def patch_all_helpers(mock_verify_inputs, mock_run_deletions, mock_get_tables):
    """Ensure module-level helpers are patched for every test."""
    yield


# ---------------------------------------------------------------------------
# Tests: common_ops.prepare_test_table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keyspace_name", ["keyspace1", "ks_truncate", "custom_ks"])
def test_prepare_test_table_creates_standard1_in_requested_keyspace(common_prepare_test_table_mocks, keyspace_name):
    tester, cluster, target_node = common_prepare_test_table_mocks

    prepare_test_table(tester, cluster, target_node, ks=keyspace_name)

    cluster.get_non_system_ks_cf_list.assert_called_once_with(db_node=target_node)
    tester.run_stress_thread.assert_called_once_with(
        stress_cmd=STANDARD1_STRESS_CMD,
        keyspace_name=keyspace_name,
        stop_test_on_failure=False,
        round_robin=True,
    )
    tester.verify_stress_thread.assert_called_once()


@pytest.mark.parametrize("keyspace_name", ["keyspace1", "ks_truncate", "custom_ks"])
def test_prepare_test_table_skips_when_requested_standard1_exists(common_prepare_test_table_mocks, keyspace_name):
    tester, cluster, target_node = common_prepare_test_table_mocks
    cluster.get_non_system_ks_cf_list.return_value = [f"{keyspace_name}.standard1"]
    cluster.get_test_keyspaces.return_value = [keyspace_name]

    prepare_test_table(tester, cluster, target_node, ks=keyspace_name)

    tester.run_stress_thread.assert_not_called()
    tester.verify_stress_thread.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: TruncateMonkey
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "runner_fixture,expected_timeout_present",
    [
        ("runner_v54", True),
        ("runner_v51", False),
    ],
)
def test_truncate_monkey_timeout_by_version(runner_fixture, expected_timeout_present, request):
    runner = request.getfixturevalue(runner_fixture)
    monkey = TruncateMonkey(runner)

    monkey.disrupt()

    target = runner.target_node
    target.run_nodetool.assert_called_once_with("flush")
    target.run_cqlsh.assert_called_once()
    cmd = target.run_cqlsh.call_args.kwargs["cmd"]
    assert "TRUNCATE ks_truncate.standard1" in cmd
    if expected_timeout_present:
        assert "USING TIMEOUT 600s" in cmd
    else:
        assert "USING TIMEOUT" not in cmd


# ---------------------------------------------------------------------------
# Tests: TruncateLargeParititionMonkey
# ---------------------------------------------------------------------------


def test_truncate_large_partition_runs_bench_and_truncates(truncate_large_monkey):
    truncate_large_monkey.disrupt()

    runner = truncate_large_monkey.runner
    runner.tester.run_stress_thread.assert_called_once()
    stress_cmd = runner.tester.run_stress_thread.call_args.kwargs["stress_cmd"]
    assert "scylla-bench" in stress_cmd
    runner.target_node.run_nodetool.assert_called_once_with("flush")
    cmd = runner.target_node.run_cqlsh.call_args.kwargs["cmd"]
    assert "TRUNCATE ks_truncate_large_partition.test_table" in cmd


# ---------------------------------------------------------------------------
# Tests: DeleteByPartitionsMonkey
# ---------------------------------------------------------------------------


def test_delete_by_partitions_issues_correct_queries(delete_partitions_monkey, mock_run_deletions):
    # Limit available partitions to 3 so the loop exhausts them (avoids needing 10 randint values)
    delete_partitions_monkey.runner.tester.partitions_attrs.max_partitions_in_test_table = 3
    # Configure monkey random to pick indices 0, 0, 0 (always first available partition)
    delete_partitions_monkey.random.randint.side_effect = [0, 0, 0]
    # CQL session returns a row with ck for each partition
    session = delete_partitions_monkey.runner.cluster.cql_session
    row = MagicMock()
    row.ck = 100
    result_mock = MagicMock()
    result_mock.one.return_value = row
    session.execute.return_value = result_mock

    delete_partitions_monkey.disrupt()

    queries = mock_run_deletions.call_args[1]["queries"]
    assert len(queries) == 3
    for query in queries:
        assert query.startswith("delete from scylla_bench.test where pk = ")


# ---------------------------------------------------------------------------
# Tests: DeleteOverlappingRowRangesMonkey
# ---------------------------------------------------------------------------


def test_delete_overlapping_generates_range_queries(delete_overlapping_monkey, mock_run_deletions):
    # Only 1 partition available so choose_partitions_for_delete picks it with one randint call
    delete_overlapping_monkey.runner.tester.partitions_attrs.max_partitions_in_test_table = 1
    session = delete_overlapping_monkey.runner.cluster.cql_session
    row = MagicMock()
    row.ck = 100
    result_mock = MagicMock()
    result_mock.one.return_value = row
    session.execute.return_value = result_mock

    # monkey.random.randint: first call picks partition index(0),
    # then for the partition: num_ranges(5), then pairs of (min_ck, max_ck)
    delete_overlapping_monkey.random.randint.side_effect = [
        0,  # choose_partitions_for_delete: pick index 0
        5,  # number of ranges
        20,
        80,  # range 1
        10,
        90,  # range 2
        30,
        70,  # range 3
        15,
        85,  # range 4
        25,
        75,  # range 5
    ]

    delete_overlapping_monkey.disrupt()

    queries = mock_run_deletions.call_args[1]["queries"]
    assert len(queries) == 5
    assert queries[0] == "delete from scylla_bench.test where pk = 0 and ck > 20 and ck < 80"
    assert queries[1] == "delete from scylla_bench.test where pk = 0 and ck > 10 and ck < 90"
    assert queries[2] == "delete from scylla_bench.test where pk = 0 and ck > 30 and ck < 70"
    assert queries[3] == "delete from scylla_bench.test where pk = 0 and ck > 15 and ck < 85"
    assert queries[4] == "delete from scylla_bench.test where pk = 0 and ck > 25 and ck < 75"


# ---------------------------------------------------------------------------
# Tests: DeleteByRowsRangeMonkey
# ---------------------------------------------------------------------------


def test_delete_by_rows_range_uses_half_partition_path(delete_rows_range_monkey, mock_run_deletions):
    # random() > 0.5 → takes half_partition path
    delete_rows_range_monkey.random.random.return_value = 0.7

    # Patch instance methods to avoid real CQL
    delete_rows_range_monkey.delete_half_partition = MagicMock(return_value={1: [0, 50]})
    delete_rows_range_monkey.delete_range_in_few_partitions = MagicMock()

    delete_rows_range_monkey.disrupt()

    delete_rows_range_monkey.delete_half_partition.assert_called_once_with("scylla_bench.test")
    delete_rows_range_monkey.delete_range_in_few_partitions.assert_called_once_with("scylla_bench.test", {1: [0, 50]})


def test_delete_by_rows_range_uses_timestamp_path(delete_rows_range_monkey, mock_run_deletions):
    # random() <= 0.5 → takes timestamp path
    delete_rows_range_monkey.random.random.return_value = 0.3

    delete_rows_range_monkey.delete_by_range_using_timestamp = MagicMock(return_value={2: [0, 80]})
    delete_rows_range_monkey.delete_range_in_few_partitions = MagicMock()

    delete_rows_range_monkey.disrupt()

    delete_rows_range_monkey.delete_by_range_using_timestamp.assert_called_once_with(
        "scylla_bench.test", log_prefix="delete_by_rows_range"
    )
    delete_rows_range_monkey.delete_range_in_few_partitions.assert_called_once_with("scylla_bench.test", {2: [0, 80]})


# ---------------------------------------------------------------------------
# Tests: AddDropColumnMonkey
# ---------------------------------------------------------------------------


def test_add_drop_column_issues_alter_table(mock_get_tables, add_drop_column_monkey):
    mock_get_tables.return_value = {"ks1": ["tbl1"]}
    # Configure monkey random for deterministic behavior:
    # randrange(1, 5) → 1 column to add
    # choices() → column name "abcde"
    add_drop_column_monkey.random.randrange.return_value = 1
    add_drop_column_monkey.random.choices.return_value = list("abcde")

    session = add_drop_column_monkey.runner.cluster.cql_session

    add_drop_column_monkey.disrupt()

    assert add_drop_column_monkey.target_table == ["ks1", "tbl1"]
    assert session.execute.call_count == 1
    cmd = str(session.execute.call_args[0][0])
    assert "ALTER TABLE tbl1 ADD ( abcde " in cmd


def test_add_drop_column_reuses_target_table(mock_get_tables, add_drop_column_monkey):
    mock_get_tables.return_value = {"ks1": ["tbl1"]}
    add_drop_column_monkey.random.randrange.return_value = 1
    # Different names on each call to avoid infinite loop in random_column_name
    add_drop_column_monkey.random.choices.side_effect = [list("abcde"), list("fghij")]

    add_drop_column_monkey.disrupt()
    add_drop_column_monkey.disrupt()

    assert add_drop_column_monkey.target_table == ["ks1", "tbl1"]


def test_add_drop_column_raises_when_no_tables(mock_get_tables, add_drop_column_monkey):
    mock_get_tables.return_value = {}

    with pytest.raises(UnsupportedNemesis, match="can't find table"):
        add_drop_column_monkey.disrupt()


def test_add_drop_column_multiple_iterations(mock_get_tables, runner):
    """Verify run_in_cycle executes multiple add_drop_column calls within the time window."""
    mock_get_tables.return_value = {"ks1": ["tbl1"]}
    monkey = AddDropColumnMonkey(runner)
    monkey.CYCLE_DURATION = 10

    # Deterministic random: always add 1 column, names are sequential
    name_counter = 0

    def make_name(*args, **kwargs):
        nonlocal name_counter
        name_counter += 1
        return list(f"col{name_counter:02d}")

    monkey.random.randrange.return_value = 1
    monkey.random.choices.side_effect = make_name
    # For drop: choice picks first available column
    monkey.random.choice.side_effect = lambda seq: seq[0]

    # Track iterations
    original_add_drop = monkey.add_drop_column
    iteration_count = 0

    def counting_add_drop(**kwargs):
        nonlocal iteration_count
        iteration_count += 1
        original_add_drop(**kwargs)

    monkey.add_drop_column = counting_add_drop

    # time.time() sequence: 3 iterations then exit
    call_count = 0

    def time_sequence():
        nonlocal call_count
        call_count += 1
        return 100.0 + (call_count - 1) * 3

    with patch(f"{MODULE}.time.time", side_effect=time_sequence):
        monkey.disrupt()

    assert iteration_count == 3
    session = runner.cluster.cql_session
    # Iteration 1: only ADD (no columns to drop yet)
    # Iteration 2+: DROP then ADD (2 calls each)
    assert session.execute.call_count == 5
    stmts = [str(c[0][0]) for c in session.execute.call_args_list]
    assert "ADD" in stmts[0]
    assert "DROP" in stmts[1]
    assert "ADD" in stmts[2]
    assert "DROP" in stmts[3]
    assert "ADD" in stmts[4]
    for stmt in stmts:
        assert "ALTER TABLE tbl1" in stmt


def test_add_drop_column_no_cleanup(mock_get_tables, runner):
    """Verify columns added are NOT cleaned up after disrupt() finishes."""
    mock_get_tables.return_value = {"ks1": ["tbl1"]}
    monkey = AddDropColumnMonkey(runner)
    monkey.random.randrange.return_value = 1
    monkey.random.choices.return_value = list("mycol")

    monkey.run_in_cycle = monkey.add_drop_column

    monkey.disrupt()

    assert monkey.columns_info["ks1"]["tbl1"]["column_names"] != {}
    assert "mycol" in monkey.columns_info["ks1"]["tbl1"]["column_names"]


# ---------------------------------------------------------------------------
# Tests: partition_deletion_divisor property
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "stress_cmd,expected",
    [
        (["scylla-bench -mode=write -workload=sequential"], 3),
        (["cassandra-stress write n=1000000"], 5),
        (None, 5),
    ],
)
def test_partition_deletion_divisor(delete_partitions_monkey, stress_cmd, expected):
    delete_partitions_monkey.runner.cluster.params = {"stress_cmd": stress_cmd}
    assert delete_partitions_monkey.partition_deletion_divisor == expected


# ---------------------------------------------------------------------------
# Tests: verify_initial_inputs_for_delete_nemesis
# ---------------------------------------------------------------------------


def test_verify_inputs_raises_when_no_scylla_bench():
    cluster = MagicMock()
    cluster.get_test_keyspaces.return_value = ["keyspace1"]
    tester = MagicMock()

    with pytest.raises(UnsupportedNemesis, match="scylla_bench"):
        verify_initial_inputs_for_delete_nemesis(cluster, tester)


def test_verify_inputs_raises_when_no_partition_attrs():
    cluster = MagicMock()
    cluster.get_test_keyspaces.return_value = ["scylla_bench"]
    tester = MagicMock()
    tester.partitions_attrs = None

    with pytest.raises(UnsupportedNemesis, match="max_partitions_in_test_table"):
        verify_initial_inputs_for_delete_nemesis(cluster, tester)


# ---------------------------------------------------------------------------
# Tests: @scylla_versions on monkey classes (decorator fix)
# ---------------------------------------------------------------------------


def test_scylla_versions_resolves_via_runner_cluster(runner_v54):
    monkey = TruncateMonkey(runner_v54)

    result = monkey.truncate_cmd_timeout_suffix(600)

    assert result == " USING TIMEOUT 600s"


def test_scylla_versions_old_version_returns_empty(runner_v51):
    monkey = TruncateMonkey(runner_v51)

    result = monkey.truncate_cmd_timeout_suffix(600)

    assert result == ""

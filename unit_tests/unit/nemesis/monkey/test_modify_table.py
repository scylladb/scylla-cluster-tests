"""Tests for sdcm.nemesis.monkey.modify_table module.

Tests focus on deterministic behavior: CQL structure, error paths,
method delegation, and the pure arithmetic in set_new_twcs_settings.
Randomly-generated values are not asserted on.
"""

from unittest.mock import patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.modify_table import (
    ModifyTableCommentMonkey,
    ModifyTableCompactionMonkey,
    ModifyTableCompressionMonkey,
    ModifyTableDefaultTimeToLiveMonkey,
    ModifyTableTwcsWindowSizeMonkey,
)
from test_lib.compaction import CompactionStrategy, TimeWindowCompactionProperties
from unit_tests.unit.nemesis import TestRunner

_MODULE = "sdcm.nemesis.monkey.modify_table"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` with a single non-system table for modify-table tests."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.unprotected"]
    base_runner.cluster.params.get.return_value = None
    return base_runner


@pytest.fixture()
def twcs_runner(runner):
    """TestRunner pre-configured for ModifyTableTwcsWindowSizeMonkey tests."""
    runner.cluster.get_all_tables_with_twcs.return_value = [
        {
            "name": "keyspace1.unprotected",
            "compaction": {
                "class": "TimeWindowCompactionStrategy",
                "compaction_window_unit": "HOURS",
                "compaction_window_size": 2,
            },
            "gc": 100,
            "dttl": 100,
        }
    ]
    runner.random.randint.return_value = 5
    runner.tester.params = {"test_duration": 0}
    return runner


@pytest.fixture()
def compression_monkey(runner):
    """Generic monkey exercising the shared modify_table_property() path."""
    return ModifyTableCompressionMonkey(runner)


@pytest.fixture()
def compaction_monkey(runner):
    """Monkey for testing compaction-specific protection."""
    return ModifyTableCompactionMonkey(runner)


@pytest.fixture()
def ttl_monkey(runner):
    """Monkey with its own table selection path."""
    return ModifyTableDefaultTimeToLiveMonkey(runner)


# ---------------------------------------------------------------------------
# Tests for modify_table_property (base helper)
# ---------------------------------------------------------------------------


def test_modify_table_property_with_explicit_ks_cf():
    """Explicit keyspace.table bypasses get_non_system_ks_cf_list."""
    monkey = ModifyTableCommentMonkey(TestRunner())
    monkey.modify_table_property(name="gc_grace_seconds", val=100, keyspace_table="myks.mytbl")

    assert monkey.runner.executed[-1] == "ALTER TABLE myks.mytbl WITH gc_grace_seconds = 100;"


def test_modify_table_property_raises_unsupported_when_no_tables():
    """Raise UnsupportedNemesis when no non-system tables exist."""
    monkey = ModifyTableCommentMonkey(TestRunner(ks_cfs=[]))

    with pytest.raises(UnsupportedNemesis, match="Non-system keyspace and table are not found"):
        monkey.modify_table_property(name="comment", val="'x'")


def test_modify_table_property_forwards_filter_out_counter(runner):
    """Verify filter_out_table_with_counter is forwarded to get_non_system_ks_cf_list."""
    monkey = ModifyTableCommentMonkey(runner)
    monkey.modify_table_property(name="x", val=1, filter_out_table_with_counter=True)

    monkey.runner.cluster.get_non_system_ks_cf_list.assert_called_once_with(
        db_node=monkey.runner.target_node,
        filter_out_table_with_counter=True,
        filter_out_mv=True,
    )


# ---------------------------------------------------------------------------
# Tests for individual monkey disrupt() methods
# ---------------------------------------------------------------------------


def test_comment_monkey(runner):
    """Verify disrupt() generates a random comment and applies it via ALTER TABLE."""
    with patch(f"{_MODULE}.generate_random_string", return_value="abc123") as mock_gen:
        monkey = ModifyTableCommentMonkey(runner)
        monkey.disrupt()

    mock_gen.assert_called_once_with(24)
    assert monkey.runner.executed[-1] == "ALTER TABLE keyspace1.unprotected WITH comment = 'abc123';"


def test_compaction_twcs_sets_compaction_before_ttl(runner):
    """When TimeWindowCompactionStrategy is chosen, compaction must be set before
    default_time_to_live so that the new DAYS-based window is active when ScyllaDB
    validates the TTL against twcs_max_window_count."""
    monkey = ModifyTableCompactionMonkey(runner)
    # Force the TWCS lambda to be picked
    monkey.random.choice = lambda seq: seq[2] if callable(seq[0]) else seq[0]

    monkey.disrupt()

    stmts = monkey.runner.executed
    assert len(stmts) == 2
    assert "'class': 'TimeWindowCompactionStrategy'" in stmts[0]
    assert "default_time_to_live = 4300000" in stmts[1]


def test_default_ttl_non_twcs(runner):
    """Non-TWCS table gets the hardcoded max TTL."""
    with patch(f"{_MODULE}.get_compaction_strategy", return_value=CompactionStrategy.SIZE_TIERED):
        monkey = ModifyTableDefaultTimeToLiveMonkey(runner)
        monkey.disrupt()

    assert monkey.runner.executed[-1] == "ALTER TABLE keyspace1.unprotected WITH default_time_to_live = 4300000;"


def test_default_ttl_twcs(runner):
    """TWCS table delegates to calculate_allowed_twcs_ttl."""
    with (
        patch(f"{_MODULE}.get_compaction_strategy", return_value=CompactionStrategy.TIME_WINDOW),
        patch(
            f"{_MODULE}.get_table_compaction_info",
            return_value=TimeWindowCompactionProperties(
                class_name="TimeWindowCompactionStrategy",
                compaction_window_unit="DAYS",
                compaction_window_size=1,
            ),
        ),
        patch(f"{_MODULE}.calculate_allowed_twcs_ttl", return_value=2000000) as mock_calc_ttl,
    ):
        monkey = ModifyTableDefaultTimeToLiveMonkey(runner)
        monkey.disrupt()

    assert monkey.runner.executed[-1] == "ALTER TABLE keyspace1.unprotected WITH default_time_to_live = 2000000;"
    mock_calc_ttl.assert_called_once()


def test_default_ttl_raises_unsupported_when_no_tables():
    """Raise UnsupportedNemesis when no non-system tables exist."""
    monkey = ModifyTableDefaultTimeToLiveMonkey(TestRunner(ks_cfs=[]))

    with pytest.raises(UnsupportedNemesis, match="No non-system user tables found"):
        monkey.disrupt()


# ---------------------------------------------------------------------------
# Tests for ModifyTableTwcsWindowSizeMonkey
# ---------------------------------------------------------------------------


def test_twcs_disrupt(twcs_runner):
    """Verify the full disrupt() flow: CQL statements, node restart,
    major compaction, and schema agreement."""
    monkey = ModifyTableTwcsWindowSizeMonkey(twcs_runner)
    monkey.disrupt()

    # emits 3 ALTER TABLE statements: compaction, dttl, gc
    stmts = monkey.runner.executed
    assert len(stmts) == 3
    assert "compaction = " in stmts[0]
    assert "default_time_to_live = " in stmts[1]
    assert "gc_grace_seconds = " in stmts[2]

    # stops and restarts scylla for reshape
    monkey.runner.target_node.stop_scylla.assert_called_once()
    monkey.runner.target_node.start_scylla.assert_called_once()

    # runs major compaction on the target node
    monkey.runner.target_node.run_nodetool.assert_called()
    call_args = monkey.runner.target_node.run_nodetool.call_args
    assert call_args[0][0] == "compact"
    assert call_args[1]["args"] == "keyspace1 unprotected"

    # waits for schema agreement after ALTER statements
    monkey.runner.cluster.wait_for_schema_agreement.assert_called_once()


def test_twcs_raises_unsupported_when_no_tables(twcs_runner):
    """Raise UnsupportedNemesis when no tables with TWCS are found."""
    monkey = ModifyTableTwcsWindowSizeMonkey(twcs_runner)
    monkey.runner.cluster.get_all_tables_with_twcs.return_value = []

    with pytest.raises(UnsupportedNemesis, match="No table found with TWCS"):
        monkey.disrupt()


# ---------------------------------------------------------------------------
# Tests for set_new_twcs_settings
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "randint_return, unit, initial_size, expected",
    [
        pytest.param(
            30,
            "MINUTES",
            1,
            {
                "compaction": {"compaction_window_unit": "MINUTES", "compaction_window_size": 31},
                "gc": 32550,
                "dttl": 65100,
            },
            id="minutes",
        ),
        pytest.param(
            10,
            "HOURS",
            2,
            {
                "compaction": {"compaction_window_unit": "HOURS", "compaction_window_size": 12},
                "gc": 756000,
                "dttl": 1512000,
            },
            id="hours",
        ),
        pytest.param(
            5,
            "DAYS",
            100,
            {
                "compaction": {"compaction_window_unit": "DAYS", "compaction_window_size": 1},
                "gc": 1512000,
                "dttl": 3024000,
            },
            id="days-capped",
        ),
        pytest.param(
            1,
            "UNKNOWN_UNIT",
            1,
            {
                "compaction": {"compaction_window_unit": "UNKNOWN_UNIT", "compaction_window_size": 1},
                "gc": 1512000,
                "dttl": 3024000,
            },
            id="unknown-unit-defaults",
        ),
        pytest.param(
            5,
            "HOURS",
            1,
            {
                "compaction": {"compaction_window_unit": "HOURS", "compaction_window_size": 6},
                "gc": 378000,
                "dttl": 756000,
            },
            id="gc-is-half-of-dttl",
        ),
    ],
)
def test_twcs_settings(runner, randint_return, unit, initial_size, expected):
    """Verify set_new_twcs_settings arithmetic for various window units."""
    runner.random.randint.return_value = randint_return
    monkey = ModifyTableTwcsWindowSizeMonkey(runner)

    settings = {
        "compaction": {"compaction_window_unit": unit, "compaction_window_size": initial_size},
        "gc": 100,
        "dttl": 100,
    }

    result = monkey.set_new_twcs_settings(settings)

    assert result == expected
    # invariant: gc is always half of dttl
    assert result["gc"] == result["dttl"] // 2


# ---------------------------------------------------------------------------
# Tests for _get_tables_with_explicit_property and the skip-if-explicitly-configured logic
# ---------------------------------------------------------------------------

CREATE_TABLE_WITH_TTL = "CREATE TABLE keyspace1.protected (id int PRIMARY KEY) WITH default_time_to_live = 86400;"

CREATE_TABLE_WITH_COMPACTION_AND_COMPRESSION = (
    "CREATE TABLE keyspace1.protected "
    "(key blob, c0 blob, PRIMARY KEY (key)) "
    "WITH compaction = {'class': 'LeveledCompactionStrategy'} "
    "AND compression = {'chunk_length_in_kb': '64', "
    "'sstable_compression': 'org.apache.cassandra.io.compress.ZstdCompressor'};"
)

CREATE_TABLE_WITH_GC_GRACE = "CREATE TABLE keyspace1.protected (id int PRIMARY KEY) WITH gc_grace_seconds = 3600;"

ALTER_TABLE_WITH_COMPACTION_AND_TTL = (
    "ALTER TABLE keyspace1.protected WITH compaction = "
    "{'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': '1', "
    "'compaction_window_unit': 'HOURS'} AND compression = {'sstable_compression': "
    "'org.apache.cassandra.io.compress.ZstdCompressor'} AND default_time_to_live = 10800;"
)

ALTER_TABLE_WITH_GC_AND_TTL = (
    "ALTER TABLE keyspace1.protected WITH gc_grace_seconds = 240 AND default_time_to_live = 240;"
)


def _set_params(runner, pre_create=None, post_prepare=None, compaction_strategy=None):
    """Configure runner.cluster.params.get to return different values per key."""
    lookup = {
        "pre_create_keyspace": pre_create,
        "post_prepare_cql_cmds": post_prepare,
        "compaction_strategy": compaction_strategy,
    }
    runner.cluster.params.get.side_effect = lambda key: lookup.get(key)


# ---------------------------------------------------------------------------
# _get_tables_with_explicit_property detection
# ---------------------------------------------------------------------------


def test_no_config_returns_no_protected_tables(compression_monkey):
    """No configuration set → no protected tables."""
    _set_params(compression_monkey.runner)
    assert compression_monkey._get_tables_with_explicit_property("compression") == set()


def test_non_create_or_alter_cql_ignored(compression_monkey):
    """CQL statements like CREATE KEYSPACE are ignored."""
    _set_params(
        compression_monkey.runner,
        pre_create=[
            "CREATE KEYSPACE IF NOT EXISTS keyspace1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"
        ],
    )
    assert compression_monkey._get_tables_with_explicit_property("compression") == set()


def test_pre_create_keyspace_detects_compaction_and_compression(compression_monkey):
    """CREATE TABLE with compaction + compression is detected for both properties."""
    _set_params(compression_monkey.runner, pre_create=[CREATE_TABLE_WITH_COMPACTION_AND_COMPRESSION])

    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("compaction")
    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("compression")
    assert compression_monkey._get_tables_with_explicit_property("comment") == set()


def test_post_prepare_cql_cmds_detects_properties(compression_monkey):
    """ALTER TABLE in post_prepare_cql_cmds detects compaction, compression, TTL, and gc_grace."""
    _set_params(
        compression_monkey.runner, post_prepare=[ALTER_TABLE_WITH_COMPACTION_AND_TTL, ALTER_TABLE_WITH_GC_AND_TTL]
    )

    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("compaction")
    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("compression")
    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("default_time_to_live")
    assert "keyspace1.protected" in compression_monkey._get_tables_with_explicit_property("gc_grace_seconds")
    assert compression_monkey._get_tables_with_explicit_property("comment") == set()


def test_compaction_strategy_param_only_protects_compaction(compression_monkey):
    """compaction_strategy param protects keyspace1.standard1 for compaction only."""
    _set_params(compression_monkey.runner, compaction_strategy="IncrementalCompactionStrategy")

    assert "keyspace1.standard1" in compression_monkey._get_tables_with_explicit_property("compaction")
    assert compression_monkey._get_tables_with_explicit_property("compression") == set()
    assert compression_monkey._get_tables_with_explicit_property("default_time_to_live") == set()


def test_combined_sources_merge_protected_tables(compression_monkey):
    """Tables from all three sources are merged into the protected set."""
    _set_params(
        compression_monkey.runner,
        pre_create=[CREATE_TABLE_WITH_COMPACTION_AND_COMPRESSION],
        post_prepare=[ALTER_TABLE_WITH_GC_AND_TTL],
        compaction_strategy="LeveledCompactionStrategy",
    )

    protected_compaction = compression_monkey._get_tables_with_explicit_property("compaction")
    assert "keyspace1.protected" in protected_compaction  # from pre_create_keyspace
    assert "keyspace1.standard1" in protected_compaction  # from compaction_strategy param

    protected_gc = compression_monkey._get_tables_with_explicit_property("gc_grace_seconds")
    assert "keyspace1.protected" in protected_gc  # from post_prepare_cql_cmds


# ---------------------------------------------------------------------------
# modify_table_property() skips protected tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "params_kwargs",
    [
        {"pre_create": [CREATE_TABLE_WITH_COMPACTION_AND_COMPRESSION]},
        {"post_prepare": [ALTER_TABLE_WITH_COMPACTION_AND_TTL]},
    ],
)
def test_raises_when_all_tables_protected(compression_monkey, params_kwargs):
    """Raises UnsupportedNemesis when every available table is protected."""
    _set_params(compression_monkey.runner, **params_kwargs)
    compression_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.protected"]

    with pytest.raises(UnsupportedNemesis, match="explicitly set in test configuration"):
        compression_monkey.disrupt()


def test_raises_when_all_tables_protected_by_compaction_strategy(compaction_monkey):
    """Raises UnsupportedNemesis when compaction_strategy param protects all tables."""
    _set_params(compaction_monkey.runner, compaction_strategy="LeveledCompactionStrategy")
    compaction_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.standard1"]

    with pytest.raises(UnsupportedNemesis, match="explicitly set in test configuration"):
        compaction_monkey.disrupt()


@pytest.mark.parametrize(
    "params_kwargs",
    [
        {"pre_create": [CREATE_TABLE_WITH_COMPACTION_AND_COMPRESSION]},
        {"post_prepare": [ALTER_TABLE_WITH_COMPACTION_AND_TTL]},
    ],
)
def test_mixed_tables_skips_protected(compression_monkey, params_kwargs):
    """When some tables are protected, only unprotected ones are modified."""
    _set_params(compression_monkey.runner, **params_kwargs)
    compression_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = [
        "keyspace1.protected",
        "keyspace1.unprotected",
    ]
    compression_monkey.runner.random.choice.side_effect = lambda seq: seq[0]

    compression_monkey.disrupt()

    assert not any("keyspace1.protected" in stmt for stmt in compression_monkey.runner.executed)
    assert any("keyspace1.unprotected" in stmt for stmt in compression_monkey.runner.executed)


def test_mixed_tables_skips_protected_by_compaction_strategy(compaction_monkey):
    """When compaction_strategy is set, keyspace1.standard1 is skipped for compaction."""
    _set_params(compaction_monkey.runner, compaction_strategy="LeveledCompactionStrategy")
    compaction_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = [
        "keyspace1.standard1",
        "keyspace1.unprotected",
    ]
    compaction_monkey.runner.random.choice.side_effect = lambda seq: seq[0]

    compaction_monkey.disrupt()

    assert not any("keyspace1.standard1" in stmt for stmt in compaction_monkey.runner.executed)
    assert any("keyspace1.unprotected" in stmt for stmt in compaction_monkey.runner.executed)


# ---------------------------------------------------------------------------
# ModifyTableDefaultTimeToLiveMonkey own table selection path
# ---------------------------------------------------------------------------


def test_ttl_monkey_skips_protected_picks_unprotected(ttl_monkey):
    """TTL monkey's own filter skips protected tables."""
    _set_params(ttl_monkey.runner, pre_create=[CREATE_TABLE_WITH_TTL])
    ttl_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = [
        "keyspace1.protected",
        "keyspace1.unprotected",
    ]

    with patch(f"{_MODULE}.get_compaction_strategy", return_value=CompactionStrategy.SIZE_TIERED):
        ttl_monkey.disrupt()

    assert not any("keyspace1.protected" in stmt for stmt in ttl_monkey.runner.executed)
    assert any("keyspace1.unprotected" in stmt for stmt in ttl_monkey.runner.executed)


def test_ttl_monkey_raises_when_all_tables_protected(ttl_monkey):
    """TTL monkey raises UnsupportedNemesis when all tables have explicit TTL."""
    _set_params(ttl_monkey.runner, pre_create=[CREATE_TABLE_WITH_TTL])
    ttl_monkey.runner.cluster.get_non_system_ks_cf_list.return_value = ["keyspace1.protected"]

    with pytest.raises(UnsupportedNemesis, match="explicitly set in test configuration"):
        ttl_monkey.disrupt()

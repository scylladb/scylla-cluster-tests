"""Tests for sdcm.nemesis.monkey.modify_table module.

Tests focus on deterministic behavior: CQL structure, error paths,
method delegation, the pure arithmetic in set_new_twcs_settings, and the
explicitly-configured-property protection (SCT-100).
Randomly-generated values are not asserted on.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.modify_table import (
    ModifyTableBaseMonkey,
    ModifyTableCommentMonkey,
    ModifyTableCompactionMonkey,
    ModifyTableDefaultTimeToLiveMonkey,
    ModifyTableTwcsWindowSizeMonkey,
    TableInitialProperties,
)
from test_lib.compaction import CompactionStrategy, TimeWindowCompactionProperties
from unit_tests.unit.nemesis import TestRunner

_MODULE = "sdcm.nemesis.monkey.modify_table"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture(autouse=True)
def table_initial_properties():
    """Give every test its own registry, since the base monkey keeps it on the class.

    ``refresh()`` is stubbed out — how the registry captures schema state is covered
    by test_table_initial_properties.py.  Here tests declare the pre-nemesis state
    directly in ``initial`` and only exercise the monkeys' table selection.
    """
    registry = TableInitialProperties()
    registry.defaults = {}
    registry.refresh = MagicMock()
    ModifyTableBaseMonkey.table_initial_properties = registry
    return registry


@pytest.fixture()
def runner(base_runner, table_initial_properties):
    """``base_runner`` with a single eligible non-system table for modify-table tests."""
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.tbl1"]
    table_initial_properties.initial["ks1.tbl1"] = {}
    return base_runner


@pytest.fixture()
def twcs_runner(runner):
    """TestRunner pre-configured for ModifyTableTwcsWindowSizeMonkey tests."""
    runner.cluster.get_all_tables_with_twcs.return_value = [
        {
            "name": "ks1.tbl1",
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
    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.tbl1 WITH comment = 'abc123';"


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

    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.tbl1 WITH default_time_to_live = 4300000;"


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

    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.tbl1 WITH default_time_to_live = 2000000;"
    mock_calc_ttl.assert_called_once()


def test_default_ttl_raises_unsupported_when_no_tables():
    """Raise UnsupportedNemesis when no non-system tables exist."""
    monkey = ModifyTableDefaultTimeToLiveMonkey(TestRunner(ks_cfs=[]))

    with pytest.raises(UnsupportedNemesis, match="Non-system keyspace and table are not found"):
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
    assert call_args[1]["args"] == "ks1 tbl1"

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
# Tests for the explicitly-configured-property protection (SCT-100)
# ---------------------------------------------------------------------------


@pytest.fixture()
def two_table_runner(base_runner, table_initial_properties):
    """``base_runner`` with two non-system tables, so one of them can be excluded.

    Both start with default properties; a test marks ``ks1.configured`` by putting
    the property value it cares about into the registry's pre-nemesis snapshot.
    """
    base_runner.cluster.get_non_system_ks_cf_list.return_value = ["ks1.configured", "ks1.free"]
    table_initial_properties.initial = {"ks1.configured": {}, "ks1.free": {}}
    return base_runner


def test_explicitly_configured_table_is_skipped(two_table_runner, table_initial_properties):
    """A table whose property is explicitly configured is excluded from selection."""
    table_initial_properties.initial["ks1.configured"] = {"comment": "set by the test"}
    monkey = ModifyTableCommentMonkey(two_table_runner)

    monkey.modify_table_property(name="comment", val="'x'")

    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.free WITH comment = 'x';"


def test_exclusion_is_property_specific(two_table_runner, table_initial_properties):
    """A table excluded for one property is still fair game for other properties."""
    table_initial_properties.initial["ks1.configured"] = {"compression": {"sstable_compression": "ZstdCompressor"}}
    monkey = ModifyTableCommentMonkey(two_table_runner)

    monkey.modify_table_property(name="comment", val="'x'")

    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.configured WITH comment = 'x';"


def test_all_tables_configured_raises_unsupported(two_table_runner, table_initial_properties):
    """Raise UnsupportedNemesis instead of overwriting intentional settings."""
    table_initial_properties.initial["ks1.configured"] = {"comment": "set by the test"}
    table_initial_properties.initial["ks1.free"] = {"comment": "set by the test too"}
    monkey = ModifyTableCommentMonkey(two_table_runner)

    with pytest.raises(UnsupportedNemesis, match="explicitly configured"):
        monkey.modify_table_property(name="comment", val="'x'")
    assert not monkey.runner.executed


def test_selection_refreshes_the_registry(two_table_runner, table_initial_properties):
    """The registry is refreshed before every pick, so the pre-nemesis snapshot is
    taken before the first ALTER and new tables keep being captured later on."""
    monkey = ModifyTableCommentMonkey(two_table_runner)

    monkey.modify_table_property(name="comment", val="'x'")

    table_initial_properties.refresh.assert_called_once_with(two_table_runner.cluster, two_table_runner.target_node)


def test_default_ttl_skips_configured_table(two_table_runner, table_initial_properties):
    """ModifyTableDefaultTimeToLiveMonkey's own selection path respects the exclusion."""
    table_initial_properties.initial["ks1.configured"] = {"default_time_to_live": 1000}
    with patch(f"{_MODULE}.get_compaction_strategy", return_value=CompactionStrategy.SIZE_TIERED):
        monkey = ModifyTableDefaultTimeToLiveMonkey(two_table_runner)
        monkey.disrupt()

    assert monkey.runner.executed[-1] == "ALTER TABLE ks1.free WITH default_time_to_live = 4300000;"


def test_compaction_twcs_skips_ttl_configured_table(two_table_runner, table_initial_properties):
    """The TWCS path sets both compaction and TTL, so a configured TTL excludes a table."""
    table_initial_properties.initial["ks1.configured"] = {"default_time_to_live": 1000}
    monkey = ModifyTableCompactionMonkey(two_table_runner)
    # Force the TWCS lambda to be picked
    monkey.random.choice = lambda seq: seq[2] if callable(seq[0]) else seq[0]

    monkey.disrupt()

    stmts = monkey.runner.executed
    assert len(stmts) == 2
    assert all("ks1.free" in stmt for stmt in stmts)


def test_twcs_window_monkey_skips_explicitly_configured_tables(twcs_runner, table_initial_properties):
    """TWCS window monkey must not touch TWCS tables the test configured itself."""
    table_initial_properties.initial["ks1.tbl1"] = {"compaction": {"class": "TimeWindowCompactionStrategy"}}
    monkey = ModifyTableTwcsWindowSizeMonkey(twcs_runner)

    with pytest.raises(UnsupportedNemesis, match="All TWCS tables"):
        monkey.disrupt()
    assert not monkey.runner.executed

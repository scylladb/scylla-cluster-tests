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
    ModifyTableDefaultTimeToLiveMonkey,
    ModifyTableTwcsWindowSizeMonkey,
)
from test_lib.compaction import CompactionStrategy, TimeWindowCompactionProperties
from unit_tests.nemesis import TestRunner

_MODULE = "sdcm.nemesis.monkey.modify_table"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def runner():
    """Default TestRunner for tests that don't need special configuration."""
    runner = TestRunner(ks_cfs=["ks1.tbl1"])
    runner.random.choice.side_effect = lambda seq: seq[0]
    return runner


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


def test_compaction_twcs_sets_ttl_before_compaction(runner):
    """When TimeWindowCompactionStrategy is chosen, default_time_to_live must be set first."""
    monkey = ModifyTableCompactionMonkey(runner)
    # Force the TWCS lambda to be picked
    monkey.random.choice = lambda seq: seq[2] if callable(seq[0]) else seq[0]

    monkey.disrupt()

    stmts = monkey.runner.executed
    assert len(stmts) == 2
    assert "default_time_to_live = 4300000" in stmts[0]
    assert "'class': 'TimeWindowCompactionStrategy'" in stmts[1]


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

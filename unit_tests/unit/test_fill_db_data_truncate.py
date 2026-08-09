"""Tests for truncate_tables(): one slow TRUNCATE must not silently skip the rest.

The failure this guards against is indirect. truncate_tables() used to abort on the first
exception, so a single timeout left every later table holding the previous stage's rows;
fill_db_data() then re-inserted on top and the next verify_db_data() failed an assertion on
whichever table happened to keep a row - nowhere near the truncate that actually broke.
"""

import logging
from unittest.mock import MagicMock

import pytest
from cassandra import OperationTimedOut

from sdcm.fill_db_data import (
    FillDatabaseData,
    TruncateTablesError,
    MAX_TRUNCATE_FAILURES,
    TRUNCATE_TIMEOUT,
)


class _Harness:
    """Minimal stand-in for FillDatabaseData: truncate_tables() only needs these members."""

    def __init__(self, items, failing=()):
        self.all_verification_items = items
        self.log = logging.getLogger(__name__)
        self.failing = set(failing)
        self.truncated = []
        self._execute_and_log = FillDatabaseData._execute_and_log.__get__(self)

    def truncate_table(self, session, truncate):
        if truncate in self.failing:
            raise OperationTimedOut(f"simulated client timeout for {truncate}")
        self.truncated.append(truncate)


def _item(name, table, skip=""):
    return {"name": name, "truncates": [f"TRUNCATE {table}"], "skip": skip}


def _truncate_tables(harness, session=None):
    return FillDatabaseData.truncate_tables(harness, session or MagicMock())


def test_all_tables_truncated_when_nothing_fails():
    harness = _Harness([_item("a", "t_a"), _item("b", "t_b"), _item("c", "t_c")])

    _truncate_tables(harness)

    assert harness.truncated == ["TRUNCATE t_a", "TRUNCATE t_b", "TRUNCATE t_c"]


def test_one_failure_does_not_skip_the_remaining_tables():
    """The regression: a timeout on the first table used to abort the whole pass."""
    harness = _Harness([_item("a", "t_a"), _item("b", "t_b"), _item("c", "t_c")], failing=["TRUNCATE t_a"])

    with pytest.raises(TruncateTablesError):
        _truncate_tables(harness)

    # t_b and t_c must still have been truncated despite t_a failing first.
    assert harness.truncated == ["TRUNCATE t_b", "TRUNCATE t_c"]


def test_error_names_every_table_left_untruncated():
    harness = _Harness(
        [_item("alpha", "t_a"), _item("beta", "t_b"), _item("gamma", "t_c")],
        failing=["TRUNCATE t_a", "TRUNCATE t_c"],
    )

    with pytest.raises(TruncateTablesError) as excinfo:
        _truncate_tables(harness)

    message = str(excinfo.value)
    assert "alpha" in message and "gamma" in message
    assert "beta" not in message
    assert "2 table(s)" in message


def test_skipped_items_are_not_truncated():
    harness = _Harness([_item("a", "t_a"), _item("b", "t_b", skip="disabled for a reason")])

    _truncate_tables(harness)

    assert harness.truncated == ["TRUNCATE t_a"]


def test_gives_up_after_the_failure_cap_instead_of_burning_the_budget():
    """Every failure costs a full TRUNCATE_TIMEOUT, so a hopeless cluster must not run ~100 of them."""
    items = [_item(f"t{i}", f"t_{i}") for i in range(MAX_TRUNCATE_FAILURES + 5)]
    harness = _Harness(items, failing=[f"TRUNCATE t_{i}" for i in range(len(items))])

    with pytest.raises(TruncateTablesError) as excinfo:
        _truncate_tables(harness)

    assert "gave up" in str(excinfo.value)
    assert f"{MAX_TRUNCATE_FAILURES} table(s)" in str(excinfo.value)


def test_no_early_exit_while_under_the_failure_cap():
    items = [_item(f"t{i}", f"t_{i}") for i in range(MAX_TRUNCATE_FAILURES + 3)]
    # One fewer failure than the cap, so every remaining table still gets attempted.
    failing = [f"TRUNCATE t_{i}" for i in range(MAX_TRUNCATE_FAILURES - 1)]
    harness = _Harness(items, failing=failing)

    with pytest.raises(TruncateTablesError) as excinfo:
        _truncate_tables(harness)

    assert "gave up" not in str(excinfo.value)
    assert len(harness.truncated) == len(items) - len(failing)


def test_truncate_table_applies_the_timeout_on_both_sides():
    """A server-side USING TIMEOUT is cosmetic if the driver gives up first."""
    session = MagicMock()
    harness = MagicMock()

    FillDatabaseData.truncate_table(harness, session, "TRUNCATE t_a")

    query, kwargs = session.execute.call_args[0][0], session.execute.call_args[1]
    assert query == f"TRUNCATE t_a USING TIMEOUT {TRUNCATE_TIMEOUT}s"
    assert kwargs["timeout"] == TRUNCATE_TIMEOUT


def test_truncate_table_keeps_an_explicit_using_timeout():
    session = MagicMock()
    harness = MagicMock()

    FillDatabaseData.truncate_table(harness, session, "TRUNCATE t_a USING TIMEOUT 30s")

    assert session.execute.call_args[0][0] == "TRUNCATE t_a USING TIMEOUT 30s"

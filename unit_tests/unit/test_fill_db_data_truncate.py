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
    MAX_TRUNCATE_FAILURES,
    TRUNCATE_TIMEOUT,
    FillDatabaseData,
    TruncateTablesError,
)


class FakeFillDb:
    """Minimal stand-in for FillDatabaseData: truncate_tables() only needs these members."""

    def __init__(self, items, failing=()):
        """Store the verification items and the truncate statements that should fail."""
        self.all_verification_items = items
        self.log = logging.getLogger(__name__)
        self.failing = set(failing)
        self.truncated = []
        self._execute_and_log = FillDatabaseData._execute_and_log.__get__(self)

    def truncate_table(self, session, truncate):
        """Raise a client timeout for failing statements, otherwise record the truncate as done."""
        if truncate in self.failing:
            raise OperationTimedOut(f"simulated client timeout for {truncate}")
        self.truncated.append(truncate)


def make_item(name, table, skip=""):
    """Build a verification item that truncates ``table``."""
    return {"name": name, "truncates": [f"TRUNCATE {table}"], "skip": skip}


def run_truncate_tables(fake, session=None):
    """Run FillDatabaseData.truncate_tables against ``fake`` with a mock session by default."""
    return FillDatabaseData.truncate_tables(fake, session or MagicMock())


def test_all_tables_truncated_when_nothing_fails():
    """Every table is truncated, in order, when no statement fails."""
    fake = FakeFillDb([make_item("a", "t_a"), make_item("b", "t_b"), make_item("c", "t_c")])

    run_truncate_tables(fake)

    assert fake.truncated == ["TRUNCATE t_a", "TRUNCATE t_b", "TRUNCATE t_c"]


def test_one_failure_does_not_skip_the_remaining_tables():
    """The regression: a timeout on the first table used to abort the whole pass."""
    fake = FakeFillDb([make_item("a", "t_a"), make_item("b", "t_b"), make_item("c", "t_c")], failing=["TRUNCATE t_a"])

    with pytest.raises(TruncateTablesError):
        run_truncate_tables(fake)

    # t_b and t_c must still have been truncated despite t_a failing first.
    assert fake.truncated == ["TRUNCATE t_b", "TRUNCATE t_c"]


def test_error_names_every_table_left_untruncated():
    """TruncateTablesError names each table that was left untruncated and counts them."""
    fake = FakeFillDb(
        [make_item("alpha", "t_a"), make_item("beta", "t_b"), make_item("gamma", "t_c")],
        failing=["TRUNCATE t_a", "TRUNCATE t_c"],
    )

    with pytest.raises(TruncateTablesError) as excinfo:
        run_truncate_tables(fake)

    message = str(excinfo.value)
    assert "alpha" in message and "gamma" in message
    assert "beta" not in message
    assert "2 table(s)" in message


def test_skipped_items_are_not_truncated():
    """Items with a skip reason are left alone and never truncated."""
    fake = FakeFillDb([make_item("a", "t_a"), make_item("b", "t_b", skip="disabled for a reason")])

    run_truncate_tables(fake)

    assert fake.truncated == ["TRUNCATE t_a"]


def test_gives_up_after_the_failure_cap_instead_of_burning_the_budget():
    """Every failure costs a full TRUNCATE_TIMEOUT, so a hopeless cluster must not run ~100 of them."""
    items = [make_item(f"t{i}", f"t_{i}") for i in range(MAX_TRUNCATE_FAILURES + 5)]
    fake = FakeFillDb(items, failing=[f"TRUNCATE t_{i}" for i in range(len(items))])

    with pytest.raises(TruncateTablesError) as excinfo:
        run_truncate_tables(fake)

    assert "gave up" in str(excinfo.value)
    assert f"{MAX_TRUNCATE_FAILURES} table(s)" in str(excinfo.value)


def test_no_early_exit_while_under_the_failure_cap():
    """Below the failure cap, every remaining table is still attempted."""
    items = [make_item(f"t{i}", f"t_{i}") for i in range(MAX_TRUNCATE_FAILURES + 3)]
    # One fewer failure than the cap, so every remaining table still gets attempted.
    failing = [f"TRUNCATE t_{i}" for i in range(MAX_TRUNCATE_FAILURES - 1)]
    fake = FakeFillDb(items, failing=failing)

    with pytest.raises(TruncateTablesError) as excinfo:
        run_truncate_tables(fake)

    assert "gave up" not in str(excinfo.value)
    assert len(fake.truncated) == len(items) - len(failing)


def test_truncate_table_applies_the_timeout_on_both_sides():
    """A server-side USING TIMEOUT is cosmetic if the driver gives up first."""
    session = MagicMock()
    harness = MagicMock()

    FillDatabaseData.truncate_table(harness, session, "TRUNCATE t_a")

    query, kwargs = session.execute.call_args[0][0], session.execute.call_args[1]
    assert query == f"TRUNCATE t_a USING TIMEOUT {TRUNCATE_TIMEOUT}s"
    assert kwargs["timeout"] == TRUNCATE_TIMEOUT


def test_truncate_table_keeps_an_explicit_using_timeout():
    """A TRUNCATE that already carries USING TIMEOUT is sent unchanged."""
    session = MagicMock()
    harness = MagicMock()

    FillDatabaseData.truncate_table(harness, session, "TRUNCATE t_a USING TIMEOUT 30s")

    assert session.execute.call_args[0][0] == "TRUNCATE t_a USING TIMEOUT 30s"

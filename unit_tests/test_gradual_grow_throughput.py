# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""
Unit tests for the dynamic keyspace/table resolution logic in the gradual
performance testing framework (PerformanceRegressionPredefinedStepsTest).

These tests validate the get_test_table_names() behaviour without importing
the full heavy module stack by duplicating only the helper methods under test.
This mirrors the pattern used in other lightweight unit tests in this
repository (e.g. test_dns_readiness.py).
"""

import pytest


# ---------------------------------------------------------------------------
# Minimal re-implementation of the methods under test.
# These are copied from performance_regression_gradual_grow_throughput.py
# so they can be exercised without importing the full module tree.
# ---------------------------------------------------------------------------


def _is_latte_command(stress_cmd):
    cmd = stress_cmd[0] if isinstance(stress_cmd, list) else stress_cmd
    return "latte " in cmd and " run " in cmd


def _parse_perf_stress_tables(perf_stress_tables: dict) -> list[tuple[str, str]]:
    """Parse ``perf_stress_tables`` dict into a flat list of ``(keyspace, table)`` tuples."""
    tables: list[tuple[str, str]] = []
    for keyspace, table_value in perf_stress_tables.items():
        if isinstance(table_value, list):
            for tbl in table_value:
                tables.append((keyspace, tbl))
        else:
            tables.append((keyspace, str(table_value)))
    return tables


def _get_test_table_names(params, stress_cmds) -> list[tuple[str, str]]:
    stress_cmd = stress_cmds[0] if isinstance(stress_cmds, list) else stress_cmds
    stress_tool = stress_cmd.split(" ")[0]

    perf_stress_tables = params.get("perf_stress_tables") or {}

    tables: list[tuple[str, str]] = []
    if perf_stress_tables:
        tables = _parse_perf_stress_tables(perf_stress_tables)

    if _is_latte_command(stress_cmds) and not tables:
        if latte_schema_parameters := params.get("latte_schema_parameters"):
            keyspace = latte_schema_parameters.get("keyspace", "")
            table = latte_schema_parameters.get("table", "")
            if keyspace and table:
                tables = [(keyspace, table)]
            elif keyspace and not table:
                raise ValueError(
                    f"'perf_stress_tables' (or 'latte_schema_parameters.table' for latte) is required for "
                    f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
                )

    if not tables:
        raise ValueError(
            f"'perf_stress_tables' (or 'latte_schema_parameters' for latte) is required for "
            f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
        )
    return tables


# ---------------------------------------------------------------------------
# _parse_perf_stress_tables helper
# ---------------------------------------------------------------------------


class TestParsePerfStressTables:
    """Tests for the _parse_perf_stress_tables helper function."""

    def test_single_pair(self):
        """Single keyspace with a single table."""
        result = _parse_perf_stress_tables({"keyspace1": "standard1"})
        assert result == [("keyspace1", "standard1")]

    def test_multiple_keyspaces(self):
        """Multiple keyspaces, each with one table."""
        result = _parse_perf_stress_tables({"ks1": "tbl1", "ks2": "tbl2"})
        assert result == [("ks1", "tbl1"), ("ks2", "tbl2")]

    def test_multiple_tables_per_keyspace(self):
        """One keyspace with a list of tables."""
        result = _parse_perf_stress_tables({"ks1": ["tbl1", "tbl2"]})
        assert result == [("ks1", "tbl1"), ("ks1", "tbl2")]

    def test_mixed_single_and_list(self):
        """Mix of single-table and multi-table keyspaces."""
        result = _parse_perf_stress_tables({"ks1": "tbl1", "ks2": ["tbl2a", "tbl2b"]})
        assert result == [("ks1", "tbl1"), ("ks2", "tbl2a"), ("ks2", "tbl2b")]

    def test_empty_dict(self):
        """Empty dict returns empty list."""
        result = _parse_perf_stress_tables({})
        assert result == []


# ---------------------------------------------------------------------------
# cassandra-stress / cql-stress / scylla-bench: read from perf_stress_tables
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "cassandra-stress write cl=QUORUM n=1000",
        "cql-stress-cassandra-stress write cl=QUORUM n=1000",
        "scylla-bench -workload=sequential -mode=write",
    ],
)
def test_get_test_table_names_reads_from_yaml_params(cmd):
    """Non-latte tools must read keyspace and table from perf_stress_tables param."""
    result = _get_test_table_names(
        {"perf_stress_tables": {"mykeyspace": "mytable"}},
        [cmd],
    )
    assert result == [("mykeyspace", "mytable")]


@pytest.mark.parametrize(
    "cmd",
    [
        "cassandra-stress write cl=QUORUM n=1000",
        "scylla-bench -workload=sequential -mode=write",
    ],
)
def test_get_test_table_names_multiple_pairs(cmd):
    """Multiple keyspace.table pairs are returned correctly."""
    result = _get_test_table_names(
        {"perf_stress_tables": {"ks1": "tbl1", "ks2": "tbl2"}},
        [cmd],
    )
    assert result == [("ks1", "tbl1"), ("ks2", "tbl2")]


def test_get_test_table_names_multiple_tables_per_keyspace():
    """Multiple tables within a single keyspace."""
    result = _get_test_table_names(
        {"perf_stress_tables": {"ks1": ["tbl1", "tbl2"]}},
        ["cassandra-stress write cl=QUORUM n=1000"],
    )
    assert result == [("ks1", "tbl1"), ("ks1", "tbl2")]


def test_get_test_table_names_raises_when_tables_missing():
    """Raise a clear ValueError when perf_stress_tables is not configured."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"perf_stress_tables": None},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


def test_get_test_table_names_raises_when_tables_empty():
    """Raise a clear ValueError when perf_stress_tables is empty."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"perf_stress_tables": {}},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


# ---------------------------------------------------------------------------
# latte: keyspace and table from perf_stress_tables
# ---------------------------------------------------------------------------


def test_get_test_table_names_latte_from_perf_stress_tables():
    """Latte keyspace/table resolved from perf_stress_tables."""
    result = _get_test_table_names(
        {"perf_stress_tables": {"my_ks": "my_tbl"}},
        ["latte run /some/script.rn -f write"],
    )
    assert result == [("my_ks", "my_tbl")]


def test_get_test_table_names_latte_perf_stress_tables_take_priority():
    """perf_stress_tables take priority over latte_schema_parameters."""
    result = _get_test_table_names(
        {
            "perf_stress_tables": {"from_perf": "from_perf_tbl"},
            "latte_schema_parameters": {"keyspace": "from_schema", "table": "from_schema_tbl"},
        },
        ["latte run /some/script.rn -f write"],
    )
    assert result == [("from_perf", "from_perf_tbl")]


# ---------------------------------------------------------------------------
# latte: keyspace and table from latte_schema_parameters (fallback)
# ---------------------------------------------------------------------------


def test_get_test_table_names_latte_from_schema_params():
    """Latte keyspace/table resolved from latte_schema_parameters when perf_stress_tables not set."""
    result = _get_test_table_names(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
        ["latte run /some/script.rn -f write"],
    )
    assert result == [("my_latte_ks", "my_latte_tbl")]


def test_get_test_table_names_latte_partial_schema_params_keyspace_only():
    """latte_schema_parameters has keyspace but no table — should raise ValueError."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks"}},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# latte: missing configuration raises ValueError
# ---------------------------------------------------------------------------


def test_get_test_table_names_latte_raises_when_keyspace_unresolvable():
    """Raise a clear ValueError when latte keyspace cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"latte_schema_parameters": {}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_names_latte_raises_when_table_unresolvable():
    """Raise a clear ValueError when latte table cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"latte_schema_parameters": {"keyspace": "my_ks"}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_names_latte_raises_when_no_params_at_all():
    """Raise a clear ValueError when no configuration is provided for latte."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# Non-latte tools ignore latte_schema_parameters
# ---------------------------------------------------------------------------


def test_get_test_table_names_non_latte_ignores_schema_params():
    """Non-latte tools should not fall back to latte_schema_parameters."""
    with pytest.raises(ValueError, match="perf_stress_tables"):
        _get_test_table_names(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )

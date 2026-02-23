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

These tests validate the get_test_table_name() behaviour without importing
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


def _get_test_table_name(params, stress_cmds):
    stress_cmd = stress_cmds[0] if isinstance(stress_cmds, list) else stress_cmds
    stress_tool = stress_cmd.split(" ")[0]

    keyspace = params.get("perf_stress_keyspace") or ""
    table = params.get("perf_stress_table") or ""

    if _is_latte_command(stress_cmds) and (not keyspace or not table):
        if latte_schema_parameters := params.get("latte_schema_parameters"):
            if not keyspace:
                keyspace = latte_schema_parameters.get("keyspace", "")
            if not table:
                table = latte_schema_parameters.get("table", "")

    if not keyspace:
        raise ValueError(
            f"'perf_stress_keyspace' (or 'latte_schema_parameters.keyspace' for latte) is required for "
            f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
        )
    if not table:
        raise ValueError(
            f"'perf_stress_table' (or 'latte_schema_parameters.table' for latte) is required for "
            f"'{stress_tool}' gradual performance tests. Please add it to your test YAML configuration."
        )
    return keyspace, table


# ---------------------------------------------------------------------------
# cassandra-stress / cql-stress / scylla-bench: read from YAML params
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "cassandra-stress write cl=QUORUM n=1000",
        "cql-stress-cassandra-stress write cl=QUORUM n=1000",
        "scylla-bench -workload=sequential -mode=write",
    ],
)
def test_get_test_table_name_reads_from_yaml_params(cmd):
    """Non-latte tools must read keyspace and table from perf_stress_keyspace/perf_stress_table params."""
    keyspace, table = _get_test_table_name(
        {"perf_stress_keyspace": "mykeyspace", "perf_stress_table": "mytable"},
        [cmd],
    )
    assert keyspace == "mykeyspace"
    assert table == "mytable"


def test_get_test_table_name_raises_when_keyspace_missing():
    """Raise a clear ValueError when perf_stress_keyspace is not configured."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"perf_stress_keyspace": None, "perf_stress_table": "standard1"},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


def test_get_test_table_name_raises_when_table_missing():
    """Raise a clear ValueError when perf_stress_table is not configured."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"perf_stress_keyspace": "keyspace1", "perf_stress_table": None},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


# ---------------------------------------------------------------------------
# latte: keyspace and table from perf_stress_keyspace / perf_stress_table
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_perf_stress_params():
    """Latte keyspace/table resolved from perf_stress_keyspace/perf_stress_table."""
    keyspace, table = _get_test_table_name(
        {"perf_stress_keyspace": "my_ks", "perf_stress_table": "my_tbl"},
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "my_ks"
    assert table == "my_tbl"


def test_get_test_table_name_latte_perf_stress_params_take_priority():
    """perf_stress_keyspace/perf_stress_table take priority over latte_schema_parameters."""
    keyspace, table = _get_test_table_name(
        {
            "perf_stress_keyspace": "from_perf",
            "perf_stress_table": "from_perf_tbl",
            "latte_schema_parameters": {"keyspace": "from_schema", "table": "from_schema_tbl"},
        },
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "from_perf"
    assert table == "from_perf_tbl"


# ---------------------------------------------------------------------------
# latte: keyspace and table from latte_schema_parameters (fallback)
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_schema_params():
    """Latte keyspace/table resolved from latte_schema_parameters when perf_stress_* not set."""
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "my_latte_ks"
    assert table == "my_latte_tbl"


def test_get_test_table_name_latte_partial_perf_stress_with_schema_fallback():
    """perf_stress_keyspace set, table falls back to latte_schema_parameters."""
    keyspace, table = _get_test_table_name(
        {
            "perf_stress_keyspace": "from_perf",
            "perf_stress_table": None,
            "latte_schema_parameters": {"keyspace": "from_schema", "table": "from_schema_tbl"},
        },
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "from_perf"
    assert table == "from_schema_tbl"


def test_get_test_table_name_latte_partial_schema_params_keyspace_only():
    """latte_schema_parameters has keyspace but no table — should raise ValueError."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks"}},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# latte: missing configuration raises ValueError
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_raises_when_keyspace_unresolvable():
    """Raise a clear ValueError when latte keyspace cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"latte_schema_parameters": {}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_name_latte_raises_when_table_unresolvable():
    """Raise a clear ValueError when latte table cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_ks"}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_name_latte_raises_when_no_params_at_all():
    """Raise a clear ValueError when no configuration is provided for latte."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# Non-latte tools ignore latte_schema_parameters
# ---------------------------------------------------------------------------


def test_get_test_table_name_non_latte_ignores_schema_params():
    """Non-latte tools should not fall back to latte_schema_parameters."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )

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

Tests import the production code directly to ensure we're validating real behaviour.
"""

from types import SimpleNamespace

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module


def _get_test_table_name(params, stress_cmds):
    """Call the production get_test_table_name with a minimal mock instance."""
    instance = SimpleNamespace(params=params)
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest.get_test_table_name(instance, stress_cmds)


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

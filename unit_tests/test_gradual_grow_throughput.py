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

These tests validate the refactored get_test_table_name() behaviour without
importing the full heavy module stack by duplicating only the three pure
helper methods under test.  This mirrors the pattern used in other lightweight
unit tests in this repository (e.g. test_dns_readiness.py).
"""

import re
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Minimal re-implementation of the three methods under test.
# These are copied verbatim from performance_regression_gradual_grow_throughput.py
# so they can be exercised without importing the full module tree.
# ---------------------------------------------------------------------------


def _is_latte_command(stress_cmd):
    cmd = stress_cmd[0] if isinstance(stress_cmd, list) else stress_cmd
    return "latte " in cmd and " run " in cmd


def _parse_latte_params_from_rune_script(sct_root, stress_cmd):
    """Parse default keyspace and table values from a latte rune script file."""
    script_name_regex = re.compile(r"([/\w-]*\.rn)")
    match = script_name_regex.search(stress_cmd)
    if not match:
        return "", ""

    script_name = match.group(0)
    script_path = Path(sct_root) / script_name
    if not script_path.exists():
        return "", ""

    script_content = script_path.read_text(encoding="utf-8")
    keyspace = ""
    table = ""
    for param_name in ("keyspace", "table"):
        param_regex = re.compile(rf'latte::param!\("{re.escape(param_name)}",\s*"([^"]+)"\)')
        if m := param_regex.search(script_content):
            if param_name == "keyspace":
                keyspace = m.group(1)
            else:
                table = m.group(1)
    return keyspace, table


def _resolve_latte_keyspace_and_table(params, log, sct_root, stress_cmd):
    """Dynamically resolve keyspace and table name for latte commands."""
    keyspace = ""
    table = ""
    if latte_schema_parameters := params.get("latte_schema_parameters"):
        keyspace = latte_schema_parameters.get("keyspace", "")
        table = latte_schema_parameters.get("table", "")

    if not keyspace or not table:
        parsed_keyspace, parsed_table = _parse_latte_params_from_rune_script(sct_root, stress_cmd)
        if not keyspace:
            keyspace = parsed_keyspace
        if not table:
            table = parsed_table

    if not keyspace:
        raise ValueError(
            "Cannot determine latte keyspace. Add 'keyspace' to 'latte_schema_parameters' in your test YAML, "
            'or ensure the rune script defines it via latte::param!("keyspace", "...").'
        )
    if not table:
        log.warning(
            "Cannot determine latte table name. Add 'table' to 'latte_schema_parameters' in your test YAML "
            "if post_prepare_cql_cmds filtering by table is needed."
        )
    return keyspace, table


def _get_test_table_name(params, log, sct_root, stress_cmds):
    stress_cmd = stress_cmds[0] if isinstance(stress_cmds, list) else stress_cmds
    if _is_latte_command(stress_cmds):
        return _resolve_latte_keyspace_and_table(params, log, sct_root, stress_cmd)

    stress_tool = stress_cmd.split(" ")[0]
    keyspace = params.get("stress_keyspace")
    table = params.get("stress_table")
    if not keyspace:
        raise ValueError(
            f"'stress_keyspace' is required for '{stress_tool}' gradual performance tests. "
            "Please add it to your test YAML configuration."
        )
    if not table:
        raise ValueError(
            f"'stress_table' is required for '{stress_tool}' gradual performance tests. "
            "Please add it to your test YAML configuration."
        )
    return keyspace, table


# Use the real SCT root so that latte_cs_alike.rn can be found
SCT_ROOT = Path(__file__).parent.parent


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
    """Non-latte tools must read keyspace and table from stress_keyspace/stress_table params."""
    keyspace, table = _get_test_table_name(
        {"stress_keyspace": "mykeyspace", "stress_table": "mytable"},
        MagicMock(),
        SCT_ROOT,
        [cmd],
    )
    assert keyspace == "mykeyspace"
    assert table == "mytable"


def test_get_test_table_name_raises_when_keyspace_missing():
    """Raise a clear ValueError when stress_keyspace is not configured."""
    with pytest.raises(ValueError, match="stress_keyspace"):
        _get_test_table_name(
            {"stress_keyspace": None, "stress_table": "standard1"},
            MagicMock(),
            SCT_ROOT,
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


def test_get_test_table_name_raises_when_table_missing():
    """Raise a clear ValueError when stress_table is not configured."""
    with pytest.raises(ValueError, match="stress_table"):
        _get_test_table_name(
            {"stress_keyspace": "keyspace1", "stress_table": None},
            MagicMock(),
            SCT_ROOT,
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


# ---------------------------------------------------------------------------
# latte: keyspace and table from latte_schema_parameters
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_schema_params():
    """Latte keyspace/table resolved from latte_schema_parameters."""
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
        MagicMock(),
        SCT_ROOT,
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "my_latte_ks"
    assert table == "my_latte_tbl"


def test_get_test_table_name_latte_keyspace_only_in_schema_params():
    """Latte keyspace from latte_schema_parameters; table resolved from local rune script."""
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks"}},
        MagicMock(),
        SCT_ROOT,
        ["latte run data_dir/latte/latte_cs_alike.rn -f write"],
    )
    assert keyspace == "my_latte_ks"
    # table resolved from latte_cs_alike.rn default: "standard1"
    assert table == "standard1"


# ---------------------------------------------------------------------------
# latte: keyspace/table parsed from rune script (no latte_schema_parameters)
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_rune_script():
    """Latte keyspace and table resolved from rune script defaults."""
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {}},
        MagicMock(),
        SCT_ROOT,
        ["latte run data_dir/latte/latte_cs_alike.rn -f write"],
    )
    # defaults defined in latte_cs_alike.rn
    assert keyspace == "keyspace1"
    assert table == "standard1"


def test_get_test_table_name_latte_raises_when_keyspace_unresolvable():
    """Raise a clear ValueError when latte keyspace cannot be determined."""
    with pytest.raises(ValueError, match="latte keyspace"):
        _get_test_table_name(
            {"latte_schema_parameters": {}},
            MagicMock(),
            SCT_ROOT,
            # external script not present in the repo
            ["latte run external/script/not_local.rn -f write"],
        )


def test_get_test_table_name_latte_warns_when_table_unresolvable():
    """Log a warning (not raise) when latte table cannot be determined."""
    log = MagicMock()
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks"}},
        log,
        SCT_ROOT,
        # external script not present in the repo
        ["latte run external/script/not_local.rn -f write"],
    )
    assert keyspace == "my_latte_ks"
    assert table == ""
    log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# _parse_latte_params_from_rune_script edge cases
# ---------------------------------------------------------------------------


def test_parse_latte_params_from_nonexistent_script():
    """Return empty strings when the rune script file does not exist."""
    ks, tbl = _parse_latte_params_from_rune_script(SCT_ROOT, "latte run no/such/file.rn")
    assert ks == ""
    assert tbl == ""


def test_parse_latte_params_no_rn_in_cmd():
    """Return empty strings when the command contains no .rn file reference."""
    ks, tbl = _parse_latte_params_from_rune_script(SCT_ROOT, "latte run")
    assert ks == ""
    assert tbl == ""


def test_parse_latte_params_from_tmp_script():
    """Parse keyspace and table defaults from a temporary rune script."""
    rune_content = """
use latte::*;
const KEYSPACE = latte::param!("keyspace", "test_keyspace_name");
const TABLE = latte::param!("table", "test_table_name");
pub async fn schema(db) {}
"""
    with tempfile.NamedTemporaryFile(suffix=".rn", mode="w", delete=False) as f:
        f.write(rune_content)
        tmp_path = f.name

    # Build a fake SCT root so the relative path works
    tmp_dir = Path(tmp_path).parent
    script_relative = Path(tmp_path).name

    ks, tbl = _parse_latte_params_from_rune_script(tmp_dir, f"latte run {script_relative}")
    assert ks == "test_keyspace_name"
    assert tbl == "test_table_name"

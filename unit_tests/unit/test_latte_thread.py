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
# Copyright (c) 2021 ScyllaDB

import re

import pytest
import yaml

from sdcm import sct_abs_path
from sdcm.stress.latte_thread import (
    LatteStressThread,
    find_latte_fn_names,
    find_latte_tags,
    get_latte_operation_type,
)

pytestmark = [
    pytest.mark.usefixtures("events"),
]

COMPLEX_CONFIG_FILES = (
    "test-cases/upgrades/rolling-upgrade.yaml",
    "configurations/azure/azure_rolling_upgrade.yaml",
    "configurations/minicloud/rolling-upgrade.yaml",
    "unit_tests/test_data/test_scylla_yaml_builders/rolling-upgrade.yaml",
)
COMPLEX_SCHEMA_SCRIPT = "data_dir/latte/complex_schema.rn"
COMPLEX_CMD_EXPECTATIONS = {
    "stress_cmd_complex_prepare": ("write", ["insert"]),
    "stress_cmd_complex_verify_read": ("read", ["read_by_key", "read_by_email"]),
    "stress_cmd_complex_verify_more": (
        "mixed",
        [
            "read_by_key",
            "read_by_ck",
            "read_by_email",
            "update_static",
            "update_ttl",
            "update_diff1_ts",
            "update_diff2_ts",
            "update_same1_ts",
            "update_same2_ts",
        ],
    ),
    "stress_cmd_complex_verify_delete": ("write", ["delete_row"]),
}


def test_05_latte_parse_final_output():
    latte = LatteStressThread(
        loader_set=["fake-loader"],
        stress_cmd="fake",
        timeout=1,
        node_list=["fake-db-node-1"],
        params={"cluster_backend": "aws"},
    )
    with open(sct_abs_path("data_dir/latte_stress_output.log"), "r", encoding="utf-8") as latte_output:
        stress_result = type("FakeStressResult", (), {"stdout": latte_output.read()})

    parsed_output = latte.parse_final_output(stress_result)

    assert isinstance(parsed_output, dict)
    assert "latency 99th percentile" in parsed_output
    assert parsed_output["latency 99th percentile"] == "6.206"
    assert "latency mean" in parsed_output
    assert parsed_output["latency mean"] == "2.272"
    assert "op rate" in parsed_output
    assert parsed_output["op rate"] == "160100"


@pytest.mark.parametrize(
    "cmd,items",
    (
        ("latte run /foo/bar.rn %swrite -q -r 500", ["write"]),
        ("latte run /foo/bar.rn %sread -q -r 500", ["read"]),
        ("latte run /foo/bar.rn %scustom -q -r 500", ["custom"]),
        ("latte run /foo/bar.rn %sread,write -q -r 500", ["read", "write"]),
        ("latte run /foo/bar.rn %sread,custom,write,user -q -r 500", ["read", "custom", "write", "user"]),
        ("latte run /foo/bar.rn %sfoo_bar:1,quuz_tea:2 -q -r 500", ["foo_bar", "quuz_tea"]),
    ),
)
def test_find_latte_fn_names(cmd, items):
    fn_params = ("-f ", "-f=", "--function ", "--function=", "--functions ", "--functions=")
    for fn_param in fn_params:
        result = find_latte_fn_names(cmd % fn_param)
        assert len(result) > 0
        assert len(result) == len(items), f"Expected: {items}, Actual: {result}"
        for item in items:
            assert item in result


@pytest.mark.parametrize(
    "cmd,expected_operation_type",
    (
        ("latte run /foo/bar.rn %swrite -q -r 500", "write"),
        ("latte run /foo/bar.rn %swrite_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_write -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_insert -q -r 500", "write"),
        ("latte run /foo/bar.rn %supdate -q -r 500", "write"),
        ("latte run /foo/bar.rn %supdate_batch -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_update -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_foo,update_bar -q -r 500", "write"),
        ("latte run /foo/bar.rn %sdelete -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_delete -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_delete_by_one -q -r 500", "write"),
        ("latte run /foo/bar.rn %scounter_write -q -r 500", "counter_write"),
        ("latte run /foo/bar.rn %sread -q -r 500", "read"),
        ("latte run /foo/bar.rn %sread_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %scount -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %smulti_get -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_get_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all,get_single -q -r 500", "read"),
        ("latte run /foo/bar.rn %scounter_read -q -r 500", "counter_read"),
        ("latte run /foo/bar.rn %sread,write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite:1,read:2 -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sbatch_insert:1,read_all:2,get_bar:0.5 -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sread,counter_write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sread,counter_read -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite,counter_read -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite,counter_write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %scustom -q -r 500", "user"),
        ("latte run /foo/bar.rn %suser_profile -q -r 500", "user"),
        ("latte run /foo/bar.rn %sfoo_bar:1,quuz_tea:2 -q -r 500", "user"),
        ("latte run /foo/bar.rn %sread,write,custom -q -r 500", "user"),
    ),
)
def test_get_latte_operation_type(cmd, expected_operation_type):
    fn_params = ("-f ", "-f=", "--function ", "--function=", "--functions ", "--functions=")
    for fn_param in fn_params:
        result = get_latte_operation_type(cmd % fn_param)
        assert expected_operation_type == result


@pytest.mark.parametrize(
    "cmd,items",
    (
        ("%s --tag=latte-prepare-01 -q -r 500", ["latte-prepare-01"]),
        ("%s --tag latte-main-01 -q -r 500", ["latte-main-01"]),
        ("%s --tag  latte-prepare-01,write  -q -r 500", ["latte-prepare-01", "write"]),
        ("%s --tag=latte-main-01,read    -q -r 500", ["latte-main-01", "read"]),
        ("%s  --tag=latte-main-01  --tag   write,table1    -q -r 500", ["latte-main-01", "write", "table1"]),
        ("%s --tag=latte-main-01,read  --tag table2  -q -r 500", ["latte-main-01", "read", "table2"]),
        ("%s --tag=latte-main-01,read -q -r 500 --tag table2", ["latte-main-01", "read", "table2"]),
    ),
)
def test_find_latte_tags(cmd, items):
    result = find_latte_tags(cmd % "latte run /foo/bar.rn")
    assert len(result) > 0
    assert len(result) == len(items), f"Expected: {items}, Actual: {result}"
    for item in items:
        assert item in result


def _load_complex_cmd(config_file, param_name):
    with open(sct_abs_path(config_file), encoding="utf-8") as config:
        return yaml.safe_load(config)[param_name]


@pytest.mark.parametrize("config_file", COMPLEX_CONFIG_FILES)
@pytest.mark.parametrize("param_name", sorted(COMPLEX_CMD_EXPECTATIONS))
def test_complex_schema_cmds_are_classified(config_file, param_name):
    """Each complex command must run the rune script and map onto the right latte metrics."""
    cmd = _load_complex_cmd(config_file, param_name)
    expected_operation_type, expected_fn_names = COMPLEX_CMD_EXPECTATIONS[param_name]

    assert COMPLEX_SCHEMA_SCRIPT in cmd
    assert find_latte_fn_names(cmd) == expected_fn_names
    assert get_latte_operation_type(cmd) == expected_operation_type


@pytest.mark.parametrize("config_file", COMPLEX_CONFIG_FILES)
@pytest.mark.parametrize("param_name", ("stress_cmd_complex_prepare", "stress_cmd_complex_verify_read"))
def test_complex_schema_cmds_cover_every_row(config_file, param_name):
    """The workload writes one row per cycle, so these steps must run exactly 'row_count' cycles."""
    cmd = _load_complex_cmd(config_file, param_name)

    cycles = re.search(r"-d (\d+)", cmd)
    row_count = re.search(r"-P row_count=(\d+)", cmd)
    assert cycles and row_count, f"missing -d or -P row_count in: {cmd}"
    assert cycles.group(1) == row_count.group(1)

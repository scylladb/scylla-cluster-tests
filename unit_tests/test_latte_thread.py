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

import pytest
import re
import requests

from sdcm import sct_abs_path
from sdcm.stress.latte_thread import (
    LatteStressThread,
    find_latte_fn_names,
    find_latte_tags,
    get_latte_operation_type,
)
from sdcm.utils.decorators import timeout
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
]


@pytest.mark.integration
def test_01_latte_schema(request, docker_scylla, params):
    params['enable_argus'] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = ("latte schema docker/latte/workloads/workload.rn")

    latte_thread = LatteStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params
    )

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_02_latte_load(request, docker_scylla, params):
    params['enable_argus'] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = ("latte load docker/latte/workloads/workload.rn")

    latte_thread = LatteStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params
    )

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_03_latte_run(request, docker_scylla, prom_address, params):
    params['enable_argus'] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = ("latte run --function run -d 10s docker/latte/workloads/workload.rn --generate-report")

    latte_thread = LatteStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params
    )

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        assert "sct_latte_user_gauge" in output

        regex = re.compile(r"^sct_latte_user_gauge.*?([0-9\.]*?)$", re.MULTILINE)
        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()

    output, _ = latte_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

    assert "op rate" in output[0]
    assert int(output[0]["op rate"]) > 0


@pytest.mark.integration
@pytest.mark.docker_scylla_args(ssl=True)
def test_04_latte_run_client_encrypt(request, docker_scylla, params):
    params['client_encrypt'] = True
    params['enable_argus'] = False

    loader_set = LocalLoaderSetDummy(params=params)

    # dedicated SSL certs directory for the test, to avoid conflicts during parallel tests execution
    if ssl_dir := getattr(docker_scylla, "ssl_conf_dir", None):
        for loader_node in loader_set.nodes:
            loader_node.__class__.ssl_conf_dir = property(lambda self: ssl_dir)

    cmd = ("latte run -d 10s docker/latte/workloads/workload.rn --generate-report")

    latte_thread = LatteStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5,
        params=params,
    )

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    output, _ = latte_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

    assert "op rate" in output[0]
    assert int(output[0]["op rate"]) > 0


def test_05_latte_parse_final_output():
    latte = LatteStressThread(
        loader_set=["fake-loader"], stress_cmd="fake", timeout=1,
        node_list=["fake-db-node-1"], params={"cluster_backend": "aws"})
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
    "cmd,items", (
        ("latte run /foo/bar.rn %swrite -q -r 500", ["write"]),
        ("latte run /foo/bar.rn %sread -q -r 500", ["read"]),
        ("latte run /foo/bar.rn %scustom -q -r 500", ["custom"]),
        ("latte run /foo/bar.rn %sread,write -q -r 500", ["read", "write"]),
        ("latte run /foo/bar.rn %sread,custom,write,user -q -r 500", ["read", "custom", "write", "user"]),
        ("latte run /foo/bar.rn %sfoo_bar:1,quuz_tea:2 -q -r 500", ["foo_bar", "quuz_tea"]),
    )
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
    "cmd,expected_operation_type", (
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
    )
)
def test_get_latte_operation_type(cmd, expected_operation_type):
    fn_params = ("-f ", "-f=", "--function ", "--function=", "--functions ", "--functions=")
    for fn_param in fn_params:
        result = get_latte_operation_type(cmd % fn_param)
        assert expected_operation_type == result


@pytest.mark.parametrize(
    "cmd,items", (
        ("%s --tag=latte-prepare-01 -q -r 500", ["latte-prepare-01"]),
        ("%s --tag latte-main-01 -q -r 500", ["latte-main-01"]),
        ("%s --tag  latte-prepare-01,write  -q -r 500", ["latte-prepare-01", "write"]),
        ("%s --tag=latte-main-01,read    -q -r 500", ["latte-main-01", "read"]),
        ("%s  --tag=latte-main-01  --tag   write,table1    -q -r 500", ["latte-main-01", "write", "table1"]),
        ("%s --tag=latte-main-01,read  --tag table2  -q -r 500", ["latte-main-01", "read", "table2"]),
        ("%s --tag=latte-main-01,read -q -r 500 --tag table2", ["latte-main-01", "read", "table2"]),
    )
)
def test_find_latte_tags(cmd, items):
    result = find_latte_tags(cmd % 'latte run /foo/bar.rn')
    assert len(result) > 0
    assert len(result) == len(items), f"Expected: {items}, Actual: {result}"
    for item in items:
        assert item in result

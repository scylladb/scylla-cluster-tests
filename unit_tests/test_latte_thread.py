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
import requests

from sdcm.stress.latte_thread import (
    LatteStressThread,
    find_latte_fn_names,
    get_latte_operation_type,
)
from sdcm.utils.decorators import timeout
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
]


@pytest.mark.integration
def test_01_latte_schema(request, docker_scylla, params):
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
    loader_set = LocalLoaderSetDummy()
    loader_set.params = params

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

    output = latte_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


@pytest.mark.integration
@pytest.mark.docker_scylla_args(ssl=True)
def test_04_latte_run_client_encrypt(request, docker_scylla, params):
    params['client_encrypt'] = True

    loader_set = LocalLoaderSetDummy(params=params)

    cmd = ("latte run -d 10s docker/latte/workloads/workload.rn --generate-report")

    latte_thread = LatteStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=5,
        params=params,
    )

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    output = latte_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

    assert "op rate" in output[0]
    assert int(output[0]["op rate"]) > 0


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
        ("latte run /foo/bar.rn %supdate_bach -q -r 500", "write"),
        ("latte run /foo/bar.rn %sbatch_update -q -r 500", "write"),
        ("latte run /foo/bar.rn %sinsert_foo,update_bar -q -r 500", "write"),

        ("latte run /foo/bar.rn %sread -q -r 500", "read"),
        ("latte run /foo/bar.rn %sread_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_read_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect -q -r 500", "read"),
        ("latte run /foo/bar.rn %sselect_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_select_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %smulti_get -q -r 500", "read"),
        ("latte run /foo/bar.rn %sdo_get_all -q -r 500", "read"),
        ("latte run /foo/bar.rn %sget_all,get_single -q -r 500", "read"),

        ("latte run /foo/bar.rn %sread,write -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %swrite:1,read:2 -q -r 500", "mixed"),
        ("latte run /foo/bar.rn %sbatch_insert:1,read_all:2,get_bar:0.5 -q -r 500", "mixed"),

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

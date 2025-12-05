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
import requests

import pytest

from sdcm import sct_abs_path
from sdcm.stress.latte_thread import LatteStressThread
from sdcm.utils.decorators import timeout
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
]


@pytest.mark.integration
def test_01_latte_schema(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()
    loader_set.params = params

    cmd = "latte schema docker/latte/workloads/workload.rn"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_02_latte_load(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()
    loader_set.params = params

    cmd = "latte load docker/latte/workloads/workload.rn"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_03_latte_run(request, docker_scylla, prom_address, params):
    loader_set = LocalLoaderSetDummy()
    loader_set.params = params

    cmd = "latte run --function run -d 10s docker/latte/workloads/workload.rn --generate-report"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(r"^sct_latte_run_gauge.*?([0-9\.]*?)$", re.MULTILINE)
        assert "sct_latte_run_gauge" in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()

    output = latte_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

    assert "op rate" in output[0]
    assert int(output[0]["op rate"]) > 0


@pytest.mark.integration
@pytest.mark.docker_scylla_args(ssl=True)
def test_04_latte_run_client_encrypt(request, docker_scylla, params):
    params["client_encrypt"] = True

    loader_set = LocalLoaderSetDummy()
    loader_set.params = params

    cmd = "latte run -d 10s docker/latte/workloads/workload.rn --generate-report"

    latte_thread = LatteStressThread(
        loader_set,
        cmd,
        node_list=[docker_scylla],
        timeout=5,
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

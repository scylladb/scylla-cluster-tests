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
import os
import glob
import requests

from sdcm.stress.latte_thread import LatteStressThread
from sdcm.utils.decorators import timeout
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
]


@pytest.mark.integration
def test_01_latte_schema(request, docker_scylla, params):
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = "latte schema docker/latte/workloads/workload.rn"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_02_latte_load(request, docker_scylla, params):
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = "latte load docker/latte/workloads/workload.rn"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    latte_thread.get_results()


@pytest.mark.integration
def test_03_latte_run(request, docker_scylla, prom_address, params):
    params["enable_argus"] = False
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = "latte run --function run -d 10s docker/latte/workloads/workload.rn --generate-report"

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=5, params=params)

    def cleanup_thread():
        latte_thread.kill()

    request.addfinalizer(cleanup_thread)

    latte_thread.run()

    @timeout(timeout=120)
    def check_metrics():
        # Surface any stress thread exception early instead of waiting the full timeout
        for future in latte_thread.results_futures:
            if future.done():
                # _run_stress swallows exceptions internally, so check the result
                result = future.result()
                if result:
                    _loader, result_dict, stress_event = result
                    if not result_dict:
                        raise AssertionError(
                            f"Latte stress thread completed but returned empty result. "
                            f"Event severity: {stress_event.severity if hasattr(stress_event, 'severity') else 'N/A'}, "
                            f"Event errors: {stress_event.errors if hasattr(stress_event, 'errors') else 'N/A'}"
                        )
        output = requests.get(f"http://{prom_address}/metrics").text
        # Gather diagnostic info about log files and docker containers
        log_files = glob.glob(os.path.join(loader_set.nodes[0].logdir, "latte-*.log"))
        log_info = {}
        for lf in log_files:
            try:
                log_info[lf] = os.path.getsize(lf)
            except OSError:
                log_info[lf] = "not found"
        assert "sct_latte_user_gauge" in output, (
            f"Metric not found. Futures done: {[f.done() for f in latte_thread.results_futures]}. "
            f"Log files: {log_info}"
        )

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
    params["client_encrypt"] = True
    params["enable_argus"] = False

    loader_set = LocalLoaderSetDummy(params=params)

    # dedicated SSL certs directory for the test, to avoid conflicts during parallel tests execution
    if ssl_dir := getattr(docker_scylla, "ssl_conf_dir", None):
        for loader_node in loader_set.nodes:
            loader_node.__class__.ssl_conf_dir = property(lambda self: ssl_dir)

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

    output, _ = latte_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

    assert "op rate" in output[0]
    assert int(output[0]["op rate"]) > 0

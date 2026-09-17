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

from sdcm.stress.latte_thread import LatteStressThread
from sdcm.utils.decorators import timeout
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
]

COMPLEX_SCHEMA_SCRIPT = "data_dir/latte/complex_schema.rn"
COMPLEX_ROW_COUNT = 500
COMPLEX_INSERT_CMD = (
    f"latte run {COMPLEX_SCHEMA_SCRIPT} -f insert --consistency ONE"
    f" -d {COMPLEX_ROW_COUNT} -P row_count={COMPLEX_ROW_COUNT}"
)
COMPLEX_VERIFY_READ_CMD = (
    f"latte run {COMPLEX_SCHEMA_SCRIPT} -f read_by_key:1 -f read_by_email:1 --consistency ONE"
    f" -d {COMPLEX_ROW_COUNT} -P row_count={COMPLEX_ROW_COUNT}"
)


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
        output = requests.get(f"http://{prom_address}/metrics").text
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


def _run_complex_workload(request, docker_scylla, params, cmd, schema_params=None):
    """Run one complex-schema latte command to completion and return its errors per loader."""
    params["enable_argus"] = False
    params["latte_schema_parameters"] = {"replication_factor": 1, **(schema_params or {})}
    loader_set = LocalLoaderSetDummy(params=params)

    latte_thread = LatteStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=300, params=params)
    request.addfinalizer(latte_thread.kill)
    latte_thread.run()

    _, errors = latte_thread.parse_results()
    return errors


@pytest.mark.integration
def test_05_latte_complex_schema_validates_data(request, docker_scylla, params):
    """The complex workload creates its schema, populates it and reads every row back clean."""
    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_INSERT_CMD)
    assert not errors, f"complex prepare step failed: {errors}"

    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_VERIFY_READ_CMD)
    assert not errors, f"complex verify-read step failed on intact data: {errors}"


@pytest.mark.integration
def test_06_latte_complex_schema_detects_corrupted_row(request, docker_scylla, params):
    """A single corrupted column must fail the verify-read step, otherwise it validates nothing."""
    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_INSERT_CMD)
    assert not errors, f"complex prepare step failed: {errors}"

    # 'static_int' is a static column, so corrupting it needs only the partition key, whose
    # blob literal is safe to pass through the shell. Its expected values are generated from
    # 'hash_range(idx, 2_147_483_647)', so the upper bound below never matches one.
    cqlsh = "cqlsh"
    if (user := params.get("authenticator_user")) and (password := params.get("authenticator_password")):
        cqlsh += f" -u {user} -p {password}"

    selected = docker_scylla.run(f'{cqlsh} -e "SELECT key FROM keyspace_complex.user_with_ck LIMIT 1;"')
    key_match = re.search(r"0x[0-9a-f]+", selected.stdout)
    assert key_match, f"could not find a row to corrupt in: {selected.stdout}"
    docker_scylla.run(
        f'{cqlsh} -e "UPDATE keyspace_complex.user_with_ck SET static_int = 2147483647'
        f' WHERE key = {key_match.group(0)};"'
    )

    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_VERIFY_READ_CMD)
    assert errors, "complex verify-read step passed although one row was corrupted"


@pytest.mark.integration
def test_07_latte_complex_schema_with_tablets_disabled(request, docker_scylla, params):
    """The tablets schema parameter must land on the keyspace, not the table."""
    schema_params = {"tablets": "false", "enable_tablets": "false", "use_tablets": "false"}

    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_INSERT_CMD, schema_params=schema_params)
    assert not errors, f"complex prepare step failed with tablets disabled: {errors}"

    errors = _run_complex_workload(request, docker_scylla, params, COMPLEX_VERIFY_READ_CMD, schema_params=schema_params)
    assert not errors, f"complex verify-read step failed with tablets disabled: {errors}"

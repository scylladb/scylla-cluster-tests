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
# Copyright (c) 2022 ScyllaDB

import pytest

from sdcm.scylla_bench_thread import ScyllaBenchThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events", ),
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "extra_cmd",
    argvalues=[
        pytest.param("", id="regular"),
        pytest.param("-tls", id="tls", marks=[pytest.mark.docker_scylla_args(ssl=True)]),
        pytest.param("""-keyspace='"5_keyspace"' """, id="quoted_keyspace"),
    ],
)
def test_01_scylla_bench(request, docker_scylla, params, extra_cmd):
    loader_set = LocalLoaderSetDummy(params=params)
    cmd = (
        f"scylla-bench -workload=sequential {extra_cmd} -mode=write -replication-factor=1 -partition-count=10 "
        "-clustering-row-count=5555 -clustering-row-size=uniform:10..20 -concurrency=10 "
        "-connection-count=10 -consistency-level=one -rows-per-request=10 -timeout=60s -duration=1m"
    )
    bench_thread = ScyllaBenchThread(
        loader_set=loader_set,
        stress_cmd=cmd,
        node_list=[docker_scylla],
        timeout=120,
        params=params,
    )

    def cleanup_thread():
        bench_thread.kill()

    request.addfinalizer(cleanup_thread)

    bench_thread.run()

    summaries, errors = bench_thread.parse_results()

    assert not errors
    assert summaries[0]["Clustering row size"] == "Uniform(min=10, max=20)"


@pytest.mark.parametrize("extra_cmd,error_at_row_limit_exists", (
    ("", True),
    ("-error-limit=0", False),
    ("-error-limit=1", False),
    ("-error-limit=2", False),
    ("-error-limit=10", False),
    ("-error-limit 0", False),
    ("-error-limit 10", False),
    ("-retry-number=0", True),
    ("-retry-number=1", True),
    ("-retry-number=2", False),
    ("-retry-number=10", False),
    ("-retry-number= 10", False),
    ("-retry-number 10", False),
    ("-retry-number  10", False),
    ("-retry-number 1", True),
    ("-retry-number 2", False),
    ("-error-at-row-limit=13", True),
    ("-error-at-row-limit 15", True),
))
def test_02_scylla_bench_check_error_at_row_limit_addition(params, extra_cmd, error_at_row_limit_exists):
    loader_set = LocalLoaderSetDummy(nodes=[])
    cmd = (
        f"scylla-bench -workload=sequential {extra_cmd} -mode=write"
        " -replication-factor=1 -partition-count=10 -clustering-row-count=5555"
        " -clustering-row-size=uniform:10..20 -concurrency=10 -connection-count=10"
        " -consistency-level=one -rows-per-request=10 -timeout=60s -duration=1m"
    )
    bench_thread = ScyllaBenchThread(
        stress_cmd=cmd, loader_set=loader_set, timeout=120, node_list=[], params=params)

    assert bench_thread.stress_cmd.count('-error-at-row-limit') == int(error_at_row_limit_exists)
    assert error_at_row_limit_exists == ('-error-at-row-limit' in bench_thread.stress_cmd)
    if '-error-at-row-limit' in extra_cmd:
        actual_value = bench_thread.stress_cmd.split('-error-at-row-limit')[-1].replace("=", "")
        actual_value = int(actual_value.strip().split(" ")[0])
        assert int(extra_cmd.split("=" if "=" in extra_cmd else " ")[-1]) == actual_value

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
    pytest.mark.usefixtures("events",),
    pytest.mark.skip(reason="those are integration tests only"),
]


@pytest.mark.parametrize("extra_cmd", argvalues=[
    pytest.param('', id="regular"),
    pytest.param('-tls', id="tls", marks=[pytest.mark.docker_scylla_args(ssl=True)])])
def test_01_scylla_bench(request, docker_scylla, params, extra_cmd):
    loader_set = LocalLoaderSetDummy()

    cmd = (
        f"scylla-bench -workload=sequential {extra_cmd} -mode=write -replication-factor=1 -partition-count=10 "
        + "-clustering-row-count=5555 -clustering-row-size=uniform:10..20 -concurrency=10 "
        + "-connection-count=10 -consistency-level=one -rows-per-request=10 -timeout=60s -duration=1m"
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

    summaries, errors = bench_thread.verify_results()

    assert not errors
    assert summaries[0]["Clustering row size"] == "Uniform(min=10, max=20)"

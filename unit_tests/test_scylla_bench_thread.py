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
from cassandra.cluster import Cluster  # pylint: disable=no-name-in-module

from sdcm.scylla_bench_thread import ScyllaBenchThread
from sdcm.utils.docker_utils import running_in_docker
from unit_tests.dummy_remote import LocalLoaderSetDummy
from test_lib.scylla_bench_tools import create_scylla_bench_table_query

pytestmark = [
    pytest.mark.usefixtures("events",),
    pytest.mark.skip(reason="those are integration tests only"),
]


@pytest.fixture(scope="session")
def create_cql_ks_and_table(docker_scylla):
    if running_in_docker():
        address = f"{docker_scylla.internal_ip_address}:9042"
    else:
        address = docker_scylla.get_port("9042")
    node_ip, port = address.split(":")
    port = int(port)

    cluster_driver = Cluster([node_ip], port=port)
    create_table_query = create_scylla_bench_table_query(
        compaction_strategy="SizeTieredCompactionStrategy", seed=None
    )

    with cluster_driver.connect() as session:
        # pylint: disable=no-member
        session.execute(
            """
                CREATE KEYSPACE IF NOT EXISTS scylla_bench WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
        )
        session.execute(create_table_query)


def test_01_scylla_bench(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()

    cmd = (
        "scylla-bench -workload=sequential -mode=write -replication-factor=1 -partition-count=10 "
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

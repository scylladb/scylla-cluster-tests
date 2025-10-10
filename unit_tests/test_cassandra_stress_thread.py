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

from sdcm.stress_thread import CassandraStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
    pytest.mark.xdist_group("docker_heavy"),
]


def test_01_cassandra_stress(request, docker_scylla, params):
    params['cs_debug'] = True
    params['use_hdrhistogram'] = True

    loader_set = LocalLoaderSetDummy(params=params)

    cmd = (
        """cassandra-stress write cl=ONE duration=1m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) """
        """compaction(strategy=SizeTieredCompactionStrategy)' -mode cql3 native """
        """-rate threads=10 -pop seq=1..10000000 -log interval=5"""
    )

    cs_thread = CassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_02_cassandra_stress_user_profile(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = (
        "cassandra-stress user profile=/tmp/cassandra-stress-custom.yaml ops'(insert=1,simple1=1)' "
        "cl=ONE duration=1m -mode cql3 native -rate threads=1"
    )

    cs_thread = CassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


@pytest.mark.docker_scylla_args(ssl=True)
def test_03_cassandra_stress_client_encrypt(request, docker_scylla, params):

    loader_set = LocalLoaderSetDummy(params=params)

    cmd = (
        "cassandra-stress write cl=ONE duration=1m -schema 'compaction(strategy=SizeTieredCompactionStrategy) "
        "replication(strategy=NetworkTopologyStrategy,replication_factor=1)' -mode cql3 native -rate threads=10 "
        "-pop seq=1..10000000 -log interval=5 "
    )

    cs_thread = CassandraStressThread(
        loader_set,
        cmd,
        node_list=[docker_scylla],
        timeout=120,
        client_encrypt=True,
        params=params,
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_04_cassandra_stress_multi_region(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy(params=params)
    loader_set.test_config.set_multi_region(True)
    request.addfinalizer(lambda: loader_set.test_config.set_multi_region(False))
    cmd = (
        """cassandra-stress write cl=ONE duration=1m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) """
        """compaction(strategy=SizeTieredCompactionStrategy)' -mode cql3 native """
        """-rate threads=10 -pop seq=1..10000000 -log interval=5"""
    )

    cs_thread = CassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


@pytest.mark.parametrize("compressor, cql_compression", [
    ("Deflate", "none"), ("LZ4", "lz4"), ("Snappy", "snappy"), ("Zstd", "none")
])
def test_05_cassandra_stress_compression(request, docker_scylla, params, compressor, cql_compression):
    loader_set = LocalLoaderSetDummy(params=params)

    cmd = (
        "cassandra-stress write cl=ONE duration=5s -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) "
        f"compression={compressor}Compressor' -mode cql3 native compression={cql_compression.lower()} -rate threads=1"
    )

    cs_thread = CassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    output, _ = cs_thread.parse_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0

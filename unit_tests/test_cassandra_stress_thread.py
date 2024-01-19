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

from unittest.mock import MagicMock

import pytest

from sdcm.stress_thread import CassandraStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


def test_01_cassandra_stress(request, docker_scylla, params):
    params['cs_debug'] = True
    loader_set = LocalLoaderSetDummy()

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

    output = cs_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_02_cassandra_stress_user_profile(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()

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

    output = cs_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


@pytest.mark.docker_scylla_args(ssl=True)
def test_03_cassandra_stress_client_encrypt(request, docker_scylla, params):

    loader_set = LocalLoaderSetDummy()

    cmd = (
        """cassandra-stress write cl=ONE duration=1m -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) """
        """compaction(strategy=SizeTieredCompactionStrategy)' -mode cql3 native """
        """-rate threads=10 -pop seq=1..10000000 -log interval=5"""
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

    output = cs_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_03_cassandra_stress_multi_region(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()
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

    output = cs_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


@pytest.mark.parametrize("fail_metric", (
        pytest.param(True, id="error"),
        pytest.param(False, id='ok'),
))
def test_04_round_robin(request, docker_scylla, params, fail_metric):
    
    # for testing we reset the global state, so we can test multiple times
    CassandraStressThread.should_wait_for_schema = False

    prometheus_db = MagicMock()
    if fail_metric:
        prometheus_db.metric_has_data = MagicMock(side_effect = Exception('Test'))
    params['round_robin'] = True

    # set specific load so we skip loader search to match running version
    params['stress_image']['cassandra-stress'] = 'docker.io/scylladb/scylla-nightly:5.2.0-dev-0.20220820.516089beb0b8'
    loader_set = LocalLoaderSetDummy()

    cmds = [
        "cassandra-stress write no-warmup cl=ALL n=100 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1)' -mode cql3 native -rate threads=1  -pop seq=1..100",
        "cassandra-stress write no-warmup cl=ALL n=100 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1)' -mode cql3 native -rate threads=1  -pop seq=100..200",
        "cassandra-stress write no-warmup cl=ALL n=100 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1)' -mode cql3 native -rate threads=1  -pop seq=200..300",
        "cassandra-stress write no-warmup cl=ALL n=100 -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1)' -mode cql3 native -rate threads=1  -pop seq=300..400",
    ]
    cs_threads =  [CassandraStressThread(loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params, prometheus_db=prometheus_db).run() for cmd in cmds]

    def cleanup_thread():
        for cs_thread in cs_threads:
            cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    for cs_thread in cs_threads:
        output = cs_thread.get_results()
        assert "latency mean" in output[0]
        assert float(output[0]["latency mean"]) > 0

        assert "latency 99th percentile" in output[0]
        assert float(output[0]["latency 99th percentile"]) > 0

    prometheus_db.metric_has_data.assert_called_once_with('sum(sct_cassandra_stress_write_gauge{type="ops"}) by (instance) '
                                                          '+ sum(sct_cassandra_stress_read_gauge{type="ops"}) by (instance) '
                                                          '+ sum(sct_cassandra_stress_mixed_gauge{type="ops"}) by (instance) '
                                                          '+ sum(sct_cassandra_stress_user_gauge{type="ops"}) by (instance)', n=10)
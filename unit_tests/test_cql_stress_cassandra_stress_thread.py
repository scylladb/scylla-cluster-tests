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
# Copyright (c) 2023 ScyllaDB

import re
import pytest
import requests

from sdcm.cql_stress_cassandra_stress_thread import CqlStressCassandraStressThread
from sdcm.utils.decorators import timeout
from unit_tests.dummy_remote import LocalLoaderSetDummy


pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


def test_01_cql_stress_cassandra_stress(request, docker_scylla, prom_address, params):
    loader_set = LocalLoaderSetDummy()

    cmd = (
        """cql-stress-cassandra-stress write cl=ONE duration=1m """
        """-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) """
        """compaction(strategy=SizeTieredCompactionStrategy)' """
        """-rate threads=10 -pop seq=1..10000000"""
    )

    cs_thread = CqlStressCassandraStressThread(
        loader_set, cmd, node_list=[docker_scylla], timeout=120, params=params
    )

    def cleanup_thread():
        cs_thread.kill()

    request.addfinalizer(cleanup_thread)

    cs_thread.run()

    @timeout(timeout=60)
    def check_metrics():
        output = requests.get("http://{}/metrics".format(prom_address)).text
        regex = re.compile(
            r"^sct_cql_stress_cassandra_stress_write_gauge.*?([0-9\.]*?)$", re.MULTILINE)
        assert "sct_cql_stress_cassandra_stress_write_gauge" in output

        matches = regex.findall(output)
        assert all(float(i) > 0 for i in matches), output

    check_metrics()

    output = cs_thread.get_results()
    assert "latency mean" in output[0]
    assert float(output[0]["latency mean"]) > 0

    assert "latency 99th percentile" in output[0]
    assert float(output[0]["latency 99th percentile"]) > 0


def test_02_cql_stress_cassandra_stress_multi_region(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy()
    loader_set.test_config.set_multi_region(True)
    request.addfinalizer(
        lambda: loader_set.test_config.set_multi_region(False))
    cmd = (
        """cql-stress-cassandra-stress write cl=ONE duration=1m """
        """-schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=1) """
        """compaction(strategy=SizeTieredCompactionStrategy)' """
        """-rate threads=10 -pop seq=1..10000000"""
    )

    cs_thread = CqlStressCassandraStressThread(
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

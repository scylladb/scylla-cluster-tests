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

from sdcm.gemini_thread import GeminiStressThread
from unit_tests.dummy_remote import LocalLoaderSetDummy

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
]


class DBCluster:  # pylint: disable=too-few-public-methods
    def __init__(self, nodes):
        self.nodes = nodes

    def get_node_cql_ips(self):
        return [node.cql_address for node in self.nodes]


def test_01_gemini_thread(request, docker_scylla, params):
    params.update({'gemini_table_options': ['gc_grace_seconds=60']})
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = DBCluster([docker_scylla])

    cmd = " ".join(
        [
            "--duration=1m",
            "--warmup=0",
            "--concurrency=5",
            "--mode=write",
            "--cql-features=basic",
            "--max-mutation-retries=100",
            "--max-mutation-retries-backoff=100ms",
            "--replication-strategy=\"{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}\"",
            "--table-options=\"cdc = {'enabled': true, 'ttl': 0}\"",
            "--use-server-timestamps=true",
        ]
    )

    gemini_thread = GeminiStressThread(
        loaders=loader_set,
        stress_cmd=cmd,
        test_cluster=test_cluster,
        oracle_cluster=test_cluster,
        timeout=360,
        params=params,
    )

    def cleanup_thread():
        gemini_thread.kill()

    request.addfinalizer(cleanup_thread)

    expected_gemini_arguments = " --table-options=\"gc_grace_seconds=60\" --level=info --request-timeout=60s --connect-timeout=60s --consistency=QUORUM --async-objects-stabilization-backoff=1s --async-objects-stabilization-attempts=10 --dataset-size=large --oracle-host-selection-policy=token-aware --test-host-selection-policy=token-aware --drop-schema=true --fail-fast=true --materialized-views=false --use-lwt=false --use-counters=false --max-tables=1 --max-columns=16 --min-columns=8 --max-partition-keys=6 --min-partition-keys=2 --max-clustering-keys=4 --min-clustering-keys=2 --partition-key-distribution=normal --token-range-slices=512 --partition-key-buffer-reuse-size=100 --statement-log-file-compression=zstd --duration=1m --warmup=0 --concurrency=5 --mode=write --cql-features=basic --max-mutation-retries=100 --max-mutation-retries-backoff=100ms --replication-strategy=\"{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}\" --table-options=\"cdc = {'enabled': true, 'ttl': 0}\" --use-server-timestamps=true"
    gemini_cmd = gemini_thread._generate_gemini_command()
    assert expected_gemini_arguments in gemini_cmd
    gemini_thread.run()

    results = gemini_thread.get_gemini_results()
    gemini_thread.verify_gemini_results(results)


def test_gemini_thread_without_cluster(request, docker_scylla, params):
    loader_set = LocalLoaderSetDummy(params=params)
    test_cluster = DBCluster([docker_scylla])
    cmd = " ".join(
        [
            "--duration=1m",
            "--warmup=0",
            "--concurrency=5",
            "--mode=write",
            "--cql-features=basic",
            "--max-mutation-retries=100",
            "--max-mutation-retries-backoff=100ms",
            "--replication-strategy=\"{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}\"",
            "--table-options=\"cdc = {'enabled': true, 'ttl': 0}\"",
            "--use-server-timestamps=true",
        ]
    )

    gemini_thread = GeminiStressThread(
        loaders=loader_set,
        stress_cmd=cmd,
        test_cluster=test_cluster,
        oracle_cluster=None,
        timeout=360,
        params=params,
    )

    def cleanup_thread():
        gemini_thread.kill()

    request.addfinalizer(cleanup_thread)

    gemini_thread.run()

    results = gemini_thread.get_gemini_results()
    gemini_thread.verify_gemini_results(results)

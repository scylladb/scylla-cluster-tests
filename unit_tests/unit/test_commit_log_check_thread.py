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
# Copyright (c) 2026 ScyllaDB

"""Unit tests for the commitlog size watchdog parameters."""

import contextlib
import logging
import re
from unittest.mock import MagicMock, patch

import pytest
from invoke import Result

from sdcm.cluster import BaseNode
from sdcm.commit_log_check_thread import CommitlogConfigParams, PrometheusQueries
from sdcm.utils.distro import Distro


# Values captured from a real 14-shard node (longevity-100gb-4h-oci-test #121).
SHARDS = 14
# /commitlog/metrics/max_disk_size sums the per-shard limit over all shards of the node.
MAX_DISK_SIZE = 93335846912
PER_SHARD_LIMIT = MAX_DISK_SIZE // SHARDS
SEGMENT_SIZE_IN_MB = 32

# `seastar-cpu-map.sh -n scylla` prints two lines per shard: one for the reactor thread and one
# for its `syscall` helper thread, whose name carries no shard number. Counting the word "shard"
# in this output yields twice the real shard count, which used to halve the commitlog limit.
SEASTAR_CPU_MAP_STDOUT = "".join(
    f"shard: {shard}, cpu: {shard + 2}\nshard: syscall, cpu: {shard + 2}\n" for shard in range(SHARDS)
)

SCYLLA_ARGS_STDOUT = (
    'SCYLLA_ARGS="--blocked-reactor-notify-ms 500 --abort-on-lsa-bad-alloc 1 '
    f'--abort-on-internal-error 1 --smp {SHARDS} --log-to-syslog 1 --default-log-level info"\n'
)


class ShardsNode(BaseNode):
    """BaseNode stub that keeps the real `scylla_shards` resolution chain but skips provisioning."""

    distro = Distro.CENTOS7

    def __init__(self, remoter):
        self.remoter = remoter
        self.name = "fake-db-node-1"
        self.parent_cluster = None
        self.log = logging.getLogger(self.name)

    @property
    def is_nonroot_install(self) -> bool:
        return False


class FakeSession:
    """CQL session stub answering the `system.config` lookups the watchdog makes."""

    def __init__(self, config: dict[str, str]):
        self.config = config

    def execute(self, query: str) -> MagicMock:
        name = re.search(r"name='(\w+)'", query).group(1)
        return MagicMock(**{"one.return_value.value": self.config[name]})


class FakeDbCluster:
    """Minimal db_cluster stub exposing what `CommitlogConfigParams` reads."""

    def __init__(self, node: ShardsNode, config: dict[str, str]):
        self.data_nodes = [node]
        # nodes[0] is deliberately a different object: the watchdog must not mix the two.
        self.nodes = [MagicMock(), node]
        self.config = config

    @contextlib.contextmanager
    def cql_connection_patient(self, node, connect_timeout):
        assert node is self.data_nodes[0], "commitlog params must be collected from a data node"
        yield FakeSession(self.config)


@pytest.fixture(name="db_cluster")
def fixture_db_cluster(fake_remoter):
    fake_remoter.result_map = {
        re.compile(r".*seastar-cpu-map\.sh.*"): Result(stdout=SEASTAR_CPU_MAP_STDOUT, exited=0),
        re.compile(r'.*grep "\^SCYLLA_ARGS=".*'): Result(stdout=SCYLLA_ARGS_STDOUT, exited=0),
    }
    node = ShardsNode(remoter=fake_remoter(hostname="127.0.0.1", user="test"))
    return FakeDbCluster(
        node=node,
        config={
            "commitlog_use_hard_size_limit": "true",
            "commitlog_segment_size_in_mb": str(SEGMENT_SIZE_IN_MB),
        },
    )


@contextlib.contextmanager
def mocked_max_disk_size(value: int = MAX_DISK_SIZE):
    with patch("sdcm.commit_log_check_thread.RemoteCurlClient") as curl_client:
        curl_client.return_value.run_remoter_curl.return_value.stdout = str(value)
        yield curl_client


def test_commitlog_config_params_derives_per_shard_limit_from_scylla_shards(db_cluster):
    """`total_space` must be the per-shard limit, i.e. the API sum divided by the real shard count.

    Counting the word "shard" in `seastar-cpu-map.sh` output returned 2x the shard count, which
    halved the limit and made the watchdog fire on healthy nodes.
    """
    with mocked_max_disk_size():
        params = CommitlogConfigParams(db_cluster)

    assert params.smp == SHARDS
    assert params.max_disk_size == MAX_DISK_SIZE
    assert params.total_space == PER_SHARD_LIMIT
    assert params.use_hard_size_limit is True
    assert params.segment_size_in_mb == SEGMENT_SIZE_IN_MB


def test_commitlog_config_params_reads_max_disk_size_from_a_data_node(db_cluster):
    """The API call must target the same data node used for the CQL and shard-count lookups."""
    with mocked_max_disk_size() as curl_client:
        CommitlogConfigParams(db_cluster)

    assert curl_client.call_args.kwargs["node"] is db_cluster.data_nodes[0]


def test_commitlog_config_params_no_shards_raises_value_error(db_cluster):
    """A node we cannot get a shard count from must fail loudly instead of dividing by zero."""
    with patch.object(ShardsNode, "scylla_shards", 0), mocked_max_disk_size():
        with pytest.raises(ValueError, match="Failed to get number of Scylla shards"):
            CommitlogConfigParams(db_cluster)


def test_overflow_query_threshold_does_not_flag_a_healthy_shard(db_cluster):
    """A shard at 3.2 GiB is healthy on a 6.2 GiB per-shard limit and must not be queried for.

    3456106496 is the value that kept the OCI run alarming for 4 hours against the halved limit.
    """
    with mocked_max_disk_size():
        queries = PrometheusQueries(CommitlogConfigParams(db_cluster))

    threshold = int(re.search(r">=\((\d+)", queries.overflow_commit_log_directory).group(1))
    assert threshold == PER_SHARD_LIMIT
    assert threshold > 3456106496

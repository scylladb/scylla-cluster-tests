"""Unit tests for SCT-730: parallelize CassandraAWSCluster.wait_for_init.

Verifies that the DB-up wait is issued for every node concurrently instead of
serially (which previously took up to N * timeout).
"""

import threading
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseCluster
from sdcm.cluster_aws import CassandraAWSCluster


def _make_nodes(count):
    # `MagicMock(name=...)` does NOT set an accessible `.name` attribute (it's a reserved
    # constructor kwarg used for the mock's repr); assign the attribute explicitly instead.
    nodes = []
    for i in range(count):
        node = MagicMock()
        node.name = f"node-{i}"
        nodes.append(node)
    return nodes


@pytest.fixture
def cassandra_aws_cluster():
    with patch.object(CassandraAWSCluster, "__init__", lambda self, **kw: None):
        cluster = CassandraAWSCluster()
        cluster.log = MagicMock()
        cluster.nodes = []
        # run_func_parallel is defined on the BaseCluster mixin; bind the real implementation.
        cluster.run_func_parallel = BaseCluster.run_func_parallel.__get__(cluster)
        return cluster


def test_cassandra_wait_for_init_waits_for_all_nodes(cassandra_aws_cluster):
    nodes = _make_nodes(3)
    cassandra_aws_cluster.nodes = nodes
    waited_nodes = []
    lock = threading.Lock()

    def _fake_wait_for(func, node, **kwargs):
        with lock:
            waited_nodes.append(node)

    # wait_for_init is wrapped by wait_for_init_wrap; call the underlying function directly.
    with patch("sdcm.cluster_aws.wait.wait_for", side_effect=_fake_wait_for):
        CassandraAWSCluster.wait_for_init.__wrapped__(cassandra_aws_cluster)

    assert set(waited_nodes) == set(nodes)


def test_cassandra_wait_for_init_runs_in_parallel(cassandra_aws_cluster):
    nodes = _make_nodes(3)
    cassandra_aws_cluster.nodes = nodes
    barrier = threading.Barrier(3, timeout=10)

    def _fake_wait_for(func, node, **kwargs):
        barrier.wait()  # blocks unless all 3 run concurrently

    with patch("sdcm.cluster_aws.wait.wait_for", side_effect=_fake_wait_for):
        # would raise BrokenBarrierError if nodes were waited on serially
        CassandraAWSCluster.wait_for_init.__wrapped__(cassandra_aws_cluster)


def test_cassandra_wait_for_init_propagates_node_timeout(cassandra_aws_cluster):
    nodes = _make_nodes(3)
    cassandra_aws_cluster.nodes = nodes

    def _fake_wait_for(func, node, **kwargs):
        if node is nodes[1]:
            raise TimeoutError(f"{node.name} did not come up in time")

    with patch("sdcm.cluster_aws.wait.wait_for", side_effect=_fake_wait_for):
        with pytest.raises(TimeoutError, match="did not come up in time"):
            CassandraAWSCluster.wait_for_init.__wrapped__(cassandra_aws_cluster)

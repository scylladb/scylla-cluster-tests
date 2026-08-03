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
    return [MagicMock(name=f"node-{i}") for i in range(count)]


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

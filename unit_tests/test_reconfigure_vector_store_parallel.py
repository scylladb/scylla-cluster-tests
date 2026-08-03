"""Unit tests for SCT-729: parallelize VectorStoreSetAWS._reconfigure_vector_store_nodes.

Verifies every node is reconfigured concurrently and that a failure on any node is
re-raised (fail-fast preserved).
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseCluster
from sdcm.cluster_aws import VectorStoreSetAWS


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
def vector_store_cluster():
    with patch.object(VectorStoreSetAWS, "__init__", lambda self, **kw: None):
        cluster = VectorStoreSetAWS()
        cluster.log = MagicMock()
        cluster.nodes = []
        # run_func_parallel is defined on the BaseCluster mixin; bind the real implementation.
        cluster.run_func_parallel = BaseCluster.run_func_parallel.__get__(cluster)
        return cluster


def test_reconfigure_vector_store_nodes_processes_all(vector_store_cluster):
    nodes = _make_nodes(3)
    vector_store_cluster.nodes = nodes

    vector_store_cluster._reconfigure_vector_store_nodes()

    for node in nodes:
        node.configure_vector_store_service.assert_called_once()
        assert node.remoter.run.call_count == 2  # stop + start


def test_reconfigure_vector_store_nodes_reraises(vector_store_cluster):
    nodes = _make_nodes(3)
    nodes[1].configure_vector_store_service.side_effect = RuntimeError("boom")
    vector_store_cluster.nodes = nodes

    with pytest.raises(RuntimeError, match="boom"):
        vector_store_cluster._reconfigure_vector_store_nodes()

    # All nodes run concurrently: the other nodes still get fully reconfigured
    # (stop + start) even though nodes[1] failed and its exception propagates.
    # This differs from the original serial loop, which stopped at the first
    # failing node and never touched subsequent nodes.
    for node in (nodes[0], nodes[2]):
        node.configure_vector_store_service.assert_called_once()
        assert node.remoter.run.call_count == 2  # stop + start

    # nodes[1] failed between stop and start, so only the stop command ran.
    assert nodes[1].remoter.run.call_count == 1


def test_reconfigure_vector_store_nodes_empty_is_noop(vector_store_cluster):
    vector_store_cluster.nodes = []
    vector_store_cluster._reconfigure_vector_store_nodes()  # must not raise

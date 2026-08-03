"""Unit tests for SCT-729 (Docker parity): parallelize
VectorStoreSetDocker._reconfigure_vector_store_nodes.

Mirrors test_reconfigure_vector_store_parallel.py (AWS) for the Docker backend
implementation, which had the same serial per-node loop pattern.
"""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseCluster
from sdcm.cluster_docker import VectorStoreSetDocker


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
def vector_store_docker_cluster():
    with patch.object(VectorStoreSetDocker, "__init__", lambda self, **kw: None):
        cluster = VectorStoreSetDocker()
        cluster.log = MagicMock()
        cluster.nodes = []
        # run_func_parallel is defined on the BaseCluster mixin; bind the real implementation.
        cluster.run_func_parallel = BaseCluster.run_func_parallel.__get__(cluster)
        return cluster


@patch("sdcm.cluster_docker.ContainerManager")
def test_reconfigure_vector_store_nodes_processes_all(mock_container_manager, vector_store_docker_cluster):
    mock_container_manager.is_running.return_value = False
    nodes = _make_nodes(3)
    vector_store_docker_cluster.nodes = nodes

    vector_store_docker_cluster._reconfigure_vector_store_nodes()

    assert mock_container_manager.destroy_container.call_count == 3
    assert mock_container_manager.run_container.call_count == 3
    assert mock_container_manager.wait_for_status.call_count == 3


@patch("sdcm.cluster_docker.ContainerManager")
def test_reconfigure_vector_store_nodes_reraises(mock_container_manager, vector_store_docker_cluster):
    nodes = _make_nodes(3)
    vector_store_docker_cluster.nodes = nodes
    mock_container_manager.is_running.return_value = False

    def _run_container(node, _name):
        if node is nodes[1]:
            raise RuntimeError("boom")

    mock_container_manager.run_container.side_effect = _run_container

    with pytest.raises(RuntimeError, match="boom"):
        vector_store_docker_cluster._reconfigure_vector_store_nodes()

    # All nodes run concurrently, so destroy_container still runs for every node.
    assert mock_container_manager.destroy_container.call_count == 3


def test_reconfigure_vector_store_nodes_empty_is_noop(vector_store_docker_cluster):
    vector_store_docker_cluster.nodes = []
    vector_store_docker_cluster._reconfigure_vector_store_nodes()  # must not raise

"""Tests for DockerCluster rack assignment logic in _create_nodes and _get_nodes."""

import pytest
from unittest.mock import MagicMock, patch

from sdcm.cluster_docker import DockerCluster
from sdcm.utils.docker_utils import ContainerManager


def _make_mock_node(node_index, rack):
    """Create a mock node with the given index and rack."""
    node = MagicMock()
    node.node_index = node_index
    node.rack = rack
    node.enable_auto_bootstrap = False
    return node


def _make_mock_container(node_index):
    """Create a mock container with the given NodeIndex label."""
    container = MagicMock()
    container.labels = {"NodeIndex": str(node_index)}
    container.name = f"test-node-{node_index}"
    return container


def _mock_create_node(node_index, container=None, rack=0):
    """Side-effect for _create_node that returns a mock node with proper attributes."""
    return _make_mock_node(node_index, rack)


@pytest.fixture
def docker_cluster():
    """Create a DockerCluster instance bypassing __init__."""
    cluster = object.__new__(DockerCluster)
    cluster.nodes = []
    cluster.racks_count = 3
    cluster.node_prefix = "test-node"
    return cluster


# --- _create_nodes: rack assignment tests ---


@pytest.mark.parametrize(
    "racks_count,node_count,rack_param,expected_racks",
    [
        pytest.param(3, 6, None, [0, 1, 2, 0, 1, 2], id="round-robin-3-racks"),
        pytest.param(2, 5, None, [0, 1, 0, 1, 0], id="round-robin-2-racks"),
        pytest.param(1, 4, None, [0, 0, 0, 0], id="single-rack"),
        pytest.param(3, 3, 5, [5, 5, 5], id="explicit-rack-overrides-round-robin"),
    ],
)
def test_create_nodes_rack_assignment(
    docker_cluster: DockerCluster, racks_count, node_count, rack_param, expected_racks
):
    """Verify _create_nodes assigns racks via round-robin or explicit override."""
    docker_cluster.racks_count = racks_count

    with patch.object(DockerCluster, "_create_node", side_effect=_mock_create_node) as mock_create:
        docker_cluster._create_nodes(count=node_count, rack=rack_param)

    actual_racks = [call.kwargs["rack"] for call in mock_create.call_args_list]
    assert actual_racks == expected_racks


def test_create_nodes_enable_auto_bootstrap_propagation(docker_cluster):
    """Verify enable_auto_bootstrap is set on each created node."""
    with patch.object(DockerCluster, "_create_node", side_effect=_mock_create_node):
        nodes = docker_cluster._create_nodes(count=3, enable_auto_bootstrap=True)

    assert len(nodes) == 3
    assert all(n.enable_auto_bootstrap is True for n in nodes)


# --- _get_nodes: rack derivation from discovered containers ---


def test_get_nodes_derives_consistent_racks(docker_cluster):
    """Verify _get_nodes computes rack = node_index % racks_count for discovered containers."""
    containers = [_make_mock_container(i) for i in range(6)]
    expected_racks = [0, 1, 2, 0, 1, 2]

    with (
        patch.object(ContainerManager, "get_containers_by_prefix", return_value=containers),
        patch.object(DockerCluster, "_create_node", side_effect=_mock_create_node) as mock_create,
    ):
        docker_cluster._get_nodes()

    actual_racks = [call.kwargs["rack"] for call in mock_create.call_args_list]
    assert actual_racks == expected_racks


# --- _get_new_node_indexes: gap-filling logic ---


def test_get_new_node_indexes_fills_gaps(docker_cluster):
    """Verify indexes fill gaps in existing node list before extending."""
    docker_cluster.nodes = [
        _make_mock_node(0, rack=0),
        _make_mock_node(2, rack=2),
    ]

    result = docker_cluster._get_new_node_indexes(count=2)

    assert result == [1, 3]


def test_get_new_node_indexes_no_existing_nodes(docker_cluster):
    """Verify indexes start from 0 when no nodes exist."""
    result = docker_cluster._get_new_node_indexes(count=3)

    assert result == [0, 1, 2]


def test_get_nodes_empty_container_list(docker_cluster):
    """Verify _get_nodes returns empty list when no containers found."""
    with (
        patch.object(ContainerManager, "get_containers_by_prefix", return_value=[]),
        patch.object(DockerCluster, "_create_node", side_effect=_mock_create_node) as mock_create,
    ):
        result = docker_cluster._get_nodes()

    mock_create.assert_not_called()
    assert result == []

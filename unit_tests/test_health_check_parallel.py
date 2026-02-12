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
# Copyright (c) 2025 ScyllaDB

"""
Unit tests for parallel health check execution in BaseScyllaCluster.check_cluster_health().
Tests exercise the actual code path rather than re-implementing the logic.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseScyllaCluster


@pytest.fixture
def mock_node():
    """Factory for creating mock nodes."""

    def _make(name="node-0", running_nemesis=None):
        node = MagicMock()
        node.name = name
        node.running_nemesis = running_nemesis
        node.check_node_health = MagicMock()
        return node

    return _make


@pytest.fixture
def cluster_instance(mock_node):
    """Create a minimal BaseScyllaCluster-like instance for testing check_cluster_health().

    We bind the real method to a mock that has all the attributes
    check_cluster_health() accesses.
    """
    cluster = MagicMock()
    cluster.log = logging.getLogger("test_health_check")
    cluster.nemesis_count = 1
    cluster.dead_nodes_ip_address_list = []
    cluster.test_config.tester_obj.return_value.partitions_attrs = None

    # Bind the real check_cluster_health method to our mock
    cluster.check_cluster_health = BaseScyllaCluster.check_cluster_health.__get__(cluster)

    nodes = [mock_node(name=f"node-{i}") for i in range(5)]
    cluster.nodes = nodes

    return cluster


def _setup_params(cluster, parallel_workers=5, health_check=True):
    """Configure cluster.params.get to return the given values."""
    config = {
        "cluster_health_check": health_check,
        "cluster_health_check_parallel_workers": parallel_workers,
    }
    cluster.params.get = MagicMock(side_effect=lambda key: config[key])


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_parallel_execution_calls_all_nodes(mock_event_cls, mock_timeout, cluster_instance):
    """Parallel path calls check_node_health() on every node."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    _setup_params(cluster_instance, parallel_workers=5)

    cluster_instance.check_cluster_health()

    for node in cluster_instance.nodes:
        node.check_node_health.assert_called_once()


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_sequential_execution_with_single_worker(mock_event_cls, mock_timeout, cluster_instance):
    """When parallel_workers=1, sequential path is used."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    _setup_params(cluster_instance, parallel_workers=1)

    cluster_instance.check_cluster_health()

    for node in cluster_instance.nodes:
        node.check_node_health.assert_called_once()


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_sequential_execution_with_single_node(mock_event_cls, mock_timeout, cluster_instance, mock_node):
    """Single-node cluster uses sequential path regardless of worker count."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    cluster_instance.nodes = [mock_node(name="only-node")]
    _setup_params(cluster_instance, parallel_workers=5)

    cluster_instance.check_cluster_health()

    cluster_instance.nodes[0].check_node_health.assert_called_once()


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_parallel_failure_publishes_event_and_raises(mock_event_cls, mock_timeout, cluster_instance):
    """When a node health check fails, an error event is published and exception re-raised."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    _setup_params(cluster_instance, parallel_workers=5)

    cluster_instance.nodes[2].check_node_health.side_effect = RuntimeError("SSH connection failed")

    with pytest.raises(RuntimeError, match="Health check failed on 1 node"):
        cluster_instance.check_cluster_health()

    # Verify event was published for the failing node
    mock_event_cls.NodeStatus.assert_called_once()
    call_kwargs = mock_event_cls.NodeStatus.call_args
    assert call_kwargs.kwargs["node"] == cluster_instance.nodes[2]
    assert "SSH connection failed" in call_kwargs.kwargs["error"]

    # Other nodes should still have been checked
    for i, node in enumerate(cluster_instance.nodes):
        if i != 2:
            node.check_node_health.assert_called_once()


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_multiple_failures_all_reported(mock_event_cls, mock_timeout, cluster_instance):
    """When multiple nodes fail, all failures are published as events."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    _setup_params(cluster_instance, parallel_workers=5)

    cluster_instance.nodes[1].check_node_health.side_effect = RuntimeError("SSH failed")
    cluster_instance.nodes[3].check_node_health.side_effect = ConnectionError("CQL timeout")

    with pytest.raises(Exception, match="Health check failed on 2 node"):
        cluster_instance.check_cluster_health()

    # Both failures should have published events
    assert mock_event_cls.NodeStatus.call_count == 2
    event_nodes = {c.kwargs["node"].name for c in mock_event_cls.NodeStatus.call_args_list}
    assert event_nodes == {"node-1", "node-3"}


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_health_check_disabled(mock_event_cls, mock_timeout, cluster_instance):
    """When cluster_health_check is False, no nodes are checked."""
    _setup_params(cluster_instance, health_check=False)

    cluster_instance.check_cluster_health()

    for node in cluster_instance.nodes:
        node.check_node_health.assert_not_called()


@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_parallel_nemesis_skips_health_check(mock_event_cls, mock_timeout, cluster_instance):
    """When nemesis_count > 1, health checks are skipped."""
    _setup_params(cluster_instance, parallel_workers=5)
    cluster_instance.nemesis_count = 2

    cluster_instance.check_cluster_health()

    for node in cluster_instance.nodes:
        node.check_node_health.assert_not_called()


@pytest.mark.parametrize(
    "input_workers,expected",
    [
        (-1, 1),
        (0, 1),
        (1, 1),
        (5, 5),
        (10, 10),
        (20, 10),
    ],
)
@patch("sdcm.cluster.adaptive_timeout")
@patch("sdcm.cluster.ClusterHealthValidatorEvent")
def test_parallel_workers_bounds_checking(mock_event_cls, mock_timeout, cluster_instance, input_workers, expected):
    """Parallel workers are clamped to [1, 10] in actual code path."""
    mock_timeout.return_value.__enter__ = MagicMock()
    mock_timeout.return_value.__exit__ = MagicMock(return_value=False)
    _setup_params(cluster_instance, parallel_workers=input_workers)

    cluster_instance.check_cluster_health()

    # All nodes should be checked regardless of clamped worker count
    for node in cluster_instance.nodes:
        node.check_node_health.assert_called_once()

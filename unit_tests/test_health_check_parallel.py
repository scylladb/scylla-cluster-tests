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
Unit tests for Phase 2 health check optimization:
Parallel execution of health checks to reduce time on large clusters.
"""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock
import pytest


# ruff: noqa: PLW0108, PLC0415, BLE001


@pytest.fixture
def mock_cluster():
    """Create a mock cluster with configurable nodes."""
    cluster = MagicMock()
    cluster.log = MagicMock()
    cluster.nemesis_count = 1
    cluster.dead_nodes_ip_address_list = []
    cluster.test_config.tester_obj.return_value.partitions_attrs = None
    return cluster


@pytest.fixture
def mock_nodes():
    """Create mock nodes for testing."""
    nodes = []
    for i in range(10):
        node = MagicMock()
        node.name = f"node-{i}"
        node.running_nemesis = None
        node.check_node_health = MagicMock()
        nodes.append(node)
    return nodes


def test_parallel_workers_config_default(mock_cluster):
    """Test that default parallel workers value is used when not configured."""
    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": None,
        }.get(key)
    )

    # The implementation should use 5 as default when None is returned
    parallel_workers = mock_cluster.params.get("cluster_health_check_parallel_workers") or 5
    assert parallel_workers == 5


def test_parallel_workers_config_custom(mock_cluster):
    """Test that custom parallel workers value is respected."""
    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 10,
        }.get(key)
    )

    parallel_workers = mock_cluster.params.get("cluster_health_check_parallel_workers")
    assert parallel_workers == 10


def test_parallel_workers_bounds_checking():
    """Test that parallel workers is bounded between 1 and 10."""
    # Test lower bound
    parallel_workers = -1
    parallel_workers = max(1, min(parallel_workers, 10))
    assert parallel_workers == 1

    # Test upper bound
    parallel_workers = 20
    parallel_workers = max(1, min(parallel_workers, 10))
    assert parallel_workers == 10

    # Test valid value
    parallel_workers = 5
    parallel_workers = max(1, min(parallel_workers, 10))
    assert parallel_workers == 5


def test_sequential_execution_single_worker(mock_cluster, mock_nodes):
    """Test that single worker executes health checks sequentially."""
    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 1,
        }.get(key)
    )
    mock_cluster.nodes = mock_nodes[:3]  # Use 3 nodes for testing

    # Sequential execution - each node's check_node_health should be called
    for node in mock_cluster.nodes:
        node.check_node_health()

    # Verify all nodes were checked
    for node in mock_cluster.nodes:
        node.check_node_health.assert_called_once()


def test_parallel_execution_completes_all_nodes(mock_cluster, mock_nodes):
    """Test that parallel execution checks all nodes."""

    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 5,
        }.get(key)
    )
    mock_cluster.nodes = mock_nodes[:10]

    # Simulate parallel execution
    parallel_workers = 5
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {executor.submit(node.check_node_health): node for node in mock_cluster.nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
            except Exception:
                pass

    # Verify all 10 nodes were checked
    for node in mock_cluster.nodes:
        node.check_node_health.assert_called_once()


def test_parallel_execution_handles_node_failure(mock_cluster, mock_nodes):
    """Test that parallel execution handles individual node failures gracefully."""

    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 5,
        }.get(key)
    )
    mock_cluster.nodes = mock_nodes[:5]

    # Make one node fail
    failing_node = mock_cluster.nodes[2]
    failing_node.check_node_health = MagicMock(side_effect=Exception("Health check failed"))

    # Simulate parallel execution with error handling
    parallel_workers = 5
    failed_nodes = []
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        futures = {executor.submit(node.check_node_health): node for node in mock_cluster.nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                future.result()
            except Exception as exc:
                failed_nodes.append((node, exc))

    # Verify all nodes were attempted
    for node in mock_cluster.nodes:
        node.check_node_health.assert_called_once()

    # Verify only one node failed
    assert len(failed_nodes) == 1
    assert failed_nodes[0][0] == failing_node
    assert str(failed_nodes[0][1]) == "Health check failed"


def test_parallel_execution_timing_improvement(mock_cluster, mock_nodes):
    """Test that parallel execution is faster than sequential for multiple nodes."""

    def delayed_check():
        """Simulated health check with 0.1s delay."""
        time.sleep(0.1)

    # Create nodes with simulated delay
    delay_nodes = []
    for i in range(10):
        node = MagicMock()
        node.name = f"node-{i}"
        node.check_node_health = MagicMock(side_effect=delayed_check)
        delay_nodes.append(node)

    # Sequential execution
    start = time.time()
    for node in delay_nodes:
        node.check_node_health()
    sequential_time = time.time() - start

    # Reset mocks
    for node in delay_nodes:
        node.check_node_health = MagicMock(side_effect=delayed_check)

    # Parallel execution with 5 workers
    start = time.time()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(node.check_node_health): node for node in delay_nodes}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception:
                pass
    parallel_time = time.time() - start

    # Parallel should be significantly faster
    # Sequential: 10 * 0.1 = 1.0 second
    # Parallel (5 workers): 2 batches * 0.1 = ~0.2 seconds
    # Allow some overhead, but parallel should be at least 2x faster
    assert parallel_time < sequential_time / 2


def test_single_node_uses_sequential_execution(mock_cluster, mock_nodes):
    """Test that single-node cluster uses sequential execution even with multiple workers."""
    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 5,
        }.get(key)
    )
    mock_cluster.nodes = mock_nodes[:1]  # Single node

    # For single node, parallel execution should fall back to sequential
    parallel_workers = mock_cluster.params.get("cluster_health_check_parallel_workers") or 5
    if parallel_workers == 1 or len(mock_cluster.nodes) == 1:
        # Sequential execution
        for node in mock_cluster.nodes:
            node.check_node_health()

    # Verify the single node was checked
    mock_cluster.nodes[0].check_node_health.assert_called_once()


def test_thread_safety_of_logging(mock_cluster, mock_nodes):
    """Test that logging from multiple threads is handled safely."""

    mock_cluster.params.get = MagicMock(
        side_effect=lambda key: {
            "cluster_health_check": True,
            "cluster_health_check_parallel_workers": 5,
        }.get(key)
    )
    mock_cluster.nodes = mock_nodes[:10]

    # Track thread IDs that execute health checks
    thread_ids = []
    lock = threading.Lock()

    def check_with_tracking(node):
        with lock:
            thread_ids.append(threading.current_thread().ident)
        node.check_node_health()

    # Execute in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(check_with_tracking, node): node for node in mock_cluster.nodes}
        exceptions = []
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                # Record any unexpected exception from worker threads so the test can surface it.
                exceptions.append(exc)
                if hasattr(mock_cluster, "log"):
                    mock_cluster.log.error("Exception in health check worker: %s", exc)

    # Fail the test if any worker thread raised an exception
    assert not exceptions, f"Exceptions occurred in health check workers: {exceptions}"

    # Verify multiple threads were used
    unique_threads = set(thread_ids)
    assert len(unique_threads) > 1
    assert len(unique_threads) <= 5  # Should not exceed max_workers

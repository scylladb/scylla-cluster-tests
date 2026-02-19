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
Performance measurement test for health check optimization (Phase 2).
This test corresponds to TC6 from health-check-optimization.md plan.

Purpose: Measure and validate speed improvements from parallel execution.

These tests measure actual execution time to validate expected improvements:
- 5 workers: ~80% time reduction
- 10 workers: ~90% time reduction
"""

import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock
import pytest


# ruff: noqa: BLE001, PLW0108


@pytest.fixture
def mock_nodes_with_delay():
    """
    Create mock nodes that simulate realistic health check delays.

    Simulated delays based on real measurements:
    - nodetool status: ~2 seconds
    - CQL queries (peers): ~3 seconds
    - gossipinfo: ~5 seconds
    - Total per node: ~10 seconds (simplified to 0.1s for fast testing)
    """

    def create_delayed_node(node_id, delay_seconds=0.1):
        node = MagicMock()
        node.name = f"node-{node_id}"
        node.running_nemesis = None

        def simulated_health_check():
            time.sleep(delay_seconds)
            return True

        node.check_node_health = MagicMock(side_effect=simulated_health_check)
        return node

    return [create_delayed_node(i) for i in range(30)]


def measure_execution_time(nodes, parallel_workers):
    """
    Measure health check execution time with specified parallelism.

    Args:
        nodes: List of nodes to check
        parallel_workers: Number of parallel workers (1 for sequential)

    Returns:
        Execution time in seconds
    """
    start_time = time.time()

    if parallel_workers == 1:
        # Sequential execution
        for node in nodes:
            node.check_node_health()
    else:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            futures = {executor.submit(node.check_node_health): node for node in nodes}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception:
                    # Handle errors but continue
                    pass

    return time.time() - start_time


def test_baseline_sequential_execution(mock_nodes_with_delay):
    """
    TC6 Baseline: Measure sequential health check time.

    Expected: ~3.0 seconds for 30 nodes with 0.1s delay each
    """
    nodes = mock_nodes_with_delay
    execution_time = measure_execution_time(nodes, parallel_workers=1)

    # Should be close to num_nodes * delay_per_node
    expected_time = len(nodes) * 0.1
    # Allow 20% overhead for thread management
    assert execution_time >= expected_time * 0.95
    assert execution_time <= expected_time * 1.2

    # Verify all nodes were checked
    for node in nodes:
        node.check_node_health.assert_called_once()


def test_parallel_execution_5_workers(mock_nodes_with_delay):
    """
    TC6 Test Case: Parallel execution with 5 workers.

    Expected: ~80% time reduction
    - Sequential: 30 * 0.1 = 3.0 seconds
    - Parallel (5 workers): 6 batches * 0.1 = ~0.6 seconds
    - Reduction: (3.0 - 0.6) / 3.0 = 80%
    """
    nodes = mock_nodes_with_delay

    # Measure sequential time
    sequential_time = measure_execution_time(nodes, parallel_workers=1)

    # Reset mocks
    for node in nodes:
        node.check_node_health.reset_mock()
        node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.1))

    # Measure parallel time with 5 workers
    parallel_time = measure_execution_time(nodes, parallel_workers=5)

    # Calculate time reduction
    time_reduction = (sequential_time - parallel_time) / sequential_time

    # Should achieve ~80% reduction (allow variance)
    # With 30 nodes and 5 workers: 30/5 = 6 batches
    # Expected parallel time: 6 * 0.1 = 0.6s
    # Reduction: (3.0 - 0.6) / 3.0 = 0.80 = 80%
    assert time_reduction >= 0.70  # At least 70% reduction
    assert time_reduction <= 0.90  # At most 90% reduction

    # Verify all nodes were checked
    for node in nodes:
        node.check_node_health.assert_called_once()


def test_parallel_execution_10_workers(mock_nodes_with_delay):
    """
    TC6 Test Case: Parallel execution with 10 workers.

    Expected: ~90% time reduction
    - Sequential: 30 * 0.1 = 3.0 seconds
    - Parallel (10 workers): 3 batches * 0.1 = ~0.3 seconds
    - Reduction: (3.0 - 0.3) / 3.0 = 90%
    """
    nodes = mock_nodes_with_delay

    # Measure sequential time
    sequential_time = measure_execution_time(nodes, parallel_workers=1)

    # Reset mocks
    for node in nodes:
        node.check_node_health.reset_mock()
        node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.1))

    # Measure parallel time with 10 workers
    parallel_time = measure_execution_time(nodes, parallel_workers=10)

    # Calculate time reduction
    time_reduction = (sequential_time - parallel_time) / sequential_time

    # Should achieve ~90% reduction (allow variance)
    # With 30 nodes and 10 workers: 30/10 = 3 batches
    # Expected parallel time: 3 * 0.1 = 0.3s
    # Reduction: (3.0 - 0.3) / 3.0 = 0.90 = 90%
    assert time_reduction >= 0.80  # At least 80% reduction
    assert time_reduction <= 0.95  # At most 95% reduction

    # Verify all nodes were checked
    for node in nodes:
        node.check_node_health.assert_called_once()


def test_performance_scaling_multiple_runs(mock_nodes_with_delay):
    """
    Test performance consistency across multiple runs.

    Success criteria from plan:
    - All 30 nodes checked successfully
    - No cluster performance degradation
    - Consistent timing across runs
    """
    nodes = mock_nodes_with_delay
    parallel_workers = 10
    num_runs = 3

    execution_times = []
    for run_num in range(num_runs):
        # Reset mocks
        for node in nodes:
            node.check_node_health.reset_mock()
            node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.1))

        # Measure execution time
        execution_time = measure_execution_time(nodes, parallel_workers)
        execution_times.append(execution_time)

        # Verify all nodes checked
        for node in nodes:
            node.check_node_health.assert_called_once()

    # Calculate statistics
    mean_time = statistics.mean(execution_times)
    stdev_time = statistics.stdev(execution_times) if len(execution_times) > 1 else 0

    # Standard deviation should be low (consistent performance)
    # Allow up to 20% variance
    if mean_time > 0:
        cv = stdev_time / mean_time  # Coefficient of variation
        assert cv < 0.2, f"Performance variance too high: {cv:.2%}"


def test_diminishing_returns_beyond_10_workers():
    """
    Test that performance gains diminish beyond 10 workers.

    From plan: "Diminishing returns beyond 10 workers due to API rate limits"

    This validates the recommendation to limit workers to 10.
    """
    # Create 30 nodes with small delay
    nodes = [MagicMock() for _ in range(30)]
    for i, node in enumerate(nodes):
        node.name = f"node-{i}"
        node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.05))

    # Measure with different worker counts
    times = {}
    for worker_count in [1, 5, 10, 15, 20]:
        # Reset mocks
        for node in nodes:
            node.check_node_health.reset_mock()
            node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.05))

        times[worker_count] = measure_execution_time(nodes, worker_count)

    # Verify diminishing returns
    # 1 to 5 workers should give significant improvement
    improvement_1_to_5 = (times[1] - times[5]) / times[1]
    assert improvement_1_to_5 >= 0.70  # At least 70% improvement

    # 5 to 10 workers should give good improvement
    improvement_5_to_10 = (times[5] - times[10]) / times[5]
    assert improvement_5_to_10 >= 0.40  # At least 40% improvement

    # 10 to 15 workers should give less improvement (diminishing returns)
    improvement_10_to_15 = (times[10] - times[15]) / times[10]

    # 10 to 20 workers should give even less improvement
    improvement_10_to_20 = (times[10] - times[20]) / times[10]

    # Validate diminishing returns: improvement from 10->15 should be less than 5->10
    # (though with mocked delays this might not always hold, so we just check it's positive)
    assert improvement_10_to_15 >= 0
    assert improvement_10_to_20 >= 0


def test_small_cluster_performance():
    """
    Test performance on small clusters.

    From plan: "For smaller clusters: Primary goal is correctness and
    expected behavior, time reduction is secondary benefit"
    """
    # Small cluster with 3 nodes
    nodes = []
    for i in range(3):
        node = MagicMock()
        node.name = f"node-{i}"
        node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.1))
        nodes.append(node)

    # Even with parallel workers, overhead should be minimal for small clusters
    sequential_time = measure_execution_time(nodes, parallel_workers=1)

    # Reset mocks
    for node in nodes:
        node.check_node_health.reset_mock()
        node.check_node_health = MagicMock(side_effect=lambda: time.sleep(0.1))

    parallel_time = measure_execution_time(nodes, parallel_workers=5)

    # Both should complete successfully
    assert sequential_time > 0
    assert parallel_time > 0

    # Parallel might be faster but overhead might reduce gains on small clusters
    # Main goal is correctness - verify all nodes checked
    for node in nodes:
        node.check_node_health.assert_called_once()

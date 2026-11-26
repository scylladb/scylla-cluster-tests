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

"""Tests for `ClusterTester.stop_resources_stop_tasks_threads`, which waits for the nodes at the same time."""

import threading
import types
from unittest.mock import ANY, MagicMock, patch

import pytest

from sdcm.sct_events import Severity
from sdcm.tester import ClusterTester

# how long the wait of a node waits for the waits of the other nodes, a serial run fails after it
OVERLAP_TIMEOUT = 10
CLUSTER_NAME = "db-cluster"


@pytest.fixture
def framework_event():
    """Patch TestFrameworkEvent in sdcm.tester and yield the mock."""
    with patch("sdcm.tester.TestFrameworkEvent") as event:
        yield event


def make_nodes(count):
    """Return count MagicMock nodes named node-0, node-1, ..."""
    nodes = []
    for idx in range(count):
        node = MagicMock()
        node.name = f"node-{idx}"
        nodes.append(node)
    return nodes


def call_stop_resources_stop_tasks_threads(nodes):
    """Call ClusterTester.stop_resources_stop_tasks_threads on a cluster of the given nodes."""
    cluster = MagicMock()
    cluster.nodes = nodes
    cluster.__str__.return_value = CLUSTER_NAME
    return ClusterTester.stop_resources_stop_tasks_threads(MagicMock(), cluster)


def test_stops_all_nodes_before_waiting_for_any(framework_event):
    """Every node gets stop_task_threads before any node waits, and no event is published."""
    nodes = make_nodes(5)
    lock = threading.Lock()
    calls = []

    def record(action, node):
        def side_effect(*_, **__):
            with lock:
                calls.append((action, node.name))

        return side_effect

    for node in nodes:
        node.stop_task_threads.side_effect = record("stop", node)
        node.wait_till_tasks_threads_are_stopped.side_effect = record("wait", node)

    call_stop_resources_stop_tasks_threads(nodes)

    assert calls[: len(nodes)] == [("stop", node.name) for node in nodes]
    assert sorted(calls[len(nodes) :]) == [("wait", node.name) for node in nodes]
    framework_event.assert_not_called()


def test_waits_for_the_nodes_at_the_same_time(framework_event):
    """All node waits run concurrently: each one returns only once every other node is waiting too."""
    nodes = make_nodes(5)
    barrier = threading.Barrier(len(nodes))
    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.side_effect = lambda: barrier.wait(timeout=OVERLAP_TIMEOUT)

    call_stop_resources_stop_tasks_threads(nodes)

    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.assert_called_once_with()
    framework_event.assert_not_called()


def test_silences_the_failure_of_a_node_and_waits_for_the_others(framework_event):
    """A failing node wait is published as a TestFrameworkEvent and the other nodes are still waited for."""
    nodes = make_nodes(5)
    error = RuntimeError("node-1 threads are stuck")
    nodes[1].wait_till_tasks_threads_are_stopped.side_effect = error

    assert call_stop_resources_stop_tasks_threads(nodes) is None

    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.assert_called_once_with()
    framework_event.assert_called_once_with(
        source="MagicMock",
        source_method=f"stop_resources_stop_tasks_threads(cluster={CLUSTER_NAME})",
        message=f"stop_resources_stop_tasks_threads(cluster={CLUSTER_NAME}) (silenced) failed with:",
        exception=error,
        severity=Severity.ERROR,
        trace=ANY,
    )
    assert isinstance(framework_event.call_args.kwargs["trace"], types.FrameType)

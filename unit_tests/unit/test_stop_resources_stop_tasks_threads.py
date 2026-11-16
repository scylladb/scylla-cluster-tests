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
from unittest.mock import MagicMock, patch

import pytest

from sdcm.tester import ClusterTester

# how long the wait of a node waits for the waits of the other nodes, a serial run fails after it
OVERLAP_TIMEOUT = 10
CLUSTER_NAME = "db-cluster"


@pytest.fixture
def framework_event():
    with patch("sdcm.tester.TestFrameworkEvent") as event:
        yield event


def _make_nodes(count):
    nodes = []
    for idx in range(count):
        node = MagicMock()
        node.name = f"node-{idx}"
        nodes.append(node)
    return nodes


def _stop_resources_stop_tasks_threads(nodes):
    cluster = MagicMock()
    cluster.nodes = nodes
    cluster.__str__.return_value = CLUSTER_NAME
    return ClusterTester.stop_resources_stop_tasks_threads(MagicMock(), cluster)


def test_stops_all_nodes_before_waiting_for_any(framework_event):
    nodes = _make_nodes(5)
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

    _stop_resources_stop_tasks_threads(nodes)

    assert calls[: len(nodes)] == [("stop", node.name) for node in nodes]
    assert sorted(calls[len(nodes) :]) == [("wait", node.name) for node in nodes]
    framework_event.assert_not_called()


def test_waits_for_the_nodes_at_the_same_time(framework_event):
    nodes = _make_nodes(5)
    barrier = threading.Barrier(len(nodes))
    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.side_effect = lambda: barrier.wait(timeout=OVERLAP_TIMEOUT)

    _stop_resources_stop_tasks_threads(nodes)

    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.assert_called_once_with()
    framework_event.assert_not_called()


def test_silences_the_failure_of_a_node_and_waits_for_the_others(framework_event):
    nodes = _make_nodes(5)
    error = RuntimeError("node-1 threads are stuck")
    nodes[1].wait_till_tasks_threads_are_stopped.side_effect = error

    assert _stop_resources_stop_tasks_threads(nodes) is None

    for node in nodes:
        node.wait_till_tasks_threads_are_stopped.assert_called_once_with()
    framework_event.assert_called_once()
    assert (
        framework_event.call_args.kwargs["source_method"]
        == f"stop_resources_stop_tasks_threads(cluster={CLUSTER_NAME})"
    )
    assert framework_event.call_args.kwargs["exception"] is error

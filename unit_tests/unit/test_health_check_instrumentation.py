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
# Copyright (c) 2026 ScyllaDB

"""
Unit tests for health check timing instrumentation.

Phase 3 of docs/plans/infrastructure/health-check-optimization.md: the health check must report
where it spent its time -- per state-gathering operation, per retry attempt, and split between
working and waiting -- without changing which checks run or when the gate passes.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseNode, BaseScyllaCluster
from sdcm.utils.health_checker import NodeHealthCheckStats

GATHER_OPERATIONS = {"nodetool_status", "peers", "gossip", "raft_group0", "token_ring"}


@pytest.fixture
def node():
    """A node with the real node_health_events/check_node_health bound to it."""
    node = MagicMock()
    node.name = "node-1"
    node.parent_cluster.params.get = MagicMock(return_value=True)
    node.parent_cluster.dead_nodes_ip_address_list = []
    node.get_nodes_status.return_value = {}
    node.get_peers_info.return_value = {}
    node.get_gossip_info.return_value = {}
    node.raft.get_group0_members.return_value = []
    node.get_token_ring_members.return_value = []
    node.node_health_events = BaseNode.node_health_events.__get__(node)
    node.check_node_health = BaseNode.check_node_health.__get__(node)
    return node


class NodeStatus:
    """Stand-in for ClusterHealthValidatorEvent.NodeStatus.

    check_node_health() only names the event class and publishes or suppresses it, so a real event
    would pull in the events device for no added coverage. The class name is what gets recorded.
    """

    def __init__(self):
        self.publish = MagicMock()
        self.dont_publish = MagicMock()


def _health_event():
    return NodeStatus()


class TestNodeHealthCheckStats:
    def test_working_time_is_the_sum_of_operations(self):
        stats = NodeHealthCheckStats(node_name="node-1")
        stats.operation_time = {"gossip": 2.0, "peers": 3.0}

        assert stats.working_time == 5.0

    def test_measure_accumulates_across_attempts(self):
        stats = NodeHealthCheckStats(node_name="node-1")

        for _ in range(3):
            with stats.measure("gossip"):
                pass

        assert "gossip" in stats.operation_time
        assert stats.operation_time["gossip"] >= 0

    def test_measure_records_time_even_when_the_operation_raises(self):
        stats = NodeHealthCheckStats(node_name="node-1")

        with pytest.raises(ValueError):
            with stats.measure("peers"):
                raise ValueError("boom")

        assert "peers" in stats.operation_time

    def test_waiting_is_tracked_separately_from_working(self):
        stats = NodeHealthCheckStats(node_name="node-1")
        stats.operation_time = {"gossip": 1.0}

        with stats.measure_waiting():
            pass

        assert stats.working_time == 1.0
        assert stats.waiting_time >= 0


class TestNodeHealthEvents:
    def test_times_every_gather_operation(self, node):
        stats = NodeHealthCheckStats(node_name=node.name)

        node.node_health_events(stats=stats)

        assert set(stats.operation_time) == GATHER_OPERATIONS

    def test_works_without_a_stats_object(self, node):
        """The stats argument is optional, so existing callers keep working."""
        assert list(node.node_health_events()) == []


class TestCheckNodeHealth:
    def test_returns_none_when_health_check_is_disabled(self, node):
        node.parent_cluster.params.get = MagicMock(return_value=False)

        assert node.check_node_health() is None

    def test_healthy_node_reports_a_single_attempt_and_no_waiting(self, node):
        stats = node.check_node_health()

        assert stats.attempts == 1
        assert stats.causes == []
        assert stats.waiting_time == 0.0
        assert set(stats.operation_time) == GATHER_OPERATIONS

    @patch("sdcm.cluster.time.sleep")
    def test_records_the_validator_that_caused_each_retry(self, mock_sleep, node):
        # unhealthy for the first two attempts, healthy on the third
        node.node_health_events = MagicMock(side_effect=[iter([_health_event()]), iter([_health_event()]), iter([])])

        stats = node.check_node_health()

        assert stats.attempts == 3
        assert stats.causes == ["NodeStatus", "NodeStatus"]
        assert mock_sleep.call_count == 2

    @patch("sdcm.cluster.time.sleep")
    def test_records_a_cause_for_the_final_failing_attempt(self, mock_sleep, node):
        node.node_health_events = MagicMock(side_effect=lambda **_: iter([_health_event()]))

        stats = node.check_node_health(retries=3)

        assert stats.attempts == 3
        # one cause per attempt, including the last one that published rather than retried
        assert stats.causes == ["NodeStatus"] * 3
        assert mock_sleep.call_count == 2

    @patch("sdcm.cluster.time.sleep")
    def test_measures_waiting_time_between_attempts(self, mock_sleep, node):
        node.node_health_events = MagicMock(side_effect=[iter([_health_event()]), iter([])])

        stats = node.check_node_health()

        assert stats.attempts == 2
        assert stats.waiting_time >= 0


class TestClusterTimingSummary:
    @pytest.fixture
    def cluster(self):
        cluster = MagicMock()
        cluster.log = logging.getLogger("test_health_check_instrumentation")
        cluster._log_health_check_timing = BaseScyllaCluster._log_health_check_timing.__get__(cluster)
        return cluster

    def test_ignores_nodes_whose_check_was_disabled(self, cluster, caplog):
        with caplog.at_level(logging.DEBUG):
            cluster._log_health_check_timing([None, None], elapsed=1.0)

        assert "Cluster health check took" not in caplog.text

    def test_reports_the_working_and_waiting_split(self, cluster, caplog):
        healthy = NodeHealthCheckStats(node_name="node-1", attempts=1, operation_time={"gossip": 2.0})
        retried = NodeHealthCheckStats(
            node_name="node-2", attempts=4, operation_time={"gossip": 3.0}, waiting_time=45.0
        )

        with caplog.at_level(logging.DEBUG):
            cluster._log_health_check_timing([healthy, retried, None], elapsed=50.0)

        assert "50.0s for 2 node(s)" in caplog.text
        assert "5.0s working, 45.0s waiting" in caplog.text
        assert "1 node(s) needed more than one attempt" in caplog.text
        assert "node-2 (4 attempts, 45.0s waiting)" in caplog.text
        assert "node-1" not in caplog.text.split("Nodes that retried:")[-1]

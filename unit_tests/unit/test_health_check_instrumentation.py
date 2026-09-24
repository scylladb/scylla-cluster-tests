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

import itertools
import logging
import time
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import BaseNode, BaseScyllaCluster
from sdcm.utils.health_checker import NodeHealthCheckStats, timed_validator

GATHER_OPERATIONS = {"nodetool_status", "peers", "gossip", "raft_group0", "token_ring"}
#: gathering plus running the validators over what was gathered -- what a full check records
CHECK_OPERATIONS = GATHER_OPERATIONS | {"validation"}


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
        assert set(stats.operation_time) == CHECK_OPERATIONS

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


class TestFailurePathIsStillMeasured:
    """A node whose check raises is the case most worth measuring, so it must not vanish."""

    def test_summary_is_logged_even_when_the_check_raises(self, node, caplog):
        node.node_health_events = MagicMock(side_effect=RuntimeError("ssh died"))

        with caplog.at_level(logging.DEBUG):
            with pytest.raises(RuntimeError):
                node.check_node_health()

        assert "Health check timing for node `node-1'" in caplog.text

    def test_partial_stats_stay_reachable_on_the_node(self, node):
        node.node_health_events = MagicMock(side_effect=RuntimeError("ssh died"))

        with pytest.raises(RuntimeError):
            node.check_node_health()

        # the gate reads this to account for a node that never returned
        assert isinstance(node.last_health_check_stats, NodeHealthCheckStats)
        assert node.last_health_check_stats.node_name == "node-1"

    def test_operation_time_survives_a_later_operation_raising(self, node):
        node.get_gossip_info = MagicMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            node.check_node_health()

        stats = node.last_health_check_stats
        # the operations that completed before the failure are still accounted for
        assert "nodetool_status" in stats.operation_time
        assert "peers" in stats.operation_time


class TestOperationTimeResolution:
    """Sub-100ms operations must not be rounded into a single indistinguishable bucket."""

    def test_cheap_operations_are_distinguishable_in_the_summary(self, caplog):
        stats = NodeHealthCheckStats(node_name="node-1", attempts=1)
        stats.operation_time = {"peers": 0.081, "raft_group0": 0.074, "token_ring": 0.5}

        with caplog.at_level(logging.DEBUG):
            stats.log_summary()

        # at %.1f all three collapse to "0.1s"/"0.5s" and the two cheapest become identical
        assert "peers=0.081s" in caplog.text
        assert "raft_group0=0.074s" in caplog.text
        assert "token_ring=0.500s" in caplog.text


class TestValidationIsMeasured:
    """The validators run outside node_health_events, so they need their own measurement.

    node_health_events() returns a lazy chain: gathering happens there, but running the five
    validators over what was gathered happens when check_node_health() consumes it. That time
    used to fall outside every measure block, so working_time under-reported the gate.
    """

    def test_a_full_check_records_validation_alongside_gathering(self, node):
        stats = node.check_node_health()

        assert set(stats.operation_time) == CHECK_OPERATIONS

    def test_working_time_includes_validation(self, node):
        stats = node.check_node_health()

        assert stats.working_time == pytest.approx(sum(stats.operation_time.values()))
        assert stats.working_time >= stats.operation_time["validation"]

    def test_time_spent_in_the_validators_lands_under_validation(self, node):
        """A slow validator must be charged to validation, not left out of the totals.

        The work has to happen when the chain is *consumed*, not when node_health_events() is
        called -- that is exactly the laziness that made this time invisible in the first place.
        """

        def slow_validators(stats=None):
            def chain():
                time.sleep(0.05)
                yield from ()

            return chain()

        node.node_health_events = MagicMock(side_effect=slow_validators)

        stats = node.check_node_health()

        assert stats.operation_time["validation"] >= 0.05

    def test_gathering_alone_does_not_record_validation(self, node):
        """node_health_events() on its own only gathers -- nothing has consumed the chain yet."""
        stats = NodeHealthCheckStats(node_name=node.name)

        node.node_health_events(stats=stats)

        assert "validation" not in stats.operation_time


class TestValidatorBreakdown:
    """The 'validation' total says over half the gate is validators, but not which one.

    Per-validator timing is what tells a fixed overhead apart from real per-node work: a cost
    that ignores cluster size is not the validators walking node entries.
    """

    VALIDATORS = {
        "nodes_status",
        "gossip_vs_status",
        "schema_version",
        "nulls_in_peers",
        "group0_tokenring",
    }

    def test_every_validator_is_timed_separately(self, node):
        stats = node.check_node_health()

        assert set(stats.validator_time) == self.VALIDATORS

    def test_breakdown_is_not_counted_as_extra_work(self, node):
        """validator_time is a split of the 'validation' entry, so it must stay out of the sum."""
        stats = node.check_node_health()

        assert stats.working_time == pytest.approx(sum(stats.operation_time.values()))
        assert set(stats.operation_time).isdisjoint(stats.validator_time)

    def test_breakdown_accounts_for_the_validation_total(self, node):
        stats = node.check_node_health()

        assert sum(stats.validator_time.values()) <= stats.operation_time["validation"]
        assert stats.unattributed_validation_time >= 0

    def test_time_is_charged_to_the_validator_that_spent_it(self):
        """A slow validator must show up as itself, not smeared over its neighbours."""
        stats = NodeHealthCheckStats(node_name="node-1")

        def quick():
            yield from ()

        def slow():
            time.sleep(0.05)
            yield from ()

        chain = itertools.chain(
            timed_validator(stats, "quick_one", quick()),
            timed_validator(stats, "slow_one", slow()),
        )
        assert next(chain, None) is None

        assert stats.validator_time["slow_one"] >= 0.05
        assert stats.validator_time["quick_one"] < 0.05

    def test_summary_reports_the_breakdown(self, caplog):
        stats = NodeHealthCheckStats(node_name="node-1", attempts=1)
        stats.operation_time = {"validation": 2.5}
        stats.validator_time = {"nodes_status": 2.0, "schema_version": 0.25}

        with caplog.at_level(logging.DEBUG):
            stats.log_summary()

        assert "nodes_status=2.000s" in caplog.text
        assert "schema_version=0.250s" in caplog.text
        # 2.5 measured, 2.25 attributed: the gap is time outside every validator
        assert "unattributed=0.250s" in caplog.text

    def test_accepts_any_iterable_not_just_a_generator(self):
        """One validator delegates to the node's raft helper and returns whatever that hands back.

        Taking next() on the object instead of on its iterator loops forever against a Mock and
        raises TypeError against a plain sequence, so the iterator protocol has to be used.
        """
        stats = NodeHealthCheckStats(node_name="node-1")

        events = list(timed_validator(stats, "delegating", ["first", "second"]))

        assert events == ["first", "second"]
        assert "delegating" in stats.validator_time

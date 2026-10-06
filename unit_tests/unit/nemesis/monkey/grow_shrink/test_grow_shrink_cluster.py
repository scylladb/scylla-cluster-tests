"""Tests for GrowShrinkClusterNemesis."""

from unittest.mock import MagicMock, patch

from sdcm.nemesis.monkey.grow_shrink import GrowShrinkClusterNemesis
from unit_tests.unit.nemesis.monkey.grow_shrink import MODULE


def test_runs_steady_state_once_then_grows_and_shrinks(runner, make_data_node):
    """The first run captures steady-state latency, then grows and shrinks the cluster."""
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 5}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": None,
        "nemesis_double_load_during_grow_shrink_duration": 0,
    }
    runner.has_steady_run = False
    runner.steady_state_latency = MagicMock()

    with (
        patch(f"{MODULE}.grow_cluster", return_value=[make_data_node("new1")]) as grow,
        patch(f"{MODULE}.shrink_cluster") as shrink,
        patch(f"{MODULE}._double_cluster_load") as double_load,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    runner.steady_state_latency.assert_called_once_with()
    assert runner.has_steady_run is True
    grow.assert_called_once_with(runner, rack=None)
    # instance type is not configured, so the exact nodes are not pinned for the shrink
    shrink.assert_called_once_with(runner, rack=None, new_nodes=None)
    double_load.assert_not_called()


def test_shrinks_exact_nodes_when_instance_type_configured(runner, make_data_node):
    """A dedicated grow/shrink instance type pins the shrink to the freshly added nodes."""
    new_nodes = [make_data_node("new1")]
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 0}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": "i4i.large",
        "nemesis_double_load_during_grow_shrink_duration": 0,
    }
    runner.has_steady_run = False
    runner.steady_state_latency = MagicMock()

    with (
        patch(f"{MODULE}.grow_cluster", return_value=new_nodes),
        patch(f"{MODULE}.shrink_cluster") as shrink,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    runner.steady_state_latency.assert_not_called()
    shrink.assert_called_once_with(runner, rack=None, new_nodes=new_nodes)


def test_doubles_load_between_grow_and_shrink(runner):
    """A configured double-load duration triggers the extra load run after the grow."""
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 0}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": None,
        "nemesis_double_load_during_grow_shrink_duration": 15,
    }
    runner.has_steady_run = True

    with (
        patch(f"{MODULE}.grow_cluster", return_value=[]),
        patch(f"{MODULE}.shrink_cluster"),
        patch(f"{MODULE}._double_cluster_load") as double_load,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    double_load.assert_called_once_with(runner, 15)

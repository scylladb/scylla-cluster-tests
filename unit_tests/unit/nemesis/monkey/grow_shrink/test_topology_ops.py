"""Tests for the grow/shrink primitives in sdcm.nemesis.utils.topology_ops."""

from unittest.mock import call, patch

import pytest

from sdcm.nemesis.utils import DefaultValue
from sdcm.nemesis.utils.topology_ops import (
    decommission_nodes_by_criteria,
    grow_cluster,
    shrink_cluster,
)
from unit_tests.unit.nemesis.monkey.grow_shrink import OPS_MODULE

# ---------------------------------------------------------------------------
# grow_cluster
# ---------------------------------------------------------------------------


def test_grow_cluster_adds_all_nodes_at_once_when_parallel_operations_enabled(runner, make_data_node):
    """With parallel node operations a single add_new_nodes call adds every node."""
    new_nodes = [make_data_node("new1"), make_data_node("new2")]
    runner.add_new_nodes.return_value = new_nodes
    runner.tester.params = {"nemesis_add_node_cnt": 2, "nemesis_grow_shrink_instance_type": None}

    with patch(f"{OPS_MODULE}.time.sleep"):
        result = grow_cluster(runner, rack=None)

    assert result == new_nodes
    runner.add_new_nodes.assert_called_once_with(count=2, rack=None, instance_type=None)
    assert runner.node_allocator.unset_running_nemesis.call_args_list == [
        call(node, runner.current_disruption) for node in new_nodes
    ]


def test_grow_cluster_round_robins_racks_when_operations_are_serial(runner, make_data_node):
    """Without parallel node operations nodes are added one by one, spread over racks."""
    runner.cluster.parallel_node_operations = False
    runner.cluster.racks_count = 2
    runner.tester.params = {"nemesis_add_node_cnt": 3, "nemesis_grow_shrink_instance_type": "i4i.large"}
    runner.add_new_nodes.side_effect = lambda **kwargs: [make_data_node(f"new{kwargs['rack']}")]

    with patch(f"{OPS_MODULE}.time.sleep"):
        result = grow_cluster(runner, rack=None)

    assert [c.kwargs["rack"] for c in runner.add_new_nodes.call_args_list] == [0, 1, 0]
    assert all(c.kwargs["instance_type"] == "i4i.large" for c in runner.add_new_nodes.call_args_list)
    assert len(result) == 3


def test_grow_cluster_defaults_rack_to_zero_on_kubernetes(runner):
    """On k8s an unspecified rack is pinned to rack 0 instead of round-robin."""
    runner._is_it_on_kubernetes.return_value = True
    runner.tester.params = {"nemesis_add_node_cnt": 1, "nemesis_grow_shrink_instance_type": None}

    with patch(f"{OPS_MODULE}.time.sleep"):
        grow_cluster(runner, rack=None)

    runner.add_new_nodes.assert_called_once_with(count=1, rack=0, instance_type=None)


# ---------------------------------------------------------------------------
# shrink_cluster
# ---------------------------------------------------------------------------


def test_shrink_cluster_decommissions_down_to_initial_size(runner, make_data_node):
    """Only the nodes added on top of the initial cluster size are decommissioned."""
    runner.cluster.data_nodes = [make_data_node(f"node{i}") for i in range(5)]
    runner.tester.params = {"nemesis_add_node_cnt": 3, "n_db_nodes": [3]}

    with patch(f"{OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=None)

    decommission.assert_called_once_with(runner, 2, None, is_seed=DefaultValue, dc_idx=0, exact_nodes=None)


def test_shrink_cluster_passes_exact_nodes_through(runner, make_data_node):
    """When exact nodes are given they are decommissioned instead of freshly picked ones."""
    exact_nodes = [make_data_node("new1")]
    runner.cluster.data_nodes = [make_data_node(f"node{i}") for i in range(4)]
    runner.tester.params = {"nemesis_add_node_cnt": 1, "n_db_nodes": [3]}

    with patch(f"{OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=None, new_nodes=exact_nodes)

    assert decommission.call_args.kwargs["exact_nodes"] == exact_nodes


def test_shrink_cluster_uses_k8s_pods_per_cluster_as_initial_size(runner, make_data_node):
    """On k8s the initial size comes from k8s_n_scylla_pods_per_cluster and seeds are not filtered."""
    runner._is_it_on_kubernetes.return_value = True
    runner.cluster.data_nodes = [make_data_node(f"node{i}") for i in range(5)]
    runner.tester.params = {
        "nemesis_add_node_cnt": 3,
        "n_db_nodes": [3],
        "k8s_n_scylla_pods_per_cluster": 4,
    }

    with patch(f"{OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=1)

    decommission.assert_called_once_with(runner, 1, 1, is_seed=None, dc_idx=0, exact_nodes=None)


def test_shrink_cluster_raises_when_cluster_is_already_at_initial_size(runner, make_data_node):
    """Shrinking below the configured cluster size is refused."""
    runner.cluster.data_nodes = [make_data_node(f"node{i}") for i in range(3)]
    runner.tester.params = {"nemesis_add_node_cnt": 2, "n_db_nodes": [3]}

    with (
        patch(f"{OPS_MODULE}.decommission_nodes_by_criteria") as decommission,
        pytest.raises(Exception, match="Not enough nodes for decommission"),
    ):
        shrink_cluster(runner, rack=None)

    decommission.assert_not_called()


# ---------------------------------------------------------------------------
# decommission_nodes_by_criteria
# ---------------------------------------------------------------------------


def test_decommission_nodes_by_criteria_marks_and_decommissions_exact_nodes(runner, make_data_node):
    """Exact nodes are claimed by the nemesis and decommissioned in one batch."""
    exact_nodes = [make_data_node("new1"), make_data_node("new2")]

    decommission_nodes_by_criteria(runner, 2, None, exact_nodes=exact_nodes)

    assert runner.node_allocator.set_running_nemesis.call_args_list == [
        call(node, runner.current_disruption) for node in exact_nodes
    ]
    runner.decommission_nodes.assert_called_once_with(exact_nodes)


def test_decommission_nodes_by_criteria_decommissions_one_by_one_when_serial(runner, make_data_node):
    """Without parallel node operations every node is decommissioned on its own."""
    runner.cluster.parallel_node_operations = False
    exact_nodes = [make_data_node("new1"), make_data_node("new2")]

    decommission_nodes_by_criteria(runner, 2, None, exact_nodes=exact_nodes)

    assert runner.decommission_nodes.call_args_list == [call([node]) for node in exact_nodes]


def test_decommission_nodes_by_criteria_selects_nodes_round_robin_over_racks(runner, make_data_node):
    """Without exact nodes, targets are selected rack by rack and released from the runner."""
    picked = [make_data_node("picked1"), make_data_node("picked2")]
    runner.set_target_node.side_effect = lambda **_: setattr(runner, "target_node", picked.pop(0))

    decommission_nodes_by_criteria(runner, 2, None, dc_idx=1)

    assert runner.set_target_node.call_args_list == [
        call(is_seed=DefaultValue, dc_idx=1, rack=0),
        call(is_seed=DefaultValue, dc_idx=1, rack=1),
    ]
    assert runner.target_node is None
    assert runner.decommission_nodes.call_count == 1


def test_decommission_nodes_by_criteria_swallows_decommission_failures(runner, make_data_node):
    """A failed decommission is reported as an event but does not propagate."""
    runner.decommission_nodes.side_effect = RuntimeError("boom")

    decommission_nodes_by_criteria(runner, 1, None, exact_nodes=[make_data_node("new1")])

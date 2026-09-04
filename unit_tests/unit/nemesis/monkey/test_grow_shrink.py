"""Tests for sdcm.nemesis.monkey.topology.grow_shrink module."""

from unittest.mock import MagicMock, call, patch

import pytest

from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass, NemesisFlags
from sdcm.nemesis.monkey.grow_shrink import (
    AddRemoveRackNemesis,
    GrowShrinkClusterNemesis,
    GrowShrinkZeroTokenNode,
    TerminateAndRemoveNodeMonkey,
)
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.nemesis.utils import DefaultValue
from sdcm.nemesis.utils.topology_ops import (
    decommission_nodes_by_criteria,
    grow_cluster,
    shrink_cluster,
)
from sdcm.utils.version_utils import MethodVersionNotFound

_MODULE = "sdcm.nemesis.monkey.grow_shrink"
_OPS_MODULE = "sdcm.nemesis.utils.topology_ops"

pytestmark = pytest.mark.usefixtures("events")


def _make_data_node(name, dc_idx=0):
    """Return a mock data node placed in the given datacenter."""
    node = MagicMock()
    node.name = name
    node.dc_idx = dc_idx
    node._is_zero_token_node = False
    return node


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with the attributes grow/shrink helpers rely on."""
    base_runner.interval = 0
    base_runner.current_disruption = "GrowShrinkCluster-deadbeef"
    base_runner.node_allocator = MagicMock()
    base_runner.monitoring_set = MagicMock()
    base_runner._is_it_on_kubernetes = MagicMock(return_value=False)
    base_runner.decommission_nodes = MagicMock()
    base_runner.add_new_nodes = MagicMock(return_value=[])
    base_runner.set_target_node = MagicMock()
    base_runner.set_target_node_pool_type = MagicMock()
    base_runner.cluster.parallel_node_operations = True
    base_runner.cluster.racks_count = 2
    base_runner.target_node.dc_idx = 0
    base_runner.target_node._is_zero_token_node = False
    base_runner.tester.params = {}
    base_runner.cluster.params = {}
    return base_runner


# ---------------------------------------------------------------------------
# grow_cluster
# ---------------------------------------------------------------------------


def test_grow_cluster_adds_all_nodes_at_once_when_parallel_operations_enabled(runner):
    """With parallel node operations a single add_new_nodes call adds every node."""
    new_nodes = [_make_data_node("new1"), _make_data_node("new2")]
    runner.add_new_nodes.return_value = new_nodes
    runner.tester.params = {"nemesis_add_node_cnt": 2, "nemesis_grow_shrink_instance_type": None}

    with patch(f"{_OPS_MODULE}.time.sleep"):
        result = grow_cluster(runner, rack=None)

    assert result == new_nodes
    runner.add_new_nodes.assert_called_once_with(count=2, rack=None, instance_type=None)
    assert runner.node_allocator.unset_running_nemesis.call_args_list == [
        call(node, runner.current_disruption) for node in new_nodes
    ]


def test_grow_cluster_round_robins_racks_when_operations_are_serial(runner):
    """Without parallel node operations nodes are added one by one, spread over racks."""
    runner.cluster.parallel_node_operations = False
    runner.cluster.racks_count = 2
    runner.tester.params = {"nemesis_add_node_cnt": 3, "nemesis_grow_shrink_instance_type": "i4i.large"}
    runner.add_new_nodes.side_effect = lambda **kwargs: [_make_data_node(f"new{kwargs['rack']}")]

    with patch(f"{_OPS_MODULE}.time.sleep"):
        result = grow_cluster(runner, rack=None)

    assert [c.kwargs["rack"] for c in runner.add_new_nodes.call_args_list] == [0, 1, 0]
    assert all(c.kwargs["instance_type"] == "i4i.large" for c in runner.add_new_nodes.call_args_list)
    assert len(result) == 3


def test_grow_cluster_defaults_rack_to_zero_on_kubernetes(runner):
    """On k8s an unspecified rack is pinned to rack 0 instead of round-robin."""
    runner._is_it_on_kubernetes.return_value = True
    runner.tester.params = {"nemesis_add_node_cnt": 1, "nemesis_grow_shrink_instance_type": None}

    with patch(f"{_OPS_MODULE}.time.sleep"):
        grow_cluster(runner, rack=None)

    runner.add_new_nodes.assert_called_once_with(count=1, rack=0, instance_type=None)


# ---------------------------------------------------------------------------
# shrink_cluster
# ---------------------------------------------------------------------------


def test_shrink_cluster_decommissions_down_to_initial_size(runner):
    """Only the nodes added on top of the initial cluster size are decommissioned."""
    runner.cluster.data_nodes = [_make_data_node(f"node{i}") for i in range(5)]
    runner.tester.params = {"nemesis_add_node_cnt": 3, "n_db_nodes": [3]}

    with patch(f"{_OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=None)

    decommission.assert_called_once_with(runner, 2, None, is_seed=DefaultValue, dc_idx=0, exact_nodes=None)


def test_shrink_cluster_passes_exact_nodes_through(runner):
    """When exact nodes are given they are decommissioned instead of freshly picked ones."""
    exact_nodes = [_make_data_node("new1")]
    runner.cluster.data_nodes = [_make_data_node(f"node{i}") for i in range(4)]
    runner.tester.params = {"nemesis_add_node_cnt": 1, "n_db_nodes": [3]}

    with patch(f"{_OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=None, new_nodes=exact_nodes)

    assert decommission.call_args.kwargs["exact_nodes"] == exact_nodes


def test_shrink_cluster_uses_k8s_pods_per_cluster_as_initial_size(runner):
    """On k8s the initial size comes from k8s_n_scylla_pods_per_cluster and seeds are not filtered."""
    runner._is_it_on_kubernetes.return_value = True
    runner.cluster.data_nodes = [_make_data_node(f"node{i}") for i in range(5)]
    runner.tester.params = {
        "nemesis_add_node_cnt": 3,
        "n_db_nodes": [3],
        "k8s_n_scylla_pods_per_cluster": 4,
    }

    with patch(f"{_OPS_MODULE}.decommission_nodes_by_criteria") as decommission:
        shrink_cluster(runner, rack=1)

    decommission.assert_called_once_with(runner, 1, 1, is_seed=None, dc_idx=0, exact_nodes=None)


def test_shrink_cluster_raises_when_cluster_is_already_at_initial_size(runner):
    """Shrinking below the configured cluster size is refused."""
    runner.cluster.data_nodes = [_make_data_node(f"node{i}") for i in range(3)]
    runner.tester.params = {"nemesis_add_node_cnt": 2, "n_db_nodes": [3]}

    with (
        patch(f"{_OPS_MODULE}.decommission_nodes_by_criteria") as decommission,
        pytest.raises(Exception, match="Not enough nodes for decommission"),
    ):
        shrink_cluster(runner, rack=None)

    decommission.assert_not_called()


# ---------------------------------------------------------------------------
# decommission_nodes_by_criteria
# ---------------------------------------------------------------------------


def test_decommission_nodes_by_criteria_marks_and_decommissions_exact_nodes(runner):
    """Exact nodes are claimed by the nemesis and decommissioned in one batch."""
    exact_nodes = [_make_data_node("new1"), _make_data_node("new2")]

    decommission_nodes_by_criteria(runner, 2, None, exact_nodes=exact_nodes)

    assert runner.node_allocator.set_running_nemesis.call_args_list == [
        call(node, runner.current_disruption) for node in exact_nodes
    ]
    runner.decommission_nodes.assert_called_once_with(exact_nodes)


def test_decommission_nodes_by_criteria_decommissions_one_by_one_when_serial(runner):
    """Without parallel node operations every node is decommissioned on its own."""
    runner.cluster.parallel_node_operations = False
    exact_nodes = [_make_data_node("new1"), _make_data_node("new2")]

    decommission_nodes_by_criteria(runner, 2, None, exact_nodes=exact_nodes)

    assert runner.decommission_nodes.call_args_list == [call([node]) for node in exact_nodes]


def test_decommission_nodes_by_criteria_selects_nodes_round_robin_over_racks(runner):
    """Without exact nodes, targets are selected rack by rack and released from the runner."""
    picked = [_make_data_node("picked1"), _make_data_node("picked2")]
    runner.set_target_node.side_effect = lambda **_: setattr(runner, "target_node", picked.pop(0))

    decommission_nodes_by_criteria(runner, 2, None, dc_idx=1)

    assert runner.set_target_node.call_args_list == [
        call(is_seed=DefaultValue, dc_idx=1, rack=0),
        call(is_seed=DefaultValue, dc_idx=1, rack=1),
    ]
    assert runner.target_node is None
    assert runner.decommission_nodes.call_count == 1


def test_decommission_nodes_by_criteria_swallows_decommission_failures(runner):
    """A failed decommission is reported as an event but does not propagate."""
    runner.decommission_nodes.side_effect = RuntimeError("boom")

    decommission_nodes_by_criteria(runner, 1, None, exact_nodes=[_make_data_node("new1")])


# ---------------------------------------------------------------------------
# GrowShrinkClusterNemesis
# ---------------------------------------------------------------------------


def test_grow_shrink_cluster_runs_steady_state_once_then_grows_and_shrinks(runner):
    """The first run captures steady-state latency, then grows and shrinks the cluster."""
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 5}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": None,
        "nemesis_double_load_during_grow_shrink_duration": 0,
    }
    runner.has_steady_run = False
    runner.steady_state_latency = MagicMock()

    with (
        patch(f"{_MODULE}.grow_cluster", return_value=[_make_data_node("new1")]) as grow,
        patch(f"{_MODULE}.shrink_cluster") as shrink,
        patch(f"{_MODULE}._double_cluster_load") as double_load,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    runner.steady_state_latency.assert_called_once_with()
    assert runner.has_steady_run is True
    grow.assert_called_once_with(runner, rack=None)
    # instance type is not configured, so the exact nodes are not pinned for the shrink
    shrink.assert_called_once_with(runner, rack=None, new_nodes=None)
    double_load.assert_not_called()


def test_grow_shrink_cluster_shrinks_exact_nodes_when_instance_type_configured(runner):
    """A dedicated grow/shrink instance type pins the shrink to the freshly added nodes."""
    new_nodes = [_make_data_node("new1")]
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 0}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": "i4i.large",
        "nemesis_double_load_during_grow_shrink_duration": 0,
    }
    runner.has_steady_run = False
    runner.steady_state_latency = MagicMock()

    with (
        patch(f"{_MODULE}.grow_cluster", return_value=new_nodes),
        patch(f"{_MODULE}.shrink_cluster") as shrink,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    runner.steady_state_latency.assert_not_called()
    shrink.assert_called_once_with(runner, rack=None, new_nodes=new_nodes)


def test_grow_shrink_cluster_doubles_load_between_grow_and_shrink(runner):
    """A configured double-load duration triggers the extra load run after the grow."""
    runner.cluster.params = {"nemesis_sequence_sleep_between_ops": 0}
    runner.tester.params = {
        "nemesis_grow_shrink_instance_type": None,
        "nemesis_double_load_during_grow_shrink_duration": 15,
    }
    runner.has_steady_run = True

    with (
        patch(f"{_MODULE}.grow_cluster", return_value=[]),
        patch(f"{_MODULE}.shrink_cluster"),
        patch(f"{_MODULE}._double_cluster_load") as double_load,
    ):
        GrowShrinkClusterNemesis(runner).disrupt()

    double_load.assert_called_once_with(runner, 15)


# ---------------------------------------------------------------------------
# AddRemoveRackNemesis
# ---------------------------------------------------------------------------


def test_add_remove_rack_grows_and_shrinks_a_brand_new_rack(runner):
    """On k8s the nemesis operates on a rack index one above the highest existing rack."""
    runner._is_it_on_kubernetes.return_value = True
    runner.cluster.params = {"scylla_version": "6.0.0"}
    runner.cluster.racks = [0, 1]

    with patch(f"{_MODULE}.grow_cluster") as grow, patch(f"{_MODULE}.shrink_cluster") as shrink:
        AddRemoveRackNemesis(runner).disrupt()

    grow.assert_called_once_with(runner, 2)
    shrink.assert_called_once_with(runner, 2)


def test_add_remove_rack_is_unsupported_outside_kubernetes(runner):
    """Adding a rack is a scylla-operator feature, so non-k8s backends are skipped."""
    runner.cluster.params = {"scylla_version": "6.0.0"}

    with pytest.raises(UnsupportedNemesis, match="not supported for non-k8s"):
        AddRemoveRackNemesis(runner).disrupt()


def test_add_remove_rack_is_skipped_on_unsupported_scylla_version(runner):
    """Versions below the documented limitation are rejected by the scylla_versions guard."""
    runner.cluster.params = {"scylla_version": "5.2.0"}

    with pytest.raises(MethodVersionNotFound):
        AddRemoveRackNemesis(runner).disrupt()


# ---------------------------------------------------------------------------
# GrowShrinkZeroTokenNode
# ---------------------------------------------------------------------------


def test_grow_shrink_zero_token_node_adds_then_decommissions_a_zero_node(runner):
    """A zero-token node is added, kept for a while, then one is decommissioned from the same DC."""
    new_znode = _make_data_node("znode-new")
    same_dc_znode = _make_data_node("znode-old", dc_idx=0)
    other_dc_znode = _make_data_node("znode-other", dc_idx=1)
    runner.cluster.params = {"use_zero_nodes": True}
    runner.cluster.zero_nodes = [other_dc_znode, same_dc_znode]
    runner._add_and_init_new_cluster_nodes = MagicMock(return_value=[new_znode])

    with patch(f"{_MODULE}.time.sleep") as sleep:
        GrowShrinkZeroTokenNode(runner).disrupt()

    runner._add_and_init_new_cluster_nodes.assert_called_once_with(count=1, is_zero_node=True)
    sleep.assert_called_once_with(300)
    # base_runner's random.choice picks the first element of the DC-filtered list
    runner.decommission_nodes.assert_called_once_with(nodes=[same_dc_znode])


def test_grow_shrink_zero_token_node_is_unsupported_without_zero_nodes(runner):
    """The nemesis is skipped when the test does not run with zero-token nodes."""
    runner.cluster.params = {"use_zero_nodes": False}

    with pytest.raises(UnsupportedNemesis, match="zero tokens support is not enabled"):
        GrowShrinkZeroTokenNode(runner).disrupt()


# ---------------------------------------------------------------------------
# TerminateAndRemoveNodeMonkey
# ---------------------------------------------------------------------------


def test_terminate_and_remove_node_removes_target_and_adds_replacement(runner):
    """The target node is removed using a live node as the verification node."""
    verification_node = _make_data_node("node2")
    up_normal_nodes = [verification_node]
    runner.cluster.params = {"db_type": "scylla"}
    runner.cluster.get_nodes_up_and_normal.return_value = up_normal_nodes
    runner.node_allocator.run_nemesis.return_value.__enter__.return_value = verification_node
    runner._remove_node_add_node = MagicMock()

    target_node = runner.target_node
    TerminateAndRemoveNodeMonkey(runner).disrupt()

    runner.cluster.get_nodes_up_and_normal.assert_called_once_with(verification_node=target_node)
    runner.node_allocator.run_nemesis.assert_called_once_with(
        nemesis_label="RemoveNodeAddNode", node_list=up_normal_nodes
    )
    runner._remove_node_add_node.assert_called_once_with(
        verification_node=verification_node, node_to_remove=target_node
    )


def test_terminate_and_remove_node_is_unsupported_on_cloud_scylla(runner):
    """Cloud deployments cover this scenario with a dedicated nemesis."""
    runner.cluster.params = {"db_type": "cloud_scylla"}
    runner._remove_node_add_node = MagicMock()

    with pytest.raises(UnsupportedNemesis, match="CloudReplaceNonResponsiveNode"):
        TerminateAndRemoveNodeMonkey(runner).disrupt()

    runner._remove_node_add_node.assert_not_called()


# ---------------------------------------------------------------------------
# Migration guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "nemesis_class",
    [GrowShrinkClusterNemesis, AddRemoveRackNemesis, GrowShrinkZeroTokenNode, TerminateAndRemoveNodeMonkey],
)
def test_moved_nemesis_are_still_discovered_by_the_registry(nemesis_class):
    """The extracted classes keep their names and stay visible to the nemesis registry."""
    registry = NemesisRegistry(base_class=NemesisBaseClass, flag_class=NemesisFlags)
    discovered = [cls for cls in registry.get_subclasses() if cls.__name__ == nemesis_class.__name__]

    assert discovered == [nemesis_class]
    assert nemesis_class.__module__ == _MODULE

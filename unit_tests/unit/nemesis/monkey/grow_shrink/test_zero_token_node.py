"""Tests for GrowShrinkZeroTokenNode."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.nemesis.monkey.grow_shrink import GrowShrinkZeroTokenNode
from unit_tests.unit.nemesis.monkey.grow_shrink import MODULE

pytestmark = pytest.mark.usefixtures("events")


def test_adds_then_decommissions_a_zero_node(runner, make_data_node):
    """A zero-token node is added, kept for a while, then one is decommissioned from the same DC."""
    new_znode = make_data_node("znode-new")
    same_dc_znode = make_data_node("znode-old", dc_idx=0)
    other_dc_znode = make_data_node("znode-other", dc_idx=1)
    runner.cluster.zero_nodes = [other_dc_znode, same_dc_znode]
    runner._add_and_init_new_cluster_nodes = MagicMock(return_value=[new_znode])

    with patch(f"{MODULE}.time.sleep") as sleep:
        GrowShrinkZeroTokenNode(runner).disrupt()

    runner._add_and_init_new_cluster_nodes.assert_called_once_with(count=1, is_zero_node=True)
    sleep.assert_called_once_with(300)
    # base_runner's random.choice picks the first element of the DC-filtered list
    runner.decommission_nodes.assert_called_once_with(nodes=[same_dc_znode])


def test_precheck_passes_with_zero_nodes_enabled(runner):
    runner.cluster.params = {"use_zero_nodes": True}

    assert GrowShrinkZeroTokenNode(runner).precheck(runner.target_node) is None


def test_precheck_rejects_tests_without_zero_nodes(runner):
    """The nemesis is skipped when the test does not run with zero-token nodes."""
    runner.cluster.params = {"use_zero_nodes": False}

    reason = GrowShrinkZeroTokenNode(runner).precheck(runner.target_node)

    assert reason == "The zero tokens support is not enabled"

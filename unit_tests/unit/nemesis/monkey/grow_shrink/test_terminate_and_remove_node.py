"""Tests for TerminateAndRemoveNodeMonkey."""

from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.monkey.grow_shrink import TerminateAndRemoveNodeMonkey

pytestmark = pytest.mark.usefixtures("events")


def test_removes_target_and_adds_replacement(runner, make_data_node):
    """The target node is removed using a live node as the verification node."""
    verification_node = make_data_node("node2")
    up_normal_nodes = [verification_node]
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


def test_precheck_passes_on_self_managed_scylla(runner):
    runner.cluster.params = {"db_type": "scylla"}

    assert TerminateAndRemoveNodeMonkey(runner).precheck(runner.target_node) is None


def test_precheck_rejects_cloud_scylla(runner):
    """Cloud deployments cover this scenario with a dedicated nemesis."""
    runner.cluster.params = {"db_type": "cloud_scylla"}

    reason = TerminateAndRemoveNodeMonkey(runner).precheck(runner.target_node)

    assert reason == (
        "Skipping this nemesis due the replace node option that supported by Cloud "
        "is tested by CloudReplaceNonResponsiveNode nemesis"
    )

"""Tests for NemesisRunner.start_and_interrupt_decommission_streaming."""

from functools import partial
from unittest.mock import MagicMock, patch

import pytest

from sdcm.nemesis import NemesisRunner


@pytest.fixture
def runner():
    """Nemesis runner whose decommission completes before the reboot, so a new node joins in its place."""
    runner = MagicMock()
    runner.target_node = MagicMock(name="decommissioned_node")
    runner._add_and_init_new_cluster_nodes.return_value = [MagicMock(name="new_node")]
    for method in ("switch_target_node", "rebuild_or_repair", "repair_nodetool_rebuild"):
        setattr(runner, method, partial(getattr(NemesisRunner, method), runner))
    with (
        patch("sdcm.nemesis.suppress_expected_unavailability_errors"),
        patch("sdcm.nemesis.ParallelObject"),
        patch("sdcm.nemesis.FailedDecommissionOperationMonitoring"),
        patch("sdcm.nemesis.adaptive_timeout"),
        patch("sdcm.nemesis.is_tablets_feature_enabled", return_value=False),
    ):
        yield runner


def test_rebuild_runs_on_new_node_after_completed_decommission(runner):
    """The decommissioned node is destroyed, so the rebuild must run on the node that replaced it (SCT-1610)."""
    old_node = runner.target_node
    new_node = runner._add_and_init_new_cluster_nodes.return_value[0]

    NemesisRunner.start_and_interrupt_decommission_streaming(runner)

    assert runner.target_node is new_node
    assert [c for c in new_node.run_nodetool.call_args_list if c.args[:1] == ("rebuild",)]
    assert not [c for c in old_node.run_nodetool.call_args_list if c.args[:1] == ("rebuild",)]

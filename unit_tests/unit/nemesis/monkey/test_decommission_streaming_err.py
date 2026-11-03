"""Tests for NemesisRunner.start_and_interrupt_decommission_streaming (DecommissionStreamingErrMonkey)."""

import contextlib
from unittest.mock import MagicMock, patch

import pytest

from sdcm.cluster import NodeStayInClusterAfterDecommission
from sdcm.nemesis import NemesisRunner
from sdcm.utils.adaptive_timeouts import Operations

_MODULE = "sdcm.nemesis"
LOG_PATTERN_TIMEOUT = 600
# nodetool timeout (log pattern timeout + 600) + 3600
LOG_PATTERN_BASED_TIMEOUT = LOG_PATTERN_TIMEOUT + 600 + 3600

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture
def runner(base_runner):
    """Runner whose decommission is interrupted by the reboot, so the node stays in the cluster."""
    base_runner.nemesis_seed = 1
    base_runner.node_allocator = MagicMock()
    base_runner.reboot_node = MagicMock()
    base_runner.rebuild_or_repair = MagicMock()
    base_runner._call_disrupt_func_after_expression_logged = MagicMock()
    base_runner.target_node.raft.get_random_log_message.return_value = MagicMock(
        timeout=LOG_PATTERN_TIMEOUT, log_message="api - decommission"
    )
    base_runner.target_node.raft.get_severity_change_filters_scylla_start_failed.return_value = []
    base_runner.cluster.verify_decommission.side_effect = NodeStayInClusterAfterDecommission("interrupted")
    return base_runner


@pytest.mark.parametrize(
    "decommission_timeout, expected",
    [
        pytest.param(20_000, 20_000, id="node-with-much-data"),
        pytest.param(60, LOG_PATTERN_BASED_TIMEOUT, id="node-with-little-data"),
    ],
)
def test_start_and_interrupt_decommission_streaming_waits_by_data_on_node(runner, decommission_timeout, expected):
    with (
        patch(f"{_MODULE}.adaptive_timeout", return_value=contextlib.nullcontext(decommission_timeout)) as timeout,
        patch(f"{_MODULE}.FailedDecommissionOperationMonitoring") as monitor,
        patch(f"{_MODULE}.ParallelObject"),
    ):
        NemesisRunner.start_and_interrupt_decommission_streaming(runner)
    timeout.assert_called_once_with(operation=Operations.DECOMMISSION, node=runner.target_node)
    assert monitor.call_args.kwargs["timeout"] == expected
    runner.target_node.wait_node_fully_start.assert_called_once()

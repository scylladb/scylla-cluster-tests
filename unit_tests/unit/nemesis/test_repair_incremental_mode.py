from unittest.mock import MagicMock, patch

import pytest

from sdcm.exceptions import WaitForTimeoutError
from sdcm.mgmt.cli import ManagerCluster
from sdcm.mgmt.common import RepairIncrementalMode, ScyllaManagerError
from sdcm.nemesis import NemesisRunner


@pytest.fixture
def nodetool_runner():
    runner = MagicMock()
    node = MagicMock()
    with (
        patch("sdcm.nemesis.adaptive_timeout"),
        patch("sdcm.nemesis.is_tablets_feature_enabled", return_value=True),
    ):
        yield runner, node


@pytest.fixture
def runner_with_timed_out_mgr_repair():
    """Nemesis runner whose Manager repair task is still running when its wait times out.

    Returns the runner and the repair task mock.
    """
    runner = MagicMock()
    mgr_task = runner.cluster.get_cluster_manager.return_value.create_repair_task.return_value
    mgr_task.wait_and_get_final_status.side_effect = WaitForTimeoutError("repair task is still RUNNING")
    return runner, mgr_task


@pytest.mark.parametrize(
    "incremental_mode,expected_args",
    [
        (RepairIncrementalMode.FULL, "--incremental-mode full"),
        (None, ""),
    ],
)
def test_run_repair_nodetool_incremental_mode(nodetool_runner, incremental_mode, expected_args):
    runner, node = nodetool_runner

    NemesisRunner.run_repair_nodetool.__wrapped__(runner, nodes=[node], incremental_mode=incremental_mode)

    calls = {c.kwargs["sub_cmd"]: c.kwargs for c in node.run_nodetool.call_args_list}
    assert calls["cluster repair"]["args"] == expected_args
    assert "args" not in calls["repair"]


@pytest.mark.parametrize("incremental_mode,flag_expected", [(RepairIncrementalMode.FULL, True), (None, False)])
def test_create_repair_task_incremental_mode(incremental_mode, flag_expected):
    mgr_cluster = ManagerCluster.__new__(ManagerCluster)
    mgr_cluster.id = "cluster-id"
    mgr_cluster.manager_node = MagicMock()
    mgr_cluster.sctool = MagicMock()
    mgr_cluster.sctool.run.return_value = MagicMock(stdout="repair/task-id", stderr="")

    with patch("sdcm.mgmt.cli.DbNodeLogger"), patch("sdcm.mgmt.cli.RepairTask"):
        mgr_cluster.create_repair_task(incremental_mode=incremental_mode)

    cmd = mgr_cluster.sctool.run.call_args.kwargs["cmd"]
    assert ("--incremental-mode full" in cmd) == flag_expected


def test_run_repair_manager_stops_task_on_timeout(runner_with_timed_out_mgr_repair):
    """A repair task still running after the timeout must be stopped, or it keeps running for the rest
    of the test and blocks later repairs and load-and-stream.
    """
    runner, mgr_task = runner_with_timed_out_mgr_repair

    with pytest.raises(WaitForTimeoutError, match="still RUNNING"):
        NemesisRunner.run_repair_manager.__wrapped__(runner)

    mgr_task.stop.assert_called_once_with()


def test_run_repair_manager_keeps_timeout_error_when_stop_fails(runner_with_timed_out_mgr_repair):
    """A failed stop must not hide the timeout, which is the actual failure of the nemesis."""
    runner, mgr_task = runner_with_timed_out_mgr_repair
    mgr_task.stop.side_effect = ScyllaManagerError("stop failed")

    with pytest.raises(WaitForTimeoutError, match="still RUNNING"):
        NemesisRunner.run_repair_manager.__wrapped__(runner)

    mgr_task.stop.assert_called_once_with()

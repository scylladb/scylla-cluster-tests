from unittest.mock import MagicMock, patch

import pytest

from sdcm.mgmt.cli import ManagerCluster
from sdcm.nemesis import NemesisRunner


@pytest.fixture
def nodetool_runner():
    runner = MagicMock()
    node = MagicMock()
    with (
        patch("sdcm.nemesis.adaptive_timeout"),
        patch("sdcm.nemesis.temporarily_disable_auto_repair"),
        patch("sdcm.nemesis.is_tablets_feature_enabled", return_value=True),
    ):
        yield runner, node


@pytest.mark.parametrize(
    "incremental_mode,expected_args",
    [
        ("full", "--incremental-mode full"),
        (None, ""),
    ],
)
def test_run_repair_nodetool_incremental_mode(nodetool_runner, incremental_mode, expected_args):
    runner, node = nodetool_runner

    NemesisRunner.run_repair_nodetool.__wrapped__(runner, nodes=[node], incremental_mode=incremental_mode)

    calls = {c.kwargs["sub_cmd"]: c.kwargs for c in node.run_nodetool.call_args_list}
    assert calls["cluster repair"]["args"] == expected_args
    assert "args" not in calls["repair"]


@pytest.mark.parametrize("incremental_mode,flag_expected", [("full", True), (None, False)])
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

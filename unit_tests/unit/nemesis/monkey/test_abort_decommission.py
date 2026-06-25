"""Tests for sdcm.nemesis.monkey.abort_decommission module."""

from unittest.mock import MagicMock, patch

import pytest
from invoke import Result, UnexpectedExit

from sdcm.cluster import NodeCleanedAfterDecommissionAborted, NodeStayInClusterAfterDecommission
from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.abort_decommission import AbortDecommissionMonkey
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit as Libssh2UnexpectedExit
from sdcm.remote.libssh2_client.result import Result as Libssh2Result

_MODULE = "sdcm.nemesis.monkey.abort_decommission"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_invoke_exit(stdout="", stderr=""):
    """Return an invoke.UnexpectedExit with .stdout / .stderr set directly."""
    r = Result(stdout=stdout, stderr=stderr, exited=1, command="nodetool", env={}, hide=())
    exc = UnexpectedExit(r)
    exc.stdout = stdout
    exc.stderr = stderr
    return exc


def _make_libssh2_exit(stdout="", stderr=""):
    """Return a Libssh2UnexpectedExit with .stdout / .stderr set directly."""
    r = Libssh2Result(stdout=stdout, stderr=stderr, exited=1)
    exc = Libssh2UnexpectedExit(r)
    exc.stdout = stdout
    exc.stderr = stderr
    return exc


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with abort-decommission specifics.

    ``base_runner`` already provides a two-node rack and deterministic random;
    this fixture adds host_id, system-log following, monitoring_set, and the
    ``add_new_nodes`` helper needed by AbortDecommissionMonkey.
    """
    base_runner.target_node.host_id = "aaaa-bbbb-cccc"
    base_runner.target_node.follow_system_log.return_value = iter(["matching log line"])

    # monitoring_set is required for reconfigure_scylla_monitoring
    base_runner.monitoring_set = MagicMock()

    # add_new_nodes returns a fresh mock node by default
    new_node = MagicMock()
    new_node.is_seed = False
    base_runner.add_new_nodes = MagicMock(return_value=[new_node])

    return base_runner


@pytest.fixture(autouse=True)
def mock_tablets():
    """Patch is_tablets_feature_enabled to return True for all tests."""
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=True):
        yield


@pytest.fixture(autouse=True)
def sequential_parallel_object():
    """Replace ParallelObject.call_objects with deterministic sequential execution.

    Runs the two callables in-process and returns successfully. Individual
    tests patch this fixture when they need an exceptional ParallelObject path.
    """

    def _call_objects(self_po):
        for fn in self_po.objects:
            fn()
        return []

    with patch(f"{_MODULE}.ParallelObject.call_objects", _call_objects):
        yield


@pytest.fixture(autouse=True)
def mock_nodetool_ops():
    """Patch wait_for_tasks and wait_for for all tests.

    wait_for_tasks returns a single task entry with a fixed task ID.
    wait_for calls the predicate immediately and returns.

    Yields the wait_for_tasks mock so tests that need to assert on it can
    receive this fixture as a named parameter.
    """
    with (
        patch(f"{_MODULE}.wait_for_tasks", return_value=[{"task_id": "task-id"}]) as mock_tasks,
        patch(f"{_MODULE}.wait_for", side_effect=lambda func, **kw: func()) as mock_wait_for,
    ):
        mock_tasks.wait_for = mock_wait_for
        yield mock_tasks


# ---------------------------------------------------------------------------
# Tests for disrupt() — successful abort
# ---------------------------------------------------------------------------


def test_disrupt_abort_succeeds(runner):
    """When verify shows the node stayed in the ring, wait for recovery."""
    runner.cluster.verify_decommission.side_effect = NodeStayInClusterAfterDecommission("still in ring")

    AbortDecommissionMonkey(runner).disrupt()

    runner.cluster.verify_decommission.assert_called_once_with(runner.target_node)
    runner.cluster.wait_for_nodes_up_and_normal.assert_called_once_with(nodes=[runner.target_node], timeout=300)
    runner.add_new_nodes.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for disrupt() — ParallelObject raises → exception branch
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "verify_side_effect, expect_replacement",
    [
        (None, True),
        (NodeStayInClusterAfterDecommission("still in ring"), False),
    ],
    ids=["decommission_completed", "abort_succeeded"],
)
def test_disrupt_parallel_object_exception(runner, verify_side_effect, expect_replacement):
    """When ParallelObject.call_objects raises, disrupt() still calls verify_decommission()
    and takes either the replacement or recovery path depending on the outcome."""
    with patch(f"{_MODULE}.ParallelObject.call_objects", side_effect=RuntimeError("simulated failure")):
        if verify_side_effect is None:
            runner.cluster.verify_decommission.return_value = None
        else:
            runner.cluster.verify_decommission.side_effect = verify_side_effect

        AbortDecommissionMonkey(runner).disrupt()

    runner.cluster.verify_decommission.assert_called_once_with(runner.target_node)

    if expect_replacement:
        runner.add_new_nodes.assert_called_once_with(count=1, rack=runner.target_node.rack)
        runner.monitoring_set.reconfigure_scylla_monitoring.assert_called_once()
    else:
        runner.cluster.wait_for_nodes_up_and_normal.assert_called_once_with(nodes=[runner.target_node], timeout=300)
        runner.add_new_nodes.assert_not_called()


# ---------------------------------------------------------------------------
# Tests for disrupt() — decommission completes → replacement node added
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "side_effect",
    [
        None,
        NodeCleanedAfterDecommissionAborted("partial"),
    ],
    ids=["full_decommission", "partial_decommission"],
)
def test_disrupt_adds_replacement(runner, side_effect):
    """When decommission completes (fully or partially despite abort), a replacement is added.

    Full decommission: verify_decommission returns normally.
    Partial decommission: verify_decommission raises NodeCleanedAfterDecommissionAborted.
    In both cases the node is already terminated internally by verify_decommission.
    """
    runner.cluster.verify_decommission.return_value = None
    runner.cluster.verify_decommission.side_effect = side_effect

    AbortDecommissionMonkey(runner).disrupt()

    runner.cluster.verify_decommission.assert_called_once_with(runner.target_node)
    runner.add_new_nodes.assert_called_once_with(count=1, rack=runner.target_node.rack)
    runner.monitoring_set.reconfigure_scylla_monitoring.assert_called_once()
    runner.cluster.wait_for_nodes_up_and_normal.assert_not_called()
    # seed status matches (both False) so set_seed_flag must not be touched
    runner.add_new_nodes.return_value[0].set_seed_flag.assert_not_called()


def test_disrupt_node_decommissioned_sets_seed_flag_when_target_was_seed(runner):
    """When the decommissioned node was a seed, the replacement inherits the seed flag."""
    runner.target_node.is_seed = True
    new_node = MagicMock()
    new_node.is_seed = False  # replacement is not yet a seed
    runner.add_new_nodes.return_value = [new_node]
    runner.cluster.verify_decommission.return_value = None

    AbortDecommissionMonkey(runner).disrupt()

    new_node.set_seed_flag.assert_called_once_with(True)
    runner.cluster.update_seed_provider.assert_called_once()


# ---------------------------------------------------------------------------
# Tests for disrupt() — guard checks
# ---------------------------------------------------------------------------


def test_disrupt_raises_unsupported_when_single_node_in_rack(runner):
    """UnsupportedNemesis must be raised when the target is the only node in its rack."""
    runner.cluster.data_nodes = [runner.target_node]

    with pytest.raises(UnsupportedNemesis, match="only one in rack"):
        AbortDecommissionMonkey(runner).disrupt()


def test_precheck_skips_when_tablets_not_enabled(runner):
    """precheck() skips when tablets feature is not enabled."""
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=False):
        assert AbortDecommissionMonkey(runner).precheck(node=runner.cluster.data_nodes[0]) == (
            "Aborting decommission is only supported with tablets."
        )


def test_precheck_keeps_when_tablets_enabled(runner):
    """precheck() keeps the nemesis when tablets feature is enabled."""
    with patch(f"{_MODULE}.is_tablets_feature_enabled", return_value=True) as mock_tablets_enabled:
        assert AbortDecommissionMonkey(runner).precheck(node=runner.cluster.data_nodes[0]) is None

    mock_tablets_enabled.assert_called_once_with(runner.cluster.data_nodes[0])


def test_disrupt_raises_unsupported_when_no_tablet_tables(runner):
    """UnsupportedNemesis raised when no test tables with tablets are found."""
    runner.cluster.get_non_system_ks_cf_with_tablets_list.return_value = []

    with pytest.raises(UnsupportedNemesis, match="No test tables with tablets found"):
        AbortDecommissionMonkey(runner).disrupt()

    runner.cluster.get_non_system_ks_cf_with_tablets_list.assert_called_once_with(db_node=runner.target_node)


# ---------------------------------------------------------------------------
# Tests for decommission_target_node()
# ---------------------------------------------------------------------------


def test_decommission_target_node_aborted_expected(runner):
    """Expected 'aborted on user request' exit is caught and logged."""
    abort_msg = "Decommission failed. See earlier errors (aborted on user request)"
    runner.target_node.run_nodetool.side_effect = _make_invoke_exit(stdout=abort_msg)

    AbortDecommissionMonkey(runner).decommission_target_node()

    runner.actions_log.info.assert_called_once_with("Decommission was aborted as expected")


def test_decommission_target_node_unexpected_exit_reraises(runner):
    """UnexpectedExit with an unrecognised message is re-raised."""
    runner.target_node.run_nodetool.side_effect = _make_invoke_exit(stdout="some other unexpected failure")

    with pytest.raises(UnexpectedExit):
        AbortDecommissionMonkey(runner).decommission_target_node()


def test_decommission_target_node_libssh2_aborted_expected(runner):
    """Expected 'aborted on user request' via Libssh2UnexpectedExit is caught and logged."""
    abort_msg = "Decommission failed. See earlier errors (aborted on user request)"
    runner.target_node.run_nodetool.side_effect = _make_libssh2_exit(stdout=abort_msg)

    AbortDecommissionMonkey(runner).decommission_target_node()

    runner.actions_log.info.assert_called_once_with("Decommission was aborted as expected")


def test_decommission_target_node_libssh2_unexpected_exit_reraises(runner):
    """Libssh2UnexpectedExit with an unrecognised message is re-raised."""
    runner.target_node.run_nodetool.side_effect = _make_libssh2_exit(stdout="some other unexpected failure")

    with pytest.raises(Libssh2UnexpectedExit):
        AbortDecommissionMonkey(runner).decommission_target_node()


# ---------------------------------------------------------------------------
# Tests for abort_decommission_task()
# ---------------------------------------------------------------------------


def test_abort_decommission_task_calls_nodetool_in_order(runner, mock_nodetool_ops):
    """abort_decommission_task must abort the task then wait for it to finish."""
    task_id = "dead-beef-1234"
    mock_nodetool_ops.return_value = [{"task_id": task_id}]
    runner.target_node.follow_system_log.return_value = iter(["Finished sending sstable_nr 1"])

    AbortDecommissionMonkey(runner).abort_decommission_task()

    mock_nodetool_ops.assert_called_once_with(
        runner.target_node,
        module="node_ops",
        timeout=60,
        filter={"entity": runner.target_node.host_id, "type": "decommission"},
    )
    runner.target_node.follow_system_log.assert_called_once_with(
        patterns=[r"stream_blob - stream_sstables\[[0-9a-f-]+\] Finished sending"]
    )
    mock_nodetool_ops.wait_for.assert_called_once()
    assert mock_nodetool_ops.wait_for.call_args.kwargs["timeout"] == 600

    # Each run_nodetool call that uses a positional first arg exposes it via c.args[0]
    subcmds = [c.args[0] for c in runner.target_node.run_nodetool.call_args_list if c.args]

    assert f"tasks abort {task_id}" in subcmds, "nodetool 'tasks abort' was never called"
    assert f"tasks wait {task_id}" in subcmds, "nodetool 'tasks wait' was never called"
    assert subcmds.index(f"tasks abort {task_id}") < subcmds.index(f"tasks wait {task_id}"), (
        "'tasks abort' must be called before 'tasks wait'"
    )

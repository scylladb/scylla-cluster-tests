"""Tests for sdcm.nemesis.monkey.abort_decommission module."""

from unittest.mock import MagicMock, patch

import pytest
from invoke import Result, UnexpectedExit

from sdcm.cluster import NodeCleanedAfterDecommissionAborted, NodeStayInClusterAfterDecommission
from sdcm.exceptions import UnsupportedNemesis
from sdcm.nemesis.monkey.abort_decommission import AbortDecommissionMonkey
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit as Libssh2UnexpectedExit
from sdcm.remote.libssh2_client.result import Result as Libssh2Result
from unit_tests.unit.nemesis import TestRunner

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
def runner():
    """TestRunner pre-configured for AbortDecommissionMonkey tests.

    Two data nodes are placed in ``rack1`` so the single-node-in-rack guard
    inside ``disrupt()`` never fires by default.
    """
    base = TestRunner()

    # target_node details
    base.target_node.name = "node1"
    base.target_node.rack = "rack1"
    base.target_node.is_seed = False
    base.target_node.host_id = "aaaa-bbbb-cccc"
    base.target_node.follow_system_log.return_value = iter(["matching log line"])

    # A second node in the same rack so the rack has > 1 node
    other_node = MagicMock()
    other_node.rack = "rack1"

    base.cluster.data_nodes = [base.target_node, other_node]

    # monitoring_set is required for reconfigure_scylla_monitoring
    base.monitoring_set = MagicMock()

    # add_new_nodes returns a fresh mock node by default
    new_node = MagicMock()
    new_node.is_seed = False
    base.add_new_nodes = MagicMock(return_value=[new_node])

    return base


@pytest.fixture(autouse=True)
def sequential_parallel_object():
    """Replace ParallelObject.call_objects with sequential execution for all tests.

    Ensures the two callables passed to ParallelObject run deterministically
    in-process so tests can assert on their side-effects without real threads.
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
        patch(f"{_MODULE}.wait_for", side_effect=lambda func, **kw: func()),
    ):
        yield mock_tasks


# ---------------------------------------------------------------------------
# Tests for disrupt() — abort succeeds (node stays in cluster)
# ---------------------------------------------------------------------------


def test_disrupt_abort_succeeds(runner):
    """When the abort works the node stays in the ring.

    verify_decommission raises NodeStayInClusterAfterDecommission, so
    wait_for_nodes_up_and_normal must be called with timeout=300 and
    add_new_nodes must NOT be called.
    """
    runner.cluster.verify_decommission.side_effect = NodeStayInClusterAfterDecommission("still in ring")

    AbortDecommissionMonkey(runner).disrupt()

    runner.cluster.verify_decommission.assert_called_once_with(runner.target_node)
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
# Tests for disrupt() — single node in rack guard
# ---------------------------------------------------------------------------


def test_disrupt_raises_unsupported_when_single_node_in_rack(runner):
    """UnsupportedNemesis must be raised when the target is the only node in its rack."""
    runner.cluster.data_nodes = [runner.target_node]

    with pytest.raises(UnsupportedNemesis, match="only one in rack"):
        AbortDecommissionMonkey(runner).disrupt()


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

    # Each run_nodetool call that uses a positional first arg exposes it via c.args[0]
    subcmds = [c.args[0] for c in runner.target_node.run_nodetool.call_args_list if c.args]

    assert f"tasks abort {task_id}" in subcmds, "nodetool 'tasks abort' was never called"
    assert f"tasks wait {task_id}" in subcmds, "nodetool 'tasks wait' was never called"
    assert subcmds.index(f"tasks abort {task_id}") < subcmds.index(f"tasks wait {task_id}"), (
        "'tasks abort' must be called before 'tasks wait'"
    )

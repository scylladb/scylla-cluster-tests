from contextlib import nullcontext
from unittest.mock import MagicMock, patch

import pytest

from sdcm.nemesis import NemesisRunner
from sdcm.remote.libssh2_client.exceptions import CommandTimedOut
from sdcm.remote.libssh2_client.result import Result
from sdcm.utils.parallel_object import ParallelObjectException
from unit_tests.unit.nemesis import make_mock_node

ADAPTIVE_TIMEOUT = 7200


def cleanup_timed_out() -> CommandTimedOut:
    """Build the error 'nodetool cleanup' raises when it hits its timeout."""
    return CommandTimedOut(result=Result(command="nodetool cleanup", stdout="", stderr="", exited=-1), timeout=1)


def nodetool_with_timed_out_cleanup(sub_cmd, **_):
    """Fake 'run_nodetool': cleanup times out, other commands succeed."""
    if sub_cmd == "cleanup":
        raise cleanup_timed_out()


def nodetool_with_timed_out_cleanup_and_failed_stop(sub_cmd, **_):
    """Fake 'run_nodetool': cleanup times out and 'stop CLEANUP' fails."""
    if sub_cmd == "cleanup":
        raise cleanup_timed_out()
    raise RuntimeError("stop failed")


def nodetool_commands(node) -> list[str]:
    """List the nodetool sub-commands run on a mock node, in order."""
    return [call.kwargs.get("sub_cmd") or call.args[0] for call in node.run_nodetool.call_args_list]


@pytest.fixture
def cleanup_runner():
    """Nemesis runner over two nodes, whose cleanup adaptive timeout is ADAPTIVE_TIMEOUT.

    Yields the runner, a node whose cleanup times out, and a node whose cleanup finishes.
    """
    timed_out_node = make_mock_node(name="node1")
    timed_out_node.run_nodetool.side_effect = nodetool_with_timed_out_cleanup
    done_node = make_mock_node(name="node2")
    runner = MagicMock()
    runner.cluster.nodes = [timed_out_node, done_node]
    with (
        patch("sdcm.nemesis.adaptive_timeout", return_value=nullcontext(ADAPTIVE_TIMEOUT)),
        patch("sdcm.nemesis.InfoEvent"),
    ):
        yield runner, timed_out_node, done_node


def test_nodetool_cleanup_on_all_nodes_parallel_bounds_cleanup_by_adaptive_timeout(cleanup_runner):
    """Each cleanup runs with the adaptive timeout, so it ends on its own instead of being abandoned."""
    runner, timed_out_node, done_node = cleanup_runner

    with pytest.raises(ParallelObjectException):
        NemesisRunner.nodetool_cleanup_on_all_nodes_parallel(runner)

    for node in (timed_out_node, done_node):
        assert node.run_nodetool.call_args_list[0].kwargs["timeout"] == ADAPTIVE_TIMEOUT


def test_nodetool_cleanup_on_all_nodes_parallel_stops_timed_out_cleanup(cleanup_runner):
    """A timed out cleanup is stopped, or it keeps running on the node into the next nemesis."""
    runner, timed_out_node, done_node = cleanup_runner

    with pytest.raises(ParallelObjectException) as exc_info:
        NemesisRunner.nodetool_cleanup_on_all_nodes_parallel(runner)

    assert nodetool_commands(timed_out_node) == ["cleanup", "stop CLEANUP"]
    assert nodetool_commands(done_node) == ["cleanup"]
    assert [type(result.exc) for result in exc_info.value.results if result.exc] == [CommandTimedOut]


def test_nodetool_cleanup_on_all_nodes_parallel_keeps_timeout_error_when_stop_fails(cleanup_runner):
    """A failed stop must not hide the timeout, which is the actual failure of the nemesis."""
    runner, timed_out_node, _ = cleanup_runner
    timed_out_node.run_nodetool.side_effect = nodetool_with_timed_out_cleanup_and_failed_stop

    with pytest.raises(ParallelObjectException) as exc_info:
        NemesisRunner.nodetool_cleanup_on_all_nodes_parallel(runner)

    assert nodetool_commands(timed_out_node) == ["cleanup", "stop CLEANUP"]
    assert [type(result.exc) for result in exc_info.value.results if result.exc] == [CommandTimedOut]

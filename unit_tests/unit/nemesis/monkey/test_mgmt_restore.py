"""Tests for the MgmtRestore cleanup guard in sdcm.nemesis.

``NemesisRunner._stop_unfinished_restore_task`` stops a Manager restore task that
is still in progress, so that the keyspace drop which follows does not race an
in-flight load&stream (SCT-880).

The drop itself is unconditional -- the next nemesis must start from the same
cluster state as before this one ran -- so the guard is best effort: it must
stop what it can and never raise, since it runs inside a ``finally`` block that
must not mask the original failure.
"""

from unittest.mock import MagicMock, PropertyMock

import pytest

from sdcm.exceptions import WaitForTimeoutError
from sdcm.mgmt.common import TaskStatus
from sdcm.nemesis import NemesisRunner

# Statuses the guard accepts as finished. 'ERROR' is absent on purpose: the Manager reports
# 'ERROR (#/4)' while retries are still pending and only the last attempt maps to ERROR_FINAL.
FINAL_STATUSES = [TaskStatus.DONE, TaskStatus.ERROR_FINAL, TaskStatus.STOPPED, TaskStatus.ABORTED]


# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def stop_unfinished_restore_task(runner, restore_task):
    """Call the method under test with the mock runner bound as ``self``.

    Args:
        runner: A ``TestRunner`` standing in for ``NemesisRunner``.
        restore_task: The Manager restore task, or ``None``.
    """
    return NemesisRunner._stop_unfinished_restore_task(runner, restore_task)


@pytest.fixture()
def restore_task():
    """A Manager restore task whose status each test sets explicitly."""
    task = MagicMock()
    task.id = "restore/01d56d06-0389-4418-af78-73637e66ab03"
    return task


# ---------------------------------------------------------------------------
# Tasks that need no stopping
# ---------------------------------------------------------------------------


def test_absent_task_is_a_no_op(base_runner):
    """create_restore_task itself failed, so there is no task to stop."""
    stop_unfinished_restore_task(base_runner, None)


@pytest.mark.parametrize(
    "status",
    [
        pytest.param(TaskStatus.DONE, id="done"),
        pytest.param(TaskStatus.ERROR_FINAL, id="error-final"),
        pytest.param(TaskStatus.STOPPED, id="stopped"),
        pytest.param(TaskStatus.ABORTED, id="aborted"),
    ],
)
def test_finished_task_is_not_stopped(base_runner, restore_task, status):
    """A task already in a final status is left alone."""
    restore_task.status = status

    stop_unfinished_restore_task(base_runner, restore_task)

    restore_task.stop.assert_not_called()
    restore_task.wait_for_status.assert_not_called()


# ---------------------------------------------------------------------------
# Tasks that must be stopped before the keyspace is dropped
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "status",
    [
        pytest.param(TaskStatus.RUNNING, id="running"),
        pytest.param(TaskStatus.STARTING, id="starting"),
        pytest.param(TaskStatus.NEW, id="new"),
        # 'ERROR' means 'ERROR (#/4)' with retries still pending, so the Manager can come back
        # and restore into the keyspace: it must be stopped like any other unfinished task.
        pytest.param(TaskStatus.ERROR, id="error-retryable"),
    ],
)
def test_unfinished_task_is_stopped(base_runner, restore_task, status):
    """An unfinished task is stopped and its termination confirmed against the strict status list."""
    restore_task.status = status

    stop_unfinished_restore_task(base_runner, restore_task)

    restore_task.stop.assert_called_once()
    restore_task.wait_for_status.assert_called_once()
    assert restore_task.wait_for_status.call_args.kwargs["list_status"] == FINAL_STATUSES


def test_slow_stop_falls_through_to_longer_wait(base_runner, restore_task):
    """stop() waits only 30s, so a slow load&stream abort is awaited instead of giving up."""
    restore_task.status = TaskStatus.RUNNING
    restore_task.stop.side_effect = WaitForTimeoutError("task did not stop within 30s")

    stop_unfinished_restore_task(base_runner, restore_task)

    restore_task.wait_for_status.assert_called_once()


# ---------------------------------------------------------------------------
# Failures must never escape into the finally block
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failing_call",
    [
        pytest.param("status", id="status-lookup-fails"),
        pytest.param("stop", id="stop-command-fails"),
        pytest.param("wait_for_status", id="task-never-terminates"),
    ],
)
def test_sctool_failure_is_swallowed(base_runner, restore_task, failing_call):
    """A failure to stop must not propagate: it would mask the error that brought us to cleanup."""
    error = RuntimeError("sctool is unreachable")
    if failing_call == "status":
        type(restore_task).status = PropertyMock(side_effect=error)
    else:
        restore_task.status = TaskStatus.RUNNING
        getattr(restore_task, failing_call).side_effect = error

    stop_unfinished_restore_task(base_runner, restore_task)

    base_runner.log.warning.assert_called()

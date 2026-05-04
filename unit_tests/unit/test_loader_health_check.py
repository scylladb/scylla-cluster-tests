# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Unit tests for the BaseLoaderSet SSH health-check mechanism."""

import threading
from unittest.mock import MagicMock, patch

from invoke import Result

from sdcm.cluster import BaseLoaderSet, LOADER_HEALTH_CHECK_RETRIES, LOADER_HEALTH_CHECK_TIMEOUT
from sdcm.sct_events import Severity
from sdcm.sct_events.loaders import LoaderNodeDownEvent


# ---------------------------------------------------------------------------
# Minimal fakes
# ---------------------------------------------------------------------------


class FakeRemoterOK:
    """Remoter that always succeeds; records each call's kwargs."""

    def __init__(self):
        self.calls = []

    def run(self, cmd, **kwargs):
        self.calls.append({"cmd": cmd, **kwargs})
        return Result(stdout="", stderr="", exited=0)


class FakeRemoterFail:
    """Remoter that always raises ConnectionError."""

    def run(self, cmd, **kwargs):
        raise ConnectionError("SSH connection refused")


class FakeRemoterFlaky:
    """Remoter that follows a scripted sequence of success (True) / failure (False)."""

    def __init__(self, sequence: list[bool]):
        self._sequence = iter(sequence)

    def run(self, cmd, **kwargs):
        ok = next(self._sequence, True)  # default to success once exhausted
        if not ok:
            raise ConnectionError("temporary failure")
        return Result(stdout="", stderr="", exited=0)


class FakeNode:
    def __init__(self, name, remoter):
        self.name = name
        self.remoter = remoter


class FakeStopEvent:
    """Replacement for threading.Event that returns False from wait() for the
    first `iterations` calls and True thereafter, making the loop run exactly
    `iterations` outer cycles deterministically.

    is_set() mirrors the current state so that the per-node stop check also
    works correctly.
    """

    def __init__(self, iterations: int):
        self._remaining = iterations
        self._stopped = False

    def wait(self, timeout=None):  # noqa: ARG002
        if self._remaining > 0:
            self._remaining -= 1
            return False  # keep looping
        self._stopped = True
        return True  # stop looping

    def is_set(self) -> bool:
        return self._stopped

    def set(self):
        self._stopped = True
        self._remaining = 0

    def clear(self):
        self._stopped = False


class FakeLoaderSet(BaseLoaderSet):
    """Minimal concrete subclass of BaseLoaderSet for unit tests."""

    def __init__(self, nodes):
        super().__init__(params={})
        self.nodes = nodes
        self.log = MagicMock()
        self.tags = {}


# ---------------------------------------------------------------------------
# Helper: run the loop with a scripted stop event
# ---------------------------------------------------------------------------


def _run_loop(loader_set, iterations, suppress_publish=True):
    """Drive _loader_health_check_loop() for exactly `iterations` outer cycles.

    Returns a list of published LoaderNodeDownEvent instances (or an empty
    list when suppress_publish=True and no events are expected).
    """
    fake_event = FakeStopEvent(iterations)
    loader_set._loader_health_check_stop_event = fake_event

    published = []

    def record(self_event):
        published.append(self_event)

    if suppress_publish:
        ctx = patch.object(LoaderNodeDownEvent, "publish", record)
    else:
        ctx = patch.object(LoaderNodeDownEvent, "publish", record)

    with ctx:
        loader_set._loader_health_check_loop()

    return published


# ---------------------------------------------------------------------------
# Event class tests
# ---------------------------------------------------------------------------


def test_loader_down_event_has_critical_severity():
    """LoaderNodeDownEvent must default to CRITICAL to trigger EventsAnalyzer.kill_test."""
    with patch.object(LoaderNodeDownEvent, "publish", MagicMock()):
        event = LoaderNodeDownEvent(node="loader-1", message="test message")
    assert event.severity == Severity.CRITICAL


def test_loader_down_event_msgfmt_contains_node_and_message():
    """The formatted message must expose both node name and free-text message."""
    with patch.object(LoaderNodeDownEvent, "publish", MagicMock()):
        event = LoaderNodeDownEvent(node="loader-42", message="gone away")
    formatted = str(event)
    assert "loader-42" in formatted
    assert "gone away" in formatted


# ---------------------------------------------------------------------------
# Thread lifecycle tests (exercise start/stop wrappers, not the loop)
# ---------------------------------------------------------------------------


def test_start_health_check_spawns_daemon_thread():
    """start_loader_health_check should spawn exactly one alive daemon thread."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("l-1", FakeRemoterOK())])
    try:
        loader_set.start_loader_health_check()
        thread = loader_set._loader_health_check_thread
        assert thread is not None
        assert thread.is_alive()
        assert thread.daemon is True
    finally:
        loader_set.stop_loader_health_check()


def test_start_health_check_is_idempotent():
    """Calling start_loader_health_check twice must not spawn a second thread."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("l-1", FakeRemoterOK())])
    try:
        loader_set.start_loader_health_check()
        first_thread = loader_set._loader_health_check_thread
        loader_set.start_loader_health_check()
        second_thread = loader_set._loader_health_check_thread
        assert first_thread is second_thread
    finally:
        loader_set.stop_loader_health_check()


def test_stop_health_check_terminates_thread():
    """stop_loader_health_check must join the thread and clear the reference."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("l-1", FakeRemoterOK())])
    loader_set.start_loader_health_check()
    assert loader_set._loader_health_check_thread is not None
    loader_set.stop_loader_health_check()
    assert loader_set._loader_health_check_thread is None


def test_stop_health_check_preserves_thread_ref_on_join_timeout():
    """If the thread is still alive after join, the reference must be kept so
    that a second thread is not accidentally spawned."""
    loader_set = FakeLoaderSet(nodes=[])
    fake_thread = MagicMock(spec=threading.Thread)
    fake_thread.name = "LoaderHealthCheck"
    fake_thread.is_alive.return_value = True  # still alive after join

    loader_set._loader_health_check_thread = fake_thread

    loader_set.stop_loader_health_check()

    # Thread reference must NOT have been cleared
    assert loader_set._loader_health_check_thread is fake_thread
    # join must have been called with the correct timeout
    fake_thread.join.assert_called_once_with(timeout=LOADER_HEALTH_CHECK_TIMEOUT + 5)


# ---------------------------------------------------------------------------
# Loop behaviour tests (call the real _loader_health_check_loop)
# ---------------------------------------------------------------------------


def test_loop_pre_stopped_event_skips_all_nodes():
    """If the stop event is already set before the loop runs, no remoter call
    should be made (the while condition fires False immediately)."""
    remoter = FakeRemoterOK()
    loader_set = FakeLoaderSet(nodes=[FakeNode("l-1", remoter)])
    # 0 iterations → loop body never runs
    _run_loop(loader_set, iterations=0)
    assert remoter.calls == []


def test_loop_uses_correct_remoter_call_contract():
    """The health-check must call remoter.run with the contracted keyword args."""
    remoter = FakeRemoterOK()
    loader_set = FakeLoaderSet(nodes=[FakeNode("l-1", remoter)])
    _run_loop(loader_set, iterations=1)

    assert len(remoter.calls) == 1
    call = remoter.calls[0]
    assert call["cmd"] == "true"
    assert call["timeout"] == LOADER_HEALTH_CHECK_TIMEOUT
    assert call["ignore_status"] is False
    assert call["verbose"] is False
    assert call["retry"] == 0


def test_loop_no_event_on_healthy_node():
    """No LoaderNodeDownEvent must be published when SSH checks succeed."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("loader-alive", FakeRemoterOK())])
    published = _run_loop(loader_set, iterations=LOADER_HEALTH_CHECK_RETRIES + 1)
    assert published == []


def test_loop_no_event_before_retry_threshold():
    """LOADER_HEALTH_CHECK_RETRIES - 1 failures must NOT publish an event."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("loader-flaky", FakeRemoterFail())])
    published = _run_loop(loader_set, iterations=LOADER_HEALTH_CHECK_RETRIES - 1)
    assert published == []


def test_loop_event_published_at_retry_threshold():
    """Exactly LOADER_HEALTH_CHECK_RETRIES consecutive failures must publish one event."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("loader-dead", FakeRemoterFail())])
    published = _run_loop(loader_set, iterations=LOADER_HEALTH_CHECK_RETRIES)
    assert len(published) == 1
    event = published[0]
    assert event.severity == Severity.CRITICAL
    assert "loader-dead" in event.node
    assert "loader-dead" in event.message


def test_loop_event_published_only_once_per_node():
    """A down node must be reported exactly once even over many iterations."""
    loader_set = FakeLoaderSet(nodes=[FakeNode("loader-dead", FakeRemoterFail())])
    published = _run_loop(loader_set, iterations=LOADER_HEALTH_CHECK_RETRIES * 5)
    assert len(published) == 1


def test_loop_failure_counter_resets_on_success():
    """A node that fails < threshold times, then succeeds, then fails again
    needs a fresh full retry streak before triggering an event."""
    # Fail RETRIES-1 times, succeed once, then fail RETRIES more times
    n = LOADER_HEALTH_CHECK_RETRIES
    sequence = [False] * (n - 1) + [True] + [False] * n
    node = FakeNode("loader-flaky", FakeRemoterFlaky(sequence))
    loader_set = FakeLoaderSet(nodes=[node])

    published = _run_loop(loader_set, iterations=len(sequence))

    # Exactly one event after the second complete failure streak
    assert len(published) == 1


def test_loop_only_dead_node_reported_in_mixed_set():
    """Only the failing loader must produce an event; the healthy one must not."""
    nodes = [
        FakeNode("loader-alive", FakeRemoterOK()),
        FakeNode("loader-dead", FakeRemoterFail()),
    ]
    loader_set = FakeLoaderSet(nodes=nodes)
    published = _run_loop(loader_set, iterations=LOADER_HEALTH_CHECK_RETRIES + 1)

    assert len(published) == 1
    assert published[0].node == "loader-dead"


def test_loop_stop_event_mid_sweep_skips_remaining_nodes():
    """After the stop event is set during node processing, remaining nodes in
    the same sweep must not be checked."""

    checked_names = []

    class StopAfterFirstRemoter:
        def run(self, cmd, **kwargs):
            checked_names.append("first")
            # Flip the stop event so the second node should be skipped
            loader_set._loader_health_check_stop_event.set()
            return Result(stdout="", stderr="", exited=0)

    class NeverCalledRemoter:
        def run(self, cmd, **kwargs):
            checked_names.append("second")
            return Result(stdout="", stderr="", exited=0)

    nodes = [
        FakeNode("first", StopAfterFirstRemoter()),
        FakeNode("second", NeverCalledRemoter()),
    ]
    loader_set = FakeLoaderSet(nodes=nodes)
    _run_loop(loader_set, iterations=1)

    # Only the first node's remoter should have been reached
    assert checked_names == ["first"]

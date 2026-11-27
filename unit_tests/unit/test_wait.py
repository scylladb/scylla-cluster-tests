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
# Copyright (c) 2023 ScyllaDB


from __future__ import absolute_import

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from sdcm.cluster import BaseNode
from sdcm.wait import ExitByEventError, WaitForTimeoutError, wait_for, wait_for_log_lines

logging.basicConfig(level=logging.DEBUG)


class FakeClock:
    """Clock that advances only when tenacity sleeps, making retry timing deterministic."""

    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        """Return the fake current time."""
        return self.now

    def sleep(self, seconds):
        """Advance the fake time by the requested seconds instead of sleeping."""
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch):
    """Patch tenacity's clock and sleep with a FakeClock and return that clock."""
    clock = FakeClock()
    monkeypatch.setattr("tenacity.time", clock)
    monkeypatch.setattr("tenacity.nap.time", clock)
    return clock


def test_wait_simple(fake_clock):
    """A callback that always raises is called once per step until the timeout, and wait_for returns quietly."""
    calls = []

    def callback(arg1, arg2):
        calls.append((arg1, arg2))
        raise Exception("error")

    wait_for(callback, timeout=2, step=1, arg1=1, arg2=3, throw_exc=False)
    assert len(calls) == 3


def test_wait_throw_exc(fake_clock):
    """With throw_exc=True, the callback's last exception is raised once the timeout expires."""
    calls = []

    def callback(arg1, arg2):
        calls.append((arg1, arg2))
        raise Exception("error")

    with pytest.raises(Exception, match=r"error"):
        wait_for(callback, throw_exc=True, timeout=4, step=1, arg1=1, arg2=3)
    assert len(calls) == 5


def test_wait_false_return(fake_clock):
    """A callback that keeps returning False is retried once per step until the timeout."""
    calls = []

    def callback(arg1, arg2):
        calls.append((arg1, arg2))
        return False

    wait_for(callback, timeout=2, step=1, arg1=1, arg2=3, throw_exc=False)
    assert len(calls) == 3


def test_wait_false_return_reraise(fake_clock):
    """With throw_exc=True, a callback that keeps returning False ends in a timeout error."""
    calls = []

    def callback(arg1, arg2):
        calls.append((arg1, arg2))
        return False

    with pytest.raises(Exception, match="callback: timeout - 4 seconds - expired"):
        wait_for(callback, timeout=4, throw_exc=True, step=1, arg1=1, arg2=3)
    assert len(calls) == 5


def test_wait_return_value():
    """The first truthy callback result ends the wait and is returned."""
    calls = []

    def callback(arg1, arg2):
        calls.append((arg1, arg2))
        return "what ever"

    assert wait_for(callback, timeout=0.2, step=0.1, arg1=1, arg2=3, throw_exc=False) == "what ever"
    assert len(calls) == 1


@pytest.fixture
def event_stop_state(fake_clock):
    """Callback that sets the stop event on call `set_event_after` and succeeds on call `return_true_after`."""
    state = SimpleNamespace(calls=[], return_true_after=0, set_event_after=0, ev=threading.Event())

    def callback(arg1, arg2):
        state.calls.append((arg1, arg2))
        if len(state.calls) == state.set_event_after:
            state.ev.set()
        if len(state.calls) == state.return_true_after:
            return "what ever"
        return False

    state.callback = callback
    return state


@pytest.mark.parametrize("throw_exc", [pytest.param(True, id="throw_exc"), pytest.param(False, id="no_throw_exc")])
def test_stop_by_event(event_stop_state, throw_exc):
    """Setting the stop event ends the wait after the current call: ExitByEventError or a falsy result."""
    state = event_stop_state
    state.set_event_after = 1
    state.return_true_after = 3
    if throw_exc:
        with pytest.raises(ExitByEventError, match="callback: stopped by Event"):
            wait_for(
                state.callback,
                timeout=6,
                throw_exc=throw_exc,
                stop_event=state.ev,
                step=1,
                arg1=1,
                arg2=3,
            )
    else:
        res = wait_for(state.callback, timeout=6, step=1, throw_exc=throw_exc, stop_event=state.ev, arg1=1, arg2=3)
        assert not res

    assert len(state.calls) == 1


def test_stop_by_event_in_main_thread():
    """A stop event set from the main thread ends a wait_for running in a worker thread with a falsy result."""
    stop_event = threading.Event()
    first_call_done = threading.Event()

    def callback():
        first_call_done.set()
        return False

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(wait_for, func=callback, timeout=30, step=0.1, throw_exc=False, stop_event=stop_event)
        assert first_call_done.wait(timeout=30)
        stop_event.set()
        assert not future.result(timeout=30)


def test_return_result_before_stop_event_and_wait_timeout(event_stop_state):
    """A truthy result arriving before both the stop event and the timeout is returned."""
    state = event_stop_state
    state.return_true_after = 2
    state.set_event_after = 4
    res = wait_for(state.callback, timeout=6, step=1, throw_exc=False, stop_event=state.ev, arg1=1, arg2=3)
    assert res == "what ever"
    assert len(state.calls) == 2


def test_raise_by_timeout_before_set_event(event_stop_state):
    """When the timeout expires before the stop event is set, WaitForTimeoutError is raised."""
    state = event_stop_state
    state.set_event_after = 8
    state.return_true_after = 9
    with pytest.raises(WaitForTimeoutError, match="callback: timeout - 6 seconds - expired"):
        wait_for(
            state.callback,
            timeout=6,
            throw_exc=True,
            stop_event=state.ev,
            step=1,
            arg1=1,
            arg2=3,
        )
    assert len(state.calls) == 7


@pytest.mark.parametrize("throw_exc", [pytest.param(True, id="throw_exc"), pytest.param(False, id="no_throw_exc")])
def test_raise_exception_in_func_before_set_event(event_stop_state, throw_exc):
    """A callback exception is retried like a failure until the stop event ends the wait."""
    state = event_stop_state
    state.set_event_after = 5

    def callback(arg1, arg2):
        if len(state.calls) == 2:
            state.calls.append((arg1, arg2))
            raise Exception("Raise before event")
        return state.callback(arg1, arg2)

    if throw_exc:
        with pytest.raises(ExitByEventError, match="callback: stopped by Event"):
            wait_for(
                callback,
                timeout=8,
                throw_exc=throw_exc,
                stop_event=state.ev,
                step=1,
                arg1=1,
                arg2=3,
            )
    else:
        res = wait_for(callback, timeout=8, throw_exc=throw_exc, stop_event=state.ev, step=1, arg1=1, arg2=3)
        assert not res
    assert len(state.calls) == 5


def test_set_event_timeout_at_same_time(event_stop_state):
    """If event was set at same time as timeout exceeds
    and throw_exc is true wait_for will raise Exception with
    message wait_for stopped by event.
    """
    state = event_stop_state
    state.set_event_after = 7
    state.return_true_after = 8
    with pytest.raises(ExitByEventError, match="callback: stopped by Event"):
        wait_for(
            state.callback,
            timeout=6,
            throw_exc=True,
            stop_event=state.ev,
            step=1,
            arg1=1,
            arg2=3,
        )
    assert len(state.calls) == 7


class DummyNode(BaseNode):
    """Minimal BaseNode subclass for log-following tests.

    Inherits from BaseNode because wait_for_log_lines calls
    node.follow_system_log which is implemented on BaseNode.
    """

    name = "node_1"
    system_log = ""

    def __init__(self, log_path):
        self.system_log = log_path


def write_to_file(filename, lines):
    """Append the lines to the file one by one, pausing briefly after each."""
    for line in lines:
        with open(filename, "a") as f:
            f.write(f"{line}\n")
        time.sleep(0.01)


def test_wait_for_log_start_line_and_end_line(tmp_path):
    """wait_for_log_lines returns once the start and then the end line patterns appear in the node log."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    lines = [
        " [shard  0] repair - rebuild_with_repair: started with keyspaces=drop_table_during_repair",
        "some line in between",
        "[shard  0] repair - rebuild_with_repair: finished with keyspaces=drop_table_during_repair",
    ]
    write_thread = threading.Thread(target=write_to_file, args=(file_path, lines))
    write_thread.daemon = True
    file_path.touch()
    with wait_for_log_lines(
        node=node,
        start_line_patterns=["rebuild.*started with keyspaces=", "Rebuild starts"],
        end_line_patterns=["rebuild.*finished with keyspaces=", "Rebuild succeeded"],
        start_timeout=3,
        end_timeout=5,
    ):
        write_thread.start()


def test_wait_for_log_timeout_when_no_start_line(tmp_path):
    """With no start line in the log, wait_for_log_lines raises TimeoutError for the start line."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    lines = ["end", "cde", "end"]  # no start line provided
    t = threading.Thread(target=write_to_file, args=(file_path, lines))
    t.daemon = True
    file_path.touch()
    with (
        pytest.raises(TimeoutError, match="Timeout occurred while waiting for start log line"),
        wait_for_log_lines(
            node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.4, end_timeout=1.2
        ),
    ):
        t.start()


def test_wait_for_log_timeout_when_no_end_line(tmp_path):
    """With a start line but no end line in the log, wait_for_log_lines raises TimeoutError for the end line."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    lines = ["start", "cde", "start"]  # no start line provided
    t = threading.Thread(target=write_to_file, args=(file_path, lines))
    t.daemon = True
    file_path.touch()
    with (
        pytest.raises(TimeoutError, match="Timeout occurred while waiting for end log line"),
        wait_for_log_lines(
            node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.5, end_timeout=0.7
        ),
    ):
        t.start()


def test_wait_for_log_reraises_exception(tmp_path):
    """An exception raised inside the wait_for_log_lines block propagates once the end line is seen."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    lines = ["start", "cde", "end"]  # no start line provided
    t = threading.Thread(target=write_to_file, args=(file_path, lines))
    t.daemon = True
    file_path.touch()
    with (
        pytest.raises(ValueError, match="dummy error"),
        wait_for_log_lines(
            node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.5, end_timeout=0.7
        ),
    ):
        t.start()
        raise ValueError("dummy error")


def test_wait_for_log_reraises_exception_and_timeout_error(tmp_path):
    """A start-line timeout is raised with the exception from the block kept in its context."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    file_path.touch()
    with (
        pytest.raises(TimeoutError, match="Timeout occurred while waiting for start log line") as exc_info,
        wait_for_log_lines(
            node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.4, end_timeout=0.7
        ),
    ):
        raise ValueError("dummy error")
    assert "ValueError" in str(exc_info.getrepr())


def test_wait_for_log_reraises_timeout_error_with_error_context(tmp_path):
    """The end-line timeout message includes the node name and the error_msg_ctx text."""
    file_path = tmp_path / "wait_for_log_lines_1.log"
    node = DummyNode(log_path=file_path)
    # Provide lines that will not satisfy the end pattern so that a timeout occurs.
    lines = ["start", "cde", "start"]
    t = threading.Thread(target=write_to_file, args=(file_path, lines))
    t.daemon = True
    file_path.touch()
    expected_match = (
        r"Timeout occurred while waiting for end log line \['end'\] on node: node_1. Context: Wait end line"
    )
    with (
        pytest.raises(TimeoutError, match=expected_match),
        wait_for_log_lines(
            node=node,
            start_line_patterns=["start"],
            end_line_patterns=["end"],
            start_timeout=0.4,
            end_timeout=0.7,
            error_msg_ctx="Wait end line",
        ),
    ):
        t.start()
        raise ValueError("dummy error")

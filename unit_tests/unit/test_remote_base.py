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

"""Tests for the stress log writing watchers used by the command runners."""

import gc
import re
import threading

import pytest

from sdcm.remote.base import LogWriteWatcher, TimestampedLogWriteWatcher

# NOTE: latte 'LOG' section lines, the ones parsed by the 'sdcm.loader.LatteExporter'
STRESS_OUTPUT = (
    "   1.001       299         0       299       0.202     1.626     1.934"
    "     2.269     2.427     2.632     3.369     3.369\n"
    "   2.001       412         0       412       0.204     1.502     1.812"
    "     2.101     2.302     2.501     3.001     3.001\n"
    "            99.99               0.2001 ± 0.0000\n"
)
TIMESTAMP_PREFIX_PATTERN = re.compile(r"^\[\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}\] ")


def submit_in_chunks(watcher: LogWriteWatcher, stream: str, chunk_size: int) -> None:
    """Feed the watcher the way 'invoke.runners.Runner' does it - with a cumulative stream read in chunks."""
    for end in range(chunk_size, len(stream) + chunk_size, chunk_size):
        watcher.submit(stream[:end])


@pytest.mark.parametrize("chunk_size", (1, 7, 40, 1000), ids=lambda size: f"chunk-{size}")
def test_log_write_watcher_writes_the_stream_verbatim(tmp_path, chunk_size):
    """The base class writes lines as they are, so its output has to stay byte for byte the stream.

    NOTE: this passes before the fix too - 'LogWriteWatcher._format_line' is the identity, so holding
          the tail back changes nothing here. It is a guard on the new buffering, not on the bug.
    """
    log_file = tmp_path / "stress.log"
    watcher = LogWriteWatcher(str(log_file))

    submit_in_chunks(watcher, STRESS_OUTPUT, chunk_size)

    assert log_file.read_text(encoding="utf-8") == STRESS_OUTPUT


@pytest.mark.parametrize("chunk_size", (1, 7, 40, 1000), ids=lambda size: f"chunk-{size}")
def test_timestamped_log_write_watcher_does_not_split_lines(tmp_path, chunk_size):
    log_file = tmp_path / "stress.log"
    watcher = TimestampedLogWriteWatcher(str(log_file))

    submit_in_chunks(watcher, STRESS_OUTPUT, chunk_size)

    written_lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(written_lines) == len(STRESS_OUTPUT.splitlines())
    for written_line, expected_line in zip(written_lines, STRESS_OUTPUT.splitlines()):
        assert TIMESTAMP_PREFIX_PATTERN.match(written_line), f"'{written_line}' is missing the timestamp prefix"
        assert TIMESTAMP_PREFIX_PATTERN.sub("", written_line) == expected_line


def test_a_last_line_without_a_line_end_is_written_once_the_stream_is_over(tmp_path):
    """'invoke' gives a watcher no end of stream hook, so 'PendingLine' hangs the write off the death
    of the thread reading that stream -- the point at which nothing more can arrive to complete it."""
    log_file = tmp_path / "stress.log"
    watcher = TimestampedLogWriteWatcher(str(log_file))
    held_back = {}

    def watch_stdout():  # invoke reads each stream in a thread of its own
        submit_in_chunks(watcher, STRESS_OUTPUT + "   Errors: 0", chunk_size=40)
        held_back["while_the_stream_is_open"] = "Errors: 0" not in log_file.read_text(encoding="utf-8")

    thread = threading.Thread(target=watch_stdout, name="handle_stdout")
    thread.start()
    thread.join(timeout=5)
    gc.collect()

    assert held_back["while_the_stream_is_open"], "the tail must not be written while it may still grow"
    written = log_file.read_text(encoding="utf-8").splitlines()
    assert len(written) == len(STRESS_OUTPUT.splitlines()) + 1
    assert TIMESTAMP_PREFIX_PATTERN.sub("", written[-1]) == "   Errors: 0"


def test_the_line_buffering_is_per_stream(tmp_path):
    """One watcher watches both stdout and stderr, in a thread each.

    'invoke' hands each of its two reader threads its own cumulative stream and calls 'submit' on the
    same watcher object for both. That is safe only because 'StreamWatcher' subclasses
    'threading.local', which gives every thread its own 'len' and 'pending_line'. Were the state
    shared, the stderr call below would advance 'len' past the stdout tail and truncate it.
    """
    log_file = tmp_path / "stress.log"
    watcher = TimestampedLogWriteWatcher(str(log_file))
    stdout_line = STRESS_OUTPUT.splitlines(True)[0]
    stdout_held, stderr_written = threading.Event(), threading.Event()

    def watch_stdout():
        watcher.submit(stdout_line[:20])  # half a line, so it is held back
        stdout_held.set()
        stderr_written.wait(timeout=5)  # ... while the other stream reports a whole one
        watcher.submit(stdout_line)  # the rest of the line arrives

    def watch_stderr():
        stdout_held.wait(timeout=5)
        watcher.submit("warn: something\n")
        stderr_written.set()

    threads = [
        threading.Thread(target=watch_stdout, name="handle_stdout"),
        threading.Thread(target=watch_stderr, name="handle_stderr"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    written = [TIMESTAMP_PREFIX_PATTERN.sub("", line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert stdout_line.rstrip("\n") in written, f"the stdout line did not survive the stderr call: {written}"
    assert "warn: something" in written, f"the stderr line is missing: {written}"


def test_timestamped_log_write_watcher_submit_line_timestamps_each_line(tmp_path):
    """'submit_line' is the whole-line path, used by the libssh2 and agent runners."""
    log_file = tmp_path / "stress.log"
    watcher = TimestampedLogWriteWatcher(str(log_file))

    for line in STRESS_OUTPUT.splitlines(True):
        watcher.submit_line(line)

    written_lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(written_lines) == len(STRESS_OUTPUT.splitlines())
    for written_line, expected_line in zip(written_lines, STRESS_OUTPUT.splitlines()):
        assert TIMESTAMP_PREFIX_PATTERN.sub("", written_line) == expected_line


def test_log_write_watcher_submit_line_writes_the_line_as_is(tmp_path):
    """The base class adds nothing, so 'submit_line' output is byte for byte its input."""
    log_file = tmp_path / "stress.log"
    watcher = LogWriteWatcher(str(log_file))

    for line in STRESS_OUTPUT.splitlines(True):
        watcher.submit_line(line)

    assert log_file.read_text(encoding="utf-8") == STRESS_OUTPUT

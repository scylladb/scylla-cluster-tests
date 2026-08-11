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

from unittest.mock import Mock

import pytest

from sdcm.utils import hard_exit
from sdcm.utils.hard_exit import STUCK_THREAD_EXIT_CODE, exit_process, request_hard_exit


def test_exit_process_uses_sys_exit_when_not_armed(monkeypatch):
    os_exit_mock = Mock()
    monkeypatch.setattr(hard_exit.os, "_exit", os_exit_mock)

    with pytest.raises(SystemExit) as exc_info:
        exit_process(3)

    assert exc_info.value.code == 3
    os_exit_mock.assert_not_called()


def test_exit_process_hard_exits_with_distinct_code_when_armed(monkeypatch):
    os_exit_mock = Mock()
    monkeypatch.setattr(hard_exit.os, "_exit", os_exit_mock)

    alive_thread = Mock(is_alive=Mock(return_value=True))
    request_hard_exit("stuck nemesis thread", [alive_thread])
    exit_process(3)

    os_exit_mock.assert_called_once_with(STUCK_THREAD_EXIT_CODE)
    assert 3 not in os_exit_mock.call_args.args


def test_exit_process_falls_through_when_implicated_thread_finished_naturally(monkeypatch):
    """False-positive prevention: a thread that was stuck when `request_hard_exit()`
    was called can finish naturally during the remaining cleanup steps (log
    collection, `clean_resources()`, etc.) that run before the eventual
    `exit_process()` call. If none of the implicated threads are still alive by
    then, the danger that justified arming has passed: `exit_process()` must fall
    through to normal `sys.exit()` behavior, not force a hard exit that would
    silently override the real test pass/fail result.
    """
    os_exit_mock = Mock()
    monkeypatch.setattr(hard_exit.os, "_exit", os_exit_mock)

    # Was alive when the caller decided to arm; has since finished by the time
    # exit_process() actually runs.
    finished_thread = Mock(is_alive=Mock(return_value=False))
    request_hard_exit("stuck nemesis thread", [finished_thread])

    with pytest.raises(SystemExit) as exc_info:
        exit_process(3)

    assert exc_info.value.code == 3
    os_exit_mock.assert_not_called()


def test_exit_process_armed_path_does_not_call_blocking_cleanup(monkeypatch):
    """logging.shutdown()/sys.stdout.flush()/sys.stderr.flush() can block on locks
    or I/O held by the very stuck thread that triggered the hard exit -- the armed
    path must not call them, or reaching os._exit() could itself hang.

    `LOGGER` is mocked out too so this only observes calls hard_exit.py itself
    makes, not incidental flushing a real logging handler would perform when
    emitting the "Hard exit requested" record.

    A `try/except` cannot protect against a call that blocks rather than raises,
    so `LOGGER.error()` (or any other logging call) must not run in the armed
    branch of `exit_process()` either -- only `request_hard_exit()`, which runs
    in a different, non-shutdown-crisis context, is safe to log from. The mock's
    call count is reset right after arming so this only observes what
    `exit_process()` itself does.
    """
    monkeypatch.setattr(hard_exit.os, "_exit", Mock())
    logger_mock = Mock()
    monkeypatch.setattr(hard_exit, "LOGGER", logger_mock)
    logging_shutdown_mock = Mock()
    stdout_flush_mock = Mock()
    stderr_flush_mock = Mock()
    monkeypatch.setattr(hard_exit.logging, "shutdown", logging_shutdown_mock)
    monkeypatch.setattr(hard_exit.sys.stdout, "flush", stdout_flush_mock)
    monkeypatch.setattr(hard_exit.sys.stderr, "flush", stderr_flush_mock)

    alive_thread = Mock(is_alive=Mock(return_value=True))
    request_hard_exit("stuck nemesis thread", [alive_thread])
    logger_mock.error.reset_mock()  # request_hard_exit() itself logs at arm-time; that's fine
    exit_process(3)

    logging_shutdown_mock.assert_not_called()
    stdout_flush_mock.assert_not_called()
    stderr_flush_mock.assert_not_called()
    logger_mock.error.assert_not_called()

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

"""The log-follower pools must not leave a worker thread alive after stop().

A ThreadPoolExecutor worker is non-daemon and stays parked in queue.get() once its task
finishes -- only shutdown() retires it. CPython's interpreter-shutdown hook
(concurrent.futures.thread._python_exit) then joins every still-live worker with no
timeout, so a single leaked one is enough to hang the process at exit. SCT starts one of
these loggers per node, so a leak here is a leak per node. See SCT-803.
"""

import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from sdcm.utils.remote_logger import HDRHistogramFileLogger, SSHGeneralSystemdLogger


def _node(remote_pid: str = "1234") -> MagicMock:
    """A node whose remoter answers the pid-file lookup done by SSHLoggerBase.remote_pid."""
    node = MagicMock()
    node.remoter.run.return_value = SimpleNamespace(ok=bool(remote_pid), stdout=remote_pid)
    return node


@pytest.fixture(params=["ssh", "hdr"])
def logger(request, tmp_path):
    """One real logger of each kind that owns a log-follower pool."""
    if request.param == "hdr":
        return HDRHistogramFileLogger(
            node=_node(), remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
        )
    return SSHGeneralSystemdLogger(node=_node(), target_log_file=str(tmp_path / "target.log"))


def _stub_journal_task(logger) -> SimpleNamespace:
    """Replace the logger's journal task with one that parks until termination is signalled.

    Set on the instance so it shadows the class attribute regardless of the class hierarchy.
    The returned state records the pool worker the task ran on, so a test can check that very
    thread is gone after stop().
    """
    state = SimpleNamespace(entered=threading.Event(), worker=None)

    def journal_task():
        state.worker = threading.current_thread()
        state.entered.set()
        logger._termination_event.wait(timeout=30)

    logger._journal_thread = journal_task
    return state


def test_stop_releases_the_pool_worker(logger):
    """stop() must retire the pool worker; a live one would block interpreter shutdown."""
    journal = _stub_journal_task(logger)

    logger.start()
    assert journal.entered.wait(timeout=10), "journal task never started"
    assert journal.worker is not threading.current_thread(), "journal task did not run on a pool worker"

    logger.stop()

    journal.worker.join(timeout=10)
    assert not journal.worker.is_alive(), "pool worker still alive after stop() -- it would block interpreter shutdown"


def test_start_after_stop_still_works(logger):
    """stop() retires the pool, so start() must build a fresh one rather than reuse a dead one."""
    journal = _stub_journal_task(logger)

    logger.start()
    assert journal.entered.wait(timeout=10)
    logger.stop()

    journal.entered.clear()
    logger._termination_event.clear()
    logger.start()
    assert journal.entered.wait(timeout=10), "logger could not be restarted after stop()"
    logger.stop()


def test_stop_without_remote_pid_warns_instead_of_running_bare_kill(tmp_path, caplog):
    """A missing pid file must not turn into `kill -9 -`; it leaves the follower running, so say so."""
    logger = SSHGeneralSystemdLogger(node=_node(remote_pid=""), target_log_file=str(tmp_path / "target.log"))
    journal = _stub_journal_task(logger)

    logger.start()
    assert journal.entered.wait(timeout=10)
    with caplog.at_level("WARNING"):
        logger.stop()

    assert not any("kill" in str(call) for call in logger._remoter.run.call_args_list)
    assert "No remote pid file" in caplog.text

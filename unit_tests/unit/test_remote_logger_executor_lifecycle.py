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
from concurrent.futures.thread import _threads_queues
from unittest.mock import MagicMock

import pytest

from sdcm.utils.remote_logger import HDRHistogramFileLogger, SSHLoggerBase


class _StubSSHLogger(SSHLoggerBase):
    @property
    def _logger_cmd_template(self) -> str:
        return "cat {since}"

    @property
    def remote_pid(self) -> str:
        return "1234"


class _StubHDRLogger(HDRHistogramFileLogger):
    @property
    def _logger_cmd_template(self) -> str:
        return "cat {since}"

    @property
    def remote_pid(self) -> str:
        return "1234"


def _make_logger(logger_class, tmp_path):
    if issubclass(logger_class, HDRHistogramFileLogger):
        return logger_class(
            node=MagicMock(), remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
        )
    return logger_class(node=MagicMock(), target_log_file=str(tmp_path / "target.log"))


def _stub_journal_task(logger):
    """Replace the logger's journal task with one that parks until termination is signalled.

    Set on the instance so it shadows the class attribute regardless of the class hierarchy.
    """
    entered = threading.Event()

    def journal_task():
        entered.set()
        logger._termination_event.wait(timeout=30)

    logger._journal_thread = journal_task
    return entered


def _live_pool_workers() -> set[threading.Thread]:
    return {thread for thread in _threads_queues if thread.is_alive()}


@pytest.mark.parametrize("logger_class", [_StubSSHLogger, _StubHDRLogger])
def test_stop_releases_the_pool_worker(logger_class, tmp_path):
    logger = _make_logger(logger_class, tmp_path)
    entered = _stub_journal_task(logger)

    before = _live_pool_workers()
    logger.start()
    assert entered.wait(timeout=10), "journal task never started"
    started = _live_pool_workers() - before
    assert started, "logger did not start a pool worker"

    logger.stop()

    # _threads_queues is weak-keyed, so a finished worker can linger in it until it is
    # garbage collected. What matters is liveness: _python_exit() joins every tracked
    # worker, and join() on a finished thread returns at once.
    for worker in started:
        worker.join(timeout=10)
        assert not worker.is_alive(), "pool worker still alive after stop() -- it would block interpreter shutdown"


@pytest.mark.parametrize("logger_class", [_StubSSHLogger, _StubHDRLogger])
def test_start_after_stop_still_works(logger_class, tmp_path):
    """stop() retires the pool, so start() must build a fresh one rather than reuse a dead one."""
    logger = _make_logger(logger_class, tmp_path)
    entered = _stub_journal_task(logger)

    logger.start()
    assert entered.wait(timeout=10)
    logger.stop()

    entered.clear()
    logger._termination_event.clear()
    logger.start()
    assert entered.wait(timeout=10), "logger could not be restarted after stop()"
    logger.stop()

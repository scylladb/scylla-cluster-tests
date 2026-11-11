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
from concurrent import futures
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from sdcm.utils.remote_logger import HDRHistogramFileLogger, K8sClientLogger, SSHGeneralSystemdLogger


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


def test_stop_without_remote_pid_runs_no_bare_kill(tmp_path, caplog):
    """A missing pid file must not turn into `kill -9 -`; a worker that returns anyway needs no warning."""
    logger = SSHGeneralSystemdLogger(node=_node(remote_pid=""), target_log_file=str(tmp_path / "target.log"))
    journal = _stub_journal_task(logger)

    logger.start()
    assert journal.entered.wait(timeout=10)
    with caplog.at_level("WARNING"):
        logger.stop()

    assert not _kill_calls(logger._remoter.run)
    assert not _kill_calls(logger._remoter.sudo)
    assert "still running after stop()" not in caplog.text


def _follower_stuck_until(logger, release: threading.Event) -> SimpleNamespace:
    """Replace the journal task with one that ignores termination, like a _retrieve() blocked on a live follower."""
    state = SimpleNamespace(entered=threading.Event())

    def journal_task():
        state.entered.set()
        release.wait(timeout=30)

    logger._journal_thread = journal_task
    return state


def test_stop_warns_when_the_follower_outlives_both_kills(tmp_path, caplog):
    """If the worker is still blocked after the second kill, say so: it is what hangs interpreter shutdown."""
    logger = SSHGeneralSystemdLogger(node=_node(remote_pid=""), target_log_file=str(tmp_path / "target.log"))
    logger.STOP_GRACE_PERIOD = 0.2
    release = threading.Event()
    journal = _follower_stuck_until(logger, release)

    logger.start()
    assert journal.entered.wait(timeout=10)
    try:
        with caplog.at_level("WARNING"):
            logger.stop()
    finally:
        release.set()

    assert "still running after stop()" in caplog.text


@pytest.fixture(params=["ssh", "hdr"])
def real_loop_logger(request, tmp_path):
    """A logger of each kind whose remoter has no pid file, for tests that run the real journal loop."""
    node = _node(remote_pid="")
    if request.param == "hdr":
        return HDRHistogramFileLogger(
            node=node, remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
        )
    node.journal_since = None
    return SSHGeneralSystemdLogger(node=node, target_log_file=str(tmp_path / "target.log"))


def test_follower_is_not_launched_when_stop_comes_during_the_readiness_check(real_loop_logger):
    """stop() right after start() lands while the worker checks the SSH connection; it must not launch then.

    Seen on minicloud: a cassandra-stress that failed at once stopped its HDR logger 0.5s after start, and the
    tail it launched afterwards held the pool worker, and with it the interpreter exit, forever.
    """
    logger = real_loop_logger
    in_readiness_check = threading.Event()

    def is_up(*_args, **_kwargs):
        in_readiness_check.set()
        logger._termination_event.wait(timeout=10)
        return True

    logger._remoter.is_up.side_effect = is_up

    logger.start()
    assert in_readiness_check.wait(timeout=10)
    worker_task = logger._thread
    logger.stop()
    # Let the worker finish first: whatever it would launch, it launches after stop() returned
    futures.wait([worker_task], timeout=10)

    assert not any("setsid" in str(call) for call in logger._remoter.run.call_args_list)


def test_second_kill_gets_a_follower_launched_after_the_first(real_loop_logger, caplog):
    """The follower can still launch between the loop's last check and the first kill; the second kill gets it."""
    logger = real_loop_logger
    logger.STOP_GRACE_PERIOD = 1
    first_kill_done, follower_killed = threading.Event(), threading.Event()
    state = SimpleNamespace(launched=False)

    original_kill = logger._kill_remote_follower

    def kill_remote_follower():
        original_kill()
        first_kill_done.set()

    logger._kill_remote_follower = kill_remote_follower

    def journal_task():
        logger._termination_event.wait(timeout=10)
        first_kill_done.wait(timeout=10)
        state.launched = True  # launched just after the first kill, which found no pid file
        follower_killed.wait(timeout=10)

    logger._journal_thread = journal_task

    def run(cmd=None, *_args, **_kwargs):
        if cmd.startswith("test -f"):
            return SimpleNamespace(ok=state.launched, stdout="")
        return SimpleNamespace(ok=True, stdout="4321" if cmd.startswith("cat ") else "")

    def sudo(cmd, **_kwargs):
        if "/cmdline" in cmd:
            return SimpleNamespace(ok=True, stdout=f"bash {logger._remote_cmd_file} ")
        if cmd == "kill -9 -4321":
            follower_killed.set()
        return SimpleNamespace(ok=True, stdout="")

    logger._remoter.run.side_effect = run
    logger._remoter.sudo.side_effect = sudo

    logger.start()
    with caplog.at_level("WARNING"):
        logger.stop()

    assert follower_killed.is_set(), "the follower launched after the first kill was never killed"
    assert _kill_calls(logger._remoter.sudo) == [call("kill -9 -4321", ignore_status=True)]
    assert "still running after stop()" not in caplog.text


def test_k8s_client_logger_reads_multibyte_char_split_across_chunks():
    data = "2026-10-06T00:00:00Z duration=530µs\n2026-10-06T00:00:01Z next\n".encode()
    cut = data.index("µ".encode()) + 1
    chunks = [data[:cut], data[cut:], b""]
    stream = MagicMock(read=lambda _size: chunks.pop(0))
    k8s_logger = K8sClientLogger.__new__(K8sClientLogger)
    k8s_logger._termination_event = threading.Event()

    assert list(k8s_logger._read_log_lines(stream)) == [
        ("2026-10-06T00:00:00Z", "duration=530µs\n"),
        ("2026-10-06T00:00:01Z", "next\n"),
    ]


def _answer_leader_cmdline(logger, leader: str | None = None) -> None:
    """Make the remoter report `leader` as the pid's command line; by default, the follower script of `logger`."""
    if leader is None:
        leader = f"bash {logger._remote_cmd_file} "

    def sudo(cmd, **_kwargs):
        return SimpleNamespace(ok=True, stdout=leader if "/cmdline" in cmd else "")

    logger._remoter.sudo.side_effect = sudo


def _stopped_systemd_logger(tmp_path, leader: str | None = None) -> SSHGeneralSystemdLogger:
    logger = SSHGeneralSystemdLogger(node=_node(remote_pid="1522"), target_log_file=str(tmp_path / "target.log"))
    _answer_leader_cmdline(logger, leader)
    journal = _stub_journal_task(logger)
    logger.start()
    assert journal.entered.wait(timeout=10)
    logger.stop()
    return logger


def _kill_calls(remoter_method) -> list:
    return [call for call in remoter_method.call_args_list if "kill" in str(call)]


def test_stop_kills_the_follower_group_as_root(tmp_path):
    """The group holds `sudo journalctl`; an unprivileged kill can't signal it and the follower outlives stop()."""
    logger = _stopped_systemd_logger(tmp_path)

    assert _kill_calls(logger._remoter.sudo) == [call("kill -9 -1522", ignore_status=True)]
    assert not _kill_calls(logger._remoter.run)


@pytest.mark.parametrize(
    "leader",
    [
        pytest.param("/usr/bin/scylla --options-file /etc/scylla/scylla.yaml ", id="pid_reused_by_another_process"),
        pytest.param("bash /tmp/logger_7_0123abcd.sh ", id="pid_reused_by_another_logger"),
        pytest.param("", id="follower_already_gone"),
    ],
)
def test_stop_does_not_kill_a_group_the_follower_no_longer_leads(tmp_path, leader):
    """A root kill of a stale pgid could hit anything, e.g. scylla on a DB node: only our script's group dies."""
    logger = _stopped_systemd_logger(tmp_path, leader=leader)

    assert not _kill_calls(logger._remoter.sudo)
    assert not _kill_calls(logger._remoter.run)


@pytest.mark.parametrize("leader", [None, ""], ids=["follower_killed", "follower_already_gone"])
def test_stop_removes_the_remote_pid_and_cmd_files(tmp_path, leader):
    """A host that outlives the run would otherwise collect a pid and a cmd file per logger."""
    logger = _stopped_systemd_logger(tmp_path, leader=leader)

    logger._remoter.run.assert_any_call(
        f"rm -f {logger._remote_pid_file} {logger._remote_cmd_file}", ignore_status=True
    )


def test_hdr_logger_stop_removes_the_remote_pid_and_cmd_files(tmp_path):
    """A loader starts an HDR logger per stress command, so these files pile up fastest there."""
    logger = HDRHistogramFileLogger(
        node=_node(), remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
    )
    journal = _stub_journal_task(logger)
    logger.start()
    assert journal.entered.wait(timeout=10)
    logger.stop()

    logger._remoter.run.assert_any_call(
        f"rm -f {logger._remote_pid_file} {logger._remote_cmd_file}", ignore_status=True
    )


def test_second_stop_neither_kills_nor_warns(tmp_path, caplog):
    """Node teardown calls stop() twice; the first already removed the pid file, so the second must be a no-op."""
    logger = _stopped_systemd_logger(tmp_path)
    logger._remoter.reset_mock()
    logger._remoter.run.return_value = SimpleNamespace(ok=False, stdout="")

    with caplog.at_level("WARNING"):
        logger.stop()

    assert not logger._remoter.sudo.called
    assert not logger._remoter.run.called
    assert "No remote pid file" not in caplog.text


def test_loggers_on_one_node_keep_separate_pid_files(tmp_path):
    """A loader follows its journal and an HDR file at once; stopping one must not kill the other's follower."""
    node = _node()
    journal_logger = SSHGeneralSystemdLogger(node=node, target_log_file=str(tmp_path / "journal.log"))
    hdr_logger = HDRHistogramFileLogger(
        node=node, remote_log_file="/tmp/remote.hdr", target_log_file=str(tmp_path / "target.hdr")
    )

    assert journal_logger._remote_pid_file != hdr_logger._remote_pid_file
    assert journal_logger._remote_cmd_file != hdr_logger._remote_cmd_file

    journal_logger._retrieve(since="")
    remote_cmd = node.remoter.run.call_args.kwargs["cmd"]
    assert f"> {journal_logger._remote_pid_file};" in remote_cmd
    assert f"setsid bash {journal_logger._remote_cmd_file} " in remote_cmd

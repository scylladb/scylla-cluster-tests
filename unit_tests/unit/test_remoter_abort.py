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

"""Tests for aborting SSH commands running on a host that is gone (SCT-711).

A preempted spot instance sends no RST and SSH keepalive is disabled, so the output read loops
see EAGAIN forever. Without an abort, a stress command blocks its thread for the whole stress
duration and holds the SCT process exit.
"""

import threading
import time
from io import StringIO
from unittest.mock import MagicMock

import pytest
from ssh2.error_codes import LIBSSH2_ERROR_EAGAIN

from sdcm.remote.kubernetes_cmd_runner import KubernetesPodRunner
from sdcm.remote.libssh2_client import Client, SSHReaderThread
from sdcm.remote.libssh2_client.exceptions import CommandAborted
from sdcm.remote.local_cmd_runner import LocalCmdRunner
from sdcm.remote.remote_libssh_cmd_runner import RemoteLibSSH2CmdRunner

ABORT_WAIT_TIMEOUT = 5


class IdleSession:
    """Session of a host that vanished: select always times out with no data."""

    def __init__(self):
        self.lock = threading.Lock()

    @staticmethod
    def simple_select(timeout=None):
        """Sleep briefly, as a select on a silent socket would."""
        time.sleep(0.01)

    @staticmethod
    def eagain(func, args=(), kwargs=None, timeout=None):
        """Return None, as a call that would block returns."""
        return None

    @staticmethod
    def drop_channel(channel):
        """Accept the channel and release nothing."""


class SilentChannel:
    """Channel of a host that vanished: every read returns EAGAIN, EOF never comes."""

    @staticmethod
    def read():
        """Report EAGAIN with no stdout data."""
        return LIBSSH2_ERROR_EAGAIN, b""

    @staticmethod
    def read_stderr():
        """Report EAGAIN with no stderr data."""
        return LIBSSH2_ERROR_EAGAIN, b""

    @staticmethod
    def eof():
        """Report EAGAIN for the EOF check."""
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def wait_eof():
        """Report EAGAIN while waiting for EOF."""
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def close():
        """Report EAGAIN on close."""
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def wait_closed():
        """Report EAGAIN while waiting for the close."""
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def get_exit_status():
        """Report the -1 exit status of a command still running."""
        return -1


def run_in_thread(target) -> tuple[threading.Thread, list]:
    """Start `target` in a daemon thread and return the thread and the list collecting its exceptions."""
    raised = []

    def _wrapper():
        try:
            target()
        except Exception as exc:  # noqa: BLE001
            raised.append(exc)

    thread = threading.Thread(target=_wrapper, daemon=True)
    thread.start()
    return thread, raised


def test_no_watchers_read_loop_raises_on_abort():
    """The no-watchers read loop waits on a silent channel and raises CommandAborted once abort is set."""
    abort_event = threading.Event()
    thread, raised = run_in_thread(
        lambda: Client._process_output_no_watchers(
            IdleSession(),
            SilentChannel(),
            "utf-8",
            StringIO(),
            StringIO(),
            timeout=None,
            timeout_read_data_chunk=0.01,
            abort_event=abort_event,
        )
    )
    time.sleep(0.1)
    assert thread.is_alive(), "read loop must keep waiting while the channel is silent"

    abort_event.set()
    thread.join(ABORT_WAIT_TIMEOUT)

    assert not thread.is_alive()
    assert len(raised) == 1
    assert isinstance(raised[0], CommandAborted)


def test_reader_thread_stops_on_abort():
    """The SSH reader thread waits on a silent channel and stops with CommandAborted once abort is set."""
    abort_event = threading.Event()
    reader = SSHReaderThread(IdleSession(), SilentChannel(), None, 0.01, abort_event=abort_event)
    reader.start()
    time.sleep(0.1)
    assert reader.is_alive(), "reader must keep waiting while the channel is silent"

    abort_event.set()
    reader.join(ABORT_WAIT_TIMEOUT)

    assert not reader.is_alive()
    assert isinstance(reader.raised, CommandAborted)


def test_aborted_client_does_not_run_commands():
    """An aborted client raises CommandAborted on run before connecting."""
    client = Client(host="10.0.0.1", user="test")
    client.connect = MagicMock()

    client.abort()

    assert client.aborted
    with pytest.raises(CommandAborted):
        client.run("true")
    client.connect.assert_not_called()


def test_aborted_run_is_not_wrapped_as_retryable():
    """`FailedToReadCommandOutput` is retryable, an aborted command must not be retried."""
    client = Client(host="10.0.0.1", user="test")
    client.session = IdleSession()
    client.open_channel = SilentChannel
    client.execute = MagicMock()
    thread, raised = run_in_thread(lambda: client.run("scylla-bench -duration=6420m", timeout=None))
    time.sleep(0.1)
    assert thread.is_alive(), "command must keep running while the channel is silent"

    client.abort()
    thread.join(ABORT_WAIT_TIMEOUT)

    assert not thread.is_alive()
    assert len(raised) == 1
    assert type(raised[0]) is CommandAborted


@pytest.fixture(name="remoter")
def fixture_remoter():
    """Provide a libssh2 remoter to a fake host and stop it after the test."""
    remoter = RemoteLibSSH2CmdRunner(hostname="10.0.0.1", user="test", key_file="/tmp/test_key")
    yield remoter
    remoter.stop()


def test_abort_running_commands_reaches_connections_of_other_threads(remoter):
    """abort_running_commands aborts the own connections of two worker threads and of the main thread."""
    connections = []

    def _get_connection():
        connections.append(remoter.connection)

    threads = [threading.Thread(target=_get_connection) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    main_thread_connection = remoter.connection

    remoter.abort_running_commands()

    assert len({id(conn) for conn in connections}) == 2, "each thread must have its own connection"
    assert all(conn.aborted for conn in connections)
    assert main_thread_connection.aborted


def test_abort_running_commands_without_connections(remoter):
    """abort_running_commands completes on a remoter that has opened no connections."""
    remoter.abort_running_commands()


def test_abort_running_commands_is_noop_for_non_ssh_runners():
    """abort_running_commands is callable on a local runner and completes as a no-op."""
    # NOTE: loaders on the docker backend or with the SCT agent don't use an SSH remoter,
    #       `kill_docker_loaders` must still be able to call it on them
    LocalCmdRunner().abort_running_commands()


def test_k8s_pod_runner_tracks_connections_for_abort():
    """KubernetesPodRunner tracks the connection of a run so abort_running_commands aborts it."""
    # NOTE: `KubernetesPodRunner` keeps its own `_connections` list, tracking connections for abort
    #       must not clash with it
    runner = KubernetesPodRunner(
        kluster=MagicMock(params={}),
        template_path="template.yaml",
        template_modifiers=[],
        pod_name_template="loader",
        namespace="default",
        environ={},
    )
    connection = MagicMock()
    connection.__enter__.return_value = connection
    runner._create_connection = MagicMock(return_value=connection)

    runner._run_execute("true", new_session=True)
    runner.abort_running_commands()

    connection.run.assert_called_once()
    connection.abort.assert_called_once()

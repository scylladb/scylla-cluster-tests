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
        time.sleep(0.01)

    @staticmethod
    def eagain(func, args=(), kwargs=None, timeout=None):
        return None

    @staticmethod
    def drop_channel(channel):
        pass


class SilentChannel:
    """Channel of a host that vanished: every read returns EAGAIN, EOF never comes."""

    @staticmethod
    def read():
        return LIBSSH2_ERROR_EAGAIN, b""

    @staticmethod
    def read_stderr():
        return LIBSSH2_ERROR_EAGAIN, b""

    @staticmethod
    def eof():
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def wait_eof():
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def close():
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def wait_closed():
        return LIBSSH2_ERROR_EAGAIN

    @staticmethod
    def get_exit_status():
        return -1


def run_in_thread(target) -> tuple[threading.Thread, list]:
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
    remoter = RemoteLibSSH2CmdRunner(hostname="10.0.0.1", user="test", key_file="/tmp/test_key")
    yield remoter
    remoter.stop()


def test_abort_running_commands_reaches_connections_of_other_threads(remoter):
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
    remoter.abort_running_commands()


def test_abort_running_commands_is_noop_for_non_ssh_runners():
    # NOTE: loaders on the docker backend or with the SCT agent don't use an SSH remoter,
    #       `kill_docker_loaders` must still be able to call it on them
    LocalCmdRunner().abort_running_commands()

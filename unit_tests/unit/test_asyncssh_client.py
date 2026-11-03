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

"""Tests for the asyncssh client (SCT-1396) against an in-process asyncssh server that runs commands in a local shell."""

import asyncio
import gc
import threading
import time

import asyncssh
import pytest

from sdcm.remote import asyncssh_client
from sdcm.remote.asyncssh_client import Client, get_loop, submit
from sdcm.remote.libssh2_client.exceptions import CommandAborted, CommandTimedOut, UnexpectedExit

SERVER_WAIT = 10


class NoAuthServer(asyncssh.SSHServer):
    def begin_auth(self, username: str) -> bool:
        return False


async def pump(source: asyncio.StreamReader, sink) -> None:
    while chunk := await source.read(65536):
        sink.write(chunk)


async def run_in_local_shell(process: asyncssh.SSHServerProcess) -> None:
    shell = await asyncio.create_subprocess_shell(
        process.command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    try:
        # forward both pipes to EOF before exit: process.redirect() can lose the last chunk when exit() closes first
        await asyncio.gather(pump(shell.stdout, process.stdout), pump(shell.stderr, process.stderr))
        process.exit(await shell.wait())
    finally:
        if shell.returncode is None:
            shell.kill()


async def start_server() -> asyncssh.SSHAcceptor:
    return await asyncssh.create_server(
        NoAuthServer,
        "127.0.0.1",
        0,
        server_host_keys=[asyncssh.generate_private_key("ssh-ed25519")],
        process_factory=run_in_local_shell,
        encoding=None,
    )


@pytest.fixture(scope="module")
def ssh_port():
    server = submit(start_server()).result(timeout=SERVER_WAIT)
    yield server.sockets[0].getsockname()[1]
    server.close()


@pytest.fixture
def client(ssh_port):
    with Client(host="127.0.0.1", user="test", port=ssh_port, allow_agent=False) as ssh_client:
        yield ssh_client


def test_run_collects_output_and_exit_status(client):
    result = client.run("echo out; echo err >&2")

    assert (result.stdout, result.stderr, result.exited) == ("out\n", "err\n", 0)


def test_non_zero_exit_raises_unless_warn(client):
    assert client.run("exit 3", warn=True).exited == 3
    with pytest.raises(UnexpectedExit) as exc_info:
        client.run("exit 4")
    assert exc_info.value.result.exited == 4


def test_timeout_raises_command_timed_out(client):
    with pytest.raises(CommandTimedOut):
        client.run("sleep 5", timeout=0.5)
    assert client.run("true").ok, "the connection must stay usable after a timed out command"


def test_watchers_get_each_line_while_command_runs(client):
    class Recorder:
        def __init__(self):
            self.lines = []

        def submit_line(self, line):
            self.lines.append(line)

    recorder = Recorder()
    client.run("printf 'a\\nb\\n'; sleep 0.2; printf c", watchers=[recorder])

    assert recorder.lines == ["a\n", "b\n", "c\n"]


def test_abort_from_another_thread_stops_running_and_later_commands(client):
    threading.Timer(0.3, client.abort).start()
    started = time.perf_counter()

    with pytest.raises(CommandAborted):
        client.run("sleep 30")
    assert time.perf_counter() - started < 5
    with pytest.raises(CommandAborted):
        client.run("true")


def new_client(port, user="test"):
    return Client(host="127.0.0.1", user=user, port=port, allow_agent=False)


def test_key_file_disables_the_agent():
    # as the libssh2 client: agent keys offered first can use up sshd MaxAuthTries before the key file
    assert Client(host="h", user="u", pkey="/key")._connect_options()["agent_path"] is None
    assert Client(host="h", user="u")._connect_options()["agent_path"] == ()
    assert Client(host="h", user="u", allow_agent=False)._connect_options()["agent_path"] is None


def test_dropped_client_closes_its_connection(ssh_port):
    # RemoteCmdRunnerBase drops a thread's client when the thread ends, without calling close()
    ssh_client = new_client(ssh_port, user="dropped")
    ssh_client.run("true")
    conn = asyncssh_client._pool[ssh_client.key][0].task.result()

    del ssh_client
    gc.collect()

    submit(conn.wait_closed()).result(timeout=SERVER_WAIT)


def test_clients_of_a_host_share_connections_up_to_max_channels(ssh_port):
    # one client per thread, as RemoteCmdRunnerBase has; 12 commands at once need 2 connections of 10 channels
    clients = [new_client(ssh_port, user="shared") for _ in range(12)]
    threads = [threading.Thread(target=c.run, args=("sleep 0.5",)) for c in clients]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    key = clients[0].key

    assert len(asyncssh_client._pool[key]) == 2

    conns = [entry.task.result() for entry in asyncssh_client._pool[key]]
    clients[0].close()
    assert key in asyncssh_client._pool, "other clients still use the host"
    for c in clients[1:]:
        c.close()
    for conn in conns:
        submit(conn.wait_closed()).result(timeout=SERVER_WAIT)


def test_fork_child_gets_a_new_loop(monkeypatch):
    parent_loop = get_loop()
    # keep the module server's loop for the other tests
    monkeypatch.setattr(asyncssh_client, "_loop", parent_loop)

    asyncssh_client._reset_loop_after_fork()
    child_loop = get_loop()

    assert child_loop is not parent_loop
    child_loop.call_soon_threadsafe(child_loop.stop)

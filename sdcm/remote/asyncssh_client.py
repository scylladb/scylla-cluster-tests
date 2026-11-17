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

"""Synchronous SSH client on top of asyncssh.

All connections of a process live on one event loop, run by a daemon thread. A caller thread submits a coroutine
to that loop and blocks on its result. The client keeps the `run()` contract of `libssh2_client.Client`: the same
`Result` and the same exceptions, so `RemoteCmdRunnerBase` and its retry logic work unchanged.
"""

import asyncio
import codecs
import logging
import os
import queue
import threading
import weakref
from collections import Counter
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeoutError
from time import perf_counter, sleep

import asyncssh

from .libssh2_client.exceptions import (
    AuthenticationException,
    CommandAborted,
    CommandTimedOut,
    ConnectError,
    ConnectTimeout,
    FailedToReadCommandOutput,
    FailedToRunCommand,
    UnexpectedExit,
)
from .libssh2_client.result import Result
from .libssh2_client.timings import Timings

# extra time a caller waits for the loop beyond the command timeout, so the coroutine reports the timeout itself
LOOP_WAIT_GRACE = 10
READ_CHUNK_SIZE = 65536
DEFAULT_RUN_TIMEOUT = None
ENCRYPTION_ALGS = ("aes128-gcm@openssh.com", "aes256-gcm@openssh.com", "chacha20-poly1305@openssh.com", "aes128-ctr")
# sshd `MaxSessions` default: the channels one connection may carry at the same time
MAX_CHANNELS = 10

# asyncssh logs every channel open/close at INFO: noise in sct.log and ~5% of the loop thread CPU at 200 sessions
asyncssh.set_log_level(logging.WARNING)

# ponytail: one loop is what the GIL allows; more only spread hosts over threads. Upgrade path: a loop per core on a
# free-threaded build, once cryptography ships cp3xxt wheels SCT can install
LOOP_COUNT = 1

_loop_lock = threading.Lock()
_loops: list[asyncio.AbstractEventLoop] = []


class _SharedConnection:
    """One SSH connection that every client of a host shares, a channel per command. Only the loop thread uses it."""

    def __init__(self, connect):
        self.task = asyncio.ensure_future(connect())
        self.channels = 0

    def dead(self) -> bool:
        if not self.task.done():
            return False
        return self.task.cancelled() or self.task.exception() is not None or self.task.result().is_closed()

    def close(self) -> None:
        if not self.task.done():
            self.task.cancel()
        elif not self.dead():
            self.task.result().close()


# loop thread state: host key -> its shared connections, and how many clients (one per remoter thread) use the host
_pool: dict[tuple, list[_SharedConnection]] = {}
_clients: Counter = Counter()
_background: set[asyncio.Task] = set()


def _reset_loop_after_fork() -> None:
    """Loop threads do not survive a fork, so the child starts its own loops, and connections, on first use."""
    global _loop_lock  # noqa: PLW0603
    _loops.clear()
    _loop_lock = threading.Lock()
    _pool.clear()
    _clients.clear()
    _background.clear()


os.register_at_fork(after_in_child=_reset_loop_after_fork)


def _start_loop(index: int) -> asyncio.AbstractEventLoop:
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, name=f"asyncssh-loop-{index}", daemon=True).start()
    return loop


def get_loop(route: tuple | None = None) -> asyncio.AbstractEventLoop:
    """Return the event loop that runs the connections of `route` (a host key), start the loops on first use.

    A host always maps to the same loop: its shared connections, and the pool state of its key, belong to that loop.
    """
    with _loop_lock:
        if not _loops or any(loop.is_closed() for loop in _loops):
            _loops[:] = [_start_loop(i) for i in range(LOOP_COUNT)]
        return _loops[hash(route) % len(_loops)]


def submit(coro, route: tuple | None = None) -> Future:
    return asyncio.run_coroutine_threadsafe(coro, get_loop(route))


def _call_on_loop(route: tuple, func, *args) -> None:
    """Run `func` on the loop of `route`, from any thread, without waiting. No loops (after a fork): nothing to do."""
    if _loops:
        loop = _loops[hash(route) % len(_loops)]
        if not loop.is_closed():
            loop.call_soon_threadsafe(func, *args)


def _add_client(key: tuple) -> None:
    _clients[key] += 1


def _remove_client(key: tuple) -> None:
    """The last client of a host closes the host's connections."""
    _clients[key] -= 1
    if _clients[key] <= 0:
        del _clients[key]
        for shared in _pool.pop(key, []):
            shared.close()


async def _acquire(key: tuple, connect) -> tuple[_SharedConnection, asyncssh.SSHClientConnection]:
    """Reserve a channel on a connection of the host, open a new connection when all are full or dead."""
    shared = _pool.setdefault(key, [])
    shared[:] = [s for s in shared if not s.dead()]
    entry = next((s for s in shared if s.channels < MAX_CHANNELS), None)
    if entry is None:
        entry = _SharedConnection(connect)
        shared.append(entry)
    entry.channels += 1
    try:
        # shield: a waiter that times out or aborts must not cancel the connect other clients wait on
        return entry, await asyncio.shield(entry.task)
    except BaseException:
        entry.channels -= 1
        raise


def _release_when_closed(entry: _SharedConnection, process: asyncssh.SSHClientProcess) -> None:
    """Free the channel slot once sshd closed the channel too, so a next command stays within `MaxSessions`."""

    async def release():
        try:
            await process.wait_closed()
        finally:
            entry.channels -= 1

    task = asyncio.ensure_future(release())
    _background.add(task)
    task.add_done_callback(_background.discard)


class Client:
    """Sync SSH client with the interface of `libssh2_client.Client` that `RemoteCmdRunnerBase` uses.

    `RemoteCmdRunnerBase` creates one client per thread. The clients of a host share its connections, so a new
    thread costs no SSH handshake. A client is a counted handle: the host's connections close with the last one.
    """

    def __init__(
        self,
        host: str,
        user: str,
        port: int = 22,
        pkey: str | None = None,
        password: str | None = None,
        allow_agent: bool = True,
        forward_ssh_agent: bool = False,
        keepalive_seconds: int = 60,
        timings: Timings | None = None,
        proxy: "Client | None" = None,
    ):
        self.host = host
        self.user = user
        self.port = port
        self.pkey = pkey
        self.password = password
        self.allow_agent = allow_agent
        self.forward_ssh_agent = forward_ssh_agent
        self.keepalive_seconds = keepalive_seconds
        self.timings = timings or Timings()
        self.proxy = proxy
        self.key = (host, port, user, pkey, password, proxy.key if proxy else None)
        # the loop of the outermost proxy: a tunnel connection only works on the loop it was opened on
        self.route = proxy.route if proxy else self.key
        self._registered = False
        self._finalizer: weakref.finalize | None = None
        self._abort_event = threading.Event()
        self._running: Future | None = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    # --- connection ---

    def _connect_options(self) -> dict:
        return dict(
            # the runner user's ~/.ssh/config does not apply to test nodes, and parsing it costs ~20ms CPU per connect
            config=None,
            # asyncssh builds 3 cipher objects per packet for chacha20-poly1305, AES-GCM is one AEAD call
            encryption_algs=ENCRYPTION_ALGS,
            username=self.user,
            port=self.port,
            password=self.password,
            client_keys=[self.pkey] if self.pkey else [],
            known_hosts=None,
            # a key file means that key only, as the libssh2 client: asyncssh would offer every agent key first, and
            # past MaxAuthTries (6) sshd disconnects before it reaches the file; failures also feed PerSourcePenalties
            agent_path=() if self.allow_agent and not self.pkey else None,
            agent_forwarding=self.forward_ssh_agent,
            keepalive_interval=self.keepalive_seconds or 0,
            connect_timeout=self.timings.ssh_session_timeout,
            login_timeout=self.timings.auth_timeout,
        )

    async def _open(self) -> asyncssh.SSHClientConnection:
        """Open a new connection to the host. Through the proxy, the tunnel holds a proxy channel while it lives."""
        if self.proxy is None:
            return await asyncssh.connect(self.host, **self._connect_options())
        proxy_key = self.proxy.key
        _add_client(proxy_key)
        try:
            proxy_entry, tunnel = await _acquire(proxy_key, self.proxy._open)
        except BaseException:
            _remove_client(proxy_key)
            raise

        async def release_proxy(conn=None):
            if conn is not None:
                await conn.wait_closed()
            proxy_entry.channels -= 1
            _remove_client(proxy_key)

        try:
            conn = await asyncssh.connect(self.host, tunnel=tunnel, **self._connect_options())
        except BaseException:
            await release_proxy()
            raise
        task = asyncio.ensure_future(release_proxy(conn))
        _background.add(task)
        task.add_done_callback(_background.discard)
        return conn

    async def _aregister(self) -> None:
        """Count this client in, and make sure the host has a live connection."""
        _add_client(self.key)
        try:
            entry, _ = await _acquire(self.key, self._open)
            entry.channels -= 1
        except BaseException:
            _remove_client(self.key)
            raise

    def _connect_once(self) -> None:
        wait = (self.timings.ssh_session_timeout or 0) + (self.timings.auth_timeout or 0) + LOOP_WAIT_GRACE
        future = submit(self._aregister(), self.route)
        try:
            future.result(timeout=wait)
        except FutureTimeoutError:
            if future.cancel():
                raise
            future.result()  # it finished between the timeout and the cancel
        self._registered = True
        # RemoteCmdRunnerBase drops a thread's client with the thread, without close()
        self._finalizer = weakref.finalize(self, _call_on_loop, self.route, _remove_client, self.key)

    def connect(self, timeout: float | None = None) -> None:
        """Connect and authenticate, retrying until `timeout` passes, as `libssh2_client.Client.connect` does."""
        if timeout is None:
            timeout = self.timings.connect_timeout
        end_time = perf_counter() + (timeout or 0)
        delays = iter(self.timings.connect_delays)
        delay = next(delays)
        while True:
            try:
                self._connect_once()
                return
            except asyncssh.PermissionDenied as exc:
                self.disconnect()
                raise AuthenticationException(str(exc)) from exc
            except Exception as exc:  # noqa: BLE001
                self.disconnect()
                if not timeout:
                    raise ConnectError(str(exc)) from exc
                if perf_counter() > end_time:
                    raise ConnectTimeout(
                        f"Failed to connect in {timeout} seconds, last error: ({type(exc).__name__}){exc!s}"
                    ) from exc
            delay = next(delays, delay)
            sleep(delay)

    open = connect

    def disconnect(self) -> None:
        """Drop this client's handle. A connection other clients still use stays open; a dead one gets replaced."""
        if not self._registered:
            return
        self._registered = False
        self._finalizer.detach()
        _call_on_loop(self.route, _remove_client, self.key)

    def close(self) -> None:
        self.disconnect()

    # --- abort ---

    def abort(self) -> None:
        """Make the running command, and every command after it, fail right away. Safe from any thread."""
        self._abort_event.set()
        if (running := self._running) is not None:
            running.cancel()

    @property
    def aborted(self) -> bool:
        return self._abort_event.is_set()

    # --- commands ---

    def check_if_alive(self, timeout: float | None = None) -> bool:
        return self.run("true", timeout=timeout or self.timings.check_if_alive_timeout).ok

    @staticmethod
    async def _read_stream(stream, decoder, sink: list, lines: queue.Queue | None) -> None:
        partial = ""
        while chunk := await stream.read(READ_CHUNK_SIZE):
            text = decoder.decode(chunk)
            sink.append(text)
            if lines is not None:
                partial += text
                *complete, partial = partial.split("\n")
                for line in complete:
                    lines.put(line + "\n")
        tail = decoder.decode(b"", final=True)
        sink.append(tail)
        if lines is not None and (partial := partial + tail):
            lines.put(partial + "\n")

    async def _arun(self, command: str, encoding: str, env: dict | None, timeout, lines: queue.Queue | None):
        stdout, stderr = [], []
        timed_out = False
        entry, conn = await _acquire(self.key, self._open)
        try:
            process = await conn.create_process(command, env=env or (), encoding=None)
        except BaseException:
            entry.channels -= 1
            raise
        _release_when_closed(entry, process)
        try:
            async with asyncio.timeout(timeout):
                await asyncio.gather(
                    self._read_stream(process.stdout, self._decoder(encoding), stdout, lines),
                    self._read_stream(process.stderr, self._decoder(encoding), stderr, lines),
                )
                await process.wait_closed()
        except TimeoutError:
            timed_out = True
        finally:
            process.close()
        return process.returncode, "".join(stdout), "".join(stderr), timed_out

    @staticmethod
    def _decoder(encoding: str):
        return codecs.getincrementaldecoder(encoding)(errors="replace")

    def run(
        self,
        command: str,
        warn: bool = False,
        encoding: str = "utf-8",
        hide=True,
        watchers=None,
        env=None,
        replace_env=False,
        in_stream=False,
        timeout=None,
    ) -> Result:
        """Run a command, wait for it to end and return its `Result`, with the semantics of the libssh2 client."""
        if timeout is None:
            timeout = self.timings.read_command_output_timeout
        result = Result(
            command=command,
            encoding=encoding,
            env=env,
            hide=("stderr", "stdout") if hide else (),
            pty=False,
            exited=None,
            shell=(env or {}).get("SHELL", "/bin/bash"),
            stdout="",
            stderr="",
        )
        if self.aborted:
            raise CommandAborted(f"Command was not run, connection to {self.host} was aborted: {command}")
        try:
            if not self._registered:
                self.connect()
        except Exception as exc:  # noqa: BLE001
            raise FailedToRunCommand(result, exc) from exc

        lines = queue.Queue() if watchers else None
        future = self._running = submit(self._arun(command, encoding, env, timeout, lines), self.route)
        wait = timeout + LOOP_WAIT_GRACE if timeout else None
        try:
            if watchers:
                self._feed_watchers(future, lines, watchers, wait)
            exited, result.stdout, result.stderr, timed_out = future.result(timeout=wait)
        except Exception as exc:  # noqa: BLE001
            future.cancel()
            if self.aborted:
                raise CommandAborted(f"Command was aborted, connection to {self.host} is aborted: {command}") from exc
            if isinstance(exc, asyncssh.ChannelOpenError):
                raise FailedToRunCommand(result, exc) from exc
            # a dead shared connection is replaced on the next acquire; drop the handle as the libssh2 client does
            self.disconnect()
            raise FailedToReadCommandOutput(result, exc) from exc
        finally:
            self._running = None
        result.exited = exited
        if timed_out:
            raise CommandTimedOut(result, timeout)
        if not warn and exited != 0:
            raise UnexpectedExit(result)
        return result

    def _feed_watchers(self, future: Future, lines: queue.Queue, watchers, wait: float | None) -> None:
        # an end marker after the last line: waiting on the queue alone would add a poll interval to every command
        future.add_done_callback(lambda _: lines.put(None))
        end_time = perf_counter() + wait if wait else None
        while True:
            if end_time is not None and perf_counter() > end_time:
                raise FutureTimeoutError()
            try:
                line = lines.get(timeout=self.timings.interactive_read_data_chunk_timeout)
            except queue.Empty:
                continue
            if line is None:
                return
            for watcher in watchers:
                watcher.submit_line(line)

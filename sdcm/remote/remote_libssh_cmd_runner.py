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
# Copyright (c) 2020 ScyllaDB

import os
import threading
import time
import socket

from .libssh2_client import Client as LibSSH2Client, Timings
from .libssh2_client.exceptions import (
    AuthenticationException,
    UnknownHostException,
    ConnectError,
    FailedToReadCommandOutput,
    CommandTimedOut,
    FailedToRunCommand,
    OpenChannelTimeout,
    SocketRecvError,
    UnexpectedExit,
    Failure,
)
from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent

from .base import RetryableNetworkException
from .remote_base import RemoteCmdRunnerBase

# Lightweight minicloud guest that cannot open an SSH channel is likely out of memory to fork sshd.
# SCT retries past it, so the failure may only show up later on a non-retried command
MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD = 10


class RemoteLibSSH2CmdRunner(RemoteCmdRunnerBase, ssh_transport="libssh2", default=True):
    """Remoter that mimic RemoteCmdRunner, under the hood it runs libssh2 client, instead of paramiko
    Main problem in libssh2 - is that it is not thread safe, we mitigate this problem by having
      _connection_thread_map - a dictionary in which we bind thread to the libssh2 session.
    Whenever remoter read self.connection, we return value from _connection_thread_map associated with current thread,
      And if it is not there, we create it.
    """

    connection: LibSSH2Client
    exception_unexpected = UnexpectedExit
    exception_failure = Failure
    exception_retryable = (
        # Exceptions that are not signaling about
        AuthenticationException,
        UnknownHostException,
        ConnectError,
        FailedToReadCommandOutput,
        CommandTimedOut,
        FailedToRunCommand,
        OpenChannelTimeout,
        SocketRecvError,
        socket.timeout,
    )
    _minicloud_channel_timeouts: dict[str, int] = {}
    _minicloud_channel_timeouts_lock = threading.Lock()
    _minicloud_channel_timeout_alerted = False

    def _create_connection(self) -> LibSSH2Client:
        return LibSSH2Client(
            host=self.hostname,
            user=self.user,
            port=self.port,
            pkey=os.path.expanduser(self.key_file) if self.key_file else None,
            timings=Timings(keepalive_timeout=0, connect_timeout=self.connect_timeout),
        )

    def is_up(self, timeout: float = 30) -> bool:
        end_time = time.perf_counter() + timeout
        while time.perf_counter() <= end_time:
            try:
                if self.connection.check_if_alive(timeout):
                    return True
            except Exception:  # noqa: BLE001
                try:
                    self.connection.close()
                    self.connection.open(timeout)
                except Exception:  # noqa: BLE001
                    pass
        return False

    def _record_minicloud_channel_timeout(self) -> None:
        """Surface repeated SSH channel timeouts across the minicloud guests as memory starvation."""
        from sdcm.test_config import TestConfig  # noqa: PLC0415 - circular import avoidance
        from sdcm.utils.minicloud.endpoint import is_minicloud_active  # noqa: PLC0415

        params = getattr(TestConfig().tester_obj(), "params", None)
        if not is_minicloud_active(params):
            return

        cls = RemoteLibSSH2CmdRunner
        with cls._minicloud_channel_timeouts_lock:
            counts = cls._minicloud_channel_timeouts
            counts[self.hostname] = counts.get(self.hostname, 0) + 1

            # the alert is based on the cluster-wide timeout total, not per guest
            total = sum(counts.values())
            if total < MINICLOUD_CHANNEL_TIMEOUT_ALERT_THRESHOLD or cls._minicloud_channel_timeout_alerted:
                return
            cls._minicloud_channel_timeout_alerted = True
            breakdown = ", ".join(f"{host}={hits}" for host, hits in sorted(counts.items()))

        TestFrameworkEvent(
            source="RemoteLibSSH2CmdRunner",
            source_method="_record_minicloud_channel_timeout",
            message=(
                f"{total} SSH channel timeouts across the minicloud guests ({breakdown}) - they are short of memory "
                "to fork sshd. SCT retries them, but lack of resources may kill other commands that are not retried. "
                "Consider reducing number of guests, or give guest OS more memory with minicloud_scylla_reserve_memory."
            ),
            severity=Severity.WARNING,
        ).publish_or_dump()

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool, suppress_errors: bool = False) -> bool:
        if not suppress_errors:
            self.log.error(exc, exc_info=exc)
        inner = exc.exception if isinstance(exc, FailedToRunCommand) else exc
        if isinstance(inner, OpenChannelTimeout):
            self._record_minicloud_channel_timeout()
        if isinstance(exc, FailedToRunCommand) and not new_session:
            self.log.debug("Reestablish the session...")
            try:
                self.connection.disconnect()
            except Exception:  # noqa: BLE001
                pass
            try:
                self.connection.connect()
            except Exception:  # noqa: BLE001
                pass
        if self._is_error_retryable(str(exc)) or isinstance(exc, self.exception_retryable):
            raise RetryableNetworkException(str(exc), original=exc)
        return True

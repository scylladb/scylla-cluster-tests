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

import os
import socket
import time

import asyncssh

from .asyncssh_client import Client as AsyncSSHClient
from .base import RetryableNetworkException
from .libssh2_client import Timings
from .libssh2_client.exceptions import (
    AuthenticationException,
    CommandTimedOut,
    ConnectError,
    ConnectTimeout,
    FailedToReadCommandOutput,
    FailedToRunCommand,
    Failure,
    UnexpectedExit,
)
from .remote_base import RemoteCmdRunnerBase


class RemoteAsyncSSHCmdRunner(RemoteCmdRunnerBase, ssh_transport="asyncssh"):
    """Remoter on asyncssh: a pure Python SSH client, so no native SSH package to build on a new CPython.

    Keeps the per-thread connection model of `RemoteCmdRunnerBase`; all connections share one event loop thread.
    """

    connection: AsyncSSHClient
    exception_unexpected = UnexpectedExit
    exception_failure = Failure
    exception_retryable = (
        AuthenticationException,
        ConnectError,
        ConnectTimeout,
        FailedToReadCommandOutput,
        CommandTimedOut,
        FailedToRunCommand,
        asyncssh.ConnectionLost,
        socket.timeout,
    )

    def _create_connection(self) -> AsyncSSHClient:
        proxy = None
        if self.proxy_host:
            proxy = AsyncSSHClient(
                host=self.proxy_host,
                user=self.proxy_user,
                port=self.proxy_port,
                pkey=os.path.expanduser(self.proxy_key) if self.proxy_key else None,
                password=self.proxy_password,
                timings=Timings(connect_timeout=self.connect_timeout),
            )
        return AsyncSSHClient(
            host=self.hostname,
            user=self.user,
            port=self.port,
            pkey=os.path.expanduser(self.key_file) if self.key_file else None,
            password=self.password,
            timings=Timings(connect_timeout=self.connect_timeout),
            proxy=proxy,
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

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool, suppress_errors: bool = False) -> bool:
        if not suppress_errors:
            self.log.error(exc, exc_info=exc)
        if isinstance(exc, FailedToRunCommand) and not new_session:
            self.log.debug("Reestablish the session...")
            self.connection.disconnect()
        if self._is_error_retryable(str(exc)) or isinstance(exc, self.exception_retryable):
            raise RetryableNetworkException(str(exc), original=exc)
        return True

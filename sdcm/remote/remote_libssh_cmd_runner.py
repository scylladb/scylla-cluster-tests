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

from typing import Optional, List
import os
import time
import threading
import socket

from invoke.watchers import StreamWatcher

from sdcm.utils.decorators import retrying

from .libssh2_client import Client as LibSSH2Client, Timings
from .libssh2_client.exceptions import AuthenticationException, UnknownHostException, ConnectError, \
    FailedToReadCommandOutput, CommandTimedOut, FailedToRunCommand, OpenChannelTimeout, SocketRecvError, \
    UnexpectedExit, Failure
from .libssh2_client.result import Result
from .base import RetryableNetworkException
from .remote_base import RemoteCmdRunnerBase


class RemoteLibSSH2CmdRunner(RemoteCmdRunnerBase, ssh_transport='libssh2'):  # pylint: disable=too-many-instance-attributes
    """Remoter that mimic RemoteCmdRunner, under the hood it runs libssh2 client, instead of paramiko
    Main problem in libssh2 - is that it is not thread safe, we mitigate this problem by having
      _connection_thread_map - a dictionary in which we bind thread to the libssh2 session.
    Whenever remoter read self.connection, we return value from _connection_thread_map associated with current thread,
      And if it is not there, we create it.
    """
    connection_thread_map = threading.local()
    exception_unexpected = UnexpectedExit
    exception_failure = Failure
    exception_retryable = (
        # Exceptions that are not signaling about
        AuthenticationException, UnknownHostException, ConnectError, FailedToReadCommandOutput,
        CommandTimedOut, FailedToRunCommand, OpenChannelTimeout, SocketRecvError, socket.timeout
    )

    @property
    def connection(self) -> LibSSH2Client:
        """
        Map connection to current thread.
        If there is no such thread, create it.
        """
        connection = getattr(self.connection_thread_map, str(id(self)), None)
        if connection is None:
            connection = self._create_connection()
            setattr(self.connection_thread_map, str(id(self)), connection)
        return connection

    def _create_connection(self) -> LibSSH2Client:
        return LibSSH2Client(
            host=self.hostname,
            user=self.user,
            port=self.port,
            pkey=os.path.expanduser(self.key_file),
            timings=Timings(keepalive_timeout=300, connect_timeout=self.connect_timeout)
        )

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException,))
    def run(self, cmd: str, timeout: Optional[float] = None, ignore_status: bool = False,  # pylint: disable=too-many-arguments
            verbose: bool = True, new_session: bool = False, log_file: Optional[str] = None, retry: int = 1,
            watchers: Optional[List[StreamWatcher]] = None) -> Result:

        watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        @retrying(n=retry)
        def _run():
            try:
                if verbose:
                    self.log.debug('Running command "%s"...', cmd)
                start_time = time.perf_counter()
                command_kwargs = dict(
                    command=cmd, warn=ignore_status,
                    encoding='utf-8', hide=True,
                    watchers=watchers, timeout=timeout,
                    in_stream=False
                )
                if new_session:
                    with self._create_connection() as connection:
                        result = connection.run(**command_kwargs)
                else:
                    result = self.connection.run(**command_kwargs)
                result.duration = time.perf_counter() - start_time
                result.exit_status = result.exited
                return result
            except self.exception_retryable as ex:
                self.log.error(ex)
                if isinstance(ex, FailedToRunCommand) and not new_session:
                    self.log.debug('Reestablish the session...')
                    try:
                        self.connection.disconnect()
                    except:  # pylint: disable=bare-except
                        pass
                    try:
                        self.connection.connect()
                    except:  # pylint: disable=bare-except
                        pass
                if self._is_error_retryable(str(ex)) or isinstance(ex, self.exception_retryable):
                    raise RetryableNetworkException(str(ex), original=ex)
                raise
            except Exception as details:  # pylint: disable=broad-except
                if hasattr(details, "result"):
                    self._print_command_results(details.result, verbose, ignore_status)  # pylint: disable=no-member
                raise

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        return result

    def is_up(self, timeout: float = 30) -> bool:
        end_time = time.perf_counter() + timeout
        while time.perf_counter() <= end_time:
            try:
                if self.connection.check_if_alive(timeout):
                    return True
            except:  # pylint: disable=bare-except
                try:
                    self.connection.close()
                    self.connection.open(timeout)
                except:  # pylint: disable=bare-except
                    pass
        return False

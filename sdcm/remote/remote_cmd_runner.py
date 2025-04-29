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
import threading

from fabric import Connection, Config
from paramiko import SSHException
from paramiko.ssh_exception import NoValidConnectionsError, AuthenticationException
from invoke.watchers import StreamWatcher
from invoke.exceptions import UnexpectedExit, Failure

from .base import RetryableNetworkException, SSHConnectTimeoutError
from .remote_base import RemoteCmdRunnerBase


class RemoteCmdRunner(RemoteCmdRunnerBase, ssh_transport='fabric', default=True):
    connection: Connection
    ssh_config: Config = None
    ssh_is_up: threading.Event = None
    ssh_up_thread: Optional[threading.Thread] = None
    ssh_up_thread_termination: threading.Event = None
    exception_unexpected = UnexpectedExit
    exception_failure = Failure
    exception_retryable = (
        NoValidConnectionsError, SSHException, EOFError, AuthenticationException, ConnectionResetError,
        ConnectionAbortedError, ConnectionError, ConnectionRefusedError
    )

    def _prepare(self):
        self.ssh_is_up = threading.Event()
        self.ssh_up_thread_termination = threading.Event()
        self.ssh_config = Config(overrides={
            'load_ssh_config': False,
            'UserKnownHostsFile': self.known_hosts_file,
            'ServerAliveInterval': 300,
            'StrictHostKeyChecking': 'no',
            # NOTE: 'gateway' define explicitely to avoid errors reaching the 'gateway' config attr
            'gateway': None,
        })
        self.start_ssh_up_thread()
        super()._prepare()

    def _ssh_ping(self) -> bool:
        try:
            # creating new connection each time in order not to interfere the main connection to decrease probability
            # of the EOF bug https://github.com/paramiko/paramiko/issues/1584
            with self._create_connection() as connection:
                result = connection.run("true", timeout=30, warn=False, encoding='utf-8', hide=True)
                return result.ok
        except AuthenticationException as auth_exception:
            # in order not to overwhelm SSH server with connection attempts that can cause further connection drops
            # we will slow down our tries. We need this due to "MaxStartups 10:30:100" that is default for sshd
            self.log.debug("%s: sleeping %s seconds before next retry", auth_exception, self.auth_sleep_time)
            self.ssh_up_thread_termination.wait(self.auth_sleep_time)
            return False
        except Exception as details:  # noqa: BLE001
            self.log.debug(details)
            return False

    def ssh_ping_thread(self):
        while not self.ssh_up_thread_termination.is_set():
            result = self._ssh_ping()
            if result:
                self.ssh_is_up.set()
            else:
                self.ssh_is_up.clear()
            self.ssh_up_thread_termination.wait(5)

    def start_ssh_up_thread(self):
        self.ssh_up_thread = threading.Thread(target=self.ssh_ping_thread, name='SSHPingThread', daemon=True)
        self.ssh_up_thread.start()

    def stop_ssh_up_thread(self):
        self.ssh_up_thread_termination.set()
        if self.ssh_up_thread:
            self.ssh_up_thread.join(5)
            self.ssh_up_thread = None

    def is_up(self, timeout: float = 30) -> bool:
        return self.ssh_is_up.wait(float(timeout))

    def stop(self):
        if self.ssh_is_up:
            self.ssh_is_up.clear()
        self.stop_ssh_up_thread()
        super().stop()

    def _run_pre_run(self, cmd: str, timeout: Optional[float] = None,
                     ignore_status: bool = False, verbose: bool = True, new_session: bool = False,
                     log_file: Optional[str] = None, retry: int = 1, watchers: Optional[List[StreamWatcher]] = None):
        if not self.is_up(timeout=self.connect_timeout):
            raise SSHConnectTimeoutError(
                'Unable to run "%s": failed connecting to "%s" during %ss' %
                (cmd, self.hostname, self.connect_timeout)
            )

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool) -> bool:
        self.log.error(exc)
        self.ssh_is_up.clear()
        if self._is_error_retryable(str(exc)):
            raise RetryableNetworkException(str(exc), original=exc)
        return True

    def _create_connection(self) -> Connection:
        gateway_connection = None
        if self.proxy_host:
            gateway_connection = Connection(
                host=self.proxy_host,
                user=self.proxy_user,
                port=self.proxy_port,
                config=self.ssh_config,
                connect_timeout=self.connect_timeout,
                connect_kwargs={
                    "key_filename": os.path.expanduser(self.proxy_key) if self.proxy_key else None
                }
            )

        return Connection(
            host=self.hostname,
            user=self.user,
            port=self.port,
            config=self.ssh_config,
            connect_timeout=self.connect_timeout,
            connect_kwargs={
                "key_filename": os.path.expanduser(self.key_file),
            } if self.key_file else None,
            gateway=gateway_connection
        )

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
import getpass
import socket
from fabric import Connection
from invoke.exceptions import UnexpectedExit, Failure
from invoke.runners import Result
from invoke.watchers import StreamWatcher
from sdcm.utils.decorators import retrying
from .base import CommandRunner, RetryableNetworkException


class LocalCmdRunner(CommandRunner):
    def __init__(self, hostname: str = None, user: str = None):
        if hostname is None:
            hostname = socket.gethostname()
        if user is None:
            user = getpass.getuser()
        super().__init__(user=user, hostname=hostname)

    def get_init_arguments(self) -> dict:
        """
        Return instance parameters required to rebuild instance
        """
        return {'hostname': self.hostname, 'user': self.user}

    def _create_connection(self) -> Connection:
        return Connection(host=self.hostname, user=self.user)

    def is_up(self, timeout: float = None) -> bool:
        return True

    def run(self, cmd: str, timeout: Optional[float] = None, ignore_status: bool = False,
            verbose: bool = True, new_session: bool = False, log_file: Optional[str] = None, retry: int = 1,
            watchers: Optional[List[StreamWatcher]] = None, change_context: bool = False, timestamp_logs: bool = False) -> Result:

        if timestamp_logs:
            watchers = self._setup_watchers_with_timestamps(verbose=verbose, log_file=log_file, additional_watchers=watchers)
        else:
            watchers = self._setup_watchers(verbose=verbose, log_file=log_file, additional_watchers=watchers)

        # in `@retrying` retry==0 means retrying `sys.maxsize * 2 + 1`, we never want that
        if retry == 0:
            retry = 1

        @retrying(n=retry)
        def _run():

            start_time = time.perf_counter()
            if verbose:
                self.log.debug('Running command "%s"...', cmd)
            try:
                command_kwargs = dict(
                    command=cmd, warn=ignore_status,
                    encoding='utf-8',
                    hide=True,
                    watchers=watchers,
                    timeout=timeout,
                    env=os.environ, replace_env=True,
                    in_stream=False
                )
                if new_session:
                    with self._create_connection() as connection:
                        result = connection.local(**command_kwargs)
                else:
                    result = self.connection.local(**command_kwargs)
                result.duration = time.perf_counter() - start_time
                result.exit_status = result.exited
                return result

            except (Failure, UnexpectedExit) as details:
                if hasattr(details, "result"):
                    self._print_command_results(details.result, verbose, ignore_status)
                raise

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        return result

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException,))
    def receive_files(
            self, src: str, dst: str, delete_dst: bool = False, preserve_perm: bool = True,
            preserve_symlinks: bool = False, timeout: float = 300, sudo: bool = False) -> bool:
        if src == dst:
            return True
        sudo = 'sudo ' if sudo else ''
        return self.run(f"{sudo} cp {src} {dst}", timeout=timeout).ok

    @retrying(n=3, sleep_time=5, allowed_exceptions=(RetryableNetworkException,))
    def send_files(
            self, src: str, dst: str, delete_dst: bool = False, preserve_symlinks: bool = False, verbose: bool = False,
            timeout: float = 300, sudo: bool = False) -> bool:
        if src == dst:
            return True
        sudo = "sudo " if sudo else ""
        return self.run(f"{sudo} cp {src} {dst}", timeout=timeout).ok

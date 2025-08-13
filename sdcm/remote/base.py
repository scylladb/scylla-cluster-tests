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

from typing import Optional, List, Callable
from abc import abstractmethod, ABCMeta
import shlex
import logging
import re
import os
import subprocess
from textwrap import dedent

from invoke.watchers import StreamWatcher, Responder
from invoke.runners import Result
from fabric import Connection


class OutputCheckError(Exception):
    """
    Remote command output check failed.
    """


class SSHConnectTimeoutError(Exception):
    """
    Remote command output check failed.
    """


class RetryableNetworkException(Exception):
    """
        SSH protocol exception that can be safely retried
    """

    def __init__(self, *args, original):
        self.original = original
        super().__init__(*args)


class CommandRunner(metaclass=ABCMeta):
    _connection = None

    def __init__(self, hostname: str, user: str = 'root', password: str = None):
        self.hostname = hostname
        self.user = user
        self.password = password
        self.log = logging.getLogger(self.__class__.__name__)
        self._prepare()

    @property
    def connection(self) -> Connection:
        if self._connection is None:
            self._connection = self._create_connection()
        return self._connection

    def _prepare(self):
        pass

    def get_init_arguments(self) -> dict:
        """
        Return instance parameters required to rebuild instance
        """
        return {'hostname': self.hostname, 'user': self.user, 'password': self.password}

    @abstractmethod
    def is_up(self, timeout: Optional[float] = None) -> bool:
        """
        Return instance parameters required to rebuild instance
        """

    def __str__(self):
        return '{} [{}@{}]'.format(self.__class__.__name__, self.user, self.hostname)

    def _setup_watchers(self, verbose: bool, log_file: str, additional_watchers: list) -> List[StreamWatcher]:
        watchers = additional_watchers if additional_watchers else []
        if verbose:
            watchers.append(OutputWatcher(self.log, self.hostname))
        if log_file:
            watchers.append(LogWriteWatcher(log_file))
        return watchers

    # pylint: disable=too-many-arguments
    @abstractmethod
    def run(self,
            cmd: str,
            timeout: Optional[float] = None,
            ignore_status: bool = False,
            verbose: bool = True,
            new_session: bool = False,
            log_file: Optional[str] = None,
            retry: int = 1,
            watchers: Optional[List[StreamWatcher]] = None,
            change_context: bool = False,
            suppress_errors: bool = False
            ) -> Result:
        pass

    # pylint: disable=too-many-arguments
    def sudo(self,
             cmd: str,
             timeout: Optional[float] = None,
             ignore_status: bool = False,
             verbose: bool = True,
             new_session: bool = False,
             log_file: Optional[str] = None,
             retry: int = 1,
             watchers: Optional[List[StreamWatcher]] = None,
             user: Optional[str] = 'root') -> Result:
        if user != self.user:
            if user == 'root':
                cmd = f"sudo {cmd}"
            else:
                cmd = f"sudo -u {user} {cmd}"
        return self.run(cmd=cmd,
                        timeout=timeout,
                        ignore_status=ignore_status,
                        verbose=verbose,
                        new_session=new_session,
                        log_file=log_file,
                        retry=retry,
                        watchers=watchers)

    @abstractmethod
    def _create_connection(self):
        pass

    def _print_command_results(self, result: Result, verbose: bool, ignore_status: bool):
        """When verbose=True and ignore_status=True that means nothing will be printed in any case"""
        hostname = self.hostname
        if verbose and not result.failed:
            if result.stderr:
                self.log.debug('<%s>: STDERR: %s', hostname, result.stderr)

            self.log.debug('<%s>: Command "%s" finished with status %s', hostname, result.command, result.exited)
            return

        if verbose and result.failed and not ignore_status:
            self.log.error('<%s>: Error executing command: "%s"; Exit status: %s',
                           hostname, result.command, result.exited)
            if result.stdout:
                self.log.debug('<%s>: STDOUT: %s', hostname, result.stdout[-240:])
            if result.stderr:
                self.log.debug('<%s>: STDERR: %s', hostname, result.stderr)
            return

    @staticmethod
    def _is_error_retryable(err_str: str) -> bool:
        """Check that exception can be safely retried"""
        exceptions = ("Authentication timeout", "Error reading SSH protocol banner", "Timeout opening channel",
                      "Unable to open channel", "Key-exchange timed out waiting for key negotiation",
                      "ssh_exchange_identification: Connection closed by remote host", "No existing session",
                      "timed out",
                      )
        for exception_str in exceptions:
            if exception_str in err_str:
                return True
        return False

    @staticmethod
    def _scp_remote_escape(filename: str) -> str:
        """
        Escape special chars for SCP use.

        Bis-quoting has to be used with scp for remote files, "bis-quoting"
        as in quoting x 2. SCP does not support a newline in the filename.

        :param filename: the filename string to escape.

        :returns: The escaped filename string. The required englobing double
            quotes are NOT added and so should be added at some point by
            the caller.
        """
        escape_chars = r' !"$&' + "'" + r'()*,:;<=>?[\]^`{|}'

        new_name = []
        for char in filename:
            if char in escape_chars:
                new_name.append("\\%s" % (char,))
            else:
                new_name.append(char)

        return shlex.quote("".join(new_name))

    @staticmethod
    def _make_ssh_command(user: str = "root",  # pylint: disable=too-many-arguments
                          port: int = 22, opts: str = '', hosts_file: str = '/dev/null',
                          key_file: str = None, connect_timeout: float = 300, alive_interval: float = 300,
                          extra_ssh_options: str = '') -> str:
        assert isinstance(connect_timeout, int)
        ssh_full_path = subprocess.check_output(['which', 'ssh']).decode().strip()
        base_command = ssh_full_path
        base_command += " " + extra_ssh_options
        base_command += (" -a -x %s -o StrictHostKeyChecking=no "
                         "-o UserKnownHostsFile=%s -o BatchMode=yes "
                         "-o ConnectTimeout=%d -o ServerAliveInterval=%d "
                         "-l %s -p %d")
        if key_file is not None:
            base_command += ' -i %s' % os.path.expanduser(key_file)
        assert connect_timeout > 0  # can't disable the timeout
        return base_command % (opts, hosts_file, connect_timeout,
                               alive_interval, user, port)


class OutputWatcher(StreamWatcher):  # pylint: disable=too-few-public-methods
    def __init__(self, log: logging.Logger, hostname: str):
        super().__init__()
        self.hostname = hostname
        self.len = 0
        self.log = log

    def submit(self, stream: str) -> list:
        stream_buffer = stream[self.len:]

        while '\n' in stream_buffer:
            out_buf, rest_buf = stream_buffer.split('\n', 1)
            self.log.debug("<%s>: %s", self.hostname, out_buf)
            stream_buffer = rest_buf
        self.len = len(stream) - len(stream_buffer)
        return []

    def submit_line(self, line: str):
        self.log.debug("<%s>: %s", self.hostname, line.rstrip('\n'))


class LogWriteWatcher(StreamWatcher):  # pylint: disable=too-few-public-methods
    def __init__(self, log_file: str):
        super().__init__()
        self.len = 0
        self.log_file = log_file
        # open fail with line buffering, so prom stats would be as accuracte as possible
        # pylint: disable=consider-using-with
        self.file_object = open(self.log_file, "a+", encoding="utf-8", buffering=1)

    def submit(self, stream: str) -> list:
        stream_buffer = stream[self.len:]

        self.file_object.write(stream_buffer)

        self.len = len(stream)
        return []

    def submit_line(self, line: str):
        self.file_object.write(line)


class FailuresWatcher(Responder):
    def __init__(self, sentinel, callback=None, raise_exception=True):
        super().__init__(None, None)
        self.sentinel = sentinel
        self.failure_index = 0
        self.callback = callback
        self.raise_exception = raise_exception

    def process_all_matching_lines(self, stream, index):
        new_ = stream[index:]
        for line in new_.splitlines():
            if re.findall(self.sentinel, line, re.S):
                self._process_line(line)

    def submit(self, stream):
        index = getattr(self, "failure_index")
        # Also check stream for our failure sentinel
        # Error out if we seem to have failed after a previous response.
        if self.pattern_matches(stream, self.sentinel, "failure_index"):
            self.process_all_matching_lines(stream, index)
        return []

    def submit_line(self, line: str):
        line = line.rstrip('\n')
        if self.pattern_matches(line, self.sentinel, "failure_index"):
            self._process_line(line)

    def _process_line(self, line):
        err = 'command failed found {!r} in \n{!r}'.format(self.sentinel, line)
        if callable(self.callback):
            self.callback(self.sentinel, line)
        if self.raise_exception:
            raise OutputCheckError(err)


def shell_script_cmd(cmd: str,
                     shell_cmd: str = "bash -cxe",
                     quote: str = '"',
                     preprocessor: Callable[[str], str] = dedent) -> str:
    return f"{shell_cmd} {quote}{preprocessor(cmd)}{quote}"

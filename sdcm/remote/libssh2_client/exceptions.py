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

import traceback

from ssh2.exceptions import SocketRecvError

from .result import Result


__all__ = (
    'AuthenticationException', 'UnknownHostException', 'ConnectError', 'ConnectTimeout', 'PKeyFileError',
    'UnexpectedExit', 'CommandTimedOut', 'FailedToReadCommandOutput', 'OpenChannelTimeout', 'Failure',
    'FailedToRunCommand', 'SocketRecvError'
)


class AuthenticationException(Exception):
    pass


class OpenChannelTimeout(Exception):
    pass


class UnknownHostException(Exception):
    pass


class ConnectError(Exception):
    pass


class ConnectTimeout(Exception):
    pass


class PKeyFileError(Exception):
    pass


class Failure(Exception):
    """
    A copy from invoke.exceptions.Failure
    Exception subclass representing failure of a command execution.

    "Failure" may mean the command executed and the shell indicated an unusual
    result (usually, a non-zero exit code), or it may mean something else, like
    a ``sudo`` command which was aborted when the supplied password failed
    authentication.

    Two attributes allow introspection to determine the nature of the problem:

    * ``result``: a `.Result` instance with info about the command being
      executed and, if it ran to completion, how it exited.
    * ``reason``: a wrapped exception instance if applicable (e.g. a
      `.StreamWatcher` raised `WatcherError`) or ``None`` otherwise, in which
      case, it's probably a `Failure` subclass indicating its own specific
      nature, such as `UnexpectedExit` or `CommandTimedOut`.

    This class is only rarely raised by itself; most of the time `.Runner.run`
    (or a wrapper of same, such as `.Context.sudo`) will raise a specific
    subclass like `UnexpectedExit` or `AuthFailure`.

    .. versionadded:: 1.0
    """

    def __init__(self, result: Result, reason: str = None):
        self.result = result
        self.reason = reason

    def streams_for_display(self) -> tuple:
        """
        Return stdout/err streams as necessary for error display.

        Subject to the following rules:

        - If a given stream was *not* hidden during execution, a placeholder is
          used instead, to avoid printing it twice.
        - Only the last 10 lines of stream text is included.
        - PTY-driven execution will lack stderr, and a specific message to this
          effect is returned instead of a stderr dump.

        :returns: Two-tuple of stdout, stderr strings.

        .. versionadded:: 1.3
        """
        already_printed = " already printed"
        if "stdout" not in self.result.hide:
            stdout = already_printed
        else:
            stdout = self.result.tail("stdout")
        if self.result.pty:
            stderr = " n/a (PTYs have no stderr)"
        elif "stderr" not in self.result.hide:
            stderr = already_printed
        else:
            stderr = self.result.tail("stderr")
        return stdout, stderr

    def __repr__(self) -> str:
        return self._repr()

    def _repr(self, **kwargs) -> str:
        """
        Return ``__repr__``-like value from inner result + any kwargs.
        """
        # TODO: expand?
        # TODO: truncate command?
        template = "<{}: cmd={!r}{}>"
        rest = ""
        if kwargs:
            rest = " " + " ".join(
                "{}={}".format(key, value) for key, value in kwargs.items()
            )
        return template.format(
            self.__class__.__name__, self.result.command, rest
        )


class UnexpectedExit(Failure):
    """
    A copy from invoke.exceptions.UnexpectedExit
    A shell command ran to completion but exited with an unexpected exit code.

    Its string representation displays the following:

    - Command executed;
    - Exit code;
    - The last 10 lines of stdout, if it was hidden;
    - The last 10 lines of stderr, if it was hidden and non-empty (e.g.
      pty=False; when pty=True, stderr never happens.)

    .. versionadded:: 1.0
    """

    def __str__(self) -> str:
        stdout, stderr = self.streams_for_display()
        command = self.result.command
        exited = self.result.exited
        template = """Encountered a bad command exit code!

Command: {!r}

Exit code: {}

Stdout:{}

Stderr:{}

"""
        return template.format(command, exited, stdout, stderr)

    def __repr__(self) -> str:
        return self._repr(exited=self.result.exited)


class CommandTimedOut(Failure):
    def __init__(self, result: Result, timeout: float):
        super().__init__(result)
        self.timeout = timeout

    def __repr__(self) -> str:
        return self._repr(timeout=self.timeout)

    def __str__(self) -> str:
        stdout, stderr = self.streams_for_display()
        command = self.result.command
        template = """Command did not complete within {} seconds!

Command: {!r}

Stdout:{}

Stderr:{}

"""
        return template.format(self.timeout, command, stdout, stderr)


class FailedToReadCommandOutput(Failure):
    """
    A copy from invoke.exceptions.FailedToReadCommandOutput
    A shell command has started, but it was failed to read it's final status.

    Its string representation displays the following:

    - Command executed;
    - Exit code;
    - The last 10 lines of stdout, if it was hidden;
    - The last 10 lines of stderr, if it was hidden and non-empty (e.g.
    - Traceback where it has failed;f
      pty=False; when pty=True, stderr never happens.)

    .. versionadded:: 1.0
    """

    def __init__(self, result: Result, exception: Exception):
        super().__init__(result)
        self.exception = exception

    def __repr__(self) -> str:
        return self._repr(exception=self.exception)

    def __str__(self) -> str:
        stdout, stderr = self.streams_for_display()
        command = self.result.command
        template = """Failed to read command output due to exception!

Command: {!r}

Stdout:{}

Stderr:{}

Exception:{}
{}
"""
        return template.format(command, stdout, stderr, ''.join(traceback.format_tb(self.exception.__traceback__)), self.exception)


class FailedToRunCommand(Failure):
    """
    A copy-cat from invoke.exceptions.Failed
    A command has not started.

    Its string representation displays the following:

    - Command executed;
    - Exit code;
    - The last 10 lines of stdout, if it was hidden;
    - The last 10 lines of stderr, if it was hidden and non-empty (e.g.
    - Traceback where it has failed;f
      pty=False; when pty=True, stderr never happens.)

    .. versionadded:: 1.0
    """

    def __init__(self, result: Result, exception: Exception):
        super().__init__(result)
        self.exception = exception

    def __repr__(self) -> str:
        return self._repr(exception=self.exception)

    def __str__(self) -> str:
        stdout, stderr = self.streams_for_display()
        command = self.result.command
        template = """Failed to run a command due to exception!

Command: {!r}

Stdout:{}

Stderr:{}

Exception:{}
{}
"""
        return template.format(command, stdout, stderr, ''.join(traceback.format_tb(self.exception.__traceback__)), self.exception)

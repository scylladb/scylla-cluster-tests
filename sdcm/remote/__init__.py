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

from .local_cmd_runner import LocalCmdRunner
from .remote_cmd_runner import RemoteCmdRunner
from .remote_base import RemoteCmdRunnerBase
from .base import FailuresWatcher, RetryableNetworkException, SSHConnectTimeoutError, shell_script_cmd

# RemoteLibSSH2CmdRunner requires ssh2-python which is not available on macOS
# due to build issues. Import it conditionally.
try:
    from .remote_libssh_cmd_runner import RemoteLibSSH2CmdRunner
    from .libssh2_client.exceptions import UnexpectedExit as Libssh2_UnexpectedExit
    from .libssh2_client.exceptions import Failure as Libssh2_Failure
    _LIBSSH2_AVAILABLE = True
except ImportError:
    RemoteLibSSH2CmdRunner = None
    # Provide dummy exceptions that will never be raised when libssh2 is not available
    # These are used in exception handlers throughout the codebase
    class Libssh2_UnexpectedExit(Exception):
        """Placeholder exception when ssh2-python is not available"""
        pass
    
    class Libssh2_Failure(Exception):
        """Placeholder exception when ssh2-python is not available"""
        pass
    
    _LIBSSH2_AVAILABLE = False


__all__ = (
    "LocalCmdRunner",
    "RemoteLibSSH2CmdRunner",
    "RemoteCmdRunner",
    "NETWORK_EXCEPTIONS",
    "LOCALRUNNER",
    "RemoteCmdRunnerBase",
    "FailuresWatcher",
    "RetryableNetworkException",
    "SSHConnectTimeoutError",
    "shell_script_cmd",
    "Libssh2_UnexpectedExit",
    "Libssh2_Failure",
)


NETWORK_EXCEPTIONS = (
    (SSHConnectTimeoutError, RetryableNetworkException)
    + (RemoteLibSSH2CmdRunner.get_retryable_exceptions() if _LIBSSH2_AVAILABLE else ())
    + RemoteCmdRunner.get_retryable_exceptions()
)
LOCALRUNNER = LocalCmdRunner()

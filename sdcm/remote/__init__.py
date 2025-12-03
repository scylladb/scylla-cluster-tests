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
from .remote_libssh_cmd_runner import RemoteLibSSH2CmdRunner
from .remote_base import RemoteCmdRunnerBase
from .base import FailuresWatcher, RetryableNetworkException, SSHConnectTimeoutError, shell_script_cmd


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
)


NETWORK_EXCEPTIONS = (
    (SSHConnectTimeoutError, RetryableNetworkException)
    + RemoteLibSSH2CmdRunner.get_retryable_exceptions()
    + RemoteCmdRunner.get_retryable_exceptions()
)
LOCALRUNNER = LocalCmdRunner()

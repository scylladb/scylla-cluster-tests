import time
import logging
from uuid import uuid4
from pathlib import Path
from contextlib import ExitStack

from sdcm.remote.remote_cmd_runner import RemoteCmdRunnerBase
from sdcm.remote.libssh2_client.result import Result
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit

logger = logging.getLogger(__name__)


def run_long_running_cmd(remoter: RemoteCmdRunnerBase,
                         cmd: str,
                         timeout: float | None = None,
                         ignore_status: bool = False,
                         verbose: bool = True,
                         retry: int = 0):
    """
    Run a long-running command on the remote host. The command is executed in the background and the function waits
    for the process of the command to finish.

    this function is useful for running commands that take a long time to finish, and we might get ssh disconnects during
    the command, this is designed to be able to keep on check on the remote process after the ssh reconnects.
    hence this function doesn't support retrying of the commands, and aiming at command that can be executed only once.
    i.e. nodetool decommission or nodetool darin

    The function returns the result of the command, same as remote.run() or remote.sudo() functions.
    """
    assert retry == 0, "retry is not supported for long running commands, always use it with retry=0"

    cmd_uuid = uuid4()
    cmd_stdout_log = Path('/tmp') / f'remoter_stdout_{cmd_uuid}.log'
    cmd_stderr_log = Path('/tmp') / f'remoter_stderr_{cmd_uuid}.log'
    cmd_exit_log = Path('/tmp') / f'remoter_exit_{cmd_uuid}.log'
    cmd_bash = Path('/tmp') / f'remoter_cmd_{cmd_uuid}.sh'

    with ExitStack() as stack:
        def clear_tmp_files():
            for log_file in (cmd_stdout_log, cmd_stderr_log, cmd_exit_log, cmd_bash):
                remoter.run(f'rm {log_file}', verbose=False, ignore_status=True)

        stack.callback(clear_tmp_files)
        remoter.run(f'echo "{cmd}" > {cmd_bash}')

        start_time = time.perf_counter()

        remote_command = f'(bash -o pipefail {cmd_bash} ; echo $? > {cmd_exit_log}) >& {cmd_stdout_log} 2> {cmd_stderr_log} </dev/null  &  echo $!'
        if verbose:
            logger.debug("<%s> execute run_long_running_cmd: %s", remoter.hostname, remote_command)
        pid = remoter.run(remote_command, retry=0, verbose=verbose).stdout

        remoter.run(f'while [ -e /proc/{pid.strip()} ]; do sleep 0.1; done', timeout=timeout, retry=10, verbose=False)

        stdout = str(remoter.run(f'cat {cmd_stdout_log}', verbose=False).stdout)
        stderr = str(remoter.run(f'cat {cmd_stderr_log}', verbose=False).stdout)
        exit_code = int(remoter.run(f'cat {cmd_exit_log}', verbose=False).stdout)
        result = Result(command=cmd, stdout=stdout,
                        stderr=stderr, exited=exit_code,
                        hide=('stderr', 'stdout'), pty=False)

        result.duration = time.perf_counter() - start_time
        result.exit_status = exit_code  # for compatibility with subprocess.run

        if not ignore_status and exit_code != 0:
            raise UnexpectedExit(result=result)

        return result

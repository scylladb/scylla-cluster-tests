import logging
from uuid import uuid4
from pathlib import Path

from sdcm.remote.remote_cmd_runner import RemoteCmdRunnerBase
from sdcm.remote.libssh2_client.result import Result
from sdcm.remote.libssh2_client.exceptions import UnexpectedExit

logger = logging.getLogger(__name__)


def run_long_running_cmd(remoter: RemoteCmdRunnerBase,
                         cmd: str,
                         timeout: float | None = None,
                         ignore_status: bool = False,
                         verbose: bool = True,
                         retry: int = 1):

    cmd_uuid = uuid4()
    cmd_stdout_log = Path('/tmp') / f'remoter_stdout_{cmd_uuid}.log'
    cmd_stderr_log = Path('/tmp') / f'remoter_stderr_{cmd_uuid}.log'
    cmd_exit_log = Path('/tmp') / f'remoter_exit_{cmd_uuid}.log'
    cmd_bash = Path('/tmp') / f'remoter_cmd_{cmd_uuid}.sh'

    remoter.run(f'echo "{cmd}" > {cmd_bash}')
    remote_command = f'(bash +e +x -o pipefail {cmd_bash} ; echo $? > {cmd_exit_log}) >& {cmd_stdout_log} 2> {cmd_stderr_log} </dev/null  &  jobs -p'
    if verbose:
        logger.debug("<%s> execute run_long_running_cmd: %s", remoter.hostname, remote_command)
    pid = remoter.run(remote_command, retry=0, verbose=verbose).stdout

    remoter.run(f'while [ -e /proc/{pid.strip()} ]; do sleep 0.1; done', timeout=timeout, retry=retry, verbose=False)

    stdout = remoter.run(f'cat {cmd_stdout_log}', verbose=False).stdout
    stderr = remoter.run(f'cat {cmd_stderr_log}', verbose=False).stdout
    exit_code = int(remoter.run(f'cat {cmd_exit_log}', verbose=False).stdout)
    result = Result(stdout=stdout, stderr=stderr, exited=exit_code)

    if not ignore_status and exit_code != 0:
        raise UnexpectedExit(result=result)

    for log_file in (cmd_stdout_log, cmd_stderr_log, cmd_exit_log, cmd_bash):
        remoter.run(f'rm {log_file}', verbose=False, ignore_status=True)

    return result

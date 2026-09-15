import time
import logging
from uuid import uuid4
from pathlib import Path
from contextlib import ExitStack

from invoke.exceptions import CommandTimedOut as InvokeCommandTimedOut

from sdcm.remote.remote_cmd_runner import RemoteCmdRunnerBase
from sdcm.remote.libssh2_client.result import Result
from sdcm.remote.libssh2_client.exceptions import CommandTimedOut, UnexpectedExit

logger = logging.getLogger(__name__)

# a timeout is a deadline, not a transient failure — it must never be retried
TIMEOUT_EXCEPTIONS = (CommandTimedOut, InvokeCommandTimedOut)
POLL_MAX_SSH_FAILURES = 10


def _poll_process(remoter: RemoteCmdRunnerBase, pid: str, cmd: str, deadline: float | None, timeout: float | None):
    """Wait for the remote process to exit, treating `deadline` as a hard wall-clock limit.

    An SSH disconnect resumes watching the same pid with the remaining time budget,
    while a timeout is a deadline, not a transient failure — it is never retried.
    """
    poll_cmd = f"while [ -e /proc/{pid.strip()} ]; do sleep 0.1; done"
    timed_out_result = Result(command=cmd, stdout="", stderr="", exited=-1, hide=("stderr", "stdout"), pty=False)
    ssh_failures = 0
    while True:
        remaining = deadline - time.perf_counter() if deadline else None
        if remaining is not None and remaining <= 0:
            raise CommandTimedOut(result=timed_out_result, timeout=timeout)
        try:
            remoter.run(poll_cmd, timeout=remaining, retry=0, verbose=False)
            return
        except TIMEOUT_EXCEPTIONS as exc:
            # report the actual command rather than the poll one-liner
            raise CommandTimedOut(result=timed_out_result, timeout=timeout) from exc
        except Exception:
            ssh_failures += 1
            if ssh_failures >= POLL_MAX_SSH_FAILURES:
                raise
            logger.warning(
                "<%s> polling long running command was interrupted (attempt %d/%d), reconnecting",
                remoter.hostname,
                ssh_failures,
                POLL_MAX_SSH_FAILURES,
            )
            time.sleep(5)


def run_long_running_cmd(
    remoter: RemoteCmdRunnerBase,
    cmd: str,
    timeout: float | None = None,
    ignore_status: bool = False,
    verbose: bool = True,
    retry: int = 0,
):
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
    cmd_stdout_log = Path("/tmp") / f"remoter_stdout_{cmd_uuid}.log"
    cmd_stderr_log = Path("/tmp") / f"remoter_stderr_{cmd_uuid}.log"
    cmd_exit_log = Path("/tmp") / f"remoter_exit_{cmd_uuid}.log"
    cmd_bash = Path("/tmp") / f"remoter_cmd_{cmd_uuid}.sh"

    with ExitStack() as stack:

        def clear_tmp_files():
            for log_file in (cmd_stdout_log, cmd_stderr_log, cmd_exit_log, cmd_bash):
                remoter.run(f"rm {log_file}", verbose=False, ignore_status=True)

        stack.callback(clear_tmp_files)
        remoter.run(f'echo "{cmd}" > {cmd_bash}')

        start_time = time.perf_counter()

        remote_command = f"(bash -o pipefail {cmd_bash} ; echo $? > {cmd_exit_log}) >& {cmd_stdout_log} 2> {cmd_stderr_log} </dev/null  &  echo $!"
        if verbose:
            logger.debug("<%s> execute run_long_running_cmd: %s", remoter.hostname, remote_command)
        pid = remoter.run(remote_command, retry=0, verbose=verbose).stdout

        _poll_process(remoter, pid, cmd, deadline=start_time + timeout if timeout else None, timeout=timeout)

        stdout = str(remoter.run(f"cat {cmd_stdout_log}", verbose=False).stdout)
        stderr = str(remoter.run(f"cat {cmd_stderr_log}", verbose=False).stdout)
        exit_code = int(remoter.run(f"cat {cmd_exit_log}", verbose=False).stdout)
        result = Result(
            command=cmd, stdout=stdout, stderr=stderr, exited=exit_code, hide=("stderr", "stdout"), pty=False
        )

        result.duration = time.perf_counter() - start_time
        result.exit_status = exit_code  # for compatibility with subprocess.run

        if not ignore_status and exit_code != 0:
            raise UnexpectedExit(result=result)

        return result

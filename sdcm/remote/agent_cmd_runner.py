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
# Copyright (c) 2025 ScyllaDB

import base64
import io
import logging
import os
import tarfile
import time
from typing import Optional, List
from invoke.runners import Result
from invoke.watchers import StreamWatcher

from sdcm.remote.base import CommandRunner, RetryableNetworkException, RetryMixin
from sdcm.utils.agent_client import AgentClient, AgentClientError, AgentConnectionError, AgentTimeoutError
from sdcm.utils.decorators import retrying


LOGGER = logging.getLogger(__name__)


class AgentCmdRunner(CommandRunner, RetryMixin):
    """
    Command runner that executes commands via SCT agent REST API.

    Example of usage:
        runner = AgentCmdRunner(hostname="192.168.1.10", api_key="secret", port=15000)
        result = runner.run("echo hello")
        print(result.stdout)  # "hello\\n"
    """

    exception_retryable = (AgentConnectionError, AgentTimeoutError)
    default_run_retry = 3

    def __init__(
            self, hostname: str, api_key: str, port: int = 15000, user: str = 'root', password: Optional[str] = None):
        """
        Initialize agent command runner.

        :param hostname: remote host name or IP address
        :param api_key: API key for agent authentication
        :param port: agent HTTP API port
        :param user: not used for agent (added for interface compatibility)
        :param password: not used for agent (added for interface compatibility)
        """
        super().__init__(hostname=hostname, user=user, password=password)
        self.port = port
        self.api_key = api_key
        self._agent_client = None

    def _create_connection(self) -> AgentClient:
        return AgentClient(self.hostname, self.api_key, self.port, 30)

    @property
    def agent_client(self) -> AgentClient:
        if not self._agent_client:
            self._agent_client = self._create_connection()
        return self._agent_client

    def is_up(self, timeout: Optional[float] = None) -> bool:
        try:
            return self.agent_client.is_healthy()
        except (AgentClientError, AgentConnectionError, AgentTimeoutError) as exc:
            self.log.debug("Agent health check failed: %s", exc)
            return False

    def _run_on_retryable_exception(self, exc: Exception, new_session: bool, suppress_errors: bool = False) -> bool:
        if not suppress_errors:
            self.log.error(exc, exc_info=exc)
        if isinstance(exc, self.exception_retryable):
            raise RetryableNetworkException(str(exc), original=exc)
        return True

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
            timestamp_logs: bool = False) -> Result:
        """
        Execute command on remote agent.

        :param cmd: command to execute
        :param timeout: command timeout in seconds
        :param ignore_status: if True, don't raise exception on non-zero exit
        :param verbose: if True, log command output
        :param new_session: not used for agent (added for interface compatibility)
        :param log_file: file to write output to (optional)
        :param retry: number of retries on failure
        :param watchers: stream watchers
        :param change_context: not used for agent (added for interface compatibility)
        :param timestamp_logs: not used for agent (added for interface compatibility)

        :return: result object with stdout, stderr, exit code
        """
        watcher_list = self._setup_watchers(verbose, log_file, watchers or [], timestamp_logs)

        @retrying(**self._get_retry_params(retry))
        def _run():
            start_time = time.perf_counter()
            try:
                if verbose:
                    self.log.debug("Executing command on %s: %s", self.hostname, cmd)

                job = self.agent_client.execute_command(
                    command="/bin/bash", args=["-c", cmd], timeout=int(timeout) if timeout else None)
                completed_job = self.agent_client.wait_for_job(
                    job.job_id, timeout=int(timeout or 600), poll_interval=0.5)

                result = Result(
                    stdout=completed_job.stdout,
                    stderr=completed_job.stderr,
                    exited=completed_job.exit_code if completed_job.exit_code is not None else 1,
                    command=cmd)
                result.duration = time.perf_counter() - start_time
                result.exit_status = result.exited

                if watcher_list:
                    for watcher in watcher_list:
                        if hasattr(watcher, 'submit'):
                            for line in (completed_job.stdout or "").splitlines(True):
                                watcher.submit(line)
                            for line in (completed_job.stderr or "").splitlines(True):
                                watcher.submit(line)

                return result

            except self.exception_retryable as exc:
                if self._run_on_retryable_exception(exc, new_session):
                    raise
            except AgentClientError as exc:
                self.log.error("Agent API error: %s", exc)
                if not ignore_status:
                    raise
                return Result(
                    stdout="", stderr=str(exc), exited=1, command=cmd,
                    duration=time.perf_counter() - start_time, exit_status=1)
            return None

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        if result.exited != 0 and not ignore_status:
            raise Exception(f"Command '{cmd}' failed with exit code {result.exited}")
        return result

    def receive_exit_status(self):
        pass

    def send_files(self, src: str, dst: str, delete_dst: bool = False, preserve_symlinks: bool = False,
                   verbose: bool = False, sudo: bool = False) -> bool:
        """
        Upload file or directory from local path to remote host using command execution.

        This is a temp implementation until proper file transfer API is implemented in post MVP stages.

        :param src: local file or directory path
        :param dst: remote destination path
        :param delete_dst: not used for agent yet (added for interface compatibility)
        :param preserve_symlinks: not used for agent yet (added for interface compatibility)
        :param verbose: enable verbose output
        :param sudo: use sudo to write the file

        :return: indication if operation was successful
        """
        sudo_prefix = "sudo " if sudo else ""
        if os.path.isdir(src):
            with tarfile.open(fileobj=(tar_buffer := io.BytesIO()), mode='w:gz') as tar:
                tar.add(src, arcname=os.path.basename(src))
            encoded = base64.b64encode(tar_buffer.getvalue()).decode('ascii')

            self.run(f"{sudo_prefix}mkdir -p {dst}", verbose=verbose, ignore_status=False)
            self.run(f"echo '{encoded}' | (cd {dst} && {sudo_prefix}base64 -d | {sudo_prefix}tar xzf -)",
                     verbose=verbose, ignore_status=False)
        else:
            with open(src, 'rb') as f:
                encoded = base64.b64encode(f.read()).decode('ascii')

            dst_path = os.path.join(dst, os.path.basename(src)) if dst.endswith('/') else dst
            self.run(f"{sudo_prefix}mkdir -p $(dirname {dst_path})", verbose=verbose, ignore_status=False)
            self.run(f"echo '{encoded}' | {sudo_prefix}base64 -d > {dst_path}", verbose=verbose, ignore_status=False)

        return True

    def receive_files(self, src: str, dst: str, delete_dst: bool = False, preserve_perm: bool = True,
                      preserve_symlinks: bool = False, timeout: float = 300, sudo: bool = False) -> bool:
        """
        Download file from remote host to local path using command execution.

        This is a temp implementation until proper file transfer API is implemented in post MVP stages.

        :param src: remote file path
        :param dst: local destination path
        :param delete_dst: not used for agent yet (added for interface compatibility)
        :param preserve_perm: not used for agent yet (added for interface compatibility)
        :param preserve_symlinks: not used for agent yet (added for interface compatibility)
        :param timeout: command timeout
        :param sudo: Use sudo to read the file

        :return: indication if operation was successful
        """
        sudo_prefix = "sudo " if sudo else ""
        result = self.run(f"{sudo_prefix}cat {src}", timeout=timeout, ignore_status=False, verbose=False)

        os.makedirs(os.path.dirname(dst) or '.', exist_ok=True)
        with open(dst, 'w') as f:
            f.write(result.stdout)

        return True

    def ssh_debug_cmd(self) -> str:
        """Return debug command for interface compatibiliy (for agent, show API endpoint)"""
        return f"curl -H 'Authorization: Bearer {self.api_key}' http://{self.hostname}:{self.port}/health"

    def stop(self):
        """Close agent client connection"""
        self._agent_client = None

    def __repr__(self) -> str:
        return f"AgentCmdRunner(hostname={self.hostname}, port={self.port})"

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
import threading
import time
from typing import Dict, List, Optional

from invoke.runners import Result
from invoke.watchers import StreamWatcher

from sdcm.remote.base import CommandRunner, RetryableNetworkException, RetryMixin
from sdcm.utils.agent_client import (
    AgentAPIError,
    AgentClient,
    AgentClientError,
    AgentConnectionError,
    AgentTimeoutError,
)
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


class AgentCmdRunner(CommandRunner, RetryMixin):
    """
    Command runner that executes commands via SCT agent REST API.

    Example of usage:
        runner = AgentCmdRunner(hostname="192.168.1.10", api_key="secret", port=16000)
        result = runner.run("echo hello")
        print(result.stdout)  # "hello\\n"
    """

    exception_retryable = (AgentConnectionError, AgentTimeoutError)
    default_run_retry = 3

    def __init__(
        self, hostname: str, api_key: str, port: int = 16000, user: str = "root", password: Optional[str] = None
    ):
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
        self._streaming_jobs: Dict[str, Dict] = {}
        self._streaming_lock = threading.Lock()

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

    def _handle_streaming_job(
        self, job, cmd: str, watcher_list: List[StreamWatcher], verbose: bool, ignore_status: bool, start_time: float
    ) -> Result:
        """Handle a streaming command that runs indefinitely until cancelled"""
        with self._streaming_lock:
            self._streaming_jobs[job.job_id] = {"cmd": cmd, "job": job}

        if verbose:
            self.log.debug("Streaming job %s started for command: %s", job.job_id, cmd)

        poll_interval = 2.0
        last_stdout_len = last_stderr_len = 0
        current_job = job

        try:
            while True:
                time.sleep(poll_interval)

                try:
                    current_job = self.agent_client.get_job(job.job_id)
                except AgentAPIError as exc:
                    if exc.status_code == 404:
                        self.log.debug("Streaming job %s no longer exists (cancelled)", job.job_id)
                        break
                    raise

                for stream_name, last_len in [("stdout", last_stdout_len), ("stderr", last_stderr_len)]:
                    stream_data = getattr(current_job, stream_name, "")
                    if stream_data and len(stream_data) > last_len:
                        new_data = stream_data[last_len:]
                        if watcher_list:
                            for watcher in watcher_list:
                                if hasattr(watcher, "submit_line"):
                                    for line in new_data.splitlines(True):
                                        watcher.submit_line(line)
                        if stream_name == "stdout":
                            last_stdout_len = len(stream_data)
                        else:
                            last_stderr_len = len(stream_data)

                if current_job.status in ("completed", "failed", "cancelled"):
                    if verbose:
                        self.log.debug("Streaming job %s finished with status: %s", job.job_id, current_job.status)
                    break

        except AgentClientError as exc:
            self.log.warning("Streaming job %s encountered error: %s", job.job_id, exc)
        finally:
            with self._streaming_lock:
                self._streaming_jobs.pop(job.job_id, None)

        result = Result(
            stdout=current_job.stdout or "",
            stderr=current_job.stderr or "",
            exited=current_job.exit_code if current_job.exit_code is not None else 0,
            command=cmd,
        )
        result.duration = time.perf_counter() - start_time
        result.exit_status = result.exited

        return result

    def cancel_streaming_command(self, cmd_pattern: str) -> bool:
        """Cancel streaming command (like 'journalctl -f' that run indefinitely) matching the given pattern"""
        cancelled_any = False
        with self._streaming_lock:
            jobs_to_cancel = [job_id for job_id, info in self._streaming_jobs.items() if cmd_pattern in info["cmd"]]

        for job_id in jobs_to_cancel:
            try:
                self.agent_client.cancel_job(job_id)
                cancelled_any = True
                self.log.debug("Cancelled streaming job %s matching pattern '%s'", job_id, cmd_pattern)
            except AgentAPIError as exc:
                if exc.status_code == 404:
                    self.log.debug("Streaming job %s already finished", job_id)
                else:
                    self.log.warning("Failed to cancel streaming job %s: %s", job_id, exc)

        return cancelled_any

    def run(
        self,
        cmd: str,
        timeout: Optional[float] = None,
        ignore_status: bool = False,
        verbose: bool = True,
        new_session: bool = False,
        log_file: Optional[str] = None,
        retry: int = 1,
        watchers: Optional[List[StreamWatcher]] = None,
        change_context: bool = False,
        timestamp_logs: bool = False,
    ) -> Result:
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
                    command="/bin/bash", args=["-c", cmd], timeout=int(timeout) if timeout else None
                )

                # detect streaming commands
                is_streaming = log_file is not None
                if is_streaming:
                    return self._handle_streaming_job(job, cmd, watcher_list, verbose, ignore_status, start_time)

                completed_job = self.agent_client.wait_for_job(
                    job.job_id, timeout=int(timeout or 600), poll_interval=0.5
                )

                result = Result(
                    stdout=completed_job.stdout,
                    stderr=completed_job.stderr,
                    exited=completed_job.exit_code if completed_job.exit_code is not None else 1,
                    command=cmd,
                )
                result.duration = time.perf_counter() - start_time
                result.exit_status = result.exited

                if watcher_list:
                    for watcher in watcher_list:
                        if hasattr(watcher, "submit_line"):
                            for line in (completed_job.stdout or "").splitlines(True):
                                watcher.submit_line(line)
                            for line in (completed_job.stderr or "").splitlines(True):
                                watcher.submit_line(line)

                return result

            except self.exception_retryable as exc:
                if self._run_on_retryable_exception(exc, new_session):
                    raise
            except AgentClientError as exc:
                self.log.error("Agent API error: %s", exc)
                if not ignore_status:
                    raise
                result = Result(
                    stdout="",
                    stderr=str(exc),
                    exited=1,
                    command=cmd,
                )
                result.duration = time.perf_counter() - start_time
                result.exit_status = result.exited
                return result
            return None

        result = _run()
        self._print_command_results(result, verbose, ignore_status)
        if result.exited != 0 and not ignore_status:
            raise Exception(f"Command '{cmd}' failed with exit code {result.exited}")
        return result

    def receive_exit_status(self):
        pass

    def _write_large_content(self, content: str, remote_path: str, sudo_prefix: str, verbose: bool) -> None:
        """Write large content to remote file in chunks to avoid command length limitations"""
        chunk_size = 65536
        chunks = [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]

        if chunks:
            self.run(
                f"{sudo_prefix}cat > {remote_path} <<'EOF'\n{chunks[0]}\nEOF",
                verbose=verbose,
                ignore_status=False,
                timeout=300,
            )

        for chunk in chunks[1:]:
            self.run(
                f"{sudo_prefix}cat >> {remote_path} <<'EOF'\n{chunk}\nEOF",
                verbose=verbose,
                ignore_status=False,
                timeout=300,
            )

    def send_files(
        self,
        src: str,
        dst: str,
        delete_dst: bool = False,
        preserve_symlinks: bool = False,
        verbose: bool = False,
        sudo: bool = False,
    ) -> bool:
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
        src = os.path.expanduser(src)
        sudo_prefix = "sudo " if sudo else ""
        if os.path.isdir(src):
            with tarfile.open(fileobj=(tar_buffer := io.BytesIO()), mode="w:gz") as tar:
                tar.add(src, arcname=os.path.basename(src))
            encoded = base64.b64encode(tar_buffer.getvalue()).decode("ascii")

            self.run(f"{sudo_prefix}mkdir -p {dst}", verbose=verbose, ignore_status=False)
            tmp_file = f"/tmp/sct_agent_transfer_{os.getpid()}"
            self._write_large_content(encoded, tmp_file, sudo_prefix, verbose)
            self.run(
                f"cd {dst} && {sudo_prefix}base64 -d < {tmp_file} | {sudo_prefix}tar xzf - && {sudo_prefix}rm -f {tmp_file}",
                verbose=verbose,
                ignore_status=False,
                timeout=600,
            )
        else:
            with open(src, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("ascii")

            is_dir = self.run(f"test -d {dst}", verbose=verbose, ignore_status=True)
            dst_path = os.path.join(dst, os.path.basename(src)) if is_dir.exited == 0 else dst

            self.run(f"{sudo_prefix}mkdir -p $(dirname {dst_path})", verbose=verbose, ignore_status=False)
            tmp_file = f"/tmp/sct_agent_transfer_{os.getpid()}"
            self._write_large_content(encoded, tmp_file, sudo_prefix, verbose)
            self.run(
                f"{sudo_prefix}base64 -d < {tmp_file} > {dst_path} && {sudo_prefix}rm -f {tmp_file}",
                verbose=verbose,
                ignore_status=False,
                timeout=600,
            )

        return True

    def receive_files(
        self,
        src: str,
        dst: str,
        delete_dst: bool = False,
        preserve_perm: bool = True,
        preserve_symlinks: bool = False,
        timeout: float = 300,
        sudo: bool = False,
    ) -> bool:
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

        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))

        os.makedirs(os.path.dirname(dst) or ".", exist_ok=True)
        with open(dst, "w") as f:
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

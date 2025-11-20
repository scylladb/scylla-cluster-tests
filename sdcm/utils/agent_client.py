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

import logging
import time
from typing import Optional, Dict, List, Any
from dataclasses import dataclass
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


@dataclass
class AgentJob:
    """Command execution job of the SCT agent"""
    job_id: str
    command: str
    args: List[str]
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    error: str = ""
    duration_ms: int = 0
    working_dir: str = ""
    env: Dict[str, str] = None
    timeout: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AgentJob':
        return cls(
            job_id=data.get('job_id', ''),
            command=data.get('command', ''),
            args=data.get('args', []),
            status=data.get('status', ''),
            created_at=cls._parse_datetime(data.get('created_at')),
            started_at=cls._parse_datetime(data.get('started_at')),
            completed_at=cls._parse_datetime(data.get('completed_at')),
            exit_code=data.get('exit_code'),
            stdout=data.get('stdout', ''),
            stderr=data.get('stderr', ''),
            error=data.get('error', ''),
            duration_ms=data.get('duration_ms', 0),
            working_dir=data.get('working_dir', ''),
            env=data.get('env', {}),
            timeout=data.get('timeout', 0)
        )

    @staticmethod
    def _parse_datetime(dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string from API response"""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return None


class AgentClientError(Exception):
    pass


class AgentConnectionError(AgentClientError):
    pass


class AgentAPIError(AgentClientError):
    def __init__(self, message: str, status_code: Optional[int] = None, response_data: Optional[Dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_data = response_data or {}


class AgentTimeoutError(AgentClientError):
    pass


class AgentClient:
    """HTTP client for SCT agent API"""

    def __init__(self, hostname: str, api_key: str, port: int = 15000, timeout: int = 30):
        self.hostname = hostname
        self.port = port
        self.api_key = api_key
        self.base_url = f"http://{hostname}:{port}"
        self.timeout = timeout

        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST", "DELETE"])
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'SCT-Agent-Client/1.0'
        })

    def _make_request(self, method: str, url: str, operation_name: str, **kwargs) -> requests.Response:
        """
        Make an HTTP request with error handling

        :param method: HTTP method
        :param url: request URL
        :param operation_name: name of operation for error messages
        :param kwargs: arguments to pass to requests (json, params, etc.)

        :return: response object
        """
        try:
            response = self.session.request(method, url, timeout=self.timeout, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.ConnectionError as exc:
            raise AgentConnectionError(f"Cannot connect to agent at {self.base_url}: {exc}") from exc
        except requests.exceptions.Timeout as exc:
            raise AgentTimeoutError(f"{operation_name.capitalize()} timed out after {self.timeout}s") from exc
        except requests.exceptions.HTTPError as exc:
            error_data = exc.response.json() if exc.response.content else {}
            raise AgentAPIError(
                f"{operation_name.capitalize()} failed: {error_data.get('message', str(exc))}",
                status_code=exc.response.status_code,
                response_data=error_data
            ) from exc

    def health_check(self) -> Dict[str, Any]:
        """Request agent health status"""
        response = self._make_request('GET', f"{self.base_url}/health", "health check")
        return response.json()

    def is_healthy(self) -> bool:
        """Check if agent is healthy and responding"""
        try:
            return self.health_check().get('status') == 'healthy'
        except (AgentConnectionError, AgentAPIError, AgentTimeoutError):
            return False

    def execute_command(self, command: str, args: Optional[List[str]] = None,
                        working_dir: Optional[str] = None, env: Optional[Dict[str, str]] = None,
                        timeout: Optional[int] = None, tags: Optional[Dict[str, str]] = None) -> AgentJob:
        """
        Execute a command on the agent asynchronously.

        :param command: command to execute
        :param args: command arguments
        :param working_dir: working directory for command execution
        :param env: environment variables
        :param timeout: command timeout in seconds
        :param tags: custom tags for job tracking

        :return: AgentJob object with initial job status
        """
        payload = {
            'command': command,
            'args': args or [],
            **({'working_dir': working_dir} if working_dir else {}),
            **({'env': env} if env else {}),
            **({'timeout': timeout} if timeout else {}),
            **({'tags': tags} if tags else {})
        }

        response = self._make_request('POST', f"{self.base_url}/api/v1/commands",
                                      "command execution", json=payload)
        data = response.json()

        return AgentJob(
            job_id=data['job_id'],
            command=command,
            args=args or [],
            status=data['status'],
            created_at=AgentJob._parse_datetime(data['created_at']) or datetime.now())

    def get_job(self, job_id: str) -> AgentJob:
        """Get job status and results"""
        try:
            response = self._make_request('GET', f"{self.base_url}/api/v1/commands/{job_id}", "get job")
            return AgentJob.from_dict(response.json())
        except AgentAPIError as exc:
            if exc.status_code == 404:
                raise AgentAPIError(f"Job {job_id} not found", status_code=404) from exc
            raise

    def wait_for_job(self, job_id: str, timeout: int = 300, poll_interval: float = 1.0) -> AgentJob:
        """Wait for job to complete and return results"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            job = self.get_job(job_id)
            if job.status in ('completed', 'failed', 'cancelled'):
                return job
            time.sleep(poll_interval)

        raise AgentTimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

    def list_jobs(self, status: Optional[str] = None, limit: int = 100,
                  offset: int = 0) -> List[AgentJob]:
        """
        List jobs with optional filtering.

        :param status: filter by status (queued, running, completed, failed, cancelled)
        :param limit: max number of jobs to return
        :param offset: offset for pagination

        :return: list of AgentJob objects
        """
        params = {'limit': limit, 'offset': offset, **({'status': status} if status else {})}
        response = self._make_request('GET', f"{self.base_url}/api/v1/commands",
                                      "list jobs", params=params)
        data = response.json()
        return [AgentJob.from_dict(job_data) for job_data in data.get('commands', [])]

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job"""
        try:
            self._make_request('DELETE', f"{self.base_url}/api/v1/commands/{job_id}", "cancel job")
            return True
        except AgentAPIError as exc:
            if exc.status_code == 404:
                raise AgentAPIError(f"Job {job_id} not found", status_code=404) from exc
            raise

    def __repr__(self) -> str:
        return f"AgentClient(hostname={self.hostname}, port={self.port})"

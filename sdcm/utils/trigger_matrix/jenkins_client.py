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
# Copyright (c) 2026 ScyllaDB

"""Jenkins API access for the trigger matrix.

Every call that reaches Jenkins goes through `JenkinsClient`. Builds are scheduled over the
Script Console rather than the REST build API -- see `groovy` for why.

The client connects lazily: `__init__` does no I/O, and `_connect()` runs only after each
method's dry-run early return. A dry run therefore needs no Jenkins credentials, and a missing
credential still raises `JenkinsTriggerError` from the trigger call itself, where
`trigger_matrix()` catches it and records the job as failed.
"""

import fnmatch
import json
import logging
import os
import time
from dataclasses import dataclass

import jenkins as jenkins_lib
import requests

from sdcm.utils.trigger_matrix.constants import MAX_TRIGGER_RETRIES, RETRY_BACKOFF_BASE, WAIT_POLL_INTERVAL
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError
from sdcm.utils.trigger_matrix.groovy import _build_trigger_groovy
from sdcm.utils.trigger_matrix.models import BuildResult, _PendingWaitJob

logger = logging.getLogger(__name__)


@dataclass
class _ActiveBuild:
    """Internal: a build that has left the queue and is being polled to completion.

    The wait-phase counterpart of `_PendingWaitJob`, which covers the queue phase.

    `start_time` is passed in by the caller rather than defaulted through
    `default_factory=time.time`, which would resolve `time.time` once when this class body runs
    at import -- the tests fake the clock by swapping this module's `time`.
    """

    job_name: str
    build_number: int
    job_url_path: str
    collect_results: list[str]
    timeout: int
    fail_on_error: bool
    start_time: float


def _get_jenkins_client() -> tuple[jenkins_lib.Jenkins, str]:
    """Return (jenkins.Jenkins client, jenkins_url) using env vars or KeyStore fallback.

    The jenkins.Jenkins client handles CSRF crumb automatically via maybe_add_crumb().

    Priority:
    1. JENKINS_URL + JENKINS_USERNAME + JENKINS_API_TOKEN environment variables
    2. KeyStore().get_json("jenkins.json") — local developer or SCT-runner context
       (same pattern as aws_builder.py / gce_builder.py / oci_builder.py)

    Raises:
        JenkinsTriggerError: If neither source provides a valid URL or credentials.
    """
    jenkins_url = os.environ.get("JENKINS_URL", "").rstrip("/")
    username = os.environ.get("JENKINS_USERNAME", "")
    token = os.environ.get("JENKINS_API_TOKEN", "")

    if not jenkins_url or not token:
        try:
            from sdcm.keystore import KeyStore  # noqa: PLC0415 - cyclic import guard

            info = KeyStore().get_json("jenkins.json")
            jenkins_url = jenkins_url or info.get("url", "").rstrip("/")
            username = username or info.get("username", "")
            token = token or info.get("password", "")
        except Exception:  # noqa: BLE001 - KeyStore unavailable in some environments
            pass

    if not jenkins_url:
        raise JenkinsTriggerError("Jenkins URL not set: configure JENKINS_URL env var or jenkins.json in KeyStore")
    if not token:
        raise JenkinsTriggerError(
            "Jenkins API token not set: configure JENKINS_API_TOKEN env var or jenkins.json in KeyStore"
        )

    client = jenkins_lib.Jenkins(jenkins_url, username=username or None, password=token)
    return client, jenkins_url


class JenkinsClient:
    """Thin wrapper over python-jenkins for the calls the trigger matrix makes."""

    def __init__(self) -> None:
        self._client: jenkins_lib.Jenkins | None = None
        self._url: str = ""

    def _connect(self) -> tuple[jenkins_lib.Jenkins, str]:
        """Resolve credentials on first use, so constructing a client stays free of I/O."""
        if self._client is None:
            self._client, self._url = _get_jenkins_client()
        return self._client, self._url

    def _run_trigger_script(self, job_name: str, parameters: dict, return_queue_url: bool = False) -> str:
        """Run the trigger script with retries and return the payload Jenkins reported.

        Raises:
            JenkinsTriggerError: If Jenkins reports an error, or every attempt fails.
        """
        client, _ = self._connect()
        script = _build_trigger_groovy(job_name, parameters, return_queue_url=return_queue_url)

        for attempt in range(MAX_TRIGGER_RETRIES):
            try:
                result = client.run_script(script)
                if result and result.startswith("TRIGGERED:"):
                    return result[len("TRIGGERED:") :].strip()
                if result and result.startswith("ERROR:"):
                    raise JenkinsTriggerError(f"Failed to trigger {job_name}: {result[len('ERROR:') :].strip()}")
                # Unexpected output — treat as failure
                logger.warning("Unexpected script output for %s: %s", job_name, result)
            except jenkins_lib.JenkinsException as exc:
                logger.warning(
                    "Trigger attempt %d/%d for %s failed: %s", attempt + 1, MAX_TRIGGER_RETRIES, job_name, exc
                )

            if attempt < MAX_TRIGGER_RETRIES - 1:
                wait = RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.info("Retrying in %ds...", wait)
                time.sleep(wait)

        raise JenkinsTriggerError(f"Failed to trigger {job_name} after {MAX_TRIGGER_RETRIES} attempts")

    def trigger(self, job_name: str, parameters: dict, dry_run: bool = False) -> bool:
        """Trigger a Jenkins job and report whether it was accepted.

        Args:
            job_name: Full Jenkins job path.
            parameters: Parameters to pass to the job.
            dry_run: If True, log what would be triggered without making API calls.

        Returns:
            True if the job was triggered (or would be in dry-run), False on failure.
        """
        if dry_run:
            params_str = ", ".join(f"{k}={v}" for k, v in sorted(parameters.items()))
            logger.info("[DRY-RUN] Would trigger: %s with params: {%s}", job_name, params_str)
            return True

        # Connect first, outside the try: a missing credential is a configuration error that
        # has always propagated out of trigger_matrix(), not a per-job failure to report.
        self._connect()

        try:
            build_url = self._run_trigger_script(job_name, parameters)
        except JenkinsTriggerError as exc:
            logger.error("%s", exc)
            return False

        logger.info("Triggered: %s — %s", job_name, build_url)
        return True

    def trigger_with_queue(self, job_name: str, parameters: dict, dry_run: bool = False) -> str:
        """Trigger a Jenkins job and return its queue item URL, for `wait_for_builds`.

        Args:
            job_name: Full Jenkins job path.
            parameters: Parameters to pass to the job.
            dry_run: If True, simulate without triggering.

        Returns:
            Queue item URL string (empty string in dry-run mode).

        Raises:
            JenkinsTriggerError: If the job fails to trigger after retries.
        """
        if dry_run:
            params_str = ", ".join(f"{k}={v}" for k, v in sorted(parameters.items()))
            logger.info("[DRY-RUN] Would trigger (with wait): %s with params: {%s}", job_name, params_str)
            return ""

        queue_url = self._run_trigger_script(job_name, parameters, return_queue_url=True)
        logger.info("Triggered %s — %s (will wait for completion)", job_name, queue_url)
        return queue_url

    def wait_for_builds(
        self,
        pending_jobs: list[_PendingWaitJob],
        dry_run: bool = False,
        poll_interval: int = WAIT_POLL_INTERVAL,
    ) -> list[BuildResult]:
        """Wait for multiple triggered Jenkins builds to complete concurrently.

        Polls all pending builds in a single loop, completing them as they finish.

        Args:
            pending_jobs: List of pending jobs with queue URLs from trigger_jenkins_job_with_queue.
            dry_run: If True, return synthetic SUCCESS results.
            poll_interval: Seconds between poll cycles.

        Returns:
            List of BuildResult for all jobs.
        """
        if dry_run or not pending_jobs:
            return [BuildResult(job_name=p.job_name, build_number=0, result="SUCCESS") for p in pending_jobs]

        client, jenkins_url = self._connect()

        # Phase 1: Wait for all builds to leave the queue and get build numbers
        active_builds: list[_ActiveBuild] = []
        results: list[BuildResult] = []

        for pending in pending_jobs:
            try:
                build_number = self._wait_for_build_start(pending.queue_url, timeout=min(pending.timeout, 600))
                job_url_path = pending.job_name.replace("/", "/job/")
                active_builds.append(
                    _ActiveBuild(
                        job_name=pending.job_name,
                        build_number=build_number,
                        job_url_path=job_url_path,
                        collect_results=pending.collect_results,
                        timeout=pending.timeout,
                        fail_on_error=pending.fail_on_error,
                        start_time=time.time(),
                    )
                )
            except JenkinsTriggerError as exc:
                logger.error("Failed to get build number for %s: %s", pending.job_name, exc)
                job_url = f"{jenkins_url}/job/{pending.job_name.replace('/', '/job/')}"
                results.append(
                    BuildResult(job_name=pending.job_name, build_number=0, result="FAILURE", build_url=job_url)
                )

        logger.info("Waiting for %d builds to complete...", len(active_builds))

        # Phase 2: Poll all active builds until they all complete
        while active_builds:
            still_running = []
            for build in active_builds:
                elapsed = time.time() - build.start_time
                if elapsed > build.timeout:
                    logger.error(
                        "Timeout waiting for %s #%d after %ds", build.job_name, build.build_number, build.timeout
                    )
                    build_url = f"{jenkins_url}/job/{build.job_url_path}/{build.build_number}"
                    results.append(
                        BuildResult(
                            job_name=build.job_name,
                            build_number=build.build_number,
                            result="TIMEOUT",
                            build_url=build_url,
                        )
                    )
                    continue

                build_api_url = f"{jenkins_url}/job/{build.job_url_path}/{build.build_number}/api/json"
                try:
                    resp = client.jenkins_open(requests.Request("GET", build_api_url))
                    if resp:
                        build_data = json.loads(resp)
                        if not build_data.get("building", True):
                            result = build_data.get("result", "UNKNOWN")
                            build_url = f"{jenkins_url}/job/{build.job_url_path}/{build.build_number}"
                            logger.info(
                                "Build %s #%d completed: %s", str(build.job_name), build.build_number, str(result)
                            )
                            artifacts = self._collect_artifacts(
                                build.job_url_path, build.build_number, build.collect_results
                            )
                            results.append(
                                BuildResult(
                                    job_name=build.job_name,
                                    build_number=build.build_number,
                                    result=result,
                                    artifacts=artifacts,
                                    build_url=build_url,
                                )
                            )
                            continue
                except jenkins_lib.JenkinsException as exc:
                    logger.warning("Error polling %s #%d: %s", build.job_name, build.build_number, exc)

                still_running.append(build)

            active_builds = still_running
            if active_builds:
                time.sleep(poll_interval)

        return results

    def _wait_for_build_start(self, queue_url: str, timeout: int = 600) -> int:
        """Poll Jenkins queue item until a build number is assigned."""
        client, _ = self._connect()
        api_url = f"{queue_url.rstrip('/')}/api/json"
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                resp = client.jenkins_open(requests.Request("GET", api_url))
                if resp:
                    data = json.loads(resp)
                    if executable := data.get("executable"):
                        build_number = executable.get("number")
                        if build_number:
                            logger.info("Build started: #%d", build_number)
                            return int(build_number)
                    if data.get("cancelled"):
                        raise JenkinsTriggerError("Queued build was cancelled")
            except jenkins_lib.JenkinsException as exc:
                logger.warning("Error polling queue: %s", exc)
            time.sleep(5)

        raise JenkinsTriggerError(f"Build did not start within {timeout}s")

    def _collect_artifacts(self, job_url_path: str, build_number: int, patterns: list[str]) -> list[str]:
        """Return URLs for the build's artifacts whose *filename* matches one of `patterns`.

        Patterns are matched against the filename alone, never the relative path: "*.xml" finds
        `reports/results.xml`, while "reports/*.xml" matches nothing.
        """
        if not patterns:
            return []

        client, jenkins_url = self._connect()
        artifacts_url = f"{jenkins_url}/job/{job_url_path}/{build_number}/api/json?tree=artifacts[relativePath]"
        try:
            resp = client.jenkins_open(requests.Request("GET", artifacts_url))
            if not resp:
                logger.warning("Failed to fetch artifacts for build #%d", build_number)
                return []

            artifacts_data = json.loads(resp).get("artifacts", [])
            matched = []
            for artifact in artifacts_data:
                rel_path = artifact.get("relativePath", "")
                filename = rel_path.rsplit("/", 1)[-1] if "/" in rel_path else rel_path
                if any(fnmatch.fnmatch(filename, pat) for pat in patterns):
                    url = f"{jenkins_url}/job/{job_url_path}/{build_number}/artifact/{rel_path}"
                    matched.append(url)
                    logger.info("Collected artifact: %s", rel_path)

            return matched
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Error collecting artifacts for build #%d: %s", build_number, exc)
            return []

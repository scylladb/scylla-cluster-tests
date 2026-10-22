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
import re
import time
from dataclasses import dataclass

import jenkins as jenkins_lib
import requests
from botocore.exceptions import BotoCoreError, ClientError

from sdcm.keystore import KeyStore
from sdcm.utils.trigger_matrix.constants import MAX_TRIGGER_RETRIES, RETRY_BACKOFF_BASE, WAIT_POLL_INTERVAL
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError
from sdcm.utils.trigger_matrix.groovy import _build_trigger_groovy
from sdcm.utils.trigger_matrix.models import BuildResult, _PendingWaitJob

logger = logging.getLogger(__name__)


def _in_trigger_order(results: list[BuildResult], pending_jobs: list[_PendingWaitJob]) -> list[BuildResult]:
    """Sort build results in the order their jobs were triggered, not the order they finished in."""
    order = {(p.job_name, p.scylla_version): index for index, p in enumerate(pending_jobs)}
    return sorted(results, key=lambda r: order.get((r.job_name, r.scylla_version), len(order)))


@dataclass
class _QueuedBuild:
    """Internal: a triggered build that has not left the Jenkins queue yet.

    `queued_since` is passed in by the caller, for the reason given on `_ActiveBuild`.
    """

    pending: _PendingWaitJob
    queued_since: float
    queue_item_gone: bool = False


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
    scylla_version: str = ""


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
            info = KeyStore().get_json("jenkins.json")
            jenkins_url = jenkins_url or info.get("url", "").rstrip("/")
            username = username or info.get("username", "")
            token = token or info.get("password", "")
        except (BotoCoreError, ClientError, OSError, ValueError) as exc:
            # KeyStore is unavailable in some environments; the checks below report what's missing
            logger.debug("Could not read jenkins.json from KeyStore: %s", exc)

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

        Every poll cycle looks once at each build still in the queue and at each running one,
        so a build waiting for an agent never holds back the others. A build still queued after
        its job's wait_timeout is reported as NOT_STARTED; a running one gets the same timeout
        again, counted from its start.

        Args:
            pending_jobs: List of pending jobs with queue URLs from trigger_jenkins_job_with_queue.
            dry_run: If True, return synthetic SUCCESS results.
            poll_interval: Seconds between poll cycles.

        Returns:
            List of BuildResult for all jobs, in the order the jobs were triggered.
        """
        if dry_run or not pending_jobs:
            return [
                BuildResult(job_name=p.job_name, build_number=0, result="SUCCESS", scylla_version=p.scylla_version)
                for p in pending_jobs
            ]

        client, jenkins_url = self._connect()
        queued = [_QueuedBuild(pending=p, queued_since=time.time()) for p in pending_jobs]
        active_builds: list[_ActiveBuild] = []
        results: list[BuildResult] = []
        logger.info("Waiting for %d builds to start and complete...", len(queued))

        while queued or active_builds:
            still_queued = []
            for item in queued:
                if started := self._poll_queued_build(item, jenkins_url, results):
                    active_builds.append(started)
                elif started is None:
                    still_queued.append(item)
            queued = still_queued

            still_running = []
            for build in active_builds:
                if result := self._poll_active_build(client, jenkins_url, build):
                    results.append(result)
                else:
                    still_running.append(build)
            active_builds = still_running

            if queued or active_builds:
                time.sleep(poll_interval)

        return _in_trigger_order(results, pending_jobs)

    def _poll_queued_build(
        self, item: "_QueuedBuild", jenkins_url: str, results: list[BuildResult]
    ) -> "_ActiveBuild | None | bool":
        """One look at a queued build.

        Returns the started build, None while it is still queued, or False once it is done
        with: cancelled, lost, or queued for longer than its wait_timeout. A build it is done
        with has its result appended to `results`.
        """
        pending = item.pending
        job_url_path = pending.job_name.replace("/", "/job/")
        job_url = f"{jenkins_url}/job/{job_url_path}"
        if time.time() - item.queued_since > pending.timeout:
            logger.error(
                "FAILED: %s (%s) did not start within %ds", pending.job_name, pending.scylla_version, pending.timeout
            )
            result = "NOT_STARTED"
        else:
            try:
                build_number, item.queue_item_gone = self._check_build_start(
                    pending.queue_url, pending.job_name, item.queue_item_gone
                )
            except JenkinsTriggerError as exc:
                logger.error("Failed to get build number for %s: %s", pending.job_name, exc)
                result = "FAILURE"
            else:
                if not build_number:
                    return None
                logger.info("Build started: %s #%d", pending.job_name, build_number)
                return _ActiveBuild(
                    job_name=pending.job_name,
                    build_number=build_number,
                    job_url_path=job_url_path,
                    collect_results=pending.collect_results,
                    timeout=pending.timeout,
                    fail_on_error=pending.fail_on_error,
                    start_time=time.time(),
                    scylla_version=pending.scylla_version,
                )
        results.append(
            BuildResult(
                job_name=pending.job_name,
                build_number=0,
                result=result,
                build_url=job_url,
                scylla_version=pending.scylla_version,
            )
        )
        return False

    def _poll_active_build(self, client, jenkins_url: str, build: "_ActiveBuild") -> BuildResult | None:
        """One look at a running build: its result once it finished or timed out, else None."""
        build_url = f"{jenkins_url}/job/{build.job_url_path}/{build.build_number}"
        if time.time() - build.start_time > build.timeout:
            logger.error("Timeout waiting for %s #%d after %ds", build.job_name, build.build_number, build.timeout)
            return BuildResult(
                job_name=build.job_name,
                build_number=build.build_number,
                result="TIMEOUT",
                build_url=build_url,
                scylla_version=build.scylla_version,
            )

        try:
            resp = client.jenkins_open(requests.Request("GET", f"{build_url}/api/json"))
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Error polling %s #%d: %s", build.job_name, build.build_number, exc)
            return None
        if not resp:
            return None
        build_data = json.loads(resp)
        if build_data.get("building", True):
            return None

        result = build_data.get("result", "UNKNOWN")
        logger.info("Build %s #%d completed: %s", str(build.job_name), build.build_number, str(result))
        if result != "SUCCESS":
            logger.error(
                "FAILED: %s #%d (%s) — %s — %s",
                build.job_name,
                build.build_number,
                build.scylla_version,
                result,
                build_url,
            )
        return BuildResult(
            job_name=build.job_name,
            build_number=build.build_number,
            result=result,
            artifacts=self._collect_artifacts(build.job_url_path, build.build_number, build.collect_results),
            build_url=build_url,
            scylla_version=build.scylla_version,
            description=build_data.get("description") or "",
        )

    def _check_build_start(
        self, queue_url: str, job_name: str | None = None, queue_item_gone: bool = False
    ) -> tuple[int | None, bool]:
        """One look at whether a queued build has started.

        Jenkins forgets a queue item a few minutes after its build starts. Once the item is
        gone, only the builds of `job_name` are looked at, for the one with its queue id.

        Returns:
            The build number, or None while the build is still queued; and whether the queue
            item is gone, to pass back on the next call.

        Raises:
            JenkinsTriggerError: If the URL is not a queue item, the queued build was cancelled,
                or the queue item is gone and there is no job name to look the build up by.
        """
        if not (match := re.search(r"/queue/item/(\d+)/?$", queue_url)):
            raise JenkinsTriggerError(f"Not a queue item URL: {queue_url}")
        queue_id = int(match.group(1))
        if queue_item_gone:
            # The queue item will not come back: only the job's builds can tell
            return self._find_build_by_queue_id(job_name, queue_id), True

        client, _ = self._connect()
        try:
            resp = client.jenkins_open(requests.Request("GET", f"{queue_url.rstrip('/')}/api/json"))
            if resp:
                data = json.loads(resp)
                if (executable := data.get("executable")) and (build_number := executable.get("number")):
                    return int(build_number), False
                if data.get("cancelled"):
                    raise JenkinsTriggerError("Queued build was cancelled")
        except jenkins_lib.NotFoundException:
            if not job_name:
                raise JenkinsTriggerError(f"Queue item {queue_id} is gone") from None
            logger.info("Queue item %d is gone; looking for its build among the builds of %s", queue_id, job_name)
            # Look right away: Jenkins forgets a queue item minutes after its build started,
            # so the build is almost always in the job's build list already
            return self._find_build_by_queue_id(job_name, queue_id), True
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Error polling queue: %s", exc)
        return None, False

    def _wait_for_build_start(self, queue_url: str, job_name: str | None = None, timeout: int | None = None) -> int:
        """Poll a Jenkins queue item until a build number is assigned (see `_check_build_start`).

        Waits for as long as the build stays queued when `timeout` is None, and stops only
        when the queue item is cancelled.
        """
        queue_item_gone = False
        start_time = time.time()
        while timeout is None or time.time() - start_time < timeout:
            build_number, queue_item_gone = self._check_build_start(queue_url, job_name, queue_item_gone)
            if build_number:
                logger.info("Build started: #%d", build_number)
                return build_number
            time.sleep(5)

        raise JenkinsTriggerError(f"Build did not start within {timeout}s")

    def _find_build_by_queue_id(self, job_name: str, queue_id: int) -> int | None:
        """The number of the build of `job_name` that was queued as `queue_id`, if there is one."""
        client, jenkins_url = self._connect()
        job_url_path = job_name.replace("/", "/job/")
        api_url = f"{jenkins_url}/job/{job_url_path}/api/json?tree=builds[number,queueId]"
        try:
            resp = client.jenkins_open(requests.Request("GET", api_url))
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Error listing the builds of %s: %s", job_name, exc)
            return None
        for build in json.loads(resp or "{}").get("builds", []):
            if build.get("queueId") == queue_id:
                return int(build["number"])
        return None

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

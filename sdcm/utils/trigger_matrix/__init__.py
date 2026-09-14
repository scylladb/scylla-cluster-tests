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

import fnmatch
import json
import logging
import os
import time
from dataclasses import dataclass, field

import jenkins as jenkins_lib
import requests

from sdcm.utils.trigger_matrix.config import get_parameterized_cron, load_matrix_config
from sdcm.utils.trigger_matrix.constants import (
    DEFAULT_EMAIL_RECIPIENTS,
    MAX_TRIGGER_RETRIES,
    RETRY_BACKOFF_BASE,
    VERSION_RESOLUTION_STRATEGIES,
    VersionResolution,
    WAIT_POLL_INTERVAL,
)
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError, MatrixValidationError, TriggerMatrixError
from sdcm.utils.trigger_matrix.filters import filter_jobs
from sdcm.utils.trigger_matrix.images import resolve_image_architecture, resolve_scylla_version_from_image
from sdcm.utils.trigger_matrix.models import (
    BackendTarget,
    BuildResult,
    CronTriggerConfig,
    JenkinsfileEntry,
    JobConfig,
    MatrixConfig,
    _PendingWaitJob,
)
from sdcm.utils.trigger_matrix.parameters import build_job_parameters, resolve_job_path
from sdcm.utils.trigger_matrix.resolution import (
    job_uses_scylla_version,
    resolve_to_full_version,
    resolve_versions_for_targets,
    target_for_job,
)
from sdcm.utils.trigger_matrix.versions import determine_job_folder

__all__ = [
    "BackendTarget",
    "BuildResult",
    "CronTriggerConfig",
    "JenkinsTriggerError",
    "JenkinsfileEntry",
    "JobConfig",
    "MatrixConfig",
    "MatrixValidationError",
    "TriggerMatrixError",
    "VERSION_RESOLUTION_STRATEGIES",
    "VersionResolution",
    "determine_job_folder",
    "get_parameterized_cron",
    "load_matrix_config",
    "resolve_image_architecture",
    "resolve_scylla_version_from_image",
    "resolve_to_full_version",
    "trigger_matrix",
]

logger = logging.getLogger(__name__)


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


def _escape_groovy_string(value: str) -> str:
    """Escape a value for embedding in a Groovy single-quoted string literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_trigger_groovy(job_name: str, parameters: dict, return_queue_url: bool = False) -> str:
    """Build Groovy script that triggers a Jenkins job with UpstreamCause.

    Uses Jenkins Script Console to schedule a build, bypassing the REST API
    'Location' header issue (https://github.com/bndr/gojenkins/issues/248).
    Injects UpstreamCause so Jenkins UI shows 'Started by upstream project X build Y'.

    Args:
        job_name: Full Jenkins job path (slash-separated).
        parameters: Parameters to pass to the job.
        return_queue_url: If True, return queue item URL (for wait-mode polling).
            If False, return the job URL (fire-and-forget).

    Returns:
        Groovy script string.
    """
    escaped_job_name = _escape_groovy_string(job_name)
    params_groovy = ", ".join(
        f"new StringParameterValue('{_escape_groovy_string(k)}', '{_escape_groovy_string(str(v))}')"
        for k, v in parameters.items()
    )

    upstream_job = os.environ.get("JOB_NAME", "")
    upstream_build = os.environ.get("BUILD_NUMBER", "")

    if upstream_job and upstream_build:
        escaped_upstream = _escape_groovy_string(upstream_job)
        cause_section = f"""
def upstreamJob = Jenkins.instance.getItemByFullName('{escaped_upstream}')
def upstreamBuild = upstreamJob?.getBuildByNumber({upstream_build})
def cause
if (upstreamBuild) {{
    cause = new hudson.model.Cause.UpstreamCause((hudson.model.Run) upstreamBuild)
}} else {{
    cause = new hudson.model.Cause.RemoteCause('{escaped_upstream}', 'Build #{upstream_build} (upstream build not found)')
}}
"""
    else:
        cause_section = """
def cause = new hudson.model.Cause.RemoteCause('trigger-matrix', 'Triggered via SCT trigger-matrix CLI')
"""

    if return_queue_url:
        # Return the queue item URL for wait-mode polling via _wait_for_build_start()
        output_section = """
def rootUrl = Jenkins.instance.rootUrl ?: ''
def queueItem = Jenkins.instance.queue.getItems().find { it.task == targetJob }
if (queueItem) {
    println "TRIGGERED:${rootUrl}queue/item/${queueItem.id}/"
} else {
    println "TRIGGERED:${targetJob.getAbsoluteUrl()}"
}
"""
    else:
        output_section = """
println "TRIGGERED:${targetJob.getAbsoluteUrl()}"
"""

    return f"""
import jenkins.model.Jenkins
import hudson.model.*

def targetJob = Jenkins.instance.getItemByFullName('{escaped_job_name}')
if (!targetJob) {{
    println "ERROR: Job not found: {escaped_job_name}"
    return
}}
{cause_section}
def params = [{params_groovy}]
def actions = []
if (params) {{
    actions.add(new ParametersAction(params))
}}
actions.add(new CauseAction(cause))

def future = targetJob.scheduleBuild2(0, actions.toArray(new Action[0]))
if (future != null) {{
{output_section}
}} else {{
    println "ERROR: scheduleBuild2 returned null for {escaped_job_name}"
}}
"""


def trigger_jenkins_job(job_name: str, parameters: dict, dry_run: bool = False) -> bool:
    """Trigger a Jenkins job via Groovy Script Console.

    Uses /scriptText API to schedule builds, which avoids the known Jenkins
    issue where the REST build API omits the 'Location' header from the response
    (causing python-jenkins to raise EmptyResponseException and retry — triggering
    the job multiple times).

    Injects UpstreamCause so Jenkins UI displays 'Started by upstream project [X] build [Y]'
    with clickable links.

    Args:
        job_name: Full Jenkins job path.
        parameters: Parameters to pass to the job.
        dry_run: If True, print what would be triggered without making API calls.

    Returns:
        True if the job was triggered (or would be in dry-run), False on failure.
    """
    if dry_run:
        params_str = ", ".join(f"{k}={v}" for k, v in sorted(parameters.items()))
        logger.info("[DRY-RUN] Would trigger: %s with params: {%s}", job_name, params_str)
        return True

    client, jenkins_url = _get_jenkins_client()
    script = _build_trigger_groovy(job_name, parameters)

    for attempt in range(MAX_TRIGGER_RETRIES):
        try:
            result = client.run_script(script)
            if result and result.startswith("TRIGGERED:"):
                build_url = result[len("TRIGGERED:") :].strip()
                logger.info("Triggered: %s — %s", job_name, build_url)
                return True
            if result and result.startswith("ERROR:"):
                error_msg = result[len("ERROR:") :].strip()
                logger.error("Failed to trigger %s: %s", job_name, error_msg)
                return False
            # Unexpected output — treat as failure
            logger.warning("Unexpected script output for %s: %s", job_name, result)
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Trigger attempt %d/%d for %s failed: %s", attempt + 1, MAX_TRIGGER_RETRIES, job_name, exc)

        if attempt < MAX_TRIGGER_RETRIES - 1:
            wait = RETRY_BACKOFF_BASE ** (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    logger.error("Failed to trigger %s after %d attempts", job_name, MAX_TRIGGER_RETRIES)
    return False


def trigger_jenkins_job_with_queue(job_name: str, parameters: dict, dry_run: bool = False) -> str:
    """Trigger a Jenkins job via Groovy Script Console and return the queue item URL.

    Same retry behavior as trigger_jenkins_job. Returns a queue item URL
    suitable for polling via _wait_for_build_start().

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

    client, jenkins_url = _get_jenkins_client()
    script = _build_trigger_groovy(job_name, parameters, return_queue_url=True)

    for attempt in range(MAX_TRIGGER_RETRIES):
        try:
            result = client.run_script(script)
            if result and result.startswith("TRIGGERED:"):
                queue_url = result[len("TRIGGERED:") :].strip()
                logger.info("Triggered %s — %s (will wait for completion)", job_name, queue_url)
                return queue_url
            if result and result.startswith("ERROR:"):
                error_msg = result[len("ERROR:") :].strip()
                raise JenkinsTriggerError(f"Failed to trigger {job_name}: {error_msg}")
            logger.warning("Unexpected script output for %s: %s", job_name, result)
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Trigger attempt %d/%d for %s failed: %s", attempt + 1, MAX_TRIGGER_RETRIES, job_name, exc)

        if attempt < MAX_TRIGGER_RETRIES - 1:
            wait = RETRY_BACKOFF_BASE ** (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    raise JenkinsTriggerError(f"Failed to trigger {job_name} after {MAX_TRIGGER_RETRIES} attempts")


def wait_for_builds(
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

    client, jenkins_url = _get_jenkins_client()

    # Phase 1: Wait for all builds to leave the queue and get build numbers
    @dataclass
    class _ActiveBuild:
        job_name: str
        build_number: int
        job_url_path: str
        collect_results: list[str]
        timeout: int
        fail_on_error: bool
        start_time: float = field(default_factory=time.time)

    active_builds: list[_ActiveBuild] = []
    results: list[BuildResult] = []

    for pending in pending_jobs:
        try:
            build_number = _wait_for_build_start(pending.queue_url, client, timeout=min(pending.timeout, 600))
            job_url_path = pending.job_name.replace("/", "/job/")
            active_builds.append(
                _ActiveBuild(
                    job_name=pending.job_name,
                    build_number=build_number,
                    job_url_path=job_url_path,
                    collect_results=pending.collect_results,
                    timeout=pending.timeout,
                    fail_on_error=pending.fail_on_error,
                )
            )
        except JenkinsTriggerError as exc:
            logger.error("Failed to get build number for %s: %s", pending.job_name, exc)
            job_url = f"{jenkins_url}/job/{pending.job_name.replace('/', '/job/')}"
            results.append(BuildResult(job_name=pending.job_name, build_number=0, result="FAILURE", build_url=job_url))

    logger.info("Waiting for %d builds to complete...", len(active_builds))

    # Phase 2: Poll all active builds until they all complete
    while active_builds:
        still_running = []
        for build in active_builds:
            elapsed = time.time() - build.start_time
            if elapsed > build.timeout:
                logger.error("Timeout waiting for %s #%d after %ds", build.job_name, build.build_number, build.timeout)
                build_url = f"{jenkins_url}/job/{build.job_url_path}/{build.build_number}"
                results.append(
                    BuildResult(
                        job_name=build.job_name, build_number=build.build_number, result="TIMEOUT", build_url=build_url
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
                        logger.info("Build %s #%d completed: %s", str(build.job_name), build.build_number, str(result))
                        artifacts = _collect_artifacts(
                            jenkins_url, build.job_url_path, build.build_number, client, build.collect_results
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


def _wait_for_build_start(queue_url: str, client: jenkins_lib.Jenkins, timeout: int = 600) -> int:
    """Poll Jenkins queue item until a build number is assigned."""
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


def _collect_artifacts(
    jenkins_url: str,
    job_url_path: str,
    build_number: int,
    client: jenkins_lib.Jenkins,
    patterns: list[str],
) -> list[str]:
    if not patterns:
        return []

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


def send_trigger_matrix_email(
    build_results: list[BuildResult],
    scylla_version: str,
    matrix_file: str,
    email_recipients: list[str] | None = None,
    trigger_job_url: str | None = None,
) -> None:
    """Send an email report summarizing wait-mode build results.

    Args:
        build_results: List of BuildResult from wait_for_builds.
        scylla_version: Version that was tested.
        matrix_file: Path to the matrix YAML that was used.
        email_recipients: List of email addresses. Falls back to DEFAULT_EMAIL_RECIPIENTS.
    """
    recipients = email_recipients or DEFAULT_EMAIL_RECIPIENTS
    if not recipients:
        logger.warning("No email recipients configured — skipping email report")
        return

    all_passed = all(r.success for r in build_results)
    status = "PASSED" if all_passed else "FAILED"

    subject = f"[Trigger Matrix] {status} — scylla-doctor gating ({scylla_version})"

    trigger_line = (
        f'<b>Trigger job:</b> <a href="{trigger_job_url}">View trigger run</a><br/>' if trigger_job_url else ""
    )

    rows = []
    for result in build_results:
        emoji = "✅" if result.success else "❌"
        artifacts_str = ", ".join(result.artifacts) if result.artifacts else "—"
        job_cell = (
            f'<a href="{result.build_url}">{emoji} {result.job_name}</a>'
            if result.build_url
            else f"{emoji} {result.job_name}"
        )
        build_cell = (
            f'<a href="{result.build_url}">#{result.build_number}</a>'
            if result.build_url
            else f"#{result.build_number}"
        )
        rows.append(
            f"<tr><td>{job_cell}</td><td>{build_cell}</td><td><b>{result.result}</b></td><td>{artifacts_str}</td></tr>"
        )

    body = f"""<html><body>
<h2>Trigger Matrix Results — {status}</h2>
<p><b>Version:</b> {scylla_version}<br/>
<b>Matrix:</b> {matrix_file}<br/>
{trigger_line}<b>Overall:</b> {status}</p>
<table border="1" cellpadding="5" cellspacing="0">
<tr><th>Job</th><th>Build</th><th>Result</th><th>Artifacts</th></tr>
{"".join(rows)}
</table>
</body></html>"""

    try:
        from sdcm.utils.cloud_monitor.cloud_monitor import Email  # noqa: PLC0415 - optional dependency

        email_client = Email()
        email_client.send(subject=subject, content=body, recipients=recipients, html=True)
        logger.info("Email report sent to %s", recipients)
    except Exception as exc:  # noqa: BLE001 - email failure is non-fatal
        logger.warning("Failed to send email report: %s", exc)


def trigger_matrix(  # noqa: PLR0914
    matrix_file: str,
    scylla_version: str,
    filter_version: str | None = None,
    job_folder: str | None = None,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: str | None = None,
    dry_run: bool = False,
    email_recipients: list[str] | None = None,
    image_arch: str | None = None,
    version_resolution: str | None = None,
    **overrides,
) -> dict:
    """Main entry point: load matrix, filter, build params, trigger jobs.

    Args:
        matrix_file: Path to the YAML matrix file.
        scylla_version: Full version tag or branch:qualifier.
        filter_version: Original version string before resolution (e.g., "master:latest").
            Used for job folder determination, for backend-aware version resolution, and as
            branch_source_version for {branch}/{branch_id} template resolution. When None,
            scylla_version is used for all three.
        job_folder: Override auto-detected job folder.
        labels_selector: Comma-separated labels to filter jobs.
        backend: Filter by backend.
        skip_jobs: Comma-separated job names to skip.
        dry_run: If True, print what would be triggered.
        version_resolution: Override the matrix' version_resolution strategy
            (per-backend | common | aws-strict).
        **overrides: Additional parameter overrides (e.g., stress_duration, region).

    Returns:
        Dict with 'triggered', 'skipped', 'failed' job lists and the 'versions' each job
        was triggered with, keyed by job path.

    Raises:
        MatrixValidationError: If the YAML matrix file is invalid.
        TriggerMatrixError: If the version cannot be mapped to a job folder, or when
            version_resolution=common finds no build shared by all backends.
        JenkinsTriggerError: If Jenkins credentials are missing (non-dry-run).
    """
    config = load_matrix_config(matrix_file)
    scylla_version = scylla_version or config.default_scylla_version
    resolved_folder = determine_job_folder(filter_version or scylla_version, job_folder)

    skip_list = [s.strip() for s in skip_jobs.split(",") if s.strip()] if skip_jobs else []

    if skip_list:
        all_job_names = {j.job_name for j in config.jobs}
        unknown_skips = set(skip_list) - all_job_names
        if unknown_skips:
            logger.warning("Skipped jobs not found in matrix: %s", ", ".join(sorted(unknown_skips)))

    version_for_filtering = filter_version or scylla_version
    filtered = filter_jobs(
        jobs=config.jobs,
        scylla_version=version_for_filtering,
        resolved_version=scylla_version if filter_version else None,
        labels_selector=labels_selector,
        backend=backend,
        skip_jobs=skip_list,
        image_arch=image_arch,
    )

    logger.info(
        "Matrix: %s | Version: %s | Folder: %s | Jobs: %d/%d",
        matrix_file,
        scylla_version,
        resolved_folder,
        len(filtered),
        len(config.jobs),
    )

    if not filtered:
        logger.warning("No jobs matched the filters. Check your version, labels, backend, and skip_jobs settings.")

    results = {"triggered": [], "skipped": [], "failed": [], "waited": [], "versions": {}}
    filtered_names = {j.job_name for j in filtered}
    results["skipped"] = [j.job_name for j in config.jobs if j.job_name not in filtered_names]

    # Resolve the version every job gets, per backend/region, so a job never receives a
    # build that was only published on some other backend (SCT-665).
    strategy = version_resolution or config.version_resolution

    def job_target(job: JobConfig) -> BackendTarget | None:
        # Not keyed by job_name: the same job runs in the matrix more than once, on
        # different regions/architectures.
        if not job_uses_scylla_version(job, config.defaults, overrides):
            return None
        return target_for_job(job, config.defaults, region_override=overrides.get("region"))

    matrix_targets = sorted({target for target in map(job_target, filtered) if target})
    versions_by_target: dict[BackendTarget, str] = {}
    unavailable_targets: dict[BackendTarget, str] = {}
    if scylla_version and matrix_targets:
        logger.info("Resolving scylla_version with strategy '%s'", strategy)
        versions_by_target, unavailable_targets = resolve_versions_for_targets(
            original_version=version_for_filtering,
            reference_version=scylla_version,
            targets=matrix_targets,
            strategy=strategy,
        )
        for target, version in sorted(versions_by_target.items()):
            logger.info("  %s → %s", target, version)
        for target, reason in sorted(unavailable_targets.items()):
            logger.warning("  %s → not triggered: %s", target, reason)

    # Step 1: Trigger all jobs, deduplicating by resolved path to prevent
    # the same Jenkins job from being triggered twice (e.g. when both x86
    # and aarch64 entries match the same labels_selector).
    pending_wait_jobs: list[_PendingWaitJob] = []
    triggered_paths: set[str] = set()

    for job in filtered:
        full_path = resolve_job_path(job.job_name, resolved_folder)
        if full_path in triggered_paths:
            logger.debug("Skipping duplicate trigger for %s", full_path)
            continue

        job_version = scylla_version
        if target := job_target(job):
            if reason := unavailable_targets.get(target):
                logger.warning("Skipping job '%s' on %s: %s", job.job_name, target, reason)
                results["skipped"].append(job.job_name)
                continue
            job_version = versions_by_target.get(target, scylla_version)

        params = build_job_parameters(
            job, config.defaults, job_version, overrides, branch_source_version=filter_version
        )
        results["versions"][full_path] = params.get("scylla_version", "")

        if job.wait:
            try:
                queue_url = trigger_jenkins_job_with_queue(full_path, params, dry_run=dry_run)
                triggered_paths.add(full_path)
                results["triggered"].append(full_path)
                pending_wait_jobs.append(
                    _PendingWaitJob(
                        job_name=full_path,
                        queue_url=queue_url,
                        collect_results=job.collect_results,
                        timeout=job.wait_timeout,
                        fail_on_error=job.fail_on_error,
                    )
                )
            except JenkinsTriggerError as exc:
                logger.error("Failed to trigger wait-mode job %s: %s", full_path, exc)
                results["failed"].append(full_path)
        else:
            success = trigger_jenkins_job(full_path, params, dry_run=dry_run)
            if success:
                triggered_paths.add(full_path)
                results["triggered"].append(full_path)
            else:
                results["failed"].append(full_path)

    # Step 2: Wait for all wait-mode jobs concurrently
    if pending_wait_jobs:
        logger.info("All jobs triggered. Waiting for %d gating jobs to complete...", len(pending_wait_jobs))
        build_results = wait_for_builds(pending_wait_jobs, dry_run=dry_run)
        for build_result in build_results:
            results["waited"].append({"job": build_result.job_name, "build": build_result})
            if not build_result.success:
                results["failed"].append(build_result.job_name)
                if any(p.fail_on_error for p in pending_wait_jobs if p.job_name == build_result.job_name):
                    logger.error(
                        "Gating job %s failed with result=%s — %s",
                        build_result.job_name,
                        build_result.result,
                        build_result.build_url,
                    )

        send_trigger_matrix_email(
            build_results=build_results,
            scylla_version=scylla_version,
            matrix_file=matrix_file,
            email_recipients=email_recipients,
            trigger_job_url=os.environ.get("BUILD_URL", "").rstrip("/") or None,
        )

    logger.info(
        "Summary: %d triggered, %d skipped, %d failed",
        len(results["triggered"]),
        len(results["skipped"]),
        len(results["failed"]),
    )

    return results

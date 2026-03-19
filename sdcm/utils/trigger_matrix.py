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

import logging
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests
import yaml

logger = logging.getLogger(__name__)

# Regex for full version tags like: 2024.2.5-0.20250221.cb9e2a54ae6d-1
FULL_VERSION_TAG_RE = re.compile(
    r"^(?P<major>\d{4})\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"-\d+\.\d{8}\.[0-9a-f]+-\d+$"
)

# Regex for simple version strings like: 2025.4, 2025.4.0, 5.2.1
SIMPLE_VERSION_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)(?:\.\d+)?$")

# Regex for branch:qualifier like: master:latest, branch-2019.1:all
BRANCH_VERSION_RE = re.compile(r"^(?P<branch>[a-zA-Z0-9._-]+):(?P<qualifier>.+)$")

VALID_BACKENDS = {"aws", "gce", "azure", "docker"}

MAX_TRIGGER_RETRIES = 3
RETRY_BACKOFF_BASE = 2


@dataclass
class CronTriggerConfig:
    """Configuration for a cron-based trigger schedule."""

    schedule: str
    params: dict = field(default_factory=dict)


@dataclass
class JobConfig:
    """Configuration for a single Jenkins job in the trigger matrix."""

    job_name: str
    backend: str
    region: str
    labels: list[str] = field(default_factory=list)
    exclude_versions: list[str] = field(default_factory=list)
    params: dict = field(default_factory=dict)


@dataclass
class MatrixConfig:
    """Full trigger matrix configuration loaded from YAML."""

    jobs: list[JobConfig]
    defaults: dict = field(default_factory=dict)
    cron_triggers: list[CronTriggerConfig] = field(default_factory=list)


class TriggerMatrixError(Exception):
    """Base exception for trigger matrix errors."""


class MatrixValidationError(TriggerMatrixError):
    """Raised when YAML matrix file fails validation."""


class JenkinsTriggerError(TriggerMatrixError):
    """Raised when a Jenkins job trigger fails."""


def load_matrix_config(path: str | Path) -> MatrixConfig:
    """Load and validate a trigger matrix YAML file.

    Args:
        path: Path to the YAML file.

    Returns:
        MatrixConfig with validated data.

    Raises:
        MatrixValidationError: If the YAML is malformed or missing required fields.
        FileNotFoundError: If the YAML file does not exist.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Matrix file not found: {path}")

    with open(path, encoding="utf-8") as fobj:
        raw = yaml.safe_load(fobj)

    if not isinstance(raw, dict):
        raise MatrixValidationError(f"Matrix file must be a YAML mapping, got {type(raw).__name__}")

    if "jobs" not in raw:
        raise MatrixValidationError("Matrix file must contain a 'jobs' key")

    raw_jobs = raw["jobs"]
    if not isinstance(raw_jobs, list):
        raise MatrixValidationError(f"'jobs' must be a list, got {type(raw_jobs).__name__}")

    jobs = []
    for idx, raw_job in enumerate(raw_jobs):
        if not isinstance(raw_job, dict):
            raise MatrixValidationError(f"Job entry {idx} must be a mapping, got {type(raw_job).__name__}")

        if "job_name" not in raw_job:
            raise MatrixValidationError(f"Job entry {idx} missing required field 'job_name'")
        if "backend" not in raw_job:
            raise MatrixValidationError(f"Job entry {idx} ('{raw_job['job_name']}') missing required field 'backend'")

        jobs.append(
            JobConfig(
                job_name=raw_job["job_name"],
                backend=raw_job["backend"],
                region=raw_job.get("region", ""),
                labels=raw_job.get("labels", []),
                exclude_versions=raw_job.get("exclude_versions", []),
                params=raw_job.get("params", {}),
            )
        )

    cron_triggers = []
    for idx, raw_cron in enumerate(raw.get("cron_triggers", [])):
        if not isinstance(raw_cron, dict):
            raise MatrixValidationError(f"cron_triggers entry {idx} must be a mapping")
        if "schedule" not in raw_cron:
            raise MatrixValidationError(f"cron_triggers entry {idx} missing required field 'schedule'")
        cron_triggers.append(
            CronTriggerConfig(
                schedule=raw_cron["schedule"],
                params=raw_cron.get("params", {}),
            )
        )

    defaults = raw.get("defaults", {})
    if not isinstance(defaults, dict):
        raise MatrixValidationError(f"'defaults' must be a mapping, got {type(defaults).__name__}")

    return MatrixConfig(
        jobs=jobs,
        defaults=defaults,
        cron_triggers=cron_triggers,
    )


def determine_job_folder(scylla_version: str, job_folder: str | None = None) -> str:
    """Derive Jenkins job folder from version string.

    Args:
        scylla_version: Version string in any supported format.
        job_folder: Explicit override — returned as-is if provided.

    Returns:
        Jenkins job folder name (e.g., 'scylla-master', 'branch-2025.4').

    Examples:
        >>> determine_job_folder("master:latest")
        'scylla-master'
        >>> determine_job_folder("master")
        'scylla-master'
        >>> determine_job_folder("2025.4")
        'branch-2025.4'
        >>> determine_job_folder("2025.4.1")
        'branch-2025.4'
        >>> determine_job_folder("2024.2.5-0.20250221.cb9e2a54ae6d-1")
        'branch-2024.2'
        >>> determine_job_folder("master:latest", job_folder="my-folder")
        'my-folder'
    """
    if job_folder:
        return job_folder

    # Handle branch:qualifier format (e.g., "master:latest")
    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        branch = branch_match.group("branch")
        if branch == "master":
            return "scylla-master"
        return f"branch-{branch}"

    # Handle full version tags (e.g., "2024.2.5-0.20250221.cb9e2a54ae6d-1")
    full_match = FULL_VERSION_TAG_RE.match(scylla_version)
    if full_match:
        major = full_match.group("major")
        minor = full_match.group("minor")
        return f"branch-{major}.{minor}"

    # Handle simple version strings (e.g., "2025.4", "2025.4.0")
    simple_match = SIMPLE_VERSION_RE.match(scylla_version)
    if simple_match:
        major = simple_match.group("major")
        minor = simple_match.group("minor")
        return f"branch-{major}.{minor}"

    # Handle bare "master"
    if scylla_version.strip().lower() == "master":
        return "scylla-master"

    raise TriggerMatrixError(
        f"Cannot determine job folder from version '{scylla_version}'. Provide an explicit --job-folder."
    )


def filter_jobs(
    jobs: list[JobConfig],
    scylla_version: str,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: list[str] | None = None,
) -> list[JobConfig]:
    """Filter jobs based on version exclusion, labels, backend, and skip list.

    Args:
        jobs: List of job configurations to filter.
        scylla_version: Version string to check against exclude_versions.
        labels_selector: Comma-separated labels — job must have ALL listed labels.
            When None, no label filtering is applied.
        backend: Filter by backend (e.g., 'aws', 'gce', 'azure').
        skip_jobs: List of job names to skip.

    Returns:
        Filtered list of JobConfig objects.
    """
    skip_set = set(skip_jobs or [])
    required_labels = set()
    if labels_selector:
        required_labels = {label.strip() for label in labels_selector.split(",") if label.strip()}

    result = []
    for job in jobs:
        # Skip by name
        if job.job_name in skip_set:
            logger.debug("Skipping job '%s': in skip list", job.job_name)
            continue

        # Skip by backend
        if backend and job.backend != backend:
            logger.debug("Skipping job '%s': backend '%s' != '%s'", job.job_name, job.backend, backend)
            continue

        # Skip by version exclusion (prefix match)
        if _is_version_excluded(scylla_version, job.exclude_versions):
            logger.debug("Skipping job '%s': version '%s' excluded", job.job_name, scylla_version)
            continue

        # Skip by labels (AND logic: job must have ALL required labels)
        if required_labels and not required_labels.issubset(set(job.labels)):
            logger.debug(
                "Skipping job '%s': labels %s don't match selector %s", job.job_name, job.labels, required_labels
            )
            continue

        result.append(job)

    return result


def _is_version_excluded(scylla_version: str, exclude_versions: list[str]) -> bool:
    """Check if a version matches any exclusion prefix.

    The version is stripped of any branch qualifier (e.g., "master:latest" → "master")
    before matching.
    """
    if not exclude_versions:
        return False

    # Strip branch qualifier if present
    version_to_check = scylla_version
    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        version_to_check = branch_match.group("branch")

    return any(version_to_check.startswith(prefix) for prefix in exclude_versions)


def build_job_parameters(
    job: JobConfig,
    defaults: dict,
    scylla_version: str,
    cli_overrides: dict,
) -> dict:
    """Build final parameter dict for a Jenkins job.

    Priority: cli_overrides > job.params > defaults.
    Always includes scylla_version.

    Args:
        job: Job configuration.
        defaults: Default parameters from the matrix.
        scylla_version: Version string to pass to the job.
        cli_overrides: CLI-provided parameter overrides.

    Returns:
        Merged parameter dictionary.
    """
    params = dict(defaults)
    params.update(job.params)
    params.update({k: v for k, v in cli_overrides.items() if v is not None})

    # Always set scylla_version and region
    params["scylla_version"] = scylla_version
    if job.region:
        params.setdefault("region", job.region)

    return params


def resolve_job_path(job_name: str, job_folder: str) -> str:
    """Resolve a job name to a full Jenkins job path.

    Relative paths are prefixed with job_folder.
    Absolute paths (starting with '/') are used as-is with the leading '/' stripped.

    Args:
        job_name: Job name from the YAML.
        job_folder: Auto-detected or explicit job folder.

    Returns:
        Full Jenkins job path.
    """
    if job_name.startswith("/"):
        return job_name.lstrip("/")
    return f"{job_folder}/{job_name}"


def trigger_jenkins_job(job_name: str, parameters: dict, dry_run: bool = False) -> bool:
    """Trigger a Jenkins job via REST API or print in dry-run mode.

    Args:
        job_name: Full Jenkins job path.
        parameters: Parameters to pass to the job.
        dry_run: If True, print what would be triggered without making API calls.

    Returns:
        True if the job was triggered (or would be in dry-run), False on failure.

    Raises:
        JenkinsTriggerError: If the job fails to trigger after retries.
    """
    if dry_run:
        params_str = ", ".join(f"{k}={v}" for k, v in sorted(parameters.items()))
        logger.info("[DRY-RUN] Would trigger: %s with params: {%s}", job_name, params_str)
        return True

    jenkins_url = os.environ.get("JENKINS_URL", "").rstrip("/")
    jenkins_token = os.environ.get("JENKINS_API_TOKEN", "")

    if not jenkins_url:
        raise JenkinsTriggerError("JENKINS_URL environment variable not set")
    if not jenkins_token:
        raise JenkinsTriggerError("JENKINS_API_TOKEN environment variable not set")

    url = f"{jenkins_url}/job/{job_name.replace('/', '/job/')}/buildWithParameters"

    for attempt in range(MAX_TRIGGER_RETRIES):
        try:
            response = requests.post(
                url,
                params=parameters,
                headers={"Authorization": f"Bearer {jenkins_token}"},
                timeout=30,
            )
            if response.status_code in (200, 201):
                logger.info("Triggered: %s", job_name)
                return True
            if response.status_code < 500:
                # Non-retryable error (4xx)
                logger.error("Failed to trigger %s: HTTP %d - %s", job_name, response.status_code, response.text[:200])
                return False
            # 5xx — retryable
            logger.warning(
                "Trigger attempt %d/%d for %s failed: HTTP %d",
                attempt + 1,
                MAX_TRIGGER_RETRIES,
                job_name,
                response.status_code,
            )
        except requests.RequestException as exc:
            logger.warning("Trigger attempt %d/%d for %s failed: %s", attempt + 1, MAX_TRIGGER_RETRIES, job_name, exc)

        if attempt < MAX_TRIGGER_RETRIES - 1:
            wait = RETRY_BACKOFF_BASE ** (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    logger.error("Failed to trigger %s after %d attempts", job_name, MAX_TRIGGER_RETRIES)
    return False


def trigger_matrix(
    matrix_file: str,
    scylla_version: str,
    job_folder: str | None = None,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: str | None = None,
    dry_run: bool = False,
    **overrides,
) -> dict:
    """Main entry point: load matrix, filter, build params, trigger jobs.

    Args:
        matrix_file: Path to the YAML matrix file.
        scylla_version: Full version tag or branch:qualifier.
        job_folder: Override auto-detected job folder.
        labels_selector: Comma-separated labels to filter jobs.
        backend: Filter by backend.
        skip_jobs: Comma-separated job names to skip.
        dry_run: If True, print what would be triggered.
        **overrides: Additional parameter overrides (e.g., stress_duration, region).

    Returns:
        Dict with 'triggered', 'skipped', and 'failed' job lists.
    """
    config = load_matrix_config(matrix_file)
    resolved_folder = determine_job_folder(scylla_version, job_folder)

    skip_list = [s.strip() for s in skip_jobs.split(",") if s.strip()] if skip_jobs else []

    filtered = filter_jobs(
        jobs=config.jobs,
        scylla_version=scylla_version,
        labels_selector=labels_selector,
        backend=backend,
        skip_jobs=skip_list,
    )

    logger.info(
        "Matrix: %s | Version: %s | Folder: %s | Jobs: %d/%d",
        matrix_file,
        scylla_version,
        resolved_folder,
        len(filtered),
        len(config.jobs),
    )

    results = {"triggered": [], "skipped": [], "failed": []}
    results["skipped"] = [j.job_name for j in config.jobs if j not in filtered]

    for job in filtered:
        full_path = resolve_job_path(job.job_name, resolved_folder)
        params = build_job_parameters(job, config.defaults, scylla_version, overrides)

        success = trigger_jenkins_job(full_path, params, dry_run=dry_run)
        if success:
            results["triggered"].append(full_path)
        else:
            results["failed"].append(full_path)

    logger.info(
        "Summary: %d triggered, %d skipped, %d failed",
        len(results["triggered"]),
        len(results["skipped"]),
        len(results["failed"]),
    )

    return results

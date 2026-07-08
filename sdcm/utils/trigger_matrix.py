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
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import jenkins as jenkins_lib
import pydantic
import requests
import yaml
from pydantic import BaseModel, Field

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


# Regex for full version tags like:
#   2024.2.5-0.20250221.cb9e2a54ae6d-1 (release)
#   2026.2.0~dev-0.20260322.f51126483167 (dev/nightly)
FULL_VERSION_TAG_RE = re.compile(
    r"^(?P<major>\d{4})\.(?P<minor>\d+)\.(?P<patch>\d+)"
    r"[~-][a-zA-Z0-9._~]+-?\d*\.\d{8}\.[0-9a-f]+(?:-\d+)?$"
)

# Regex for simple version strings like: 2025.4, 2025.4.0, 5.2.1
SIMPLE_VERSION_RE = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)(?:\.\d+)?$")

# Regex for branch:qualifier like: master:latest, branch-2019.1:all
BRANCH_VERSION_RE = re.compile(r"^(?P<branch>[a-zA-Z0-9._-]+):(?P<qualifier>.+)$")

VALID_BACKENDS = {"aws", "gce", "azure", "docker", "oci"}

MAX_TRIGGER_RETRIES = 3
RETRY_BACKOFF_BASE = 2

# Default region used for AMI tag lookups
DEFAULT_AWS_REGION = "eu-west-1"

WAIT_POLL_INTERVAL = 30
WAIT_TIMEOUT = 14400
DEFAULT_EMAIL_RECIPIENTS = ["qa@scylladb.com"]


def is_full_version_tag(version: str) -> bool:
    """Check if a version string is a full version tag (e.g., 2024.2.5-0.20250221.cb9e2a54ae6d-1)."""
    return bool(FULL_VERSION_TAG_RE.match(version))


def resolve_to_full_version(
    scylla_version: str,
    region: str | None = None,
) -> str:
    """Resolve a partial version to a full version tag.

    If the version is already a full tag, return it as-is.
    Otherwise, look up the latest AMI matching the version and extract the full tag.

    Args:
        scylla_version: Version string in any format (e.g., master:latest, 2025.4, or full tag).
        region: AWS region for AMI lookup.

    Returns:
        Full version tag string.

    Raises:
        TriggerMatrixError: If the version cannot be resolved.
    """
    if is_full_version_tag(scylla_version):
        return scylla_version

    # For branch:qualifier or simple versions, resolve via AMI lookup
    lookup_region = region or DEFAULT_AWS_REGION
    branch_match = BRANCH_VERSION_RE.match(scylla_version)

    if branch_match:
        # e.g., master:latest, branch-2025.4:latest
        version = _resolve_version_via_branched_ami(scylla_version, lookup_region)
        if version:
            return version

    if SIMPLE_VERSION_RE.match(scylla_version):
        # e.g., 2025.4 — try as branch:latest
        version = _resolve_version_via_branched_ami(f"{scylla_version}:latest", lookup_region)
        if version:
            return version

    raise TriggerMatrixError(
        f"Cannot resolve '{scylla_version}' to a full version tag. "
        f"Provide a full version tag (e.g., 2024.2.5-0.20250221.cb9e2a54ae6d-1) "
        f"or ensure AMIs exist for the version in region '{lookup_region}'."
    )


def resolve_scylla_version_from_image(
    scylla_ami_id: str | None = None,
    gce_image_db: str | None = None,
    azure_image_db: str | None = None,
    oci_image_db: str | None = None,
    region: str | None = None,
) -> str:  # noqa: PLR0913
    """Resolve a full scylla_version from a backend image ID.

    Looks up the image metadata (tags/labels) to extract the scylla version.
    Tries each provided image parameter in order and returns the first resolved version.

    Args:
        scylla_ami_id: AWS AMI ID (e.g., ami-0123456789abcdef0).
        gce_image_db: GCE image URL or name.
        azure_image_db: Azure image ID.
        oci_image_db: OCI image OCID.
        region: AWS region for AMI lookup (defaults to eu-west-1).

    Returns:
        Full version string (e.g., '2024.2.5-0.20250221.cb9e2a54ae6d-1').

    Raises:
        TriggerMatrixError: If the version cannot be resolved from any provided image.
    """
    if scylla_ami_id:
        version = _resolve_version_from_ami(scylla_ami_id, region or DEFAULT_AWS_REGION)
        if version:
            return version

    if gce_image_db:
        version = _resolve_version_from_gce_image(gce_image_db)
        if version:
            return version

    if oci_image_db:
        version = _resolve_version_from_oci_image(oci_image_db, region=region)
        if version:
            return version

    if azure_image_db:
        version = _resolve_version_from_azure_image(azure_image_db)
        if version:
            return version

    provided = {
        k: v
        for k, v in {
            "scylla_ami_id": scylla_ami_id,
            "gce_image_db": gce_image_db,
            "azure_image_db": azure_image_db,
            "oci_image_db": oci_image_db,
        }.items()
        if v
    }
    raise TriggerMatrixError(f"Cannot resolve scylla_version from images: {provided}")


def _resolve_version_via_branched_ami(scylla_version: str, region: str) -> str:
    """Resolve a branch:qualifier version to a full tag via AMI lookup.

    Uses get_branched_ami which searches across both Scylla images account and default credentials.
    """
    try:
        from sdcm.utils.common import get_branched_ami  # noqa: PLC0415 - circular import avoidance

        amis = get_branched_ami(scylla_version, region_name=region, arch="x86_64")
        if amis:
            tags = {t["Key"]: t["Value"] for t in (amis[0].tags or [])}
            if version := tags.get("scylla_version"):
                logger.info("Resolved '%s' → full version '%s' (via AMI %s)", scylla_version, version, amis[0].image_id)
                return version
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve '%s' via AMI lookup: %s", scylla_version, exc)
    return ""


def _extract_version_from_tags(tags: dict, tag_keys: tuple[str, ...] = ("scylla_version", "ScyllaVersion")) -> str:
    """Extract scylla version from a tags dict, trying multiple key names."""
    for key in tag_keys:
        if version := tags.get(key):
            return version
    return ""


def _resolve_version_from_ami(ami_id: str, region: str) -> str:
    """Get scylla_version tag from an AWS AMI.

    Uses get_ami_tags which checks both Scylla images account and default credentials.
    Tag can be 'scylla_version' or 'ScyllaVersion' depending on the AMI.
    """
    try:
        from sdcm.utils.common import get_ami_tags  # noqa: PLC0415 - circular import avoidance

        tags = get_ami_tags(ami_id, region_name=region)
        if version := _extract_version_from_tags(tags):
            logger.info("Resolved AMI %s → scylla_version=%s", ami_id, version)
            return version
        logger.warning(
            "AMI %s has no 'scylla_version' or 'ScyllaVersion' tag. Available tags: %s", ami_id, list(tags.keys())
        )
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from AMI %s: %s", ami_id, exc)
    return ""


def _resolve_version_from_gce_image(image_name: str) -> str:
    """Get scylla_version label from a GCE image.

    Uses get_gce_image_tags which handles both URL and family-based image references.
    """
    try:
        from sdcm.utils.gce_utils import get_gce_image_tags  # noqa: PLC0415 - optional cloud dependency

        labels = get_gce_image_tags(image_name)
        if version_label := _extract_version_from_tags(labels, tag_keys=("scylla_version",)):
            # GCE labels have dashes instead of dots
            version = version_label.replace("-", ".")
            logger.info("Resolved GCE image %s → scylla_version=%s", image_name, version)
            return version
        logger.warning("GCE image %s has no 'scylla_version' label", image_name)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from GCE image %s: %s", image_name, exc)
    return ""


def _resolve_version_from_oci_image(image_id: str, region: str | None = None) -> str:
    """Get scylla_version from an OCI image using oci_utils.get_image_tags."""
    try:
        from sdcm.utils import oci_utils  # noqa: PLC0415 - optional cloud dependency

        tags = oci_utils.get_image_tags(region or "", image_id, "scylla")
        if version := _extract_version_from_tags(tags, tag_keys=("scylla_version",)):
            logger.info("Resolved OCI image %s → scylla_version=%s", image_id, version)
            return version
        logger.warning("OCI image %s has no 'scylla_version' tag", image_id)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from OCI image %s: %s", image_id, exc)
    return ""


def _resolve_version_from_azure_image(image_id: str) -> str:
    """Get scylla_version from an Azure image using azure_utils.get_image_tags."""
    try:
        import sdcm.provision.azure.utils as azure_utils  # noqa: PLC0415 - optional cloud dependency

        tags = azure_utils.get_image_tags(image_id)
        if version := _extract_version_from_tags(tags, tag_keys=("scylla_version",)):
            logger.info("Resolved Azure image %s → scylla_version=%s", image_id, version)
            return version
        logger.warning("Azure image %s has no 'scylla_version' tag", image_id)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from Azure image %s: %s", image_id, exc)
    return ""


class CronTriggerConfig(BaseModel):
    """Configuration for a cron-based trigger schedule."""

    schedule: str
    params: dict = Field(default_factory=dict)


class JobConfig(BaseModel):
    """Configuration for a single Jenkins job in the trigger matrix."""

    job_name: str
    backend: Literal["aws", "gce", "azure", "docker", "oci"]
    region: str = ""
    labels: list[str] = Field(default_factory=list)
    include_versions: list[str] = Field(default_factory=list)
    exclude_versions: list[str] = Field(default_factory=list)
    pre_release: list[str] = Field(default_factory=list)
    params: dict = Field(default_factory=dict)
    wait: bool = False
    wait_timeout: int = WAIT_TIMEOUT
    fail_on_error: bool = False
    collect_results: list[str] = Field(default_factory=list)


class MatrixConfig(BaseModel):
    """Full trigger matrix configuration loaded from YAML."""

    jobs: list[JobConfig]
    defaults: dict = Field(default_factory=dict)
    default_scylla_version: str = ""
    cron_triggers: list[CronTriggerConfig] = Field(default_factory=list)
    email_recipients: list[str] = Field(default_factory=list)


class TriggerMatrixError(Exception):
    """Base exception for trigger matrix errors."""


class MatrixValidationError(TriggerMatrixError):
    """Raised when YAML matrix file fails validation."""


class JenkinsTriggerError(TriggerMatrixError):
    """Raised when a Jenkins job trigger fails."""


@dataclass
class BuildResult:
    job_name: str
    build_number: int
    result: str  # SUCCESS, FAILURE, ABORTED, UNSTABLE
    artifacts: list[str] = field(default_factory=list)
    build_url: str = ""

    @property
    def success(self) -> bool:
        return self.result == "SUCCESS"


@dataclass
class _PendingWaitJob:
    """Internal: tracks a triggered job that needs to be waited on."""

    job_name: str
    queue_url: str
    collect_results: list[str]
    timeout: int
    fail_on_error: bool


def get_parameterized_cron(path: str | Path) -> str:
    """Extract parameterizedCron spec from a matrix YAML file.

    Returns a string suitable for the Jenkins parameterizedCron trigger,
    with one line per cron_triggers entry in the format:
        schedule % key1=val1\\nkey2=val2
    """
    config = load_matrix_config(path)
    lines = []
    for cron in config.cron_triggers:
        param_parts = ";".join(f"{k}={v}" for k, v in cron.params.items())
        lines.append(f"{cron.schedule} % {param_parts}" if param_parts else cron.schedule)
    return "\n".join(lines)


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

    raw_email = raw.get("email_recipients", [])
    if isinstance(raw_email, str):
        raw["email_recipients"] = [e.strip() for e in raw_email.split(",") if e.strip()]

    try:
        return MatrixConfig.model_validate(raw)
    except pydantic.ValidationError as exc:
        raise MatrixValidationError(str(exc)) from exc


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

    if not scylla_version:
        raise TriggerMatrixError(
            "Cannot determine job folder: scylla_version is empty. Provide a version or explicit --job-folder."
        )

    # Handle branch:qualifier format (e.g., "master:latest")
    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        branch = branch_match.group("branch")
        return "scylla-master" if branch == "master" else f"branch-{branch}"

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
    resolved_version: str | None = None,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: list[str] | None = None,
) -> list[JobConfig]:
    """Filter jobs based on version exclusion, labels, backend, and skip list.

    Args:
        jobs: List of job configurations to filter.
        scylla_version: Original version string (e.g., master:latest) for include/exclude filtering.
        resolved_version: Full resolved version (e.g., 2025.1.3-rc1-...) for pre_release matching.
            Falls back to scylla_version when not provided.
        labels_selector: Comma-separated labels — job must have ALL listed labels.
            When None, no label filtering is applied.
        backend: Filter by backend (e.g., 'aws', 'gce', 'azure').
        skip_jobs: List of job names to skip.

    Returns:
        Filtered list of JobConfig objects.
    """
    skip_set = set(skip_jobs or [])
    pre_release_version = resolved_version or scylla_version
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

        # Skip by version inclusion (prefix match — only run for listed versions)
        if job.include_versions and not _is_version_included(scylla_version, job.include_versions):
            logger.debug("Skipping job '%s': version '%s' not in include list", job.job_name, scylla_version)
            continue

        # Skip by version exclusion (prefix match)
        if _is_version_excluded(scylla_version, job.exclude_versions):
            logger.debug("Skipping job '%s': version '%s' excluded", job.job_name, scylla_version)
            continue

        # Skip by pre_release filter (substring match on "-rc1", "-rc3", etc.)
        if job.pre_release and not _is_pre_release_match(scylla_version, pre_release_version, job.pre_release):
            logger.debug(
                "Skipping job '%s': version '%s' doesn't match pre_release %s",
                job.job_name,
                scylla_version,
                job.pre_release,
            )
            continue

        # Skip by labels (AND logic: job must have ALL required labels)
        if required_labels and not required_labels.issubset(set(job.labels)):
            logger.debug(
                "Skipping job '%s': labels %s don't match selector %s", job.job_name, job.labels, required_labels
            )
            continue

        result.append(job)

    return result


def _strip_version_qualifier(scylla_version: str) -> str:
    """Strip branch qualifier from version string.

    If version matches branch:qualifier format (e.g., "master:latest"),
    returns the branch part. Otherwise returns the original string.
    """
    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        return branch_match.group("branch")
    return scylla_version


def _is_version_excluded(scylla_version: str, exclude_versions: list[str]) -> bool:
    """Check if a version matches any exclusion prefix.

    Uses prefix matching: exclude_versions=["2024.1"] excludes "2024.1",
    "2024.1.5", "2024.1-rc1", etc. The version is stripped of any branch
    qualifier (e.g., "master:latest" -> "master") before matching.
    """
    if not exclude_versions:
        return False

    version_to_check = _strip_version_qualifier(scylla_version)
    return any(version_to_check.startswith(prefix) for prefix in exclude_versions)


def _is_version_included(scylla_version: str, include_versions: list[str]) -> bool:
    """Check if a version matches any inclusion prefix.

    Same prefix-matching logic as _is_version_excluded but returns True when matched.
    """
    version_to_check = _strip_version_qualifier(scylla_version)
    return any(version_to_check.startswith(prefix) for prefix in include_versions)


def _is_pre_release_match(scylla_version: str, resolved_version: str, pre_release: list[str]) -> bool:
    """Check if version contains a pre-release tag or is 'master'.

    Mirrors groovy logic: version.contains("-${pr}"). Additionally allows
    'master' through since master builds should always run these jobs.

    Args:
        scylla_version: Original version (e.g., "master:latest", "2025.1:latest").
        resolved_version: Full resolved version (e.g., "2025.1.3-rc1-0.20250525.abc").
        pre_release: List of pre-release tags to match (e.g., ["rc1", "rc3"]).
    """
    original = _strip_version_qualifier(scylla_version)

    if original == "master":
        return True
    return any(f"-{tag}" in resolved_version for tag in pre_release)


def _extract_branch_from_version(scylla_version: str) -> str:
    """Extract branch name from a scylla_version string for template resolution.

    Examples:
        >>> _extract_branch_from_version("master:latest")
        'master'
        >>> _extract_branch_from_version("2025.4.1-0.20250601.abc123def456-1")
        '2025.4'
        >>> _extract_branch_from_version("")
        ''
    """
    if not scylla_version:
        return ""

    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        return branch_match.group("branch")

    full_match = FULL_VERSION_TAG_RE.match(scylla_version)
    if not full_match:
        full_match = SIMPLE_VERSION_RE.match(scylla_version)
    if full_match:
        return f"{full_match.group('major')}.{full_match.group('minor')}"

    if scylla_version.strip().lower() == "master":
        return "master"

    return ""


def _resolve_templates(value: object, branch: str) -> object:
    """Replace {branch} placeholder in a string value."""
    if isinstance(value, str) and "{branch}" in value:
        return value.replace("{branch}", branch)
    return value


def build_job_parameters(
    job: JobConfig,
    defaults: dict,
    scylla_version: str,
    cli_overrides: dict,
) -> dict:
    """Build final parameter dict for a Jenkins job.

    Priority: cli_overrides > job.params > defaults.
    Always includes scylla_version. Downstream jobs resolve their
    own backend-specific images from the version.

    Template variables in parameter values are resolved:
      {branch} — extracted from scylla_version (e.g., "master" from "master:latest")

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

    # Always set scylla_version (if provided) and region
    if scylla_version:
        params["scylla_version"] = scylla_version
    if job.region:
        params.setdefault("region", job.region)

    # Resolve {branch} templates in param values
    branch = _extract_branch_from_version(scylla_version)
    if branch:
        params = {k: _resolve_templates(v, branch) for k, v in params.items()}

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

    client, _ = _get_jenkins_client()

    for attempt in range(MAX_TRIGGER_RETRIES):
        try:
            client.build_job(job_name, parameters=parameters)
            logger.info("Triggered: %s", job_name)
            return True
        except jenkins_lib.JenkinsException as exc:
            logger.warning("Trigger attempt %d/%d for %s failed: %s", attempt + 1, MAX_TRIGGER_RETRIES, job_name, exc)

        if attempt < MAX_TRIGGER_RETRIES - 1:
            wait = RETRY_BACKOFF_BASE ** (attempt + 1)
            logger.info("Retrying in %ds...", wait)
            time.sleep(wait)

    logger.error("Failed to trigger %s after %d attempts", job_name, MAX_TRIGGER_RETRIES)
    return False


def trigger_jenkins_job_with_queue(job_name: str, parameters: dict, dry_run: bool = False) -> str:
    """Trigger a Jenkins job and return the queue URL for later polling.

    Uses the same triggering logic as trigger_jenkins_job but captures the
    queue location header for subsequent wait operations.

    Args:
        job_name: Full Jenkins job path.
        parameters: Parameters to pass to the job.
        dry_run: If True, simulate without triggering.

    Returns:
        Queue URL string (empty string in dry-run mode).

    Raises:
        JenkinsTriggerError: If the job fails to trigger.
    """
    if dry_run:
        params_str = ", ".join(f"{k}={v}" for k, v in sorted(parameters.items()))
        logger.info("[DRY-RUN] Would trigger (with wait): %s with params: {%s}", job_name, params_str)
        return ""

    client, jenkins_url = _get_jenkins_client()

    try:
        queue_id = client.build_job(job_name, parameters=parameters)
    except jenkins_lib.JenkinsException as exc:
        raise JenkinsTriggerError(f"Failed to trigger {job_name}: {exc}") from exc

    queue_url = f"{jenkins_url}/queue/item/{queue_id}/"
    job_url = f"{jenkins_url}/job/{job_name.replace('/', '/job/')}"
    logger.info("Triggered %s — %s (will wait for completion)", job_name, job_url)
    return queue_url


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
                        logger.info(
                            "Build %s #%d completed: %s — %s", build.job_name, build.build_number, result, build_url
                        )
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

    Raises:
        MatrixValidationError: If the YAML matrix file is invalid.
        TriggerMatrixError: If the version cannot be mapped to a job folder.
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

    results = {"triggered": [], "skipped": [], "failed": [], "waited": []}
    filtered_names = {j.job_name for j in filtered}
    results["skipped"] = [j.job_name for j in config.jobs if j.job_name not in filtered_names]

    # Step 1: Trigger all jobs (identical logic for wait and non-wait)
    pending_wait_jobs: list[_PendingWaitJob] = []

    for job in filtered:
        full_path = resolve_job_path(job.job_name, resolved_folder)
        params = build_job_parameters(job, config.defaults, scylla_version, overrides)

        if job.wait:
            try:
                queue_url = trigger_jenkins_job_with_queue(full_path, params, dry_run=dry_run)
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

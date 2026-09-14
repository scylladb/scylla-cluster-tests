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

import difflib
import fnmatch
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

import jenkins as jenkins_lib
import pydantic
import requests
import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from sdcm.utils.trigger_matrix.backends import _backend_region, split_regions
from sdcm.utils.trigger_matrix.constants import (
    DEFAULT_ARCH,
    DEFAULT_EMAIL_RECIPIENTS,
    DEFAULT_VERSION_RESOLUTION,
    MAX_TRIGGER_RETRIES,
    PER_JOB_LOCATION_PARAMS,
    RETRY_BACKOFF_BASE,
    VALID_IMAGE_BACKENDS,
    VERSION_RESOLUTION_STRATEGIES,
    VersionResolution,
    WAIT_POLL_INTERVAL,
    WAIT_TIMEOUT,
)
from sdcm.utils.trigger_matrix import images
from sdcm.utils.trigger_matrix.images import (
    resolve_image_architecture,
    resolve_scylla_version_from_image,
)
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError, MatrixValidationError, TriggerMatrixError
from sdcm.utils.trigger_matrix.versions import (
    RELEASE_VERSION_RE,
    _branch_directory_id,
    _extract_branch_from_version,
    _strip_version_qualifier,
    _version_build_date,
    determine_job_folder,
    is_full_version_tag,
    is_resolvable_partial_version,
)

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


def resolve_to_full_version(
    scylla_version: str,
    region: str | None = None,
    backend: str = "aws",
    arch: str = DEFAULT_ARCH,
) -> str:
    """Resolve a partial version to a full version tag using a backend's published images.

    If the version is already a full tag, return it as-is.
    Otherwise, look up the latest image matching the version on ``backend`` and extract
    the full tag from its tags/labels.

    Args:
        scylla_version: Version string in any format (e.g., master:latest, 2025.4, or full tag).
        region: Region for the image lookup (ignored for region-less backends like GCE).
        backend: Backend to resolve against — aws, gce, azure or oci.
        arch: CPU architecture of the images to look up (x86_64 or aarch64).

    Returns:
        Full version tag string.

    Raises:
        TriggerMatrixError: If the version cannot be resolved.
    """
    if is_full_version_tag(scylla_version):
        return scylla_version

    if RELEASE_VERSION_RE.match(scylla_version):
        return scylla_version

    lookup_region = _backend_region(backend, region)
    if is_resolvable_partial_version(scylla_version):
        # e.g., master:latest, branch-2025.4:latest, or 2025.4 (treated as a branch)
        if version := images._resolve_latest_version_for_backend(scylla_version, backend, lookup_region, arch):
            return version

    raise TriggerMatrixError(
        f"Cannot resolve '{scylla_version}' to a full version tag on backend '{backend}'. "
        f"Provide a full version tag (e.g., 2024.2.5-0.20250221.cb9e2a54ae6d-1) "
        f"or ensure images exist for the version"
        f"{f' in region {lookup_region!r}' if lookup_region else ''}."
    )


class CronTriggerConfig(BaseModel):
    """Configuration for a cron-based trigger schedule."""

    model_config = ConfigDict(extra="forbid")

    schedule: str
    params: dict = Field(default_factory=dict)


class JenkinsfileEntry(BaseModel):
    """A Jenkinsfile generated from this matrix by generate_trigger_jenkinsfiles.py."""

    model_config = ConfigDict(extra="forbid")

    path: str
    labels_selector: str = ""


class JobConfig(BaseModel):
    """Configuration for a single Jenkins job in the trigger matrix.

    The fields here are *structural* — they decide whether and how the job is triggered.
    Everything the Jenkins job itself receives (region, availability_zone,
    instance types, sub_tests, ...) belongs under `params`.
    """

    model_config = ConfigDict(extra="forbid")

    job_name: str
    backend: Literal["aws", "gce", "azure", "docker", "oci"]
    # CPU architecture the job's DB nodes run on — images are published per architecture,
    # so ARM jobs must declare it to have their version resolved against ARM images.
    # Left empty it is inferred from the job's labels — see job_arch().
    arch: str = ""

    @field_validator("arch")
    @classmethod
    def validate_arch(cls, value: str) -> str:
        if value and value not in ("aarch64", "x86_64"):
            raise ValueError(f"arch must be 'aarch64', 'x86_64', or empty; got '{value}'")
        return value

    disabled: bool = False
    labels: list[str] = Field(default_factory=list)
    include_versions: list[str] = Field(default_factory=list)
    exclude_versions: list[str] = Field(default_factory=list)
    pre_release: list[str] = Field(default_factory=list)
    job_throttle_category: str = ""
    params: dict = Field(default_factory=dict)
    wait: bool = False
    wait_timeout: int = WAIT_TIMEOUT
    fail_on_error: bool = False
    collect_results: list[str] = Field(default_factory=list)


def job_arch(job: JobConfig) -> str:
    """The architecture a job's DB nodes run on.

    Jobs that predate the `arch` field only say so through an "aarch64" label, so fall back
    to that before assuming the default. Used both to filter jobs against a supplied image's
    architecture and to look up per-backend images for version resolution — the two must
    agree on what a job's architecture is.
    """
    if job.arch:
        return job.arch
    return "aarch64" if "aarch64" in job.labels else DEFAULT_ARCH


class MatrixConfig(BaseModel):
    """Full trigger matrix configuration loaded from YAML."""

    model_config = ConfigDict(extra="forbid")

    jobs: list[JobConfig]
    defaults: dict = Field(default_factory=dict)
    default_scylla_version: str = ""
    version_resolution: VersionResolution = DEFAULT_VERSION_RESOLUTION
    cron_triggers: list[CronTriggerConfig] = Field(default_factory=list)
    email_recipients: list[str] = Field(default_factory=list)
    jenkinsfiles: list[JenkinsfileEntry] = Field(default_factory=list)


# Keys allowed directly on a job entry — anything else is a Jenkins parameter and
# must live under `params`.
JOB_LEVEL_KEYS = frozenset(JobConfig.model_fields)
MATRIX_LEVEL_KEYS = frozenset(MatrixConfig.model_fields)


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


def _did_you_mean(key: str, candidates: frozenset[str]) -> str:
    """Return a ' — did you mean ...' hint when `key` looks like a typo of a known key."""
    close = difflib.get_close_matches(key, sorted(candidates), n=1, cutoff=0.8)
    return f" — did you mean '{close[0]}'?" if close else ""


def _validate_params_block(params: object, where: str, errors: list[str]) -> None:
    """Validate a `params`/`defaults` mapping: no job-level keys, scalar values only.

    Every entry ends up as a Jenkins StringParameterValue, so nested lists/mappings
    are always a mistake — multi-valued parameters are passed as JSON strings.
    """
    if not isinstance(params, dict):
        errors.append(f"{where}: must be a mapping of Jenkins parameters, got {type(params).__name__}")
        return

    for key, value in params.items():
        if key in JOB_LEVEL_KEYS:
            errors.append(
                f"{where}: '{key}' is a job-level key and must not be nested under 'params:' — "
                f"move it up, next to 'job_name:'"
            )
        if isinstance(value, (list, dict)):
            hint = (
                f' (for a multi-region job use a JSON string: {key}: \'["eu-west-1", "eu-west-2"]\')'
                if key == "region"
                else " (pass multi-valued parameters as a JSON string)"
            )
            errors.append(
                f"{where}: '{key}' must be a scalar — Jenkins parameters are strings, got {type(value).__name__}{hint}"
            )


def _validate_job_entry(index: int, job: object, errors: list[str]) -> None:
    """Validate a single raw job entry before pydantic parsing, for locatable errors."""
    if not isinstance(job, dict):
        errors.append(f"jobs[{index}]: must be a mapping, got {type(job).__name__}")
        return

    where = f"job '{job.get('job_name', '<missing job_name>')}' (jobs[{index}])"
    for key in job:
        if key not in JOB_LEVEL_KEYS:
            errors.append(
                f"{where}: unknown job-level key '{key}' — Jenkins job parameters belong under 'params:'"
                f"{_did_you_mean(key, JOB_LEVEL_KEYS)}"
            )

    if "params" in job:
        _validate_params_block(job["params"], f"{where}: params", errors)


def validate_matrix_layout(raw: dict) -> None:
    """Check that every key in a raw matrix mapping sits where it belongs.

    Pydantic already rejects unknown keys, but its errors don't say *where* a key
    should have gone. This pass collects all misplacements at once so a YAML with
    several mistakes reports them in one go.

    Raises:
        MatrixValidationError: If any key is unknown or in the wrong section.
    """
    errors: list[str] = []

    for key in raw:
        if key not in MATRIX_LEVEL_KEYS:
            errors.append(
                f"unknown top-level key '{key}' — expected one of: {', '.join(sorted(MATRIX_LEVEL_KEYS))}"
                f"{_did_you_mean(key, MATRIX_LEVEL_KEYS)}"
            )

    if "defaults" in raw:
        _validate_params_block(raw["defaults"], "defaults", errors)

    for index, cron in enumerate(raw.get("cron_triggers") or []):
        if isinstance(cron, dict) and "params" in cron:
            _validate_params_block(cron["params"], f"cron_triggers[{index}]: params", errors)

    for index, job in enumerate(raw.get("jobs") or []):
        _validate_job_entry(index, job, errors)

    if errors:
        raise MatrixValidationError("Invalid trigger matrix layout:\n  - " + "\n  - ".join(errors))


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

    validate_matrix_layout(raw)

    try:
        return MatrixConfig.model_validate(raw)
    except pydantic.ValidationError as exc:
        raise MatrixValidationError(f"Invalid trigger matrix {path}:\n{exc}") from exc


def filter_jobs(
    jobs: list[JobConfig],
    scylla_version: str,
    resolved_version: str | None = None,
    labels_selector: str | None = None,
    backend: str | None = None,
    skip_jobs: list[str] | None = None,
    image_arch: str | None = None,
) -> list[JobConfig]:
    """Filter jobs based on version exclusion, labels, backend, skip list, and architecture.

    Args:
        jobs: List of job configurations to filter.
        scylla_version: Original version string (e.g., master:latest) for include/exclude filtering.
        resolved_version: Full resolved version (e.g., 2025.1.3-rc1-...) for pre_release matching.
            Falls back to scylla_version when not provided.
        labels_selector: Comma-separated labels — job must have ALL listed labels.
            When None, no label filtering is applied.
        backend: Filter by backend (e.g., 'aws', 'gce', 'azure').
        skip_jobs: List of job names to skip.
        image_arch: Normalized CPU architecture from the provided image ('x86_64' or 'aarch64').
            When set, filters jobs by architecture: aarch64 images only trigger jobs
            with 'aarch64' label, x86_64 images skip jobs with 'aarch64' label.

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
        if job.disabled:
            logger.debug("Skipping job '%s': disabled", job.job_name)
            continue

        # Skip by name
        if job.job_name in skip_set:
            logger.debug("Skipping job '%s': in skip list", job.job_name)
            continue

        # Skip by backend
        if backend and job.backend != backend:
            logger.debug("Skipping job '%s': backend '%s' != '%s'", job.job_name, job.backend, backend)
            continue

        # Skip by architecture (derived from supplied image)
        if image_arch:
            this_job_arch = job_arch(job)
            if image_arch == "aarch64" and this_job_arch != "aarch64":
                logger.debug("Skipping job '%s': ARM image but job arch is %s", job.job_name, this_job_arch)
                continue
            if image_arch == "x86_64" and this_job_arch == "aarch64":
                logger.debug("Skipping job '%s': x86_64 image but job arch is %s", job.job_name, this_job_arch)
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


@dataclass(frozen=True, order=True)
class BackendTarget:
    """A backend/region/arch combination that matrix jobs need an image for."""

    backend: str
    region: str = ""
    arch: str = DEFAULT_ARCH

    def __str__(self) -> str:
        return "/".join(part for part in (self.backend, self.region, self.arch) if part)


def target_for_job(job: JobConfig, defaults: dict | None = None, region_override: str | None = None) -> BackendTarget:
    """Backend/region/arch combination a job's images are looked up in.

    The region is picked the way `build_job_parameters` picks it: a job that pins its own
    `params.region` keeps it, and `region_override` (the CLI `--region`) only fills in for
    jobs that don't — otherwise a multi-DC job would be resolved against a single region
    it does not actually run in (SCT-693).
    """
    region = job.params.get("region") or region_override or (defaults or {}).get("region", "")
    return BackendTarget(job.backend, _backend_region(job.backend, region), job_arch(job))


def job_uses_scylla_version(job: JobConfig, defaults: dict, overrides: dict | None = None) -> bool:
    """Whether a job's Scylla build comes from `scylla_version` (and thus from an image).

    Rolling-upgrade jobs get an empty `scylla_version` (they install from `new_scylla_repo`)
    and PGO jobs install a `unified_package`, so neither needs a per-backend image.
    """
    merged = dict(defaults)
    merged.update(job.params)
    merged.update({k: v for k, v in (overrides or {}).items() if v is not None})

    if str(merged.get("rolling_upgrade_test", "")).lower() == "true":
        return False
    return not merged.get("unified_package")


def resolve_versions_for_targets(
    original_version: str,
    reference_version: str,
    targets: list[BackendTarget],
    strategy: VersionResolution = DEFAULT_VERSION_RESOLUTION,
) -> tuple[dict[BackendTarget, str], dict[BackendTarget, str]]:
    """Pick the scylla_version to trigger with, per backend/region, following `strategy`.

    Args:
        original_version: What the trigger was asked for (e.g. "master:latest", or a full tag).
        reference_version: `original_version` resolved against AWS — used as-is by `aws-strict`
            and for versions that don't need resolving at all.
        targets: Backend/region pairs the filtered jobs run on.
        strategy: per-backend | common | aws-strict (see VersionResolution).

    Returns:
        (version per target, reason per target that must not be triggered).

    Raises:
        TriggerMatrixError: For an unknown strategy, or when `common` finds no build that is
            published on every target.
    """
    if strategy not in VERSION_RESOLUTION_STRATEGIES:
        raise TriggerMatrixError(
            f"Unknown version resolution strategy '{strategy}'. Valid: {', '.join(VERSION_RESOLUTION_STRATEGIES)}"
        )

    reference_version = reference_version or original_version
    resolvable = is_resolvable_partial_version(original_version)
    versions: dict[BackendTarget, str] = {}
    unavailable: dict[BackendTarget, str] = {}

    if strategy == "per-backend":
        for target in targets:
            if target.backend not in VALID_IMAGE_BACKENDS:
                # Nothing to resolve against — pass the request through as the downstream
                # job would have received it before backend-aware resolution existed.
                versions[target] = reference_version if not resolvable else original_version
                continue
            if not resolvable:
                # Explicit full/RC build tags and plain release versions already pin one
                # build, but that build may only be published on some backends — verify it
                # the same way aws-strict/common do instead of stamping it blindly.
                if images.version_exists_for_backend(
                    reference_version, target.backend, target.region, target.arch
                ):
                    versions[target] = reference_version
                else:
                    unavailable[target] = f"build '{reference_version}' is not published"
                continue
            if version := images._resolve_latest_version_for_backend(
                original_version, target.backend, target.region, target.arch
            ):
                # The build was found in the target's first region — a multi-DC job also needs
                # it in the others, which the image copy may not have reached yet.
                other_regions = split_regions(target.region)[1:]
                if any(
                    not images._version_exists_in_region(version, target.backend, other, target.arch)
                    for other in other_regions
                ):
                    unavailable[target] = f"build '{version}' is not published in every region"
                    continue
                versions[target] = version
            else:
                unavailable[target] = f"no image found for '{original_version}'"
        return versions, unavailable

    if strategy == "aws-strict":
        for target in targets:
            if images.version_exists_for_backend(reference_version, target.backend, target.region, target.arch):
                versions[target] = reference_version
            else:
                unavailable[target] = f"build '{reference_version}' is not published"
        return versions, unavailable

    # common — every job must run the exact same build, so take the newest build that is
    # published on all targets (i.e. the lowest of the per-backend latest builds).
    candidates: list[str] = []
    if resolvable:
        for target in targets:
            if target.backend not in VALID_IMAGE_BACKENDS:
                continue
            version = images._resolve_latest_version_for_backend(
                original_version, target.backend, target.region, target.arch
            )
            if version and version not in candidates:
                candidates.append(version)
        candidates.sort(key=_version_build_date, reverse=True)
    elif reference_version:
        candidates = [reference_version]

    if not candidates:
        raise TriggerMatrixError(
            f"Cannot resolve '{original_version}' to a build on any of: "
            f"{', '.join(str(t) for t in targets)}. Check that images were published."
        )

    for candidate in candidates:
        missing = [t for t in targets if not images.version_exists_for_backend(candidate, t.backend, t.region, t.arch)]
        if not missing:
            logger.info("Common version for %s: %s", ", ".join(str(t) for t in targets), candidate)
            return {target: candidate for target in targets}, {}
        logger.info(
            "Build '%s' is not published on %s — trying an older build",
            candidate,
            ", ".join(str(t) for t in missing),
        )

    raise TriggerMatrixError(
        f"No build of '{original_version}' is published on all of: {', '.join(str(t) for t in targets)} "
        f"(tried: {', '.join(candidates)}). Use version_resolution=per-backend to let every backend "
        f"run its own latest build, or aws-strict to trigger only the backends that have the AWS build."
    )


def _resolve_templates(value: object, branch: str, branch_id: str) -> object:
    """Replace {branch} and {branch_id} placeholders in a string value.

    {branch} is the bare branch (e.g. "2025.1", "master") — used where the S3
    path must not be prefixed (e.g. the `scylladb-{branch}` filename segment).
    {branch_id} is the directory-prefixed form (e.g. "branch-2025.1", "master")
    — used for the S3 directory path segment.
    """
    if isinstance(value, str):
        if "{branch}" in value:
            value = value.replace("{branch}", branch)
        if "{branch_id}" in value:
            value = value.replace("{branch_id}", branch_id)
    return value


def build_job_parameters(
    job: JobConfig,
    defaults: dict,
    scylla_version: str,
    cli_overrides: dict,
    branch_source_version: str | None = None,
) -> dict:
    """Build final parameter dict for a Jenkins job.

    Priority: cli_overrides > job.params > defaults, except for the location
    parameters in PER_JOB_LOCATION_PARAMS (region, availability_zone), where a
    job's own value wins over the CLI: cli_overrides > job.params is inverted to
    job.params > cli_overrides > defaults. A global `--region` must not collapse a
    multi-DC job into a single region (SCT-693).

    Always includes scylla_version. Downstream jobs resolve their
    own backend-specific images from the version.

    Template variables in parameter values are resolved:
      {branch} — bare branch, extracted from branch_source_version (or scylla_version if
          not provided), e.g. "2025.1" or "master".
      {branch_id} — directory-prefixed branch, derived from {branch}: "branch-2025.1"
          for non-master branches, "master" (no prefix) for master (SCT-782 — this
          mirrors the unstable-repo S3 directory layout, not get_branched_repo()'s
          internal variable naming).

    Args:
        job: Job configuration.
        defaults: Default parameters from the matrix.
        scylla_version: Version string to pass to the job (typically the resolved full version).
        cli_overrides: CLI-provided parameter overrides.
        branch_source_version: Original version string used for {branch}/{branch_id} template
            resolution (e.g., "master:latest"). When provided, branch is extracted from this
            instead of scylla_version. This avoids resolving {branch} to "2026.3" when the
            original input was "master:latest".

    Returns:
        Merged parameter dictionary.
    """
    overrides = {k: v for k, v in cli_overrides.items() if v is not None}
    # Location overrides are applied *under* the job's own params — they fill in for
    # jobs that don't pin a region/AZ instead of overwriting the ones that do.
    location_overrides = {k: overrides.pop(k) for k in PER_JOB_LOCATION_PARAMS if k in overrides}

    params = dict(defaults)
    params.update(location_overrides)
    params.update(job.params)
    params.update(overrides)

    # Always set scylla_version (if provided)
    if scylla_version:
        params["scylla_version"] = scylla_version
    if job.job_throttle_category:
        params.setdefault("job_throttle_category", job.job_throttle_category)

    is_rolling_upgrade = str(params.get("rolling_upgrade_test", "")).lower() == "true"
    if is_rolling_upgrade:
        params["scylla_version"] = ""
    else:
        # new_scylla_repo is only relevant for rolling-upgrade jobs; passing it
        # to regular tests (e.g. perf) causes them to install wrong packages.
        params.pop("new_scylla_repo", None)

    # Resolve {branch}/{branch_id} templates — use the original version (e.g.,
    # "master:latest") not the resolved full tag (e.g., "2026.3.0~dev-...") which
    # yields "2026.3".
    branch = _extract_branch_from_version(branch_source_version or scylla_version)
    if branch:
        branch_id = _branch_directory_id(branch)
        params = {k: _resolve_templates(v, branch, branch_id) for k, v in params.items()}

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

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

"""Turning a job definition into the parameters and job path Jenkins is given."""

import logging

from sdcm.utils.trigger_matrix.constants import PER_JOB_LOCATION_PARAMS
from sdcm.utils.trigger_matrix.models import JobConfig
from sdcm.utils.trigger_matrix.versions import _branch_directory_id, _extract_branch_from_version

logger = logging.getLogger(__name__)


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

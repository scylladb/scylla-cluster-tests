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

"""Selecting which jobs of a matrix a given trigger should run."""

import logging

from sdcm.utils.trigger_matrix.models import JobConfig, job_arch
from sdcm.utils.trigger_matrix.versions import _strip_version_qualifier

logger = logging.getLogger(__name__)


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

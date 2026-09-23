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

"""Scylla version string parsing.

Pure string work: no cloud lookups, no pydantic models, no Jenkins. The version patterns
themselves live in `sdcm.utils.version_utils`, which is the single home for them; this
module holds the trigger-matrix predicates and extractors built on top.

Choosing *which* build a backend should run is policy, not parsing -- that lives in
`resolution`, which is allowed to call the cloud image lookups this module must not."""

import logging
import re

from sdcm.utils.trigger_matrix.errors import TriggerMatrixError
from sdcm.utils.version_utils import (
    BRANCH_VERSION_RE,
    FULL_VERSION_TAG_RE,
    RELEASE_VERSION_RE,
    SIMPLE_VERSION_RE,
)

logger = logging.getLogger(__name__)


def is_full_version_tag(version: str) -> bool:
    """Check if a version string is a full version tag (e.g., 2024.2.5-0.20250221.cb9e2a54ae6d-1)."""
    return bool(FULL_VERSION_TAG_RE.match(version))


def is_resolvable_partial_version(scylla_version: str) -> bool:
    """Check whether a version string points at "whatever is newest" rather than one build.

    Those are the versions that have to be resolved against a backend's published images:
    branch:qualifier (``master:latest``) and simple versions (``2025.4``).
    """
    if not scylla_version or is_full_version_tag(scylla_version) or RELEASE_VERSION_RE.match(scylla_version):
        return False
    return bool(BRANCH_VERSION_RE.match(scylla_version) or SIMPLE_VERSION_RE.match(scylla_version))


def _as_branch_qualifier(scylla_version: str) -> str:
    """Normalize a partial version to the branch:qualifier form all image lookups expect.

    Examples:
        >>> _as_branch_qualifier("master:latest")
        'master:latest'
        >>> _as_branch_qualifier("2025.4")
        'branch-2025.4:latest'
        >>> _as_branch_qualifier("2025.4.0")
        'branch-2025.4:latest'
    """
    if BRANCH_VERSION_RE.match(scylla_version):
        return scylla_version
    if simple_match := SIMPLE_VERSION_RE.match(scylla_version):
        return f"branch-{simple_match.group('major')}.{simple_match.group('minor')}:latest"
    return scylla_version


def _gce_label_to_version(label: str) -> str:
    """Rebuild a full version tag from the dashed `scylla_version` label of a GCE image.

    GCE labels can't hold dots or tildes, so the separators have to be put back:

        >>> _gce_label_to_version("2026-4-0-dev-0-20260804-9a3aba9e452a")
        '2026.4.0~dev-0.20260804.9a3aba9e452a'
        >>> _gce_label_to_version("2026-3-0-rc1-0-20260730-726f67a532e2")
        '2026.3.0.rc1.0.20260730.726f67a532e2'
        >>> _gce_label_to_version("2025-4-10-0-20260609-99f4121cd8e1")
        '2025.4.10-0.20260609.99f4121cd8e1'

    Returns an empty string when the result isn't a valid full version tag.
    """
    parts = label.split("-")
    if len(parts) < 4:
        logger.warning("GCE label '%s' doesn't look like a full version label", label)
        return ""
    major, minor, patch, *rest = parts
    if rest[0] == "dev":
        version = f"{major}.{minor}.{patch}~dev-" + ".".join(rest[1:])
    elif rest[0].startswith("rc"):
        version = f"{major}.{minor}.{patch}." + ".".join(rest)
    else:
        version = f"{major}.{minor}.{patch}-" + ".".join(rest)

    if not is_full_version_tag(version):
        logger.warning("GCE label '%s' doesn't map to a valid full version tag (got '%s')", label, version)
        return ""
    return version


def determine_job_folder(scylla_version: str, job_folder: str | None = None) -> str:
    """Derive Jenkins job folder from version string.

    Args:
        scylla_version: Version string in any supported format.
        job_folder: Explicit override — returned as-is if provided.

    Returns:
        Jenkins job folder name (e.g., 'scylla-master', 'scylla-2025.4').

    Examples:
        >>> determine_job_folder("master:latest")
        'scylla-master'
        >>> determine_job_folder("master")
        'scylla-master'
        >>> determine_job_folder("2025.4")
        'scylla-2025.4'
        >>> determine_job_folder("2025.4.1")
        'scylla-2025.4'
        >>> determine_job_folder("2024.2.5-0.20250221.cb9e2a54ae6d-1")
        'scylla-2024.2'
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
        return "scylla-master" if branch == "master" else f"scylla-{branch}"

    # Handle full version tags (e.g., "2024.2.5-0.20250221.cb9e2a54ae6d-1")
    full_match = FULL_VERSION_TAG_RE.match(scylla_version)
    if full_match:
        major = full_match.group("major")
        minor = full_match.group("minor")
        return f"scylla-{major}.{minor}"

    # Handle simple version strings (e.g., "2025.4", "2025.4.0")
    simple_match = SIMPLE_VERSION_RE.match(scylla_version)
    if simple_match:
        major = simple_match.group("major")
        minor = simple_match.group("minor")
        return f"scylla-{major}.{minor}"

    # Handle bare "master"
    if scylla_version.strip().lower() == "master":
        return "scylla-master"

    raise TriggerMatrixError(
        f"Cannot determine job folder from version '{scylla_version}'. Provide an explicit --job-folder."
    )


def _strip_version_qualifier(scylla_version: str) -> str:
    """Strip branch qualifier from version string.

    If version matches branch:qualifier format (e.g., "master:latest"),
    returns the branch part. Otherwise returns the original string.
    """
    branch_match = BRANCH_VERSION_RE.match(scylla_version)
    if branch_match:
        return branch_match.group("branch")
    return scylla_version


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


def _version_build_date(version: str) -> str:
    """Extract the YYYYMMDD build date from a full version tag (empty when absent)."""
    match = re.search(r"[.-](\d{8})[.-]", version)
    return match.group(1) if match else ""


def _branch_directory_id(branch: str) -> str:
    """Compute the S3 directory-prefixed branch id for a bare branch string.

    SCT-782: the unstable-repo S3 layout uses a `branch-` prefix for non-master
    branches in the directory path segment (e.g. `branch-2025.1`), while `master`
    has no prefix; the filename segment, in contrast, always stays bare (e.g.
    `scylladb-2025.1`). This helper produces the former. If `branch` already
    carries a `branch-` or `enterprise-` prefix (or is empty), it is returned
    unchanged.
    """
    if not branch or branch == "master" or branch.startswith(("branch-", "enterprise-")):
        return branch
    return f"branch-{branch}"

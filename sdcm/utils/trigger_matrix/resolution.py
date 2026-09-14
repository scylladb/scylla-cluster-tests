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

"""Deciding which Scylla build each backend/region target is triggered with.

Policy, as opposed to the parsing in `versions` and the raw lookups in `images`: this is where
the per-backend, common and aws-strict strategies live. Cloud lookups are reached through the
`images` module object so a single patch of that module covers every call site here."""

import logging

from sdcm.utils.trigger_matrix import images
from sdcm.utils.trigger_matrix.backends import _backend_region, split_regions
from sdcm.utils.trigger_matrix.constants import (
    DEFAULT_ARCH,
    DEFAULT_VERSION_RESOLUTION,
    VALID_IMAGE_BACKENDS,
    VERSION_RESOLUTION_STRATEGIES,
    VersionResolution,
)
from sdcm.utils.trigger_matrix.errors import TriggerMatrixError
from sdcm.utils.trigger_matrix.models import BackendTarget, JobConfig, job_arch
from sdcm.utils.trigger_matrix.versions import (
    RELEASE_VERSION_RE,
    _version_build_date,
    is_full_version_tag,
    is_resolvable_partial_version,
)

logger = logging.getLogger(__name__)


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
            if not resolvable or target.backend not in VALID_IMAGE_BACKENDS:
                # Nothing to resolve against — pass the request through as the downstream
                # job would have received it before backend-aware resolution existed.
                versions[target] = reference_version if not resolvable else original_version
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

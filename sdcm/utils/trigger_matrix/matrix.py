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

"""The trigger matrix entry point.

`trigger_matrix()` is the only place the loading, filtering, version resolution, parameter
building, triggering and reporting layers meet."""

import logging
import os

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError
from sdcm.utils.trigger_matrix.filters import filter_jobs
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient
from sdcm.utils.trigger_matrix.models import BackendTarget, JobConfig, _PendingWaitJob
from sdcm.utils.trigger_matrix.parameters import build_job_parameters, resolve_job_path
from sdcm.utils.trigger_matrix.reporting import send_trigger_matrix_email
from sdcm.utils.trigger_matrix.resolution import (
    job_uses_scylla_version,
    resolve_versions_for_targets,
    target_for_job,
)
from sdcm.utils.trigger_matrix.versions import determine_job_folder

logger = logging.getLogger(__name__)


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
    jenkins = JenkinsClient()
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
                queue_url = jenkins.trigger_with_queue(full_path, params, dry_run=dry_run)
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
            success = jenkins.trigger(full_path, params, dry_run=dry_run)
            if success:
                triggered_paths.add(full_path)
                results["triggered"].append(full_path)
            else:
                results["failed"].append(full_path)

    # Step 2: Wait for all wait-mode jobs concurrently
    if pending_wait_jobs:
        logger.info("All jobs triggered. Waiting for %d gating jobs to complete...", len(pending_wait_jobs))
        build_results = jenkins.wait_for_builds(pending_wait_jobs, dry_run=dry_run)
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

#!/usr/bin/env python3

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

"""
GKE cluster cleanup utility based on age/retention time.

This script cleans GKE clusters and associated resources based on their age and keep labels:
- GKE clusters
- Orphaned GKE disks (persistent volumes not attached to any cluster)
"""

import argparse
import os
from datetime import datetime, timezone

from sdcm.utils.context_managers import environment
from sdcm.utils.gce_utils import SUPPORTED_PROJECTS, GkeCleaner
from utils.cloud_cleanup import (
    DEFAULT_KEEP_HOURS,
    LOGGER,
    get_keep_hours_from_tags,
    should_keep,
)


def get_cluster_creation_time(cluster):
    """Get GKE cluster creation time."""
    # Try to get from cluster_info
    if hasattr(cluster, "cluster_info"):
        create_time_str = cluster.cluster_info.get("createTime", "")
    else:
        # Fallback to body attribute
        create_time_str = getattr(cluster, "body", {}).get("createTime", "")

    if create_time_str:
        return (
            datetime.fromisoformat(create_time_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
        )
    return datetime.now(timezone.utc).replace(tzinfo=None)


def get_cluster_tags(cluster) -> dict:
    """Get GKE cluster tags/labels as a dictionary."""
    # Try cluster_info first
    if hasattr(cluster, "cluster_info"):
        return cluster.cluster_info.get("resourceLabels", {})
    # Fallback to body attribute
    return getattr(cluster, "body", {}).get("resourceLabels", {})


def clean_gke_clusters(gke_cleaner, project_id, keep_hours=DEFAULT_KEEP_HOURS, dry_run=False):
    """Clean GKE clusters in the specified project."""
    total_clusters = 0
    deleted_clusters = 0
    kept_clusters = 0

    try:
        clusters = gke_cleaner.list_gke_clusters()
        if not clusters:
            LOGGER.info("No GKE clusters found in project: %s", project_id)
            return

        for cluster in clusters:
            total_clusters += 1
            cluster_name = cluster.name
            try:
                # Get cluster creation time and tags
                cluster_creation_time = get_cluster_creation_time(cluster)
                cluster_tags = get_cluster_tags(cluster)

                # Only process clusters created by SCT
                if cluster_tags.get("createdby", "").upper() != "SCT":
                    LOGGER.info(
                        "Skipping cluster %s - not created by SCT (createdby=%s)",
                        cluster_name,
                        cluster_tags.get("createdby"),
                    )
                    kept_clusters += 1
                    continue

                # Check if cluster should be kept
                if should_keep(cluster_creation_time, get_keep_hours_from_tags(cluster_tags, default=keep_hours)):
                    LOGGER.info(
                        "Keeping cluster %s, keep: %s, creation time: %s",
                        cluster_name,
                        cluster_tags.get("keep", "not set"),
                        cluster_creation_time,
                    )
                    kept_clusters += 1
                    continue

                # Check keep_action tag
                keep_action = cluster_tags.get("keep_action", "terminate").lower()
                if keep_action not in ("terminate", ""):
                    LOGGER.info(
                        "Skipping cluster %s due to keep_action: %s",
                        cluster_name,
                        keep_action,
                    )
                    kept_clusters += 1
                    continue

                # Delete cluster
                if not dry_run:
                    LOGGER.info(
                        "Terminating GKE cluster %s in project %s, creation time: %s",
                        cluster_name,
                        project_id,
                        cluster_creation_time,
                    )
                    try:
                        res = cluster.destroy()
                        LOGGER.info("%s deleted, result=%s", cluster_name, res)
                        deleted_clusters += 1
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.error("Error while terminating cluster %s: %s", cluster_name, exc)
                else:
                    LOGGER.info(
                        "Dry run: would terminate cluster %s, creation time: %s",
                        cluster_name,
                        cluster_creation_time,
                    )
                    deleted_clusters += 1

            except Exception as exc:  # noqa: BLE001
                LOGGER.error(
                    "Failed to process cluster %s in project %s: %s",
                    cluster_name,
                    project_id,
                    exc,
                )

    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to list GKE clusters in project %s: %s", project_id, exc)

    LOGGER.info(
        "GKE cleanup summary for project %s - Total: %d, Deleted: %d, Kept: %d",
        project_id,
        total_clusters,
        deleted_clusters,
        kept_clusters,
    )


def clean_orphaned_gke_disks(gke_cleaner, project_id, dry_run=False):
    """Clean orphaned GKE disks in the specified project."""
    try:
        orphaned_disks = gke_cleaner.list_orphaned_gke_disks()
        if not orphaned_disks:
            LOGGER.info("No orphaned GKE disks found in project: %s", project_id)
            return

        LOGGER.info(
            "Found orphaned GKE disks in project %s: %s",
            project_id,
            orphaned_disks,
        )

        total_disks = sum(len(disks) for disks in orphaned_disks.values())

        if not dry_run:
            deleted_disks = 0
            for zone, disk_names in orphaned_disks.items():
                try:
                    gke_cleaner.clean_disks(disk_names=disk_names, zone=zone)
                    LOGGER.info(
                        "Deleted orphaned GKE disks in zone %s (%s project): %s",
                        zone,
                        project_id,
                        disk_names,
                    )
                    deleted_disks += len(disk_names)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.error("Failed to delete orphaned disks in zone %s: %s", zone, exc)

            LOGGER.info(
                "Orphaned disks cleanup summary for project %s - Total: %d, Deleted: %d",
                project_id,
                total_disks,
                deleted_disks,
            )
        else:
            LOGGER.info(
                "Dry run: would delete %d orphaned GKE disks in project %s",
                total_disks,
                project_id,
            )

    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to clean orphaned GKE disks in project %s: %s", project_id, exc)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser("gke_cleanup")
    arg_parser.add_argument(
        "--duration",
        type=int,
        help="duration to keep non-tagged clusters running in hours",
        default=os.environ.get("DURATION", str(DEFAULT_KEEP_HOURS)),
    )
    arg_parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        help="do not terminate anything",
        default=os.environ.get("DRY_RUN"),
    )

    args = arg_parser.parse_args()

    is_dry_run = bool(args.dry_run)
    keep_hours = int(args.duration)

    if is_dry_run:
        LOGGER.error("'Dry run' mode on")

    for project in SUPPORTED_PROJECTS:
        with environment(SCT_GCE_PROJECT=project):
            gke_cleaner = GkeCleaner()
            clean_gke_clusters(gke_cleaner=gke_cleaner, project_id=project, keep_hours=keep_hours, dry_run=is_dry_run)
            clean_orphaned_gke_disks(gke_cleaner=gke_cleaner, project_id=project, dry_run=is_dry_run)

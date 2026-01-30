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

import logging
from pathlib import Path

import click
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOGGER = logging.getLogger(__name__)


class S3BackupCleaner:
    """Clean up manager backup directories from S3 buckets."""

    BACKUP_SUBDIRS = ["meta", "schema", "sst"]

    def __init__(self, bucket_name: str, dry_run: bool = False):
        """
        Initialize S3 backup cleaner.

        Args:
            bucket_name: Name of the S3 bucket to clean
            dry_run: If True, only print what would be deleted without actual deletion
        """
        self.bucket_name = bucket_name
        self.dry_run = dry_run
        self.s3_client = boto3.client("s3")
        self.cluster_ids_to_delete = set()
        self.snapshot_tags_to_delete = set()
        self.deletion_mode = None  # 'cluster_ids' or 'snapshot_tags'

    def load_cluster_ids_to_delete(self, file_path: str) -> None:
        """
        Load the list of cluster IDs to delete.

        Args:
            file_path: Path to file containing cluster IDs to delete (one per line)
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Cluster IDs file not found: {file_path}")

        with path.open("r") as f:
            self.cluster_ids_to_delete = {line.strip() for line in f if line.strip()}

        self.deletion_mode = "cluster_ids"
        LOGGER.info(f"Loaded {len(self.cluster_ids_to_delete)} cluster IDs to delete from {file_path}")

    def load_snapshot_tags_to_delete(self, file_path: str) -> None:
        """
        Load the list of snapshot tags to delete.

        Args:
            file_path: Path to file containing snapshot tags to delete (one per line)
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Snapshot tags file not found: {file_path}")

        with path.open("r") as f:
            self.snapshot_tags_to_delete = {line.strip() for line in f if line.strip()}

        self.deletion_mode = "snapshot_tags"
        LOGGER.info(f"Loaded {len(self.snapshot_tags_to_delete)} snapshot tags to delete from {file_path}")

    def find_cluster_ids_by_snapshot_tags(self) -> None:
        """
        Find cluster IDs that contain the specified snapshot tags.

        Scans the backup/meta/cluster/ directory to find manifest files matching
        the snapshot tags and populates self.cluster_ids_to_delete with the found cluster IDs.
        """
        snapshot_tag_to_cluster_ids = {tag: set() for tag in self.snapshot_tags_to_delete}
        prefix = "backup/meta/cluster/"

        LOGGER.info("Scanning S3 bucket to find clusters with matching snapshot tags...")

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            scanned_files = 0
            for page in pages:
                objects = page.get("Contents", [])
                for obj in objects:
                    key = obj["Key"]
                    scanned_files += 1

                    if scanned_files % 1000 == 0:
                        LOGGER.info(f"Scanned {scanned_files} files...")

                    # Check if any snapshot tag is in the filename
                    for snapshot_tag in self.snapshot_tags_to_delete:
                        if f"_tag_{snapshot_tag}_" in key:
                            # Extract cluster_id from path like:
                            # backup/meta/cluster/{cluster_id}/dc/.../task_xxx_tag_{snapshot_tag}_manifest.json.gz
                            parts = key.split("/")
                            if len(parts) >= 4 and "/".join(parts[0:3]) == "backup/meta/cluster":
                                cluster_id = parts[3]
                                snapshot_tag_to_cluster_ids[snapshot_tag].add(cluster_id)
                                LOGGER.debug(f"Found snapshot tag '{snapshot_tag}' in cluster '{cluster_id}'")

            LOGGER.info(f"Finished scanning {scanned_files} files")

            # Log results and collect all unique cluster IDs
            for snapshot_tag, cluster_ids in snapshot_tag_to_cluster_ids.items():
                if cluster_ids:
                    LOGGER.info(
                        f"Snapshot tag '{snapshot_tag}' found in {len(cluster_ids)} cluster(s): "
                        f"{', '.join(sorted(cluster_ids))}"
                    )
                    self.cluster_ids_to_delete.update(cluster_ids)
                else:
                    LOGGER.warning(f"Snapshot tag '{snapshot_tag}' not found in any cluster")

            if self.cluster_ids_to_delete:
                LOGGER.info(f"\nFound total of {len(self.cluster_ids_to_delete)} unique cluster(s) to delete")
            else:
                LOGGER.warning("No clusters found containing any of the specified snapshot tags")

        except ClientError as e:
            LOGGER.error(f"Error scanning S3 bucket: {e}")
            raise

    def delete_cluster_directory(self, cluster_id: str, subdir: str) -> int:
        """
        Delete all objects in a specific cluster directory.

        Args:
            cluster_id: The cluster ID to delete
            subdir: The subdirectory (meta, schema, or sst)

        Returns:
            Number of objects deleted
        """
        prefix = f"backup/{subdir}/cluster/{cluster_id}/"
        deleted_count = 0

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in pages:
                objects = page.get("Contents", [])
                if not objects:
                    continue

                if self.dry_run:
                    deleted_count += len(objects)
                else:
                    # Delete objects in batches of 1000 (S3 API limit)
                    delete_keys = [{"Key": obj["Key"]} for obj in objects]
                    response = self.s3_client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": delete_keys})
                    deleted = response.get("Deleted", [])
                    deleted_count += len(deleted)

                    errors = response.get("Errors", [])
                    for error in errors:
                        LOGGER.error(f"Error deleting {error['Key']}: {error['Message']}")

            if deleted_count > 0:
                action = "Would delete" if self.dry_run else "Deleted"
                LOGGER.info(f"  {action} s3://{self.bucket_name}/{prefix} ({deleted_count} objects)")

        except ClientError as e:
            LOGGER.error(f"Error deleting cluster directory {prefix}: {e}")
            raise

        return deleted_count

    def clean_up_cluster_ids(self) -> dict[str, int]:
        """
        Clean up the specified cluster IDs.

        Returns:
            Dictionary with statistics about deleted objects per subdirectory
        """
        if not self.cluster_ids_to_delete:
            LOGGER.info("No cluster IDs to delete.")
            return {}

        LOGGER.info(f"Will delete {len(self.cluster_ids_to_delete)} cluster(s):")
        for cluster_id in sorted(self.cluster_ids_to_delete):
            LOGGER.info(f"  - {cluster_id}")

        if self.dry_run:
            LOGGER.info("\n=== DRY RUN MODE - No actual deletions will be performed ===\n")

        stats = {}
        for subdir in self.BACKUP_SUBDIRS:
            LOGGER.info(f"\nProcessing backup/{subdir}/cluster/ directories:")
            total_deleted = 0

            for cluster_id in self.cluster_ids_to_delete:
                deleted = self.delete_cluster_directory(cluster_id, subdir)
                total_deleted += deleted

            stats[subdir] = total_deleted
            if total_deleted > 0:
                LOGGER.info(
                    f"Subtotal {subdir}: {total_deleted} objects {'would be deleted' if self.dry_run else 'deleted'}"
                )

        return stats

    def run(self, cluster_ids_file: str = None, snapshot_tags_file: str = None) -> None:
        """
        Run the cleanup process.

        Args:
            cluster_ids_file: Path to file containing cluster IDs to delete (optional)
            snapshot_tags_file: Path to file containing snapshot tags to delete (optional)
        """
        LOGGER.info(f"Starting S3 backup cleanup for bucket: {self.bucket_name}")
        LOGGER.info(f"Dry run mode: {self.dry_run}")

        if cluster_ids_file:
            self.load_cluster_ids_to_delete(cluster_ids_file)
        else:
            self.load_snapshot_tags_to_delete(snapshot_tags_file)
            self.find_cluster_ids_by_snapshot_tags()

        stats = self.clean_up_cluster_ids()

        LOGGER.info("\n=== Summary ===")
        total = sum(stats.values())
        LOGGER.info(f"Total objects {'would be deleted' if self.dry_run else 'deleted'}: {total}")
        for subdir, count in stats.items():
            LOGGER.info(f"  - {subdir}: {count}")

        if self.dry_run:
            LOGGER.info("\nThis was a dry run. To perform actual deletion, run without --dry-run flag")


@click.command()
@click.option("--bucket-name", required=True, help="Name of the S3 bucket containing backups")
@click.option(
    "--cluster-ids-file",
    type=click.Path(exists=True),
    help="Path to file containing cluster IDs to delete (one per line)",
)
@click.option(
    "--snapshot-tags-file",
    type=click.Path(exists=True),
    help="Path to file containing snapshot tags to delete (one per line)",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Dry run mode - only print what would be deleted without actual deletion",
)
def main(bucket_name: str, cluster_ids_file: str, snapshot_tags_file: str, dry_run: bool) -> None:
    """
    Clean up manager backup directories from AWS S3 bucket.

    This script can delete backups in two modes:

    1. By cluster IDs (--cluster-ids-file): Deletes all backups for specified clusters
    2. By snapshot tags (--snapshot-tags-file): Finds clusters containing the specified
       snapshots and deletes ALL backups for those clusters

    The backup structure is:

    \b
    bucket_name/
      backup/
        meta/cluster/cluster_id/
        schema/cluster/cluster_id/
        sst/cluster/cluster_id/

    Examples:

    \b
        # Delete all backups for specific cluster IDs (dry run)
        python delete_manager_backups_from_s3.py \\
            --bucket-name my-backup-bucket \\
            --cluster-ids-file cluster_ids_to_delete.txt \\
            --dry-run

    \b
        # Find clusters with specific snapshots and delete ALL their backups
        python delete_manager_backups_from_s3.py \\
            --bucket-name my-backup-bucket \\
            --snapshot-tags-file snapshot_tags_to_delete.txt \\
    """
    if not cluster_ids_file and not snapshot_tags_file:
        raise click.ClickException("Either --cluster-ids-file or --snapshot-tags-file must be provided")

    if cluster_ids_file and snapshot_tags_file:
        raise click.ClickException("Cannot specify both --cluster-ids-file and --snapshot-tags-file simultaneously")

    try:
        cleaner = S3BackupCleaner(bucket_name=bucket_name, dry_run=dry_run)
        cleaner.run(cluster_ids_file=cluster_ids_file, snapshot_tags_file=snapshot_tags_file)
    except Exception as e:  # noqa: BLE001
        LOGGER.error(f"Failed to clean up backups: {e}")
        raise click.ClickException(str(e))


if __name__ == "__main__":
    main()

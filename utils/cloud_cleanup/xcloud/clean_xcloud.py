#!/usr/bin/env python

import argparse
import datetime
import os
import re
import pytz

from sdcm.cloud_api_client import ScyllaCloudAPIClient
from sdcm.keystore import KeyStore

DRY_RUN = False
ENVIRONMENTS = ['lab', 'staging']

KEEP_HOURS_PATTERN = re.compile(r'-keep-(\d+)h$')


def should_delete_cluster(cluster: dict, now: datetime.datetime) -> tuple[bool, str]:
    """Determine if a ScyllaDB Cloud cluster should be deleted based on name and creation time"""
    cluster_name = cluster.get('clusterName', 'Unknown')
    created_at_str = cluster.get('createdAt')

    match = KEEP_HOURS_PATTERN.search(cluster_name)
    keep_hours = int(match.group(1)) if match else None

    if keep_hours is None:
        return False, "No keep suffix in name (likely the manually created cluster)"

    created_at = datetime.datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
    age_hours = (now - created_at).total_seconds() / 3600.0

    if age_hours > keep_hours:
        return True, f"Age {age_hours:.1f}h > keep {keep_hours}h"
    return False, f"Age {age_hours:.1f}h < keep {keep_hours}h (expires in {keep_hours - age_hours:.1f}h)"


def print_cluster(cluster: dict, msg: str):
    print(f"Cluster {cluster.get('clusterName')} (ID: {cluster.get('id')}) created at {cluster.get('createdAt')}: {msg}")


def delete_cluster(cluster: dict, api_client: ScyllaCloudAPIClient, account_id: int):
    cluster_name = cluster.get('clusterName')
    cluster_id = cluster.get('id')

    try:
        if not DRY_RUN:
            api_client.delete_cluster(account_id=account_id, cluster_id=cluster_id, cluster_name=cluster_name)
            # TODO: add updating resources state in argus
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to delete cluster {cluster_name}: {exc}")


def clean_clusters_for_environment(environment: str) -> tuple[int, int, int]:
    """Clean expired clusters for given environment"""
    print(f"\nCleaning Scylla Cloud clusters for '{environment}' environment")
    print(f"{'-' * 80}")

    try:
        credentials = KeyStore().get_cloud_rest_credentials(environment)
        api_client = ScyllaCloudAPIClient(api_url=credentials['base_url'], auth_token=credentials['api_token'])
        account_id = api_client.get_account_details().get('accountId')
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to initialize API client for '{environment}' environment: {exc}")
        return 0, 0, 0

    clusters = api_client.get_clusters(account_id=account_id, enriched=True)
    print(f"Found {len(clusters)} cluster(s) in environment '{environment}'")

    now = datetime.datetime.now(tz=pytz.utc)
    deleted_clusters, kept_clusters = 0, 0
    for cluster in clusters:
        cluster_details = api_client.get_cluster_details(
            account_id=account_id, cluster_id=cluster.get('id'), enriched=True)
        should_delete, reason = should_delete_cluster(cluster_details, now)

        print_cluster(cluster_details, f"{'DELETING' if should_delete else 'KEEPING'} - {reason}")
        if should_delete:
            delete_cluster(cluster_details, api_client, account_id)
            deleted_clusters += 1
        else:
            kept_clusters += 1

    if clusters:
        print(f"Environment '{environment}' cleanup summary: deleted {deleted_clusters}, kept {kept_clusters}")
    return len(clusters), deleted_clusters, kept_clusters


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        'xcloud_cleanup', description='Clean up expired ScyllaDB Cloud test clusters')
    arg_parser.add_argument(
        "--environments", type=str, nargs='+',
        help=f"ScyllaDB Cloud environments to clean (default: {ENVIRONMENTS})",
        default=ENVIRONMENTS)
    arg_parser.add_argument(
        "--dry-run", action="store_true",
        help="Do not delete anything, just print what would be deleted",
        default=os.environ.get('DRY_RUN'))

    arguments = arg_parser.parse_args()
    DRY_RUN = bool(arguments.dry_run)
    if DRY_RUN:
        print("DRY RUN MODE - No clusters will be deleted")

    total, deleted, kept = 0, 0, 0
    for env in arguments.environments:
        t, d, k = clean_clusters_for_environment(env)
        total += t
        deleted += d
        kept += k

    print(f"\n{'='*40}")
    print("OVERALL SUMMARY:")
    print(f"  Total clusters checked: {total}")
    print(f"  Clusters deleted: {deleted}")
    print(f"  Clusters kept: {kept}")
    print(f"{'='*40}")

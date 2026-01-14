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
# Copyright (c) 2020 ScyllaDB

"""S3 utilities for unit tests."""

import boto3
from botocore import UNSIGNED
from botocore.client import Config


def get_latest_branches_from_s3(bucket="downloads.scylladb.com", limit=3):
    """
    Dynamically discover the latest available OSS branches from S3.

    :param bucket: S3 bucket name
    :param limit: Number of latest branches to return
    :return: list of branch names (e.g., ['master', 'branch-2025.4', 'branch-2025.3'])
    """
    try:
        # Create S3 client without credentials for public bucket
        s3_client = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # Get OSS branches
        oss_branches = []
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix="unstable/scylla/", Delimiter="/")
        for prefix in response.get("CommonPrefixes", []):
            branch_path = prefix["Prefix"]
            # Extract branch name: "unstable/scylla/branch-2025.3/" -> "branch-2025.3"
            branch_name = branch_path.replace("unstable/scylla/", "").rstrip("/")
            if branch_name.startswith("branch-") or branch_name == "master":
                oss_branches.append(branch_name)

        # Sort OSS branches (master should be included, and branch-X.Y sorted by version)
        oss_branches_sorted = []
        if "master" in oss_branches:
            oss_branches_sorted.append("master")
        # Sort branch-X.Y by version number
        branch_versions = [b for b in oss_branches if b.startswith("branch-")]
        branch_versions.sort(
            key=lambda x: [int(p) if p.isdigit() else p for p in x.replace("branch-", "").split(".")], reverse=True
        )
        oss_branches_sorted.extend(branch_versions[:limit])

        return oss_branches_sorted
    except (boto3.exceptions.Boto3Error, KeyError, ValueError):
        # Fallback to hardcoded values if S3 access fails or parsing errors occur
        # Using fallback ensures tests remain functional even if S3 is temporarily unavailable
        return ["master", "branch-2025.4", "branch-2025.3", "branch-2025.2"]

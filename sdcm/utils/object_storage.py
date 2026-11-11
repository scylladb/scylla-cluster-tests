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
"""Backup bucket region and `append_scylla_yaml.object_storage_endpoints`, resolved from SCT params."""

import copy


def backup_bucket_region(params) -> str:
    """First test region, where the backup bucket lives (see `backup_bucket_location`).

    `oci_region_name` on OCI, otherwise `region_name`.
    """
    if params.get("cluster_backend") == "oci":
        regions = params.get("oci_region_name") or []
        if isinstance(regions, str):
            regions = regions.split()
        return next(iter(regions), "")
    return next(iter(params.region_names), "")


def object_storage_endpoints(params) -> list[dict]:
    """`object_storage_endpoints` with `{region}` in `name` and `aws_region` replaced by the backup bucket region."""
    endpoints = copy.deepcopy((params.get("append_scylla_yaml") or {}).get("object_storage_endpoints") or [])
    if endpoints:
        region = backup_bucket_region(params)
        for endpoint in endpoints:
            for key in ("name", "aws_region"):
                if key in endpoint:
                    endpoint[key] = endpoint[key].format(region=region)
    return endpoints

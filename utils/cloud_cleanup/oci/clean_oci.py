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

import argparse
from datetime import datetime, timezone
import os

import oci

from sdcm.provision.oci.constants import TAG_NAMESPACE
from sdcm.utils.log import setup_stdout_logger
from sdcm.utils.oci_utils import (
    delete_oci_volume_with_retry,
    OciService,
    oci_keep_action,
    SUPPORTED_REGIONS,
    get_oci_compartment_id,
    list_instances_oci,
)
from utils.cloud_cleanup import DEFAULT_KEEP_HOURS, get_keep_hours_from_tags, should_keep, update_argus_resource_status

LOGGER = setup_stdout_logger()


def _to_utc_naive(value: datetime | None) -> datetime | None:
    if not value:
        return None
    if value.tzinfo:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def clean_oci_instances(default_keep_hours: int, dry_run: bool = False) -> None:
    instances = list_instances_oci(verbose=False)

    if not instances:
        LOGGER.info("No OCI instances found")
        return

    compute_clients = {}

    for instance in instances:
        tags = (instance.defined_tags or {}).get(TAG_NAMESPACE, {})
        if tags.get("CreatedBy") != "SCT":
            continue

        if tags.get("NodeType") == "sct-runner":
            LOGGER.info("Skipping SCT Runner instance '%s'", instance.display_name)
            continue

        vm_creation_time = _to_utc_naive(instance.time_created)
        keep_hours = get_keep_hours_from_tags(tags, default=default_keep_hours)
        if vm_creation_time and should_keep(vm_creation_time, keep_hours):
            LOGGER.info(
                "Keeping OCI instance %s, keep=%s, creation time=%s",
                instance.display_name,
                tags.get("keep", "not set"),
                vm_creation_time,
            )
            continue

        region = instance.region
        if region not in compute_clients:
            compute_clients[region] = OciService().get_compute_client(region=region)
        compute_client = compute_clients[region]

        keep_action = oci_keep_action(tags)
        if keep_action in ("terminate", ""):
            LOGGER.info("Terminating OCI instance %s in region %s", instance.display_name, region)
            if dry_run:
                continue
            try:
                compute_client.terminate_instance(instance.id)
                update_argus_resource_status(tags.get("TestId", ""), instance.display_name, "terminate")
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Failed to terminate OCI instance %s: %s", instance.display_name, exc)
            continue

        if instance.lifecycle_state == "RUNNING":
            LOGGER.info(
                "Stopping OCI instance %s in region %s due to keep_action=%s",
                instance.display_name,
                region,
                keep_action,
            )
            if dry_run:
                continue
            try:
                compute_client.instance_action(instance.id, "STOP")
                update_argus_resource_status(tags.get("TestId", ""), instance.display_name, "stop")
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Failed to stop OCI instance %s: %s", instance.display_name, exc)


def clean_oci_orphan_volumes(default_keep_hours: int, dry_run: bool = False) -> None:
    compartment_id = get_oci_compartment_id()
    for region in SUPPORTED_REGIONS:
        block_storage_client = OciService().get_block_storage_client(region=region)
        try:
            volumes = list(
                oci.pagination.list_call_get_all_results_generator(
                    block_storage_client.list_volumes,
                    yield_mode="record",
                    compartment_id=compartment_id,
                    lifecycle_state="AVAILABLE",
                )
            )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to list OCI block volumes in region %s: %s", region, exc)
            continue

        for volume in volumes:
            volume_name = volume.display_name or ""
            if "-vol-" not in volume_name:
                continue

            tags = (volume.defined_tags or {}).get(TAG_NAMESPACE, {})
            if tags.get("CreatedBy") != "SCT":
                continue

            keep_hours = get_keep_hours_from_tags(tags, default=default_keep_hours)
            volume_creation_time = _to_utc_naive(volume.time_created)
            if volume_creation_time and should_keep(volume_creation_time, keep_hours):
                LOGGER.info(
                    "Keeping OCI block volume %s, keep=%s, creation time=%s",
                    volume_name,
                    tags.get("keep", "not set"),
                    volume_creation_time,
                )
                continue

            keep_action = oci_keep_action(tags)
            if keep_action not in ("terminate", ""):
                LOGGER.info(
                    "Keeping OCI block volume %s due to keep_action=%s",
                    volume_name,
                    keep_action,
                )
                continue

            LOGGER.info("Deleting OCI block volume %s (%s) in region %s", volume_name, volume.id, region)
            if dry_run:
                continue
            delete_oci_volume_with_retry(
                block_storage_client=block_storage_client,
                volume_id=volume.id,
                volume_name=volume_name,
                region=region,
                logger=LOGGER,
            )


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser("oci_cleanup")
    arg_parser.add_argument(
        "--duration",
        type=int,
        help="duration to keep non-tagged resources running in hours",
        default=os.environ.get("DURATION", DEFAULT_KEEP_HOURS),
    )
    arg_parser.add_argument(
        "--dry-run",
        action=argparse.BooleanOptionalAction,
        help="do not stop or terminate anything",
        default=os.environ.get("DRY_RUN"),
    )

    args = arg_parser.parse_args()
    is_dry_run = bool(args.dry_run)
    default_keep_hours = int(args.duration)

    if is_dry_run:
        LOGGER.error("'Dry run' mode on")

    clean_oci_instances(default_keep_hours=default_keep_hours, dry_run=is_dry_run)
    clean_oci_orphan_volumes(default_keep_hours=default_keep_hours, dry_run=is_dry_run)

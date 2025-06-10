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
# Copyright (c) 2024 ScyllaDB

import argparse
import os
from datetime import datetime, timedelta
from typing import Callable


from sdcm.utils.azure_utils import AzureService
from sdcm.utils.log import setup_stdout_logger

LOGGER = setup_stdout_logger()
azure_service = AzureService()
resource_client = azure_service.resource
compute_client = azure_service.compute
DEFAULT_KEEP_HOURS = 14


def get_keep_hours(v_m, default=DEFAULT_KEEP_HOURS):
    keep = v_m.tags.get('keep', "").lower() if v_m.tags else None
    if keep == "alive":
        return -1
    try:
        return int(keep)
    except (ValueError, TypeError):
        return default


def get_vm_creation_time(v_m, resource_group_name):
    try:
        creation_time_str = v_m.tags.get('creation_time', "") if v_m.tags else ""
        creation_time = datetime.fromisoformat(creation_time_str)
    except ValueError:
        LOGGER.info("Error parsing creation time tag of VM: %s", creation_time_str)
        # set creation time to now in instance.tags so next time will be processed properly
        creation_time = datetime.utcnow()
        tags = v_m.tags or {}
        tags.update({'creation_time': creation_time.isoformat(sep=" ", timespec="seconds")})
        try:
            compute_client.virtual_machines.begin_update(resource_group_name, v_m.name, parameters={
                "tags": tags,
            })
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info(
                "Failed to update VM tags: %s in resource group: %s with exception: %s",
                v_m.name, resource_group_name, exc)
    return creation_time


def get_rg_creation_time(resource_group):
    try:
        creation_time_str = resource_group.tags.get('creation_time', "") if resource_group.tags else ""
        creation_time = datetime.fromisoformat(creation_time_str)
    except ValueError:
        LOGGER.info("Error parsing creation time tag of RG: %s", creation_time_str)
        creation_time = datetime.utcnow()
        tags = resource_group.tags or {}
        tags.update({'creation_time': creation_time.isoformat(sep=" ", timespec="seconds")})
        resource_group.tags = tags
        try:
            resource_client.resource_groups.create_or_update(resource_group.name, resource_group)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info("Failed to update RG tags: %s with exception: %s", resource_group.name, exc)
    return creation_time


def get_keep_action(v_m) -> Callable:
    keep_action = v_m.tags.get('keep_action', "terminate").lower() if v_m.tags else "terminate"
    if not keep_action:
        keep_action = "terminate"
    return keep_action


def should_keep(creation_time, keep_hours):
    if keep_hours <= 0:
        return True
    try:
        keep_date = creation_time + timedelta(hours=keep_hours)
        return datetime.utcnow() < keep_date
    except (TypeError, ValueError) as exc:
        LOGGER.info("error while defining if should keep: %s. Keeping.", exc)
        return True


def delete_virtual_machine(resource_group_name, vm_name, dry_run=False):
    if dry_run:
        LOGGER.info("[DRY RUN] Would delete VM: %s in resource group: %s", vm_name, resource_group_name)
    else:
        LOGGER.info("Deleting VM: %s in resource group: %s", vm_name, resource_group_name)
        try:
            compute_client.virtual_machines.begin_delete(resource_group_name, vm_name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info(
                "Failed to delete VM: %s in resource group: %s with exception: %s", vm_name, resource_group_name, exc)


def stop_virtual_machine(resource_group_name, vm_name, dry_run=False):
    if dry_run:
        LOGGER.info("[DRY RUN] Would stop VM: %s in resource group: %s", vm_name, resource_group_name)
    else:
        LOGGER.info("Stopping VM: %s in resource group: %s", vm_name, resource_group_name)
        try:
            compute_client.virtual_machines.begin_deallocate(resource_group_name, vm_name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info("Failed to stop VM: %s in resource group: %s with exception: %s",
                        vm_name, resource_group_name, exc)


def delete_resource_group(resource_group_name, dry_run=False):
    if dry_run:
        LOGGER.info("[DRY RUN] Would delete resource group: %s", resource_group_name)
    else:
        LOGGER.info("Deleting resource group: %s", resource_group_name)
        try:
            resource_client.resource_groups.begin_delete(resource_group_name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info("Failed to delete resource group: %s with exception: %s", resource_group_name, exc)


def clean_azure_instances(dry_run=False):
    resource_groups = [resource_group for resource_group in resource_client.resource_groups.list() if (
        resource_group.tags.get('keep', "") != "alive" if resource_group.tags else True)]
    for resource_group in resource_groups:
        LOGGER.info("Checking resource group: %s", resource_group.name)
        rg_creation_time = get_rg_creation_time(resource_group)
        if should_keep(creation_time=rg_creation_time, keep_hours=4):
            # skip resource group if is too young - someone might not yet provision anything there
            LOGGER.info("skip resource group: %s as it's too young", resource_group.name)
            continue
        clean_group = True
        vms_to_process = []
        for v_m in compute_client.virtual_machines.list(resource_group.name):
            if should_keep(creation_time=get_vm_creation_time(v_m, resource_group.name), keep_hours=get_keep_hours(v_m)):
                LOGGER.info("Keeping VM: %s in resource group: %s", v_m.name, resource_group.name)
                clean_group = False  # skip cleaning group if there's at least one VM to keep
            elif get_keep_action(v_m) == "terminate":
                vms_to_process.append((delete_virtual_machine, v_m.name))
            else:
                vms_to_process.append((stop_virtual_machine, v_m.name))
                clean_group = False  # skip cleaning group if there's at least one VM to stop

        if clean_group:
            delete_resource_group(resource_group.name, dry_run=dry_run)
        else:
            for action, vm_name in vms_to_process:
                action(resource_group.name, vm_name, dry_run=dry_run)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser('azure_cleanup')
    arg_parser.add_argument("--duration", type=int,
                            help="duration to keep non-tagged instances running in hours",
                            default=os.environ.get('DURATION', DEFAULT_KEEP_HOURS))
    arg_parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction,
                            help="do not stop or terminate anything",
                            default=os.environ.get('DRY_RUN'))

    args = arg_parser.parse_args()

    is_dry_run = bool(args.dry_run)

    if is_dry_run:
        LOGGER.error("'Dry run' mode on")

    clean_azure_instances(dry_run=is_dry_run)

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
EKS cluster cleanup utility based on age/retention time.

This script cleans EKS clusters and associated resources based on their age and keep tags:
- EKS clusters
- Launch templates (used by EKS node groups)
- Load balancers (created by EKS services)
- CloudFormation stacks (used by EKS)
"""

import argparse
import os

import boto3

from sdcm.utils.aws_utils import EksClusterForCleaner
from sdcm.utils.common import all_aws_regions
from utils.cloud_cleanup import (
    DEFAULT_KEEP_HOURS,
    LOGGER,
    get_keep_hours_from_tags,
    should_keep,
)


def get_cluster_creation_time(cluster):
    """Get EKS cluster creation time."""
    return cluster.create_time.replace(tzinfo=None)


def get_cluster_tags(cluster) -> dict:
    """Get EKS cluster tags as a dictionary."""
    tags_dict = {}
    if hasattr(cluster, "metadata") and cluster.metadata:
        for item in cluster.metadata.get("items", []):
            tags_dict[item.get("key", "")] = item.get("value", "")
    return tags_dict


def get_tags_dict_from_aws_tags(tags_list) -> dict:
    """Convert AWS tags list to dictionary."""
    tags_dict = {}
    if tags_list:
        for tag in tags_list:
            tags_dict[tag.get("Key", "")] = tag.get("Value", "")
    return tags_dict


def clean_eks_clusters(regions, keep_hours=DEFAULT_KEEP_HOURS, dry_run=False):
    """Clean EKS clusters across specified regions."""
    total_clusters = 0
    deleted_clusters = 0
    kept_clusters = 0

    for region in regions:
        LOGGER.info("Cleaning EKS clusters in region: %s", region)
        try:
            eks_client = boto3.client("eks", region_name=region)
            cluster_names = eks_client.list_clusters()["clusters"]

            if not cluster_names:
                LOGGER.info("No EKS clusters found in region: %s", region)
                continue

            for cluster_name in cluster_names:
                total_clusters += 1
                try:
                    cluster = EksClusterForCleaner(name=cluster_name, region=region)
                    cluster_creation_time = get_cluster_creation_time(cluster)
                    cluster_tags = get_cluster_tags(cluster)

                    if ("CreatedBy", "SCT") not in cluster_tags.items():
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
                            "Terminating EKS cluster %s in region %s, creation time: %s",
                            cluster_name,
                            region,
                            cluster_creation_time,
                        )
                        try:
                            cluster.destroy()
                            LOGGER.info("%s deleted", cluster_name)
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
                        "Failed to process cluster %s in region %s: %s",
                        cluster_name,
                        region,
                        exc,
                    )

        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to list EKS clusters in region %s: %s", region, exc)

    LOGGER.info(
        "EKS cleanup summary - Total: %d, Deleted: %d, Kept: %d",
        total_clusters,
        deleted_clusters,
        kept_clusters,
    )


def clean_launch_templates(regions, keep_hours=DEFAULT_KEEP_HOURS, dry_run=False):
    """Clean launch templates across specified regions."""
    total_templates = 0
    deleted_templates = 0
    kept_templates = 0

    for region in regions:
        LOGGER.info("Cleaning launch templates in region: %s", region)
        try:
            ec2_client = boto3.client("ec2", region_name=region)

            # List all launch templates
            paginator = ec2_client.get_paginator("describe_launch_templates")
            for page in paginator.paginate():
                for template in page.get("LaunchTemplates", []):
                    template_name = template.get("LaunchTemplateName")
                    template_id = template.get("LaunchTemplateId")
                    creation_time = template.get("CreateTime").replace(tzinfo=None)

                    # Get template tags
                    tags_dict = get_tags_dict_from_aws_tags(template.get("Tags", []))
                    if ("CreatedBy", "SCT") not in tags_dict.items():
                        continue

                    total_templates += 1

                    # Check if template should be kept
                    if should_keep(creation_time, get_keep_hours_from_tags(tags_dict, default=keep_hours)):
                        LOGGER.info(
                            "Keeping launch template %s, keep: %s, creation time: %s",
                            template_name,
                            tags_dict.get("keep", "not set"),
                            creation_time,
                        )
                        kept_templates += 1
                        continue

                    # Check keep_action tag
                    keep_action = tags_dict.get("keep_action", "terminate").lower()
                    if keep_action not in ("terminate", ""):
                        LOGGER.info(
                            "Skipping launch template %s due to keep_action: %s",
                            template_name,
                            keep_action,
                        )
                        kept_templates += 1
                        continue

                    # Delete template
                    if not dry_run:
                        LOGGER.info(
                            "Deleting launch template %s in region %s, creation time: %s",
                            template_name,
                            region,
                            creation_time,
                        )
                        try:
                            ec2_client.delete_launch_template(LaunchTemplateId=template_id)
                            LOGGER.info("%s deleted", template_name)
                            deleted_templates += 1
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.error("Error while deleting launch template %s: %s", template_name, exc)
                    else:
                        LOGGER.info(
                            "Dry run: would delete launch template %s, creation time: %s",
                            template_name,
                            creation_time,
                        )
                        deleted_templates += 1

        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to list launch templates in region %s: %s", region, exc)

    LOGGER.info(
        "Launch templates cleanup summary - Total: %d, Deleted: %d, Kept: %d",
        total_templates,
        deleted_templates,
        kept_templates,
    )


def clean_load_balancers(regions, keep_hours=DEFAULT_KEEP_HOURS, dry_run=False):
    """Clean load balancers across specified regions."""
    total_lbs = 0
    deleted_lbs = 0
    kept_lbs = 0

    for region in regions:
        LOGGER.info("Cleaning load balancers in region: %s", region)
        try:
            elb_client = boto3.client("elb", region_name=region)

            # Clean Classic Load Balancers
            try:
                classic_lbs = elb_client.describe_load_balancers().get("LoadBalancerDescriptions", [])
                for lb in classic_lbs:
                    total_lbs += 1
                    lb_name = lb.get("LoadBalancerName")
                    creation_time = lb.get("CreatedTime").replace(tzinfo=None)

                    # Get tags
                    tags_response = elb_client.describe_tags(LoadBalancerNames=[lb_name])
                    tags_dict = {}
                    if tags_response.get("TagDescriptions"):
                        tags_dict = get_tags_dict_from_aws_tags(tags_response["TagDescriptions"][0].get("Tags", []))

                    if ("CreatedBy", "SCT") not in tags_dict.items():
                        continue
                    # Check if LB should be kept
                    if should_keep(creation_time, get_keep_hours_from_tags(tags_dict, default=keep_hours)):
                        LOGGER.info(
                            "Keeping load balancer %s, keep: %s, creation time: %s",
                            lb_name,
                            tags_dict.get("keep", "not set"),
                            creation_time,
                        )
                        kept_lbs += 1
                        continue

                    # Check keep_action tag
                    keep_action = tags_dict.get("keep_action", "terminate").lower()
                    if keep_action not in ("terminate", ""):
                        LOGGER.info(
                            "Skipping load balancer %s due to keep_action: %s",
                            lb_name,
                            keep_action,
                        )
                        kept_lbs += 1
                        continue

                    # Delete LB
                    if not dry_run:
                        LOGGER.info(
                            "Deleting load balancer %s in region %s, creation time: %s",
                            lb_name,
                            region,
                            creation_time,
                        )
                        try:
                            elb_client.delete_load_balancer(LoadBalancerName=lb_name)
                            LOGGER.info("%s deleted", lb_name)
                            deleted_lbs += 1
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.error("Error while deleting load balancer %s: %s", lb_name, exc)
                    else:
                        LOGGER.info(
                            "Dry run: would delete load balancer %s, creation time: %s",
                            lb_name,
                            creation_time,
                        )
                        deleted_lbs += 1
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Failed to process classic load balancers in region %s: %s", region, exc)

        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to list load balancers in region %s: %s", region, exc)

    LOGGER.info(
        "Load balancers cleanup summary - Total: %d, Deleted: %d, Kept: %d",
        total_lbs,
        deleted_lbs,
        kept_lbs,
    )


def clean_cloudformation_stacks(regions, keep_hours=DEFAULT_KEEP_HOURS, dry_run=False):
    """Clean CloudFormation stacks across specified regions."""
    total_stacks = 0
    deleted_stacks = 0
    kept_stacks = 0

    for region in regions:
        LOGGER.info("Cleaning CloudFormation stacks in region: %s", region)
        try:
            cf_client = boto3.client("cloudformation", region_name=region)

            # List all stacks (exclude deleted ones)
            paginator = cf_client.get_paginator("list_stacks")
            for page in paginator.paginate(
                StackStatusFilter=[
                    "CREATE_IN_PROGRESS",
                    "CREATE_FAILED",
                    "CREATE_COMPLETE",
                    "ROLLBACK_IN_PROGRESS",
                    "ROLLBACK_FAILED",
                    "ROLLBACK_COMPLETE",
                    "DELETE_FAILED",
                    "UPDATE_IN_PROGRESS",
                    "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
                    "UPDATE_COMPLETE",
                    "UPDATE_ROLLBACK_IN_PROGRESS",
                    "UPDATE_ROLLBACK_FAILED",
                    "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
                    "UPDATE_ROLLBACK_COMPLETE",
                ]
            ):
                for stack_summary in page.get("StackSummaries", []):
                    total_stacks += 1
                    stack_name = stack_summary.get("StackName")
                    creation_time = stack_summary.get("CreationTime").replace(tzinfo=None)

                    # Get stack details for tags
                    try:
                        stack_details = cf_client.describe_stacks(StackName=stack_name)
                        tags_dict = {}
                        if stack_details.get("Stacks"):
                            tags_dict = get_tags_dict_from_aws_tags(stack_details["Stacks"][0].get("Tags", []))
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.debug("Failed to get stack details for %s: %s", stack_name, exc)
                        tags_dict = {}

                    if ("CreatedBy", "SCT") not in tags_dict.items():
                        continue
                    # Check if stack should be kept
                    if should_keep(creation_time, get_keep_hours_from_tags(tags_dict, default=keep_hours)):
                        LOGGER.info(
                            "Keeping CloudFormation stack %s, keep: %s, creation time: %s",
                            stack_name,
                            tags_dict.get("keep", "not set"),
                            creation_time,
                        )
                        kept_stacks += 1
                        continue

                    # Check keep_action tag
                    keep_action = tags_dict.get("keep_action", "terminate").lower()
                    if keep_action not in ("terminate", ""):
                        LOGGER.info(
                            "Skipping CloudFormation stack %s due to keep_action: %s",
                            stack_name,
                            keep_action,
                        )
                        kept_stacks += 1
                        continue

                    # Delete stack
                    if not dry_run:
                        LOGGER.info(
                            "Deleting CloudFormation stack %s in region %s, creation time: %s",
                            stack_name,
                            region,
                            creation_time,
                        )
                        try:
                            cf_client.delete_stack(StackName=stack_name)
                            LOGGER.info("%s deletion initiated", stack_name)
                            deleted_stacks += 1
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.error("Error while deleting CloudFormation stack %s: %s", stack_name, exc)
                    else:
                        LOGGER.info(
                            "Dry run: would delete CloudFormation stack %s, creation time: %s",
                            stack_name,
                            creation_time,
                        )
                        deleted_stacks += 1

        except Exception as exc:  # noqa: BLE001
            LOGGER.error("Failed to list CloudFormation stacks in region %s: %s", region, exc)

    LOGGER.info(
        "CloudFormation stacks cleanup summary - Total: %d, Deleted: %d, Kept: %d",
        total_stacks,
        deleted_stacks,
        kept_stacks,
    )


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser("eks_cleanup")
    arg_parser.add_argument(
        "--duration",
        type=int,
        help="duration to keep non-tagged clusters running in hours",
        default=os.environ.get("DURATION", str(DEFAULT_KEEP_HOURS)),
    )
    arg_parser.add_argument(
        "--regions",
        type=str,
        nargs="+",
        help="AWS regions to clean (default: all regions)",
        default=None,
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
    regions_to_clean = args.regions if args.regions else all_aws_regions()

    if is_dry_run:
        LOGGER.error("'Dry run' mode on")

    clean_eks_clusters(regions=regions_to_clean, keep_hours=keep_hours, dry_run=is_dry_run)
    clean_launch_templates(regions=regions_to_clean, keep_hours=keep_hours, dry_run=is_dry_run)
    clean_load_balancers(regions=regions_to_clean, keep_hours=keep_hours, dry_run=is_dry_run)
    clean_cloudformation_stacks(regions=regions_to_clean, keep_hours=keep_hours, dry_run=is_dry_run)

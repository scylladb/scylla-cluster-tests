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
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple
from collections import defaultdict

from botocore.exceptions import ClientError
import boto3

from sdcm.exceptions import CapacityReservationError
from sdcm.utils.common import all_aws_regions, ParallelObject
from sdcm.utils.get_username import get_username

LOGGER = logging.getLogger(__name__)


class SCTCapacityReservation:
    """Class for managing capacity reservations for AWS instances.

    Serves namespacing for capacity reservations and provides methods for creating and cancelling reservations."""
    reservations: Dict[str, Dict[str, str]] = {}

    @staticmethod
    def _get_cr_request_based_on_sct_config(params) -> Tuple[dict[str, int], int]:
        instance_counts = defaultdict(int)
        nemesis_node_count = params.get("nemesis_add_node_cnt") or 0

        cluster_max_size = (params.get("cluster_target_size") or params.get("n_db_nodes"))

        if nemesis_grow_shrink_instance_type := params.get("nemesis_grow_shrink_instance_type"):
            instance_counts[nemesis_grow_shrink_instance_type] += nemesis_node_count
        else:
            cluster_max_size += nemesis_node_count

        instance_counts[params.get("instance_type_db")] += cluster_max_size
        instance_counts[params.get("instance_type_loader")] += params.get("n_loaders")
        # don't reserve capacity for monitor - as usually it's not a problem to spin it
        duration = params.get("test_duration")
        instance_counts = {k: v for k, v in instance_counts.items() if v > 0}  # remove 0 values
        return instance_counts, duration

    @classmethod
    def get_cr_from_aws(cls, params, force_fetch=False) -> None:
        """Retrieves capacity reservations for given test_id from AWS."""
        if not cls.is_capacity_reservation_enabled(params) and not force_fetch:
            LOGGER.info("Capacity reservation is not enabled. Skipping reservation.")
            return
        test_id = params.get("reuse_cluster") or params.get("test_id")
        ec2 = boto3.client('ec2', region_name=params.region_names[0])
        reservations = ec2.describe_capacity_reservations(
            Filters=[
                {
                    'Name': 'tag:test_id',
                    'Values': [test_id]
                },
                {
                    'Name': 'state',
                    'Values': ['active']
                }
            ]
        )
        result = {}
        availability_zone = params.get("availability_zone")
        for reservation in reservations['CapacityReservations']:
            availability_zone = reservation['AvailabilityZone'][-1]
            instance_type = reservation['InstanceType']
            if availability_zone not in result:
                result[availability_zone] = {}
            result[availability_zone][instance_type] = reservation['CapacityReservationId']
        if result:
            LOGGER.info("Found capacity reservations: %s", result)
            params["availability_zone"] = availability_zone
        else:
            LOGGER.info("No capacity reservations found.")
        cls.reservations = result

    @staticmethod
    def _get_supported_availability_zones(ec2, instance_types: List[str], initial_az: str) -> List[str]:
        response = ec2.describe_instance_type_offerings(
            LocationType='availability-zone',
            Filters=[
                {
                    'Name': 'instance-type',
                    'Values': list(instance_types)
                },
            ]
        )
        offerings = response['InstanceTypeOfferings']
        azs = set.intersection(
            *[{offering['Location'] for offering in offerings if offering['InstanceType'] == instance_type}
              for instance_type in instance_types]
        )
        azs = list(azs)
        try:  # put initial az as first one to try
            azs.remove(initial_az)
            azs.insert(0, initial_az)
        except ValueError:
            LOGGER.warning("Initial availability zone %s does not support required instances", initial_az)
        LOGGER.info("Supported availability zones for instance types %s: %s", instance_types, azs)
        return azs

    @staticmethod
    def is_capacity_reservation_enabled(params: dict) -> bool:
        """Returns True if capacity reservation is enabled."""
        is_single_dc = str(params.get("n_db_nodes")).isdigit() or params.get('simulated_regions') > 0
        return (params.get("cluster_backend") == "aws"
                and (params.get("test_id") or params.get("reuse_cluster"))
                and params.get('use_capacity_reservation') is True
                and params.get('instance_provision') == 'on_demand'
                and is_single_dc)

    @classmethod
    def reserve(cls, params) -> None:
        """Reserves capacity for given test params: detects required instance types and counts and creates capacity reservations.
        """
        if not cls.is_capacity_reservation_enabled(params):
            LOGGER.info("Capacity reservation is not enabled. Skipping reservation.")
            return
        cls.get_cr_from_aws(params)
        if cls.reservations:
            LOGGER.info("Capacity reservation already created. Skipping reservation.")
            return
        region = params.region_names[0]
        test_id = params.get("reuse_cluster") or params.get("test_id")
        ec2 = boto3.client('ec2', region_name=region)
        placement_group_arn = None

        if params.get("use_placement_group"):
            response = ec2.describe_placement_groups(
                Filters=[
                    {
                        'Name': 'tag:TestId',
                        'Values': [test_id]
                    }
                ]
            )
            if response['PlacementGroups']:
                placement_group_arn = [group['GroupArn']
                                       for group in response['PlacementGroups'] if group['State'] == 'available'][0]
                LOGGER.info("Using placement group '%s' for capacity reservation.", placement_group_arn)
            else:
                LOGGER.error("Available placement group not found while should.")
                raise CapacityReservationError("Failed to find available placement group.")
        request, duration = cls._get_cr_request_based_on_sct_config(params)
        LOGGER.info("Creating capacity reservation for test %s with request: %s", test_id, request)
        reservations = {}
        for availability_zone in cls._get_supported_availability_zones(ec2=ec2, instance_types=list(request.keys()),
                                                                       initial_az=region+params.get("availability_zone")):
            reservations[availability_zone[-1]] = {}
            LOGGER.info("Creating capacity reservation in %s", availability_zone)
            for instance_type, instance_count in request.items():
                cr_id = cls._create(ec2=ec2, test_id=test_id, availability_zone=availability_zone,
                                    instance_type=instance_type, instance_count=instance_count, duration=duration,
                                    placement_group_arn=placement_group_arn)
                if cr_id:
                    reservations[availability_zone[-1]][instance_type] = cr_id
                    LOGGER.info("Capacity reservation created for %s", instance_type)
                else:
                    LOGGER.info("Failed to create capacity reservation in %s", availability_zone)
                    cls._cancel_reservations(ec2, reservations)
                    if placement_group_arn:
                        LOGGER.info("waiting 30s before falling back to next AZ to release placement group")
                        time.sleep(30)
                    reservations = {}
                    LOGGER.info("Falling back to next availability zone.")
                    break
            if reservations:
                params["availability_zone"] = availability_zone[-1]
                cls.reservations = reservations
                LOGGER.info("Capacity reservations created in '%s' az: %s for duration: %s",
                            availability_zone, reservations, duration)
                return

        raise CapacityReservationError("Failed to create capacity reservation in any availability zone.")

    @classmethod
    def get_id(cls, availability_zone, instance_type: str) -> str:
        """Returns capacity reservation id for given instance type for provided az."""
        return cls.reservations[availability_zone][instance_type]

    @staticmethod
    # pylint: disable=too-many-arguments
    def _create(ec2, test_id, availability_zone, instance_type, instance_count, duration, placement_group_arn=None) -> str | None:
        additional_params = {}
        if placement_group_arn:
            additional_params["PlacementGroupArn"] = placement_group_arn
            LOGGER.info("using placement group for CR: %s", placement_group_arn)
        try:
            response = ec2.create_capacity_reservation(
                InstanceType=instance_type,
                InstancePlatform='Linux/UNIX',
                InstanceMatchCriteria='targeted',
                AvailabilityZone=availability_zone,
                InstanceCount=instance_count,
                EndDateType='limited',
                EndDate=datetime.utcnow() + timedelta(minutes=duration),
                TagSpecifications=[
                    {
                        'ResourceType': 'capacity-reservation',
                        'Tags': [
                            {
                                'Key': 'test_id',
                                'Value': test_id
                            },
                            {
                                'Key': 'RunByUser',
                                'Value': get_username()
                            },
                            {
                                'Key': 'JenkinsJobTag',
                                'Value': os.environ.get('BUILD_TAG')
                            }

                        ]
                    },
                ],
                **additional_params
            )
            return response['CapacityReservation']['CapacityReservationId']
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.info("Failed to create capacity reservation for %s. Error: %s", instance_type, exc)
            return None

    @classmethod
    def cancel(cls, params) -> None:
        """Cancels all capacity reservations."""
        if not cls.reservations:
            LOGGER.info("No capacity reservations to cancel.")
            return
        ec2 = boto3.client('ec2', region_name=params.region_names[0])
        cls._cancel_reservations(ec2, cls.reservations)
        cls.reservations = {}

    @staticmethod
    def _cancel_reservations(ec2, reservations: Dict[str, Dict[str, str]]) -> None:
        """Cancels all capacity reservations."""
        for reservation in reservations.values():
            for instance_type, cr_id in reservation.items():
                try:
                    ec2.cancel_capacity_reservation(CapacityReservationId=cr_id)
                    LOGGER.info("Capacity reservation %s for %s cancelled successfully.", cr_id, instance_type)
                except ClientError as exp:
                    LOGGER.error("Failed to cancel capacity reservation %s. Error: %s", cr_id, exp)

    @classmethod
    def cancel_all_regions(cls, test_id) -> None:
        """Finds and cancels capacity reservations for all regions in parallel."""
        regions = all_aws_regions()
        if not test_id:
            LOGGER.warning("No test_id provided. Skipping capacity reservation cancellation.")
            return

        def cancel_region(region):
            ec2 = boto3.client('ec2', region_name=region)
            try:
                reservations = ec2.describe_capacity_reservations(
                    Filters=[
                        {
                            'Name': 'tag:test_id',
                            'Values': [test_id]
                        },
                        {
                            'Name': 'state',
                            'Values': ['active']
                        }
                    ]
                )
                if not reservations['CapacityReservations']:
                    LOGGER.info("There are no CRs to remove in region %s.", region)
                for reservation in reservations['CapacityReservations']:
                    try:
                        ec2.cancel_capacity_reservation(CapacityReservationId=reservation['CapacityReservationId'])
                        LOGGER.info("Capacity reservation %s in region %s cancelled successfully.",
                                    reservation['CapacityReservationId'], region)
                    except ClientError as exp:
                        LOGGER.error("Failed to cancel capacity reservation %s in region %s. Error: %s",
                                     reservation['CapacityReservationId'], region, exp)

            except ClientError as exp:
                LOGGER.error("Failed to describe capacity reservations in region %s. Error: %s", region, exp)

        ParallelObject(regions, timeout=60, num_workers=len(regions)).run(cancel_region, ignore_exceptions=True)

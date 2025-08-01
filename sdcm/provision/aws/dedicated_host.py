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
import time
import random
import itertools
from typing import Dict

import boto3
from botocore.exceptions import ClientError
import tenacity


from sdcm.wait import exponential_retry
from sdcm.utils.aws_utils import tags_as_ec2_tags
from sdcm.utils.common import ParallelObject, all_aws_regions
from sdcm.test_config import TestConfig


LOGGER = logging.getLogger(__name__)


class SCTDedicatedHosts:
    hosts: Dict[str, Dict[str, str]] = {}

    @classmethod
    def get_host(cls, availability_zone, instance_type: str) -> str:
        """Returns dedicated host id for given instance type for provided az."""
        return cls.hosts.get(availability_zone, {}).get(instance_type)

    @staticmethod
    def is_dedicated_hosts_enabled(params: dict) -> bool:
        """Returns True if dedicated hosts is enabled."""
        return (params.get("cluster_backend") == "aws"
                and (params.get("test_id") or params.get("reuse_cluster"))
                and params.get('instance_type_db')
                and params.get('use_dedicated_host') is True
                and params.get('instance_provision') == 'on_demand')

    @staticmethod
    def _get_supported_availability_zones(ec2, instance_types: list[str], initial_az: str) -> list[str]:
        response = ec2.describe_instance_type_offerings(
            LocationType='availability-zone',
            Filters=[
                {
                    'Name': 'instance-type',
                    'Values': instance_types
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

    @classmethod
    def reserve(cls, params) -> None:

        if not cls.is_dedicated_hosts_enabled(params):
            LOGGER.info("Dedicated hosts is not enabled. Skipping reservation phase.")
            return

        TestConfig.keep_cluster(node_type='dedicated_host', val=params.get('post_behavior_dedicated_host'))

        test_id = params.get("reuse_cluster") or params.get("test_id")

        ec2 = boto3.client('ec2', region_name=params.region_names[0])

        if host_ids := params.get('aws_dedicated_host_ids'):
            response = ec2.describe_hosts(
                HostIds=host_ids,
                Filters=[
                    {
                        'Name': 'state',
                        'Values': ['available']
                    }
                ]
            )
        else:
            response = ec2.describe_hosts(Filters=[
                {
                    'Name': 'tag:TestId',
                    'Values': [test_id]
                },
                {
                    'Name': 'state',
                    'Values': ['available']
                }
            ]
            )
        LOGGER.debug(response)
        if response['Hosts']:
            host = response['Hosts'][0]
            instance_type = host.get('HostProperties').get('InstanceType')
            cls.hosts[host.get('AvailabilityZone')] = {instance_type: host.get('HostId')}
            params["availability_zone"] = host.get('AvailabilityZone')[-1]
            return
        else:
            tags = TestConfig.common_tags()
            if TestConfig.should_keep_alive('dedicated_host'):
                tags['keep'] = 'alive'
            tags['TestId'] = test_id
            region = params.region_names[0]
            for availability_zone in cls._get_supported_availability_zones(ec2=ec2, instance_types=[params.get('instance_type_db'),],
                                                                           initial_az=region+params.get("availability_zone")):
                host_id = cls.allocate(region_name=region, availability_zone=availability_zone,
                                       instance_type=params.get('instance_type_db'), quantity=1, tags=tags)

                if host_id:
                    params["availability_zone"] = availability_zone[-1]
                    cls.hosts[availability_zone] = {params.get('instance_type_db'): host_id}
                    return

            raise EnvironmentError("Failed to get dedicated host in any availability zone.")

    @staticmethod
    def allocate(region_name: str, availability_zone: str, instance_type: str, quantity: int, tags: dict):
        ec2 = boto3.client('ec2', region_name=region_name)
        try:
            response = ec2.allocate_hosts(
                InstanceType=instance_type,
                AvailabilityZone=availability_zone,
                Quantity=quantity,
                TagSpecifications=[
                    {
                        'ResourceType': 'dedicated-host',
                        'Tags': tags_as_ec2_tags(tags)
                    },
                ]
            )
            LOGGER.debug(response)
            return response['HostIds'][0]
        except Exception as e:  # noqa: BLE001
            print(f"Error allocating dedicated hosts: {e}")
            return None

    @classmethod
    def release(cls, params) -> None:
        """release all dedicated hosts."""
        if not cls.hosts:
            LOGGER.info("No dedicated hosts to release.")
            return
        if TestConfig.should_keep_alive('dedicated_host'):
            LOGGER.info("Dedicated hosts are marked as keep.")
            return
        ec2 = boto3.client('ec2', region_name=params.region_names[0])
        cls._release_hosts(ec2, cls.hosts)
        cls.reservations = {}

    @staticmethod
    def list_hosts(tags_dict: dict, region_name: str | None = None, group_as_region: bool = True, verbose: bool = True) -> list:
        hosts = {}
        aws_regions = [region_name] if region_name else all_aws_regions()

        def get_host(region):
            if verbose:
                LOGGER.info('Going to list aws region "%s"', region)
            time.sleep(random.random())
            client = boto3.client('ec2', region_name=region)
            custom_filter = []
            if tags_dict:
                custom_filter = [{'Name': 'tag:{}'.format(key),
                                  'Values': value if isinstance(value, list) else [value]}
                                 for key, value in tags_dict.items()]
            response = client.describe_hosts(Filters=custom_filter)
            hosts[region] = response.get('Hosts', [])

            if verbose:
                LOGGER.info("%s: done [%s/%s]", region, len(list(hosts.keys())), len(aws_regions))

        ParallelObject(aws_regions, timeout=100, num_workers=len(aws_regions)).run(get_host, ignore_exceptions=False)

        if not group_as_region:
            hosts = list(itertools.chain(*list(hosts.values())))  # flatten the list of lists
            total_items = len(hosts)
        else:
            total_items = sum([len(value) for _, value in hosts.items()])

        if verbose:
            LOGGER.info("Found total of {} instances.".format(total_items))

        return hosts

    @classmethod
    def release_by_tags(cls, tags_dict: dict, regions=None, dry_run=False) -> None:
        """Cancel all dedicated hosts with specific tags in AWS."""

        local_tags_dict = {k: v for k, v in tags_dict.items() if k != "NodeType"}
        assert local_tags_dict, "tags_dict not provided (can't clean all hosts)"
        if regions:
            aws_hosts = {}
            for region in regions:
                aws_hosts |= cls.list_hosts(
                    tags_dict=local_tags_dict, region_name=region, group_as_region=True)
        else:
            aws_hosts = cls.list_hosts(tags_dict=local_tags_dict, group_as_region=True)

        for region, hosts_list in aws_hosts.items():
            if not hosts_list:
                LOGGER.info("There are no hosts to release in AWS region %s", region)
                continue
            client = boto3.client('ec2', region_name=region)
            for host in hosts_list:
                if not dry_run:
                    cls._release_hosts(ec2=client, hosts={region: {'instance_type': str(host['HostId'])}})

    @staticmethod
    def _release_hosts(ec2, hosts: Dict[str, Dict[str, str]]) -> None:
        """Cancels all capacity reservations."""

        def release_hosts(host_ids):
            response = ec2.release_hosts(HostIds=host_ids)
            if errors := response.get("Unsuccessful"):
                LOGGER.info(f"Failed to release dedicated host:  {errors}")
                raise ClientError(errors[0], operation_name="release_hosts")
            return response

        def release_with_retry(host_ids):
            try:
                return exponential_retry(func=lambda: release_hosts(host_ids),
                                         logger=LOGGER,
                                         exceptions=(ClientError,))
            except tenacity.RetryError:
                raise TimeoutError(
                    f"Timeout while releasing dedicated hosts '{host_ids}'"
                ) from None

        for host in hosts.values():
            for instance_type, host_id in host.items():
                release_with_retry([host_id,])
                LOGGER.info("dedicated host %s for %s cancelled successfully.", host_id, instance_type)

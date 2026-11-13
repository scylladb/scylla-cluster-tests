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
# Copyright (c) 2021 ScyllaDB

import contextlib
import datetime
import logging
from typing import List, Union

from mypy_boto3_ec2 import EC2Client
from mypy_boto3_ec2.service_resource import Instance

from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.constants import (
    SPOT_CNT_LIMIT,
    SPOT_REQUEST_TIMEOUT,
)
from sdcm.provision.aws.dedicated_host import SCTDedicatedHosts
from sdcm.provision.aws.instance_parameters import AWSInstanceParams
from sdcm.provision.aws.utils import (
    create_spot_instance_request,
    ec2_clients,
    ec2_services,
    find_instance_by_id,
    provision_ec2_fleet,
    set_tags_on_instances,
    terminate_instances_by_id,
    wait_for_provision_request_done,
)
from sdcm.provision.common.provisioner import InstanceProvisionerBase, ProvisionParameters, TagsType
from sdcm.utils.aws_utils import tags_as_ec2_tags

LOGGER = logging.getLogger(__name__)


class AWSInstanceProvisioner(InstanceProvisionerBase):
    # TODO: Make them configurable
    _wait_interval = 5

    def provision(
        self,
        provision_parameters: ProvisionParameters,
        instance_parameters: Union[AWSInstanceParams, List[AWSInstanceParams]],
        count: int,
        tags: Union[List[TagsType], TagsType] = None,
        names: List[str] = None,
    ) -> List[Instance]:
        if tags is None:
            tags = {}
        if isinstance(tags, dict):
            tags = [tags] * count
        elif isinstance(tags, list):
            tags = tags.copy()

        if names:
            assert len(names) == count, "Names length should be equal to count"
        assert len(tags) == count, "Tags length should be equal to count"

        for node_id, name in enumerate(names):
            tag = tags[node_id]
            tag["Name"] = name

        instance_parameters = self._as_instance_parameters_list(instance_parameters)

        if provision_parameters.spot:
            return self._provision_spot_instances(
                provision_parameters=provision_parameters,
                instance_parameters=instance_parameters,
                count=count,
                tags=tags,
            )
        return self._provision_on_demand_instances(
            provision_parameters=provision_parameters,
            instance_parameters=instance_parameters,
            count=count,
            tags=tags,
        )

    @staticmethod
    def _as_instance_parameters_list(
        instance_parameters: Union[AWSInstanceParams, List[AWSInstanceParams]],
    ) -> List[AWSInstanceParams]:
        """Accept either a single instance parameter set or a list of interchangeable ones.

        A list means "any of these instance types is acceptable" and only the EC2 Fleet path can
        exploit that; every other path falls back to the first (preferred) entry.
        """
        if isinstance(instance_parameters, list):
            assert instance_parameters, "At least one instance parameters set is required"
            return instance_parameters
        return [instance_parameters]

    @staticmethod
    def _is_provision_type_fleet(count: int) -> bool:
        return count > SPOT_CNT_LIMIT

    @property
    def _spot_valid_until(self) -> datetime.datetime:
        return datetime.datetime.now() + datetime.timedelta(minutes=SPOT_REQUEST_TIMEOUT / 60 + 5)

    @staticmethod
    def _ec2_client(provision_parameters: ProvisionParameters) -> EC2Client:
        return ec2_clients[provision_parameters.region_name]

    @staticmethod
    def _full_availability_zone_name(provision_parameters: ProvisionParameters) -> str:
        return provision_parameters.region_name + provision_parameters.availability_zone

    @staticmethod
    def _provision_on_demand_instances(
        provision_parameters: ProvisionParameters,
        instance_parameters: List[AWSInstanceParams],
        count: int,
        tags: List[TagsType],
    ) -> List[Instance]:
        # RunInstances takes exactly one instance type, so only the preferred (first) one is used.
        # Diversification across several instance types is an EC2 Fleet-only capability.
        instance_parameters = instance_parameters[0]
        instance_parameters_dict = instance_parameters.model_dump(
            exclude_none=True, exclude_defaults=True, exclude_unset=True, encode_user_data=False
        )

        # picks the tags of the first instance to apply to all instances upfront
        # later those would be updated with individual tags (Name, etc.)
        instance_parameters_dict["TagSpecifications"] = [
            {"ResourceType": "instance", "Tags": tags_as_ec2_tags(tags[0])}
        ]

        if cr_id := SCTCapacityReservation.reservations.get(provision_parameters.availability_zone, {}).get(
            instance_parameters.InstanceType
        ):
            instance_parameters_dict["CapacityReservationSpecification"] = {
                "CapacityReservationTarget": {"CapacityReservationId": cr_id}
            }
        if host_id := SCTDedicatedHosts.get_host(
            provision_parameters.region_name + provision_parameters.availability_zone, instance_parameters.InstanceType
        ):
            instance_parameters_dict["Placement"] = {"HostId": host_id}
        LOGGER.info(
            "[%s] Creating %d on-demand instances using AMI id '%s' with following parameters:\n%s",
            provision_parameters.region_name,
            count,
            instance_parameters.ImageId,
            instance_parameters_dict,
        )
        instances = ec2_services[provision_parameters.region_name].create_instances(
            **instance_parameters_dict, MinCount=count, MaxCount=count
        )
        LOGGER.info("Created instances: %s.", instances)
        if instances:
            for ind, instance in enumerate(instances):
                instance_tags = tags.pop()
                set_tags_on_instances(
                    region_name=provision_parameters.region_name,
                    instance_ids=[instance.instance_id],
                    tags={"Name": f"spot_fleet_{instance.instance_id}_{ind}"} | instance_tags,
                )
        return instances

    def _provision_spot_instances(
        self,
        provision_parameters: ProvisionParameters,
        instance_parameters: List[AWSInstanceParams],
        count: int,
        tags: Union[List[TagsType], TagsType],
    ) -> List[Instance]:
        if provision_parameters.fleet or self._is_provision_type_fleet(count):
            # One all-or-nothing request for the whole count - no batches, so there is never a partial set of
            # earlier batches to roll back when a later one fails.
            return self._execute_ec2_fleet_instance_request(
                provision_parameters=provision_parameters,
                instance_parameters=instance_parameters,
                count=count,
                tags=tags,
            )
        instances = self._execute_spot_instance_request(
            provision_parameters=provision_parameters,
            instance_parameters=instance_parameters[0],
            count=count,
            tags=tags,
        )
        if 0 < len(instances) < count:
            # Plain spot requests can be partially fulfilled. Returning that partial set would silently
            # under-provision the cluster, since ProvisionPlan treats any non-empty result as success and skips
            # AZ/region/on-demand fallback; roll it back so the fallback gets a chance at the exact count.
            LOGGER.error(
                "Spot request in %s provisioned %d of %d requested instances; rolling them back.",
                provision_parameters.region_name,
                len(instances),
                count,
            )
            terminate_instances_by_id(
                region_name=provision_parameters.region_name,
                instance_ids=[instance.instance_id for instance in instances],
            )
            return []
        return instances

    def _execute_ec2_fleet_instance_request(
        self,
        provision_parameters: ProvisionParameters,
        instance_parameters: List[AWSInstanceParams],
        count: int,
        tags: List[TagsType],
    ) -> List[Instance]:
        """Provision exactly `count` spot instances with one all-or-nothing EC2 Fleet request.

        All entries of `instance_parameters` are expected to be identical except for their `InstanceType`: the
        shared part becomes the fleet's launch template and each distinct instance type becomes an override, so
        AWS can satisfy the request from whichever instance pool has capacity. Overrides keep the list order as
        their priority, so the preferred (first) instance type wins whenever its pool can take the request. This
        is what Spot Fleet could not do for us -- SCT always sent it a single-type launch specification, so one
        exhausted pool failed the whole request.
        """
        region_name = provision_parameters.region_name
        instance_parameters_dict = instance_parameters[0].model_dump(
            exclude_none=True,
            exclude_unset=True,
            exclude_defaults=True,
            encode_user_data=True,
        )
        # picks the tags of the first instance to apply to all instances upfront
        # later those would be updated with individual tags (Name, etc.)
        instance_parameters_dict["TagSpecifications"] = [
            {"ResourceType": "instance", "Tags": tags_as_ec2_tags(tags[0])}
        ]
        instance_ids = provision_ec2_fleet(
            region_name=region_name,
            instance_parameters=instance_parameters_dict,
            instance_types=list(dict.fromkeys(params.InstanceType for params in instance_parameters)),
            count=count,
            # the per-node Name is not a property of the shared fleet and launch template
            resource_tags=tags_as_ec2_tags({key: value for key, value in tags[0].items() if key != "Name"}),
            spot=provision_parameters.spot,
        )
        if not instance_ids:
            return []
        LOGGER.info("EC2 Fleet instances: %s", instance_ids)
        try:
            for ind, instance_id in enumerate(instance_ids):
                instance_tags = tags.pop()
                set_tags_on_instances(
                    region_name=region_name,
                    instance_ids=[instance_id],
                    tags={"Name": f"spot_fleet_{instance_id}_{ind}"} | instance_tags,
                )
            return [
                find_instance_by_id(region_name=region_name, instance_id=instance_id) for instance_id in instance_ids
            ]
        except Exception:
            # Never hand back half-initialized nodes, and never leak them: they belong to no cluster yet.
            terminate_instances_by_id(region_name=region_name, instance_ids=instance_ids)
            raise

    def _execute_spot_instance_request(
        self,
        provision_parameters: ProvisionParameters,
        instance_parameters: AWSInstanceParams,
        count: int,
        tags: List[TagsType],
    ) -> List[Instance]:
        # picks the tags of the first instance to apply to all instances upfront
        # later those would be updated with individual tags (Name, etc.)
        tag_specifications = [
            {"ResourceType": "spot-instances-request", "Tags": tags_as_ec2_tags(tags[0])},
        ]

        request_ids = create_spot_instance_request(
            region_name=provision_parameters.region_name,
            count=count,
            instance_parameters=instance_parameters.model_dump(
                exclude_none=True,
                exclude_unset=True,
                exclude_defaults=True,
                encode_user_data=True,
            ),
            full_availability_zone=self._full_availability_zone_name(provision_parameters),
            valid_until=self._spot_valid_until,
            tag_specifications=tag_specifications,
        )
        instance_ids = wait_for_provision_request_done(
            region_name=provision_parameters.region_name,
            request_ids=request_ids,
        )
        if not instance_ids:
            with contextlib.suppress(Exception):
                self._ec2_client(provision_parameters).cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)
            return []
        for ind, instance_id in enumerate(instance_ids):
            instance_tags = tags.pop()
            set_tags_on_instances(
                region_name=provision_parameters.region_name,
                instance_ids=[instance_id],
                tags={"Name": f"spot_{instance_id}_{ind}"} | instance_tags,
            )
        self._ec2_client(provision_parameters).cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)
        return [
            find_instance_by_id(region_name=provision_parameters.region_name, instance_id=instance_id)
            for instance_id in instance_ids
        ]

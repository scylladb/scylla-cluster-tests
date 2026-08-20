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
import uuid
from typing import List, Union

from mypy_boto3_ec2 import EC2Client
from mypy_boto3_ec2.service_resource import Instance

from sdcm.utils.aws_utils import tags_as_ec2_tags
from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.dedicated_host import SCTDedicatedHosts
from sdcm.provision.aws.instance_parameters import AWSInstanceParams
from sdcm.provision.aws.utils import (
    ec2_services,
    ec2_clients,
    find_instance_by_id,
    set_tags_on_instances,
    wait_for_provision_request_done,
    create_spot_instance_request,
    create_launch_template,
    delete_launch_template,
    create_ec2_fleet_instance_request,
    delete_ec2_fleet,
    is_ec2_fleet_unfulfillable,
    log_ec2_fleet_errors,
)
from sdcm.provision.aws.constants import (
    SPOT_CNT_LIMIT,
    EC2_FLEET_LIMIT,
    SPOT_REQUEST_TIMEOUT,
)
from sdcm.provision.common.provisioner import TagsType, ProvisionParameters, InstanceProvisionerBase

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

    def _provision_instance_limit(self, count: int) -> int:
        return EC2_FLEET_LIMIT if self._is_provision_type_fleet(count) else SPOT_CNT_LIMIT

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
        rest_to_provision = count
        provisioned_instances = []
        while rest_to_provision:
            if rest_to_provision // self._provision_instance_limit(count):
                instances_to_provision = self._provision_instance_limit(count)
            else:
                instances_to_provision = rest_to_provision

            if self._is_provision_type_fleet(count) and instances_to_provision > 1:
                new_instances = self._execute_ec2_fleet_instance_request(
                    provision_parameters=provision_parameters,
                    instance_parameters=instance_parameters,
                    count=instances_to_provision,
                    tags=tags,
                )
            else:
                new_instances = self._execute_spot_instance_request(
                    provision_parameters=provision_parameters,
                    instance_parameters=instance_parameters[0],
                    count=instances_to_provision,
                    tags=tags,
                )
            if len(new_instances) < instances_to_provision:
                # A batch under-fulfilled (the fleet path already rolled its own batch back and
                # returned []; the plain-spot path may return a partial set). Returning the earlier
                # successful batches would silently under-provision the cluster, since ProvisionPlan
                # treats any non-empty result as success and skips AZ/region/on-demand fallback.
                # Roll everything provisioned in this request back and return [] so the fallback
                # provision steps get a chance to satisfy the exact requested count.
                leftover = provisioned_instances + new_instances
                if leftover:
                    LOGGER.error(
                        "Spot batch in %s provisioned %d of %d requested instances; "
                        "rolling back %d instance(s) already provisioned in this request.",
                        provision_parameters.region_name,
                        len(new_instances),
                        instances_to_provision,
                        len(leftover),
                    )
                    self._terminate_instances(provision_parameters.region_name, leftover)
                return []
            provisioned_instances.extend(new_instances)
            rest_to_provision -= instances_to_provision
        return provisioned_instances

    @staticmethod
    def _terminate_instances(region_name: str, instances: List[Instance]) -> None:
        """Best-effort termination of instances from a rolled-back multi-batch spot request."""
        instance_ids = [instance.instance_id for instance in instances if getattr(instance, "instance_id", None)]
        if not instance_ids:
            return
        try:
            ec2_clients[region_name].terminate_instances(InstanceIds=instance_ids)
            LOGGER.info("Rolled back spot instances %s in region %s", instance_ids, region_name)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to roll back spot instances %s in region %s: %s", instance_ids, region_name, exc)

    def _execute_ec2_fleet_instance_request(
        self,
        provision_parameters: ProvisionParameters,
        instance_parameters: List[AWSInstanceParams],
        count: int,
        tags: List[TagsType],
    ) -> List[Instance]:
        """Provision `count` spot instances with a single EC2 Fleet request.

        All entries of `instance_parameters` are expected to be identical except for their
        `InstanceType`: the shared part becomes a throwaway launch template and each distinct
        instance type becomes a fleet override, so AWS can satisfy the request from whichever
        instance pool has capacity. This is what Spot Fleet could not do for us -- SCT always
        sent it a single-type launch specification, so one exhausted pool failed the whole batch.
        """
        base_parameters = instance_parameters[0]
        instance_types = list(dict.fromkeys(params.InstanceType for params in instance_parameters))
        region_name = provision_parameters.region_name

        instance_parameters_dict = base_parameters.model_dump(
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

        template_name = f"sct-fleet-{uuid.uuid4()}"
        template_id = create_launch_template(
            region_name=region_name,
            template_name=template_name,
            instance_parameters=instance_parameters_dict,
        )
        fleet_id = None
        instance_ids: List[str] = []
        try:
            fleet_id, instance_ids, errors = create_ec2_fleet_instance_request(
                region_name=region_name,
                count=count,
                template_id=template_id,
                instance_types=instance_types,
                spot=provision_parameters.spot,
            )
            log_ec2_fleet_errors(region_name=region_name, fleet_id=fleet_id, errors=errors)
            if len(instance_ids) < count:
                # Partial fulfillment is useless to SCT: the cluster needs the exact node count,
                # and leaving the extra instances around would leak them. Roll the whole batch back
                # and let the caller's AZ/region/on-demand fallback take over.
                LOGGER.error(
                    "EC2 Fleet %s in %s provisioned %d of %d requested instances%s. Rolling back.",
                    fleet_id,
                    region_name,
                    len(instance_ids),
                    count,
                    " (request is unfulfillable)" if is_ec2_fleet_unfulfillable(errors) else "",
                )
                delete_ec2_fleet(
                    region_name=region_name,
                    fleet_id=fleet_id,
                    terminate_instances=True,
                    instance_ids=instance_ids,
                )
                return []

            LOGGER.info("EC2 Fleet instances: %s", instance_ids)
            for ind, instance_id in enumerate(instance_ids):
                instance_tags = tags.pop()
                set_tags_on_instances(
                    region_name=region_name,
                    instance_ids=[instance_id],
                    tags={"Name": f"spot_fleet_{instance_id}_{ind}"} | instance_tags,
                )
            # `instant` fleets are one-shot: they never maintain or replace capacity, and AWS does
            # not allow deleting the fleet record while keeping its instances
            # (`DeleteFleets(TerminateInstances=False)` is rejected for `instant` fleets). The
            # launched instances are fully independent of the now-inert fleet record, so we hand
            # them off and leave the record for AWS to reap. Clearing `fleet_id` keeps the error
            # handler below from terminating the instances we just provisioned.
            fleet_id = None
            return [
                find_instance_by_id(region_name=region_name, instance_id=instance_id) for instance_id in instance_ids
            ]
        except Exception:
            if fleet_id:
                delete_ec2_fleet(
                    region_name=region_name,
                    fleet_id=fleet_id,
                    terminate_instances=True,
                    instance_ids=instance_ids,
                )
            raise
        finally:
            delete_launch_template(region_name=region_name, template_id=template_id)

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

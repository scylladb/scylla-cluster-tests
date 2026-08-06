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

import abc
import contextlib
import datetime
import logging
import time
from textwrap import dedent
from typing import (
    Any,
    Callable,
    List,
    Dict,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_ec2 import EC2ServiceResource, EC2Client
from mypy_boto3_ec2.service_resource import Instance
from mypy_boto3_ec2.type_defs import (
    InstanceTypeDef,
    RequestSpotLaunchSpecificationTypeDef,
    TagSpecificationTypeDef,
)

from sdcm.provision.aws.capacity_reservation import SCTCapacityReservation
from sdcm.provision.aws.constants import (
    SPOT_REQUEST_TIMEOUT,
    SPOT_REQUEST_WAITING_TIME,
    STATUS_FULFILLED,
    SPOT_PRICE_TOO_LOW,
    SPOT_CAPACITY_NOT_AVAILABLE_ERROR,
    EC2_FLEET_TYPE_INSTANT,
    EC2_FLEET_ALLOCATION_STRATEGY,
    EC2_FLEET_UNFULFILLABLE_ERROR_CODES,
)
from sdcm.provision.common.provisioner import TagsType
from sdcm.utils.common import aws_tags_to_dict, list_instances_aws


LOGGER = logging.getLogger(__name__)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class GlobalDictOfInstances(dict, metaclass=Singleton):
    @abc.abstractmethod
    def _create_instance(self, item: str) -> Any:
        pass

    def __getitem__(self, item: str) -> Any:
        if item_value := self.get(item, None):
            return item_value
        item_value = self._create_instance(item)
        self[item] = item_value
        return item_value


class Ec2ServicesDict(GlobalDictOfInstances):
    def _create_instance(self, item: str) -> EC2ServiceResource:
        return boto3.session.Session(region_name=item).resource("ec2")

    __getitem__: Callable[[str], EC2ServiceResource]


class Ec2ClientsDict(GlobalDictOfInstances):
    def _create_instance(self, item: str) -> EC2Client:
        return boto3.client(service_name="ec2", region_name=item)

    __getitem__: Callable[[str], EC2Client]


class Ec2ServiceResourcesDict(GlobalDictOfInstances):
    def _create_instance(self, item: str) -> EC2ServiceResource:
        return boto3.resource("ec2", region_name=item)

    __getitem__: Callable[[str], EC2ServiceResource]


ec2_services = Ec2ServicesDict()
ec2_clients = Ec2ClientsDict()
ec2_resources = Ec2ServiceResourcesDict()


def split_instance_types(instance_type: Union[str, List[str], None]) -> List[str]:
    """Parse an `instance_type_*` config value into an ordered list of interchangeable types.

    Accepts either an actual list (e.g. `aws_instance_type_db_alternatives`, a ``StringOrList``
    param) or a single/comma-separated string. The first entry is the preferred type; the rest
    exist so EC2 Fleet can fall back to another instance pool when spot capacity for the preferred
    type runs out. Order is preserved and duplicates removed.
    """
    if not instance_type:
        return []
    raw = instance_type if isinstance(instance_type, (list, tuple)) else [instance_type]
    types = [part.strip() for item in raw for part in str(item).split(",") if part.strip()]
    return list(dict.fromkeys(types))


def get_subnet_info(region_name: str, subnet_id: str):
    resp = ec2_clients[region_name].describe_subnets(SubnetIds=[subnet_id])
    return [subnet for subnet in resp["Subnets"] if subnet["SubnetId"] == subnet_id][0]


def convert_tags_to_aws_format(tags: TagsType) -> List[Dict[str, str]]:
    return [{"Key": str(name), "Value": str(value)} for name, value in tags.items()]


def convert_tags_to_filters(tags: TagsType) -> List[Dict[str, str]]:
    return [
        {"Name": f"tag:{name}", "Values": value if isinstance(value, list) else [value]} for name, value in tags.items()
    ]


def find_instance_descriptions_by_tags(region_name: str, tags: TagsType) -> List[InstanceTypeDef]:
    client: EC2Client = ec2_clients[region_name]
    response = client.describe_instances(Filters=convert_tags_to_filters(tags))
    return [instance for reservation in response["Reservations"] for instance in reservation["Instances"]]


def find_instances_by_tags(region_name: str, tags: TagsType, states: List[str] = None) -> List[Instance]:
    instances = []
    for instance_description in find_instance_descriptions_by_tags(region_name=region_name, tags=tags):
        if states and instance_description["State"]["Name"] not in states:
            continue
        instances.append(find_instance_by_id(region_name=region_name, instance_id=instance_description["InstanceId"]))
    return instances


def find_instance_by_id(region_name: str, instance_id: str) -> Instance:
    return ec2_resources[region_name].Instance(id=instance_id)


def set_tags_on_instances(region_name: str, instance_ids: List[str], tags: TagsType):
    end_time = time.perf_counter() + 20
    while end_time > time.perf_counter():
        with contextlib.suppress(ClientError):
            ec2_clients[region_name].create_tags(Resources=instance_ids, Tags=convert_tags_to_aws_format(tags))
            return True
    return False


def wait_for_provision_request_done(
    region_name: str,
    request_ids: List[str],
    timeout: float = SPOT_REQUEST_TIMEOUT,
    wait_interval: float = SPOT_REQUEST_WAITING_TIME,
):
    """Poll one-time spot instance requests until they are fulfilled, fail, or time out.

    EC2 Fleet requests do not go through here: `Type="instant"` fleets return their instance ids
    from `create_fleet` itself, so there is nothing to wait for.
    """
    waiting_time = 0
    provisioned_instance_ids = []
    while not provisioned_instance_ids and waiting_time < timeout:
        time.sleep(wait_interval)
        provisioned_instance_ids = get_provisioned_spot_instance_ids(region_name=region_name, request_ids=request_ids)
        if provisioned_instance_ids is None:
            break
        waiting_time += wait_interval
    return provisioned_instance_ids


def get_provisioned_spot_instance_ids(region_name: str, request_ids: List[str]) -> Optional[List[str]]:
    """
    Return list of provisioned instances if all requests where fulfilled
      if any of the requests failed it will return empty list
      if any of the requests failed critically and could not be fulfilled return None
    """
    try:
        resp = ec2_clients[region_name].describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error(
            "Failed to describe spot instance requests in region %s for request IDs %s: %s",
            region_name,
            request_ids,
            exc,
        )
        return []
    provisioned = []
    for req in resp["SpotInstanceRequests"]:
        request_id = req.get("SpotInstanceRequestId", "unknown")
        status_code = req["Status"]["Code"]
        status_message = req["Status"].get("Message", "No message provided")
        state = req["State"]

        if status_code != STATUS_FULFILLED or state != "active":
            if status_code in [SPOT_PRICE_TOO_LOW, SPOT_CAPACITY_NOT_AVAILABLE_ERROR]:
                # This code tells that query is not going to be fulfilled
                # And we need to stop the cycle
                LOGGER.error(
                    "Critical spot provisioning failure in region %s for request %s: "
                    "Status='%s', State='%s', Message='%s'. "
                    "This request cannot be fulfilled and provisioning will not retry.",
                    region_name,
                    request_id,
                    status_code,
                    state,
                    status_message,
                )
                return None
            LOGGER.warning(
                "Spot instance request not yet fulfilled in region %s for request %s: "
                "Status='%s', State='%s', Message='%s'",
                region_name,
                request_id,
                status_code,
                state,
                status_message,
            )
            return []
        provisioned.append(req["InstanceId"])
    return provisioned


def create_spot_instance_request(
    region_name: str,
    count: int,
    instance_parameters: RequestSpotLaunchSpecificationTypeDef,
    full_availability_zone: str,
    valid_until: datetime.datetime = None,
    tag_specifications: Sequence[TagSpecificationTypeDef] = None,
) -> List[str]:
    params = {
        "DryRun": False,
        "InstanceCount": count,
        "Type": "one-time",
        "LaunchSpecification": instance_parameters,
        "AvailabilityZoneGroup": full_availability_zone,
        "TagSpecifications": tag_specifications,
    }
    if valid_until:
        params["ValidUntil"] = valid_until
    resp = ec2_clients[region_name].request_spot_instances(**params)
    return [req["SpotInstanceRequestId"] for req in resp["SpotInstanceRequests"]]


EC2_FLEET_UNSUPPORTED_LAUNCH_TEMPLATE_KEYS = ("AddressingType", "SubnetId", "SecurityGroups")
# Keys accepted by RunInstances/RequestSpotFleet launch specs but rejected by
# CreateLaunchTemplate. SubnetId/SecurityGroups are already expressed through NetworkInterfaces
# in every SCT instance parameter set, so dropping them here is lossless.


def build_launch_template_data(instance_parameters: dict) -> dict:
    """Convert a RunInstances-style parameter dict into CreateLaunchTemplate `LaunchTemplateData`.

    EC2 Fleet, unlike Spot Fleet, cannot take an inline launch specification -- it only accepts a
    reference to a launch template. Everything that is shared between the instance types
    (AMI, key pair, user data, network interfaces, block devices, tags) lives in the template;
    only `InstanceType` varies, and that is supplied per-override by the fleet request.
    """
    launch_template_data = {
        key: value
        for key, value in instance_parameters.items()
        if key not in EC2_FLEET_UNSUPPORTED_LAUNCH_TEMPLATE_KEYS
    }
    # InstanceType is supplied per-override by the fleet request, so it must not be pinned here,
    # otherwise a single-type template would silently win over the diversified overrides.
    launch_template_data.pop("InstanceType", None)
    return launch_template_data


def create_launch_template(region_name: str, template_name: str, instance_parameters: dict) -> str:
    """Create a throwaway launch template backing a single EC2 Fleet request. Returns its id."""
    resp = ec2_clients[region_name].create_launch_template(
        LaunchTemplateName=template_name,
        LaunchTemplateData=build_launch_template_data(instance_parameters),
    )
    template_id = resp["LaunchTemplate"]["LaunchTemplateId"]
    LOGGER.info("Created launch template %s (%s) in region %s", template_name, template_id, region_name)
    return template_id


def delete_launch_template(region_name: str, template_id: str) -> None:
    """Best-effort cleanup of a throwaway launch template - never fail provisioning over this."""
    try:
        ec2_clients[region_name].delete_launch_template(LaunchTemplateId=template_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to delete launch template %s in region %s: %s", template_id, region_name, exc)


def create_ec2_fleet_instance_request(
    region_name: str,
    count: int,
    template_id: str,
    instance_types: List[str],
    spot: bool = True,
    tag_specifications: Sequence[TagSpecificationTypeDef] = None,
) -> Tuple[Optional[str], List[str], List[dict]]:
    """Request `count` instances via EC2 Fleet, diversified across `instance_types`.

    Uses `Type="instant"`, so the call is synchronous: the response already carries the provisioned
    instance ids and there is nothing to poll. This is the key behavioural difference from Spot
    Fleet, which required a describe/poll loop until the request became `fulfilled`.
    `ValidUntil` is intentionally never sent: AWS rejects it outright for `instant` fleets
    (`InvalidParameter: ValidUntil is not supported for given fleet type`) -- it's only valid for
    the `request`/`maintain` fleet types SCT doesn't use.

    Returns a `(fleet_id, instance_ids, errors)` tuple. `errors` is the raw `Errors` list from the
    response and is non-empty on partial fulfillment even when some instances did come up.
    """
    overrides = [{"InstanceType": instance_type} for instance_type in instance_types]
    params = {
        "LaunchTemplateConfigs": [
            {
                "LaunchTemplateSpecification": {"LaunchTemplateId": template_id, "Version": "$Latest"},
                "Overrides": overrides,
            }
        ],
        "TargetCapacitySpecification": {
            "TotalTargetCapacity": count,
            "DefaultTargetCapacityType": "spot" if spot else "on-demand",
        },
        "Type": EC2_FLEET_TYPE_INSTANT,
    }
    if spot:
        params["SpotOptions"] = {"AllocationStrategy": EC2_FLEET_ALLOCATION_STRATEGY}
    if tag_specifications:
        params["TagSpecifications"] = tag_specifications

    LOGGER.info(
        "Requesting EC2 Fleet in %s for %d instances across instance types %s",
        region_name,
        count,
        ", ".join(instance_types),
    )
    resp = ec2_clients[region_name].create_fleet(**params)
    fleet_id = resp.get("FleetId")
    errors = resp.get("Errors", [])
    instance_ids = []
    for instance_group in resp.get("Instances", []):
        instance_ids.extend(instance_group.get("InstanceIds", []))
    return fleet_id, instance_ids, errors


def is_ec2_fleet_unfulfillable(errors: List[dict]) -> bool:
    """True when the fleet errors mean retrying the same request is pointless."""
    return any(error.get("ErrorCode") in EC2_FLEET_UNFULFILLABLE_ERROR_CODES for error in errors)


def log_ec2_fleet_errors(region_name: str, fleet_id: Optional[str], errors: List[dict]) -> None:
    if not errors:
        return
    for error in errors:
        LOGGER.error(
            "EC2 Fleet %s in region %s reported error: Code='%s', Message='%s', InstanceType='%s'",
            fleet_id,
            region_name,
            error.get("ErrorCode"),
            error.get("ErrorMessage"),
            error.get("LaunchTemplateAndOverrides", {}).get("Overrides", {}).get("InstanceType"),
        )


def delete_ec2_fleet(
    region_name: str,
    fleet_id: Optional[str],
    terminate_instances: bool,
    instance_ids: Optional[List[str]] = None,
) -> None:
    """Delete an `instant` fleet record. Instances outlive it unless `terminate_instances` is set.

    When `terminate_instances` is True this also guards against a leaked-instance rollback: if the
    delete call raises or AWS reports the fleet in `UnsuccessfulFleetDeletions`, it falls back to
    terminating `instance_ids` directly so a failed rollback can never leave spot instances running.
    """
    if not fleet_id:
        return
    try:
        response = ec2_clients[region_name].delete_fleets(FleetIds=[fleet_id], TerminateInstances=terminate_instances)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to delete EC2 fleet %s in region %s: %s", fleet_id, region_name, exc)
        if terminate_instances and instance_ids:
            _terminate_instances(region_name=region_name, instance_ids=instance_ids)
        return
    if unsuccessful := response.get("UnsuccessfulFleetDeletions"):
        LOGGER.warning("EC2 fleet %s in region %s was not fully deleted: %s", fleet_id, region_name, unsuccessful)
        if terminate_instances and instance_ids:
            _terminate_instances(region_name=region_name, instance_ids=instance_ids)


def _terminate_instances(region_name: str, instance_ids: List[str]) -> None:
    """Best-effort direct termination of instances left behind by a failed fleet deletion."""
    try:
        ec2_clients[region_name].terminate_instances(InstanceIds=list(instance_ids))
        LOGGER.info("Terminated leftover EC2 fleet instances %s in region %s", instance_ids, region_name)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to terminate leftover instances %s in region %s: %s", instance_ids, region_name, exc)


def sort_by_index(item: dict) -> str:
    for tag in item["Tags"]:
        if tag["Key"] == "NodeIndex":
            return tag["Value"]
    return "0"


def network_config_ipv6_workaround_script():
    return dedent(r"""
        if grep -qi "ubuntu" /etc/os-release; then
            echo "On Ubuntu we don't need this workaround, so done"
        else
            TOKEN=$(curl -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 600")
            BASE_EC2_NETWORK_URL=http://169.254.169.254/latest/meta-data/network/interfaces/macs/
            MAC=`curl -s -H "X-aws-ec2-metadata-token: ${TOKEN}" ${BASE_EC2_NETWORK_URL}`
            IPv6_CIDR=`curl -s -H "X-aws-ec2-metadata-token: ${TOKEN}" ${BASE_EC2_NETWORK_URL}${MAC}/subnet-ipv6-cidr-blocks`

            NETWORK_DEVICE=`ip -4 route ls | grep default | grep -Po '(?<=dev )(\S+)'`

            while ! ls /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE; do sleep 1; done

            if ! grep -qi "amazon linux" /etc/os-release; then
                ip route add $IPv6_CIDR dev $NETWORK_DEVICE
                echo "ip route add $IPv6_CIDR dev $NETWORK_DEVICE" >> /etc/sysconfig/network-scripts/init.ipv6-global
            fi

            if grep -q IPV6_AUTOCONF /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE; then
                sed -i 's/^IPV6_AUTOCONF=[^ ]*/IPV6_AUTOCONF=yes/' /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE
            else
                echo "IPV6_AUTOCONF=yes" >> /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE
            fi

            if grep -q IPV6_DEFROUTE /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE; then
                sed -i 's/^IPV6_DEFROUTE=[^ ]*/IPV6_DEFROUTE=yes/' /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE
            else
                echo "IPV6_DEFROUTE=yes" >> /etc/sysconfig/network-scripts/ifcfg-$NETWORK_DEVICE
            fi

            systemctl restart network
        fi
    """)


def enable_ssm_agent_script():
    """Our images come with it masked by default. For testing we want this for debugging purposes, especially when we can't have SSH connectivity."""
    return dedent(r"""
        if ! systemctl is-active --quiet amazon-ssm-agent; then
            systemctl unmask amazon-ssm-agent
            systemctl enable amazon-ssm-agent
            systemctl start amazon-ssm-agent
        fi
    """)


def configure_set_preserve_hostname_script():
    return (
        'grep "preserve_hostname: true" /etc/cloud/cloud.cfg 1>/dev/null 2>&1 '
        '|| echo "preserve_hostname: true" >> /etc/cloud/cloud.cfg\n'
    )


# -----AWS Placement Group section -----
def create_cluster_placement_groups_aws(name: str, tags: dict, region=None, dry_run=False):
    ec2: EC2Client = ec2_clients[region]
    result = ec2.create_placement_group(
        DryRun=dry_run,
        GroupName=name,
        Strategy="cluster",
        TagSpecifications=[
            {
                "ResourceType": "placement-group",
                "Tags": [{"Key": key, "Value": value} for key, value in tags.items()]
                + [{"Key": "Name", "Value": name}],
            }
        ],
    )
    return result


def cleanup_abandoned_region(test_id: str, region: str) -> None:
    """Cleanup for a region abandoned by region fallback.

    Cancels capacity reservations for `test_id` across all regions and
    terminates stale instances tagged with `test_id` in `region`.
    """
    if not test_id:
        return

    try:
        SCTCapacityReservation.cancel_all_regions(test_id)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Abandoned-region belt: failed to cancel capacity reservations for %s: %s", test_id, exc)

    try:
        instances = list_instances_aws(tags_dict={"TestId": test_id}, region_name=region, group_as_region=True).get(
            region, []
        )
        instance_ids = [
            inst["InstanceId"]
            for inst in instances
            if aws_tags_to_dict(inst.get("Tags")).get("NodeType") != "sct-runner"
        ]

        if not instance_ids:
            return
        LOGGER.info(
            "Abandoned-region belt: terminating %d stray instance(s) in %s: %s",
            len(instance_ids),
            region,
            instance_ids,
        )
        ec2_clients[region].terminate_instances(InstanceIds=instance_ids)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Abandoned-region belt: failed to terminate stray instances in %s: %s", region, exc)

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
)

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_ec2 import EC2ServiceResource, EC2Client
from mypy_boto3_ec2.service_resource import Instance
from mypy_boto3_ec2.type_defs import (
    InstanceTypeDef,
    SpotFleetLaunchSpecificationTypeDef,
    RequestSpotLaunchSpecificationTypeDef,
    SpotFleetRequestConfigDataTypeDef,
    TagSpecificationTypeDef,
)

from sdcm.provision.aws.constants import SPOT_REQUEST_TIMEOUT, SPOT_REQUEST_WAITING_TIME, STATUS_FULFILLED, \
    SPOT_STATUS_UNEXPECTED_ERROR, SPOT_PRICE_TOO_LOW, FLEET_LIMIT_EXCEEDED_ERROR, SPOT_CAPACITY_NOT_AVAILABLE_ERROR
from sdcm.provision.common.provisioner import TagsType


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
        return boto3.session.Session(region_name=item).resource('ec2')

    __getitem__: Callable[[str], EC2ServiceResource]


class Ec2ClientsDict(GlobalDictOfInstances):
    def _create_instance(self, item: str) -> EC2Client:
        return boto3.client(service_name='ec2', region_name=item)

    __getitem__: Callable[[str], EC2Client]


class Ec2ServiceResourcesDict(GlobalDictOfInstances):
    def _create_instance(self, item: str) -> EC2ServiceResource:
        return boto3.resource('ec2', region_name=item)

    __getitem__: Callable[[str], EC2ServiceResource]


ec2_services = Ec2ServicesDict()
ec2_clients = Ec2ClientsDict()
ec2_resources = Ec2ServiceResourcesDict()


def get_subnet_info(region_name: str, subnet_id: str):
    resp = ec2_clients[region_name].describe_subnets(SubnetIds=[subnet_id])
    return [subnet for subnet in resp['Subnets'] if subnet['SubnetId'] == subnet_id][0]


def convert_tags_to_aws_format(tags: TagsType) -> List[Dict[str, str]]:
    return [{'Key': str(name), 'Value': str(value)} for name, value in tags.items()]


def convert_tags_to_filters(tags: TagsType) -> List[Dict[str, str]]:
    return [{'Name': 'tag:{}'.format(name), 'Values': value if isinstance(
        value, list) else [value]} for name, value in tags.items()]


def find_instance_descriptions_by_tags(region_name: str, tags: TagsType) -> List[InstanceTypeDef]:
    client: EC2Client = ec2_clients[region_name]
    response = client.describe_instances(Filters=convert_tags_to_filters(tags))
    return [instance for reservation in response['Reservations'] for instance in reservation['Instances']]


def find_instances_by_tags(region_name: str, tags: TagsType, states: List[str] = None) -> List[Instance]:
    instances = []
    for instance_description in find_instance_descriptions_by_tags(region_name=region_name, tags=tags):
        if states and instance_description['State']['Name'] not in states:
            continue
        instances.append(find_instance_by_id(region_name=region_name, instance_id=instance_description['InstanceId']))
    return instances


def find_instance_by_id(region_name: str, instance_id: str) -> Instance:
    return ec2_resources[region_name].Instance(id=instance_id)


def set_tags_on_instances(region_name: str, instance_ids: List[str], tags: TagsType):
    end_time = time.perf_counter() + 20
    while end_time > time.perf_counter():
        with contextlib.suppress(ClientError):
            ec2_clients[region_name].create_tags(
                Resources=instance_ids,
                Tags=convert_tags_to_aws_format(tags))
            return True
    return False


def wait_for_provision_request_done(
        region_name: str, request_ids: List[str], is_fleet: bool,
        timeout: float = SPOT_REQUEST_TIMEOUT,
        wait_interval: float = SPOT_REQUEST_WAITING_TIME):
    waiting_time = 0
    provisioned_instance_ids = []
    while not provisioned_instance_ids and waiting_time < timeout:
        time.sleep(wait_interval)
        if is_fleet:
            provisioned_instance_ids = get_provisioned_fleet_instance_ids(
                region_name=region_name, request_ids=request_ids)
        else:
            provisioned_instance_ids = get_provisioned_spot_instance_ids(
                region_name=region_name, request_ids=request_ids)
        if provisioned_instance_ids is None:
            break
        waiting_time += wait_interval
    return provisioned_instance_ids


def get_provisioned_fleet_instance_ids(region_name: str, request_ids: List[str]) -> Optional[List[str]]:
    try:
        resp = ec2_clients[region_name].describe_spot_fleet_requests(SpotFleetRequestIds=request_ids)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error(
            "Failed to describe spot fleet requests in region %s for request IDs %s: %s",
            region_name, request_ids, exc
        )
        return []
    for req in resp['SpotFleetRequestConfigs']:
        request_id = req.get('SpotFleetRequestId', 'unknown')
        fleet_state = req.get('SpotFleetRequestState', 'unknown')
        activity_status = req.get('ActivityStatus', None)

        if fleet_state == 'active' and activity_status == STATUS_FULFILLED:
            continue
        if activity_status == SPOT_STATUS_UNEXPECTED_ERROR:
            current_time = datetime.datetime.now().timetuple()
            search_start_time = datetime.datetime(
                current_time.tm_year, current_time.tm_mon, current_time.tm_mday)
            try:
                history_resp = ec2_clients[region_name].describe_spot_fleet_request_history(
                    SpotFleetRequestId=request_id,
                    StartTime=search_start_time,
                    MaxResults=10,
                )
                errors = [i['EventInformation']['EventSubType'] for i in history_resp['HistoryRecords']]
                error_messages = [
                    f"{i['EventType']}: {i['EventInformation'].get('EventDescription', 'No description')}"
                    for i in history_resp['HistoryRecords']
                ]

                for error in [FLEET_LIMIT_EXCEEDED_ERROR, SPOT_CAPACITY_NOT_AVAILABLE_ERROR]:
                    if error in errors:
                        LOGGER.error(
                            "Critical spot fleet provisioning failure in region %s for fleet request %s: "
                            "State='%s', ActivityStatus='%s', Error='%s'. "
                            "Error history: %s. "
                            "This request cannot be fulfilled and provisioning will not retry.",
                            region_name, request_id, fleet_state, activity_status, error,
                            "; ".join(error_messages)
                        )
                        return None
            except Exception as exc:  # noqa: BLE001
                LOGGER.error(
                    "Failed to retrieve spot fleet request history for %s in region %s: %s",
                    request_id, region_name, exc
                )
        LOGGER.warning(
            "Spot fleet request not yet fulfilled in region %s for request %s: "
            "State='%s', ActivityStatus='%s'",
            region_name, request_id, fleet_state, activity_status
        )
        return []
    provisioned_instances = []
    for request_id in request_ids:
        try:
            resp = ec2_clients[region_name].describe_spot_fleet_instances(SpotFleetRequestId=request_id)
        except Exception as exc:  # noqa: BLE001
            LOGGER.error(
                "Failed to describe spot fleet instances for fleet request %s in region %s: %s",
                request_id, region_name, exc
            )
            return None
        provisioned_instances.extend([inst['InstanceId'] for inst in resp['ActiveInstances']])
    return provisioned_instances


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
            region_name, request_ids, exc
        )
        return []
    provisioned = []
    for req in resp['SpotInstanceRequests']:
        request_id = req.get('SpotInstanceRequestId', 'unknown')
        status_code = req['Status']['Code']
        status_message = req['Status'].get('Message', 'No message provided')
        state = req['State']

        if status_code != STATUS_FULFILLED or state != 'active':
            if status_code in [SPOT_PRICE_TOO_LOW, SPOT_CAPACITY_NOT_AVAILABLE_ERROR]:
                # This code tells that query is not going to be fulfilled
                # And we need to stop the cycle
                LOGGER.error(
                    "Critical spot provisioning failure in region %s for request %s: "
                    "Status='%s', State='%s', Message='%s'. "
                    "This request cannot be fulfilled and provisioning will not retry.",
                    region_name, request_id, status_code, state, status_message
                )
                return None
            LOGGER.warning(
                "Spot instance request not yet fulfilled in region %s for request %s: "
                "Status='%s', State='%s', Message='%s'",
                region_name, request_id, status_code, state, status_message
            )
            return []
        provisioned.append(req['InstanceId'])
    return provisioned


def create_spot_fleet_instance_request(
        region_name: str,
        count: int,
        price: float,
        fleet_role: str,
        instance_parameters: SpotFleetLaunchSpecificationTypeDef,
        valid_until: datetime.datetime = None) -> str:
    params = SpotFleetRequestConfigDataTypeDef(
        LaunchSpecifications=[instance_parameters],
        IamFleetRole=fleet_role,
        SpotPrice=str(price),
        TargetCapacity=count,
    )
    if valid_until:
        params['ValidUntil'] = valid_until
    resp = ec2_clients[region_name].request_spot_fleet(DryRun=False, SpotFleetRequestConfig=params)
    return resp['SpotFleetRequestId']


def create_spot_instance_request(
        region_name: str,
        count: int,
        instance_parameters: RequestSpotLaunchSpecificationTypeDef,
        full_availability_zone: str,
        valid_until: datetime.datetime = None,
        tag_specifications: Sequence[TagSpecificationTypeDef] = None
) -> List[str]:
    params = {
        'DryRun': False,
        'InstanceCount': count,
        'Type': 'one-time',
        'LaunchSpecification': instance_parameters,
        'AvailabilityZoneGroup': full_availability_zone,
        'TagSpecifications': tag_specifications,
    }
    if valid_until:
        params['ValidUntil'] = valid_until
    resp = ec2_clients[region_name].request_spot_instances(**params)
    return [req['SpotInstanceRequestId'] for req in resp['SpotInstanceRequests']]


def sort_by_index(item: dict) -> str:
    for tag in item['Tags']:
        if tag['Key'] == 'NodeIndex':
            return tag['Value']
    return '0'


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
    return 'grep "preserve_hostname: true" /etc/cloud/cloud.cfg 1>/dev/null 2>&1 ' \
           '|| echo "preserve_hostname: true" >> /etc/cloud/cloud.cfg\n'


# -----AWS Placement Group section -----
def create_cluster_placement_groups_aws(name: str, tags: dict, region=None, dry_run=False):
    ec2: EC2Client = ec2_clients[region]
    result = ec2.create_placement_group(
        DryRun=dry_run, GroupName=name, Strategy='cluster',
        TagSpecifications=[{
            'ResourceType': 'placement-group',
            "Tags": [{"Key": key, "Value": value} for key, value in tags.items()] +
                    [{"Key": "Name", "Value": name}], }],)
    return result

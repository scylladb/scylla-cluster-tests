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
import functools
import json
import socket
import time
import logging
from functools import cached_property
from typing import List, Dict, get_args

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_ec2 import EC2ServiceResource, EC2Client
from mypy_boto3_ec2.literals import ArchitectureTypeType

from sdcm.provision.network_configuration import ssh_connection_ip_type, network_interfaces_count
from sdcm.utils.decorators import retrying
from sdcm.utils.aws_region import AwsRegion
from sdcm.wait import wait_for
from sdcm.test_config import TestConfig
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)
AwsArchType = ArchitectureTypeType


class EksClusterCleanupMixin:
    short_cluster_name: str
    region_name: str

    @cached_property
    def eks_client(self):
        return boto3.client('eks', region_name=self.region_name)

    @cached_property
    def ec2_client(self):
        return boto3.client('ec2', region_name=self.region_name)

    @cached_property
    def iam_client(self):
        return boto3.client('iam', region_name=self.region_name)

    @property
    def owned_object_tag_name(self):
        return f'kubernetes.io/cluster/{self.short_cluster_name}'

    @cached_property
    def cluster_owned_objects_filter(self):
        return [{"Name": f"tag:{self.owned_object_tag_name}", 'Values': ['owned']}]

    @property
    def attached_security_group_ids(self) -> List[str]:
        return [group_desc['GroupId'] for group_desc in
                self.ec2_client.describe_security_groups(Filters=self.cluster_owned_objects_filter)['SecurityGroups']]

    @property
    def attached_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names()

    @property
    def failed_to_delete_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names(status='DELETE_FAILED')

    @property
    def deleting_nodegroup_names(self) -> List[str]:
        return self._get_attached_nodegroup_names(status='DELETING')

    def _get_attached_nodegroup_names(self, status: str = None) -> List[str]:
        if status is None:
            return self.eks_client.list_nodegroups(clusterName=self.short_cluster_name)['nodegroups']
        output = []
        for nodegroup_name in self.attached_nodegroup_names:
            if status == self.eks_client.describe_nodegroup(
                    clusterName=self.short_cluster_name, nodegroupName=nodegroup_name)['nodegroup']['status']:
                output.append(nodegroup_name)
        return output

    @property
    def cluster_exists(self) -> bool:
        if self.short_cluster_name in self.eks_client.list_clusters()['clusters']:
            return True
        return False

    def destroy(self):
        for _ in range(2):
            self.destroy_nodegroups()
            if self.failed_to_delete_nodegroup_names:
                self.destroy_nodegroups(status='DELETE_FAILED')
            self.destroy_cluster()
            # Destroying of the security groups will affect load balancers and node groups that is why
            # in order to do not distract load balancers cleaning process we should have
            # destroy_attached_security_groups performed after destroy_attached_load_balancers
            # but before retrying of the destroy_nodegroups
            self.destroy_attached_security_groups()
            if not self.cluster_exists:
                break
        self.destroy_oidc_provider()

    def check_if_all_network_interfaces_detached(self, sg_id):
        for interface_description in self.ec2_client.describe_network_interfaces(
                Filters=[{'Name': 'group-id', 'Values': [sg_id]}])['NetworkInterfaces']:
            if attachment := interface_description.get('Attachment'):
                if attachment.get('AttachmentId'):
                    return False
        return True

    def delete_network_interfaces_of_sg(self, sg_id: str):
        network_interfaces = self.ec2_client.describe_network_interfaces(
            Filters=[{'Name': 'group-id', 'Values': [sg_id]}])['NetworkInterfaces']

        for interface_description in network_interfaces:
            network_interface_id = interface_description['NetworkInterfaceId']
            if attachment := interface_description.get('Attachment'):
                if attachment_id := attachment.get('AttachmentId'):
                    try:
                        self.ec2_client.detach_network_interface(AttachmentId=attachment_id, Force=True)
                    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                        LOGGER.debug("Failed to detach network interface (%s) attachment %s:\n%s",
                                     network_interface_id, attachment_id, exc)

        wait_for(self.check_if_all_network_interfaces_detached, sg_id=sg_id, timeout=120, throw_exc=False)

        for interface_description in network_interfaces:
            network_interface_id = interface_description['NetworkInterfaceId']
            try:
                self.ec2_client.delete_network_interface(NetworkInterfaceId=network_interface_id)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                LOGGER.debug("Failed to delete network interface %s :\n%s", network_interface_id, exc)

    def destroy_attached_security_groups(self):
        # EKS infra does not cleanup security group perfectly and some of them can be left alive
        # even when cluster is gone
        try:
            sg_list = self.attached_security_group_ids
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.debug("Failed to get list of security groups:\n%s", exc)
            return

        for security_group_id in sg_list:
            # EKS Nodegroup deletion can fail due to the network interfaces stuck in attached state
            # while instance is gone.
            # In this case you need to forcefully detach interfaces and delete them to make nodegroup deletion possible.
            try:
                self.delete_network_interfaces_of_sg(security_group_id)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                LOGGER.debug("destroy_attached_security_groups: %s", exc)

            try:
                self.ec2_client.delete_security_group(GroupId=security_group_id)
            except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                LOGGER.debug("Failed to delete security groups %s, due to the following error:\n%s",
                             security_group_id, exc)

    def destroy_nodegroups(self, status=None):

        def _destroy_attached_nodegroups():
            for node_group_name in self._get_attached_nodegroup_names(status=status):
                try:
                    self.eks_client.delete_nodegroup(clusterName=self.short_cluster_name, nodegroupName=node_group_name)
                except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
                    LOGGER.debug("Failed to delete nodegroup %s/%s, due to the following error:\n%s",
                                 self.short_cluster_name, node_group_name, exc)
            time.sleep(10)
            return wait_for(lambda: not self._get_attached_nodegroup_names(status='DELETING'),
                            text='Waiting till target nodegroups are deleted',
                            step=10,
                            timeout=300,
                            throw_exc=False)

        wait_for(_destroy_attached_nodegroups, timeout=400, throw_exc=False)

    def destroy_cluster(self):
        try:
            self.eks_client.delete_cluster(name=self.short_cluster_name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.debug("Failed to delete cluster %s, due to the following error:\n%s",
                         self.short_cluster_name, exc)

    def destroy_oidc_provider(self):
        try:
            oidc_providers = self.iam_client.list_open_id_connect_providers()
            for oidc_provider in oidc_providers["OpenIDConnectProviderList"]:
                oidc_provider_tags = self.iam_client.list_open_id_connect_provider_tags(
                    OpenIDConnectProviderArn=oidc_provider["Arn"])["Tags"]
                for oidc_provider_tag in oidc_provider_tags:
                    if "cluster-name" not in oidc_provider_tag.get("Key", "key-not-found"):
                        continue
                    if oidc_provider_tag.get("Value", "value-not-found") == self.short_cluster_name:
                        self.iam_client.delete_open_id_connect_provider(
                            OpenIDConnectProviderArn=oidc_provider["Arn"])
                        break
                else:
                    continue
                break
            else:
                LOGGER.warning(
                    "Couldn't find any OIDC provider associated with the '%s' EKS cluster",
                    self.short_cluster_name)
        except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
            LOGGER.warning(
                "Failed to delete OIDC provider for the '%s' cluster due to "
                "the following error:\n%s",
                self.short_cluster_name, exc)


class EksClusterForCleaner(EksClusterCleanupMixin):
    def __init__(self, name: str, region: str):
        self.short_cluster_name = name
        self.name = name
        self.region_name = region
        self.body = self.eks_client.describe_cluster(name=name)['cluster']

    @cached_property
    def metadata(self) -> dict:
        metadata = self.body['tags'].items()
        return {"items": [{"key": key, "value": value} for key, value in metadata], }

    @cached_property
    def create_time(self):
        return self.body['createdAt']


def init_monitoring_info_from_params(monitor_info: dict, params: dict, regions: List):
    if monitor_info['n_nodes'] is None:
        monitor_info['n_nodes'] = params.get('n_monitor_nodes')
    if monitor_info['type'] is None:
        monitor_info['type'] = params.get('instance_type_monitor')
    if monitor_info['disk_size'] is None:
        monitor_info['disk_size'] = params.get('root_disk_size_monitor')
    if monitor_info['device_mappings'] is None:
        if monitor_info['disk_size']:
            monitor_info['device_mappings'] = [{
                "DeviceName": ec2_ami_get_root_device_name(image_id=params.get('ami_id_monitor').split()[0],
                                                           region_name=regions[0]),
                "Ebs": {
                    "VolumeSize": monitor_info['disk_size'],
                    "VolumeType": "gp3"
                }
            }]
        else:
            monitor_info['device_mappings'] = []
    return monitor_info


def init_db_info_from_params(db_info: dict, params: dict, regions: List, root_device: str = None):
    if db_info['n_nodes'] is None:
        db_info['n_nodes'] = params.total_db_nodes
    if db_info['type'] is None:
        db_info['type'] = params.get('instance_type_db')
    if db_info['disk_size'] is None:
        db_info['disk_size'] = params.get('root_disk_size_db')
    if db_info['device_mappings'] is None and (root_device or params.get('ami_id_db_scylla')):
        if db_info['disk_size']:
            root_device = root_device if root_device else ec2_ami_get_root_device_name(
                image_id=params.get('ami_id_db_scylla').split()[0],
                region_name=regions[0])
            db_info['device_mappings'] = [{
                "DeviceName": root_device,
                "Ebs": {
                    "VolumeSize": db_info['disk_size'],
                    "VolumeType": "gp3"
                }
            }]
        else:
            db_info['device_mappings'] = []

        additional_ebs_volumes_num = params.get("data_volume_disk_num")
        if additional_ebs_volumes_num > 0:
            ebs_info = {"DeleteOnTermination": True,
                        "VolumeType": params.get("data_volume_disk_type"),
                        "VolumeSize": params.get('data_volume_disk_size')}

            if ebs_info['VolumeType'] in ['io1', 'io2', 'gp3']:
                ebs_info["Iops"] = params.get('data_volume_disk_iops')
            if ebs_info["VolumeType"] == "gp3":
                ebs_info["Throughput"] = params.get("data_volume_disk_throughput")

            for disk_char in "fghijklmnop"[:additional_ebs_volumes_num]:
                ebs_volume = {
                    "DeviceName": f"/dev/xvd{disk_char}",
                    "Ebs": ebs_info
                }

                db_info['device_mappings'].append(ebs_volume)

        LOGGER.debug(db_info['device_mappings'])
    return db_info


class EC2NetworkConfiguration:
    def __init__(self, regions: list[str], availability_zones: list[str], params: dict):
        self.regions = regions
        self.availability_zones = availability_zones
        self.params = params
        self.network_interfaces_count = network_interfaces_count(params)

    @property
    def subnets_per_region(self) -> dict:
        ec2_subnet_ids = {}
        for region in self.regions:
            aws_region = AwsRegion(region_name=region)
            ec2_subnet_ids[region] = {}
            for availability_zone in self.availability_zones:
                ec2_subnet_ids[region][availability_zone] = self.subnets_per_availability_zone(region=region,
                                                                                               aws_region=aws_region,
                                                                                               availability_zone=availability_zone)
        LOGGER.debug("All ec2 subnet ids: %s", ec2_subnet_ids)
        return ec2_subnet_ids

    @property
    def subnets(self) -> List[list]:
        # Example:
        # One region, 2 avalability zones, 2 network interfaces
        # subnets_per_region = {'eu-central-1': {'a': ['subnet-085db77751694e2a6', 'subnet-03d8900174e00a73d'],
        #                                        'b': ['subnet-084b1d12f9974e61f', 'subnet-094ed7c7c3bddd441']}}
        # Will return:
        #  [[['subnet-085db77751694e2a6', 'subnet-03d8900174e00a73d'], ['subnet-084b1d12f9974e61f', 'subnet-094ed7c7c3bddd441']]]
        return [list(azs.values()) for azs in self.subnets_per_region.values()]

    def subnets_per_availability_zone(self, region: str, aws_region: AwsRegion, availability_zone: str) -> list:
        region_subnets = []
        for index in range(self.network_interfaces_count):
            sct_subnet = aws_region.sct_subnet(region_az=region + availability_zone, subnet_index=index)
            assert sct_subnet, f"No SCT subnet configured for {region}! Run 'hydra prepare-aws-region'"
            region_subnets.append(sct_subnet.subnet_id)

        return region_subnets

    @property
    def security_groups(self):
        ec2_security_group_ids = []
        for region in self.regions:
            ec2_security_group_ids.append(self.region_security_groups(region=region,
                                                                      aws_region=AwsRegion(region_name=region)))
        return ec2_security_group_ids

    def region_security_groups(self, region: str, aws_region: AwsRegion):
        security_groups = []
        sct_sg = aws_region.sct_security_group
        assert sct_sg, f"No SCT security group configured for {region}! Run 'hydra prepare-aws-region'"
        security_groups.append(sct_sg.group_id)

        if ssh_connection_ip_type(self.params) == 'public':
            test_config = TestConfig()
            test_id = test_config.test_id()

            test_sg = aws_region.provide_sct_test_security_group(test_id)
            security_groups.append(test_sg.group_id)
        return security_groups


def get_common_params(params: dict, regions: List, credentials: List, services: List, availability_zone: str = None) -> dict:
    availability_zones = [availability_zone] if availability_zone else params.get('availability_zone').split(',')
    ec2_network_configuration = EC2NetworkConfiguration(
        regions=regions, availability_zones=availability_zones, params=params)
    return dict(ec2_security_group_ids=ec2_network_configuration.security_groups,
                ec2_subnet_id=ec2_network_configuration.subnets,
                services=services,
                credentials=credentials,
                user_prefix=params.get('user_prefix'),
                params=params,
                )


def get_ec2_network_configuration(regions: list[str], availability_zones: list[str], params: dict):
    ec2_security_group_ids = []
    ec2_subnet_ids = []
    for region in regions:
        region_subnets = []
        ec2_subnet_ids.append(region_subnets)
        aws_region = AwsRegion(region_name=region)
        for availability_zone in availability_zones:
            sct_subnet = aws_region.sct_subnet(region_az=region + availability_zone, subnet_index=0)
            assert sct_subnet, f"No SCT subnet configured for {region}! Run 'hydra prepare-aws-region'"
            region_subnets.append(sct_subnet.subnet_id)

        security_groups = []
        sct_sg = aws_region.sct_security_group
        assert sct_sg, f"No SCT security group configured for {region}! Run 'hydra prepare-aws-region'"
        security_groups.append(sct_sg.group_id)

        if ssh_connection_ip_type(params) == 'public':
            test_config = TestConfig()
            test_id = test_config.test_id()

            test_sg = aws_region.provide_sct_test_security_group(test_id)
            security_groups.append(test_sg.group_id)

        ec2_security_group_ids.append(security_groups)

    return ec2_security_group_ids, ec2_subnet_ids


def get_ec2_services(regions):
    services = []
    for region in regions:
        session = boto3.session.Session(region_name=region)
        service = session.resource('ec2')
        services.append(service)
    return services


def tags_as_ec2_tags(tags: Dict[str, str]) -> List[Dict[str, str]]:
    return [{"Key": key, "Value": value} for key, value in tags.items()]


class PublicIpNotReady(Exception):
    pass


@retrying(n=90, sleep_time=10, allowed_exceptions=(PublicIpNotReady,),
          message="Waiting for instance to get public ip")
def ec2_instance_wait_public_ip(instance):
    instance.reload()
    if instance.public_ip_address is None:
        raise PublicIpNotReady(instance)
    LOGGER.debug("[%s] Got public ip: %s", instance, instance.public_ip_address)


def ec2_ami_get_root_device_name(image_id, region_name):
    ec2_resource = boto3.resource('ec2', region_name)

    for client in (ec2_resource, get_scylla_images_ec2_resource(region_name=region_name)):
        try:
            image = client.Image(image_id)
            if image.root_device_name:
                return image.root_device_name
        except (TypeError, AttributeError, ClientError):
            pass
    raise AssertionError(f"Image '{image_id}' details not found in '{region_name}'")


@functools.cache
def get_arch_from_instance_type(instance_type: str, region_name: str) -> AwsArchType:
    arch = 'x86_64'
    if instance_type:
        client: EC2Client = boto3.client('ec2', region_name=region_name)
        instance_type_info = client.describe_instance_types(InstanceTypes=[instance_type])

        try:
            arch = instance_type_info['InstanceTypes'][0].get('ProcessorInfo', {}).get('SupportedArchitectures')[0]
        except (IndexError, KeyError):
            pass
    return arch


def get_scylla_images_ec2_resource(region_name: str) -> EC2ServiceResource:
    session = boto3.Session()
    sts = session.client("sts", region_name=region_name)
    role_info = KeyStore().get_json('aws_images_role.json')
    response = sts.assume_role(
        RoleArn=role_info['role_arn'],
        RoleSessionName=role_info['role_session_name'],
    )

    new_session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                                aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                aws_session_token=response['Credentials']['SessionToken'])

    return new_session.resource("ec2", region_name=region_name)


def get_scylla_images_ec2_client(region_name: str) -> EC2Client:
    """
    Assume a role and create a new EC2 client session for the specified region.

    Args:
        region_name (str): The AWS region name.

    Returns:
        EC2Client: A boto3 EC2 client session with the assumed role credentials.

    Raises:
        ClientError: If there is an error assuming the role or creating the session.
    """
    session = boto3.Session()
    sts = session.client("sts", region_name=region_name)
    role_info = KeyStore().get_json('aws_images_role.json')
    response = sts.assume_role(
        RoleArn=role_info['role_arn'],
        RoleSessionName=role_info['role_session_name'],
    )

    new_session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                                aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                                aws_session_token=response['Credentials']['SessionToken'])

    return new_session.client("ec2", region_name=region_name)


def get_ssm_ami(parameter: str, region_name) -> str:
    """
    get AMIs from SSM parameters

    examples:
    - '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id'
    - '/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2'

    Ref: https://discourse.ubuntu.com/t/finding-ubuntu-images-with-the-aws-ssm-parameter-store/15507
    """
    client = boto3.client('ssm', region_name=region_name)
    value = client.get_parameter(Name=parameter)
    return value['Parameter']['Value']


def is_using_aws_mock() -> bool:
    """
    check if the aws mock host is available or not
    """

    try:
        socket.gethostbyname("aws-mock.itself")
        return True
    except socket.gaierror:
        return False


def get_by_owner_ami(parameter: str, region_name) -> str:
    """
    get AMIs by owner/architecture/name filter

    for case, we have only  the owner id, and it's not publish
    in marketplace, hence we can't use the ssm parameter store.

    example:
    - '131827586825/x86_64/OL8.*' - it's oracle linux 8, and we want the latest one

    """
    ec2_resource: EC2ServiceResource = boto3.resource('ec2', region_name=region_name)

    owner, arch, name_filter = parameter.split('/')
    assert arch in get_args(AwsArchType)

    images = ec2_resource.images.filter(
        Owners=[owner],
        Filters=[
            {'Name': 'name', 'Values': [name_filter]},
            {'Name': 'architecture', 'Values': [arch]},
        ],
    )
    images = sorted(images, key=lambda x: x.creation_date, reverse=True)
    LOGGER.debug(f'found image "{images[0].name}" - {images[0].id}')
    return images[0].id


def aws_check_instance_type_supported(instance_type: str, region_name: str) -> bool:
    """
    check if the instance type is supported in the region
    """
    client: EC2Client = boto3.client('ec2', region_name=region_name)
    try:
        client.describe_instance_types(InstanceTypes=[instance_type])
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'InvalidInstanceType':
            return False
        raise
    return True


class AwsIAM:
    def __init__(self):
        self.account_id = boto3.client("sts").get_caller_identity()["Account"]
        self._policy_format = f"arn:aws:iam::{self.account_id}:policy/" + "{}"
        self._role_format = f"arn:aws:iam::{self.account_id}:role/" + "{}"

    @cached_property
    def iam_client(self):
        return boto3.client('iam')

    def get_full_arn(self, policy_name, policy_type):
        if policy_type == "policy":
            arn_format = self._policy_format
        elif policy_type == "role":
            arn_format = self._role_format
        else:
            raise TypeError(f"Unsupported policy type: {policy_type}")
        return arn_format.format(policy_name)

    def get_policy_by_name_prefix(self, prefix: str) -> list[str]:
        policies = []
        paginator = self.iam_client.get_paginator('list_policies')
        for page in paginator.paginate():
            for policy in page['Policies']:
                policy_name = policy['PolicyName']
                if policy_name.startswith(prefix):
                    policies.append(self.get_full_arn(policy_name=policy_name, policy_type="policy"))
        return policies

    def add_resource_to_iam_policy(self, policy_arn: str, resource_to_add: str) -> None:
        policy = self.iam_client.get_policy(PolicyArn=policy_arn)
        policy_version = self.iam_client.get_policy_version(
            PolicyArn=policy_arn,
            VersionId=policy['Policy']['DefaultVersionId']
        )
        policy_document = policy_version['PolicyVersion']['Document']

        for statement in policy_document['Statement']:
            if statement['Effect'] == 'Allow':
                if "/*" in statement['Resource'][0]:
                    statement['Resource'].append(resource_to_add + "/*")
                else:
                    statement['Resource'].append(resource_to_add)

        self.iam_client.create_policy_version(
            PolicyArn=policy_arn,
            PolicyDocument=json.dumps(policy_document),
            SetAsDefault=True
        )

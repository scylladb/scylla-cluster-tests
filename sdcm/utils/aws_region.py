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

import logging
from ipaddress import ip_network
from functools import cached_property, cache

import boto3
import botocore
from mypy_boto3_ec2 import EC2Client, EC2ServiceResource

from sdcm.keystore import KeyStore
from sdcm.utils.decorators import retrying
from sdcm.utils.get_username import get_username

LOGGER = logging.getLogger(__name__)


class AwsRegion:
    SCT_VPC_NAME = "SCT-2-vpc"
    SCT_VPC_CIDR_TMPL = "10.{}.0.0/16"
    SCT_SECURITY_GROUP_NAME = "SCT-2-sg"
    SCT_TEST_SECURITY_GROUP_NAME_TMPL = "SCT-2-sg-{}"
    SCT_SUBNET_NAME = "SCT-2-subnet-{availability_zone}{subnet_index}"
    SCT_INTERNET_GATEWAY_NAME = "SCT-2-igw"
    SCT_ROUTE_TABLE_NAME = "SCT-2-rt{index}"
    SCT_KEY_PAIR_NAME = "scylla_test_id_ed25519"  # TODO: change legacy name to sct-keypair-aws
    SCT_SSH_GROUP_NAME = 'SCT-ssh-sg'
    SCT_SUBNET_PER_AZ = 2  # how many subnets to configure in each region.
    SCT_NODES_ROLE_ARN = "qa-scylla-manager-backup-role"

    def __init__(self, region_name):
        self.region_name = region_name
        self.client: EC2Client = boto3.client("ec2", region_name=region_name)
        self.resource: EC2ServiceResource = boto3.resource("ec2", region_name=region_name)

        # cause import straight from common create cyclic dependency
        from sdcm.utils.common import all_aws_regions  # noqa: PLC0415

        region_index = all_aws_regions(cached=True).index(self.region_name)
        cidr = ip_network(self.SCT_VPC_CIDR_TMPL.format(region_index))
        self.vpc_ipv4_cidr = cidr

    @property
    def sct_vpc(self) -> EC2ServiceResource.Vpc:
        vpcs = self.client.describe_vpcs(Filters=[{"Name": "tag:Name", "Values": [self.SCT_VPC_NAME]}])
        LOGGER.debug("Found VPCs: %s", vpcs)
        existing_vpcs = vpcs.get("Vpcs", [])
        if len(existing_vpcs) == 0:
            return None
        assert len(existing_vpcs) == 1, \
            f"More than 1 VPC with {self.SCT_VPC_NAME} found in {self.region_name}: {existing_vpcs}"
        return self.resource.Vpc(existing_vpcs[0]["VpcId"])

    def create_sct_vpc(self):
        LOGGER.info("Going to create VPC...")
        if self.sct_vpc:
            LOGGER.warning("VPC '%s' already exists!  Id: '%s'.", self.SCT_VPC_NAME, self.sct_vpc.vpc_id)
            return self.sct_vpc.vpc_id
        else:
            result = self.client.create_vpc(CidrBlock=str(self.vpc_ipv4_cidr), AmazonProvidedIpv6CidrBlock=True)
            vpc_id = result["Vpc"]["VpcId"]
            vpc = self.resource.Vpc(vpc_id)
            vpc.modify_attribute(EnableDnsHostnames={"Value": True})
            vpc.create_tags(Tags=[{"Key": "Name", "Value": self.SCT_VPC_NAME}])
            LOGGER.info("'%s' with id '%s' created. Waiting until it becomes available...", self.SCT_VPC_NAME, vpc_id)
            vpc.wait_until_available()
            return vpc_id

    @cached_property
    def availability_zones(self):
        response = self.client.describe_availability_zones()
        return [zone["ZoneName"]for zone in response['AvailabilityZones'] if zone["State"] == "available"]

    @cache
    def get_availability_zones_for_instance_type(self, instance_type):
        response = self.client.describe_instance_type_offerings(
            LocationType='availability-zone',
            Filters=[
                {
                    'Name': 'instance-type',
                    'Values': [
                        instance_type,
                    ]
                },
            ]
        )
        return [offering['Location'] for offering in response['InstanceTypeOfferings']]

    @cached_property
    def vpc_ipv6_cidr(self):
        return ip_network(self.sct_vpc.ipv6_cidr_block_association_set[0]["Ipv6CidrBlock"])

    def az_subnet_name(self, region_az, subnet_index):
        # To support existing VPC/subnet names convention, ignore the index number for first subnet
        return self.SCT_SUBNET_NAME.format(availability_zone=region_az,
                                           subnet_index="" if not subnet_index else f"-{subnet_index}")

    def sct_subnet(self, region_az, subnet_index: int = None) -> EC2ServiceResource.Subnet:
        @retrying(n=5, message="Wait for subnet became available...")
        def wait_for_subnet_available():
            assert existing_subnets[0]['State'] == 'available', \
                f"State of {subnet_name} is not available. Fix it (delete or make it available) and re-run"

        subnet_name = self.az_subnet_name(region_az, subnet_index=subnet_index)
        subnets = self.client.describe_subnets(Filters=[{"Name": "tag:Name", "Values": [subnet_name]}])
        LOGGER.debug("Found Subnets: %s", subnets)
        existing_subnets = subnets.get("Subnets", [])
        if len(existing_subnets) == 0:
            return None
        assert len(existing_subnets) == 1, \
            f"More than 1 Subnet with {subnet_name} found in {self.region_name}: {existing_subnets}!"
        wait_for_subnet_available()
        return self.resource.Subnet(existing_subnets[0]["SubnetId"])

    def create_sct_subnet(self, region_az, ipv4_cidr, ipv6_cidr, subnet_index: int = None):
        LOGGER.info("Creating subnet(s) for %s...", region_az)
        subnet_name = self.az_subnet_name(region_az, subnet_index=subnet_index)
        if subnet := self.sct_subnet(region_az, subnet_index=subnet_index):
            subnet_id = subnet.subnet_id
            LOGGER.warning("Subnet '%s' already exists!  Id: '%s'.", subnet_name, subnet_id)
            return

        result = self.client.create_subnet(CidrBlock=str(ipv4_cidr), Ipv6CidrBlock=str(ipv6_cidr),
                                           VpcId=self.sct_vpc.vpc_id, AvailabilityZone=region_az)
        subnet_id = result["Subnet"]["SubnetId"]
        subnet = self.resource.Subnet(subnet_id)
        subnet.create_tags(Tags=[{"Key": "Name", "Value": subnet_name}])
        LOGGER.info("Configuring to automatically assign public IPv4 and IPv6 addresses...")
        self.client.modify_subnet_attribute(
            MapPublicIpOnLaunch={"Value": True},
            SubnetId=subnet_id
        )
        # for some reason boto3 throws error when both AssignIpv6AddressOnCreation and MapPublicIpOnLaunch are used
        self.client.modify_subnet_attribute(
            AssignIpv6AddressOnCreation={"Value": True},
            SubnetId=subnet_id
        )
        LOGGER.info("'%s' with id '%s' created.", subnet_name, subnet_id)

    def create_sct_subnets(self):
        ipv4_cidrs_iter = self.vpc_ipv4_cidr.subnets(6)
        ipv6_cidrs_iter = self.vpc_ipv6_cidr.subnets(8)
        # First subnet in each availability zone already exists.
        # It means that first range of CIDRs are in used.
        # For example:
        #    There are 2 availability zone.
        #    Zone A with IPv4 CIDR "10.0.0.0"
        #    Zone B with IPv4 CIDR "10.0.4.0"
        #    ipv4_cidrs_iter = ["10.0.0.0", "10.0.4.0", "10.0.8.0", "10.0.12.0"]
        # If we try to create sequentially [A-1, A-2, B-1, B2], the cidr "10.0.4.0" will be taken for A-2 and fails,
        # because of this cidr is in use by B-1
        # To prevent this failure, first the primary subnets in all AZ will be created and then the secondary subnets in all AZ
        for range_index in range(self.SCT_SUBNET_PER_AZ):
            for az_name in self.availability_zones:
                self.create_sct_subnet(region_az=az_name, ipv4_cidr=next(ipv4_cidrs_iter), ipv6_cidr=next(ipv6_cidrs_iter),
                                       subnet_index=range_index)
                self.create_sct_subnet_route_table(subnet_name=self.az_subnet_name(region_az=az_name, subnet_index=range_index),
                                                   index=range_index)
                self.associate_subnet_with_route_table(region_az=az_name, index=range_index)

    def associate_subnet_with_route_table(self, region_az: str, index: int = None):
        subnet = self.sct_subnet(region_az, subnet_index=index)
        sct_route_table = self.sct_route_table(index=index)
        response = self.client.describe_route_tables(
            Filters=[{'Name': 'association.subnet-id', 'Values': [subnet.subnet_id]}])

        # If subnet is not associated with route table or associated with main route table
        if not (associated_route_table := response['RouteTables']):
            self.client.associate_route_table(RouteTableId=sct_route_table.route_table_id, SubnetId=subnet.subnet_id)
            LOGGER.info("Subnet '%s' has been associated with '%s' route table.",
                        subnet.subnet_id, sct_route_table.route_table_id)
            return

        associated_route_table_id = associated_route_table[0].get('RouteTableId')
        # If subnet is associated with route table but not that was created for this subnet
        if associated_route_table_id != sct_route_table.route_table_id:
            self.client.replace_route_table_association(
                AssociationId=associated_route_table_id, RouteTableId=sct_route_table.route_table_id)
            LOGGER.info("Subnet '%s' has been associated with '%s' route table.",
                        subnet.subnet_id, sct_route_table.route_table_id)
        # If subnet is associated with appropriate route table
        else:
            LOGGER.info("Subnet '%s' is already associated with '%s' route table.",
                        subnet.subnet_id, sct_route_table.route_table_id)

    @property
    def sct_internet_gateway(self) -> EC2ServiceResource.InternetGateway:
        igws = self.client.describe_internet_gateways(Filters=[{"Name": "tag:Name",
                                                                "Values": [self.SCT_INTERNET_GATEWAY_NAME]}])
        LOGGER.debug("Found Internet gateways: %s", igws)
        existing_igws = igws.get("InternetGateways", [])
        if len(existing_igws) == 0:
            return None
        assert len(existing_igws) == 1, \
            f"More than 1 Internet Gateway with {self.SCT_INTERNET_GATEWAY_NAME} found " \
            f"in {self.region_name}: {existing_igws}!"
        return self.resource.InternetGateway(existing_igws[0]["InternetGatewayId"])

    def create_sct_internet_gateway(self):
        LOGGER.info("Creating Internet Gateway..")
        if self.sct_internet_gateway:
            LOGGER.warning("Internet Gateway '%s' already exists! Id: '%s'.",
                           self.SCT_INTERNET_GATEWAY_NAME, self.sct_internet_gateway.internet_gateway_id)
        else:
            result = self.client.create_internet_gateway()
            igw_id = result["InternetGateway"]["InternetGatewayId"]
            igw = self.resource.InternetGateway(igw_id)
            igw.create_tags(Tags=[{"Key": "Name", "Value": self.SCT_INTERNET_GATEWAY_NAME}])
            LOGGER.info("'%s' with id '%s' created. Attaching to '%s'",
                        self.SCT_INTERNET_GATEWAY_NAME, igw_id, self.sct_vpc.vpc_id)
            igw.attach_to_vpc(VpcId=self.sct_vpc.vpc_id)

    def sct_route_table_name(self, index: int = None):
        """
        :param index: suffix of the route table name
        """
        # For first subnet the main rout table will be used, so ignore the index number
        return self.SCT_ROUTE_TABLE_NAME.format(index="" if not index else f"-{index}")

    def sct_route_table(self, index: int = None) -> EC2ServiceResource.RouteTable:
        sct_route_table_name = self.sct_route_table_name(index=index)
        route_tables = self.client.describe_route_tables(Filters=[{"Name": "tag:Name",
                                                                   "Values": [sct_route_table_name]}])
        LOGGER.debug("Found Route Tables: %s", route_tables)
        existing_rts = route_tables.get("RouteTables", [])
        if len(existing_rts) == 0:
            return None
        assert len(existing_rts) == 1, \
            f"More than 1 Route Table with {sct_route_table_name} found in {self.region_name}: {existing_rts}!"
        return self.resource.RouteTable(existing_rts[0]["RouteTableId"])

    @cached_property
    def sct_route_tables(self) -> list:
        """Get all route tables associated with the SCT VPC"""
        route_tables = set()
        main_route_table = self.sct_route_table(index=None)
        if main_route_table:
            route_tables.add(main_route_table)

        route_tables.update(
            rt for index in range(self.SCT_SUBNET_PER_AZ) if (rt := self.sct_route_table(index=index))
        )

        LOGGER.debug("Discovered %d SCT route tables in %s", len(route_tables), self.region_name)
        return list(route_tables)

    def create_sct_subnet_route_table(self, subnet_name: str, index: int = None):
        subnet_route_table = self.sct_route_table_name(index=index)
        LOGGER.info("Configuring SCT subnet Route Table %s for subnet %s...", subnet_route_table, subnet_name)
        if sct_route_table := self.sct_route_table(index=index):
            LOGGER.warning("Route Table '%s' already exists! Id: '%s'.",
                           subnet_route_table, sct_route_table.route_table_id)
            return

        self.client.create_route_table(DryRun=False,
                                       VpcId=self.sct_vpc.vpc_id,
                                       TagSpecifications=[
                                           {'ResourceType': "route-table",
                                            'Tags': [{'Key': 'Name',
                                                      'Value': subnet_route_table
                                                      }]
                                            }],
                                       )
        LOGGER.info("Setting routing of all outbound traffic via Internet Gateway...")
        route_table = self.sct_route_table(index=index)
        route_table.create_route(DestinationCidrBlock="0.0.0.0/0",
                                 GatewayId=self.sct_internet_gateway.internet_gateway_id)
        route_table.create_route(DestinationIpv6CidrBlock="::/0",
                                 GatewayId=self.sct_internet_gateway.internet_gateway_id)

    def configure_sct_route_table(self, index: int = None):
        # add route to Internet: 0.0.0.0/0 -> igw
        LOGGER.info("Configuring main Route Table...")
        if sct_route_table := self.sct_route_table(index=index):
            LOGGER.warning("Route Table '%s' already exists! Id: '%s'.",
                           self.sct_route_table_name(index=index), sct_route_table.route_table_id)
        else:
            route_tables = list(self.sct_vpc.route_tables.all())
            assert len(route_tables) == 1, f"Only one main route table should exist for {self.SCT_VPC_NAME}. " \
                f"Found {len(route_tables)}!"
            route_table: EC2ServiceResource.RouteTable = route_tables[0]

            route_table.create_tags(Tags=[{"Key": "Name", "Value": self.sct_route_table_name(index=index)}])
            LOGGER.info("Setting routing of all outbound traffic via Internet Gateway...")
            route_table.create_route(DestinationCidrBlock="0.0.0.0/0",
                                     GatewayId=self.sct_internet_gateway.internet_gateway_id)
            route_table.create_route(DestinationIpv6CidrBlock="::/0",
                                     GatewayId=self.sct_internet_gateway.internet_gateway_id)

    @property
    def sct_security_group(self) -> EC2ServiceResource.SecurityGroup:
        security_groups = self.client.describe_security_groups(Filters=[{"Name": "tag:Name",
                                                                         "Values": [self.SCT_SECURITY_GROUP_NAME]}])
        LOGGER.debug("Found Security Groups: %s", security_groups)
        existing_sgs = security_groups.get("SecurityGroups", [])
        if len(existing_sgs) == 0:
            return None
        assert len(existing_sgs) == 1, \
            f"More than 1 Security group with {self.SCT_SECURITY_GROUP_NAME} found " \
            f"in {self.region_name}: {existing_sgs}!"
        return self.resource.SecurityGroup(existing_sgs[0]["GroupId"])

    def create_sct_security_group(self):
        """

        Custom TCP	TCP	9093	0.0.0.0/0	Allow alert manager for all
        Custom TCP	TCP	9093	::/0	Allow alert manager for all

        """
        LOGGER.info("Creating Security Group...")
        if self.sct_security_group:
            LOGGER.warning("Security Group '%s' already exists! Id: '%s'.",
                           self.SCT_SECURITY_GROUP_NAME, self.sct_security_group.group_id)
        else:
            result = self.client.create_security_group(Description='Security group that is used by SCT',
                                                       GroupName=self.SCT_SECURITY_GROUP_NAME,
                                                       VpcId=self.sct_vpc.vpc_id)
            sg_id = result["GroupId"]
            security_group = self.resource.SecurityGroup(sg_id)
            security_group.create_tags(Tags=[{"Key": "Name", "Value": self.SCT_SECURITY_GROUP_NAME}])
            LOGGER.info("'%s' with id '%s' created. ", self.SCT_SECURITY_GROUP_NAME, self.sct_security_group.group_id)
            LOGGER.info("Creating common ingress rules...")
            security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "UserIdGroupPairs": [
                            {
                                "Description": "Allow ALL traffic inside the Security group",
                                "GroupId": sg_id,
                                "UserId": security_group.owner_id
                            }
                        ]
                    },
                ]
            )

    def get_sct_test_security_group(self, test_id) -> EC2ServiceResource.SecurityGroup:
        name = self.SCT_TEST_SECURITY_GROUP_NAME_TMPL.format(test_id)
        security_groups = self.client.describe_security_groups(Filters=[{"Name": "tag:Name",
                                                                         "Values": [name]},
                                                                        {"Name": "tag:TestId",
                                                                         "Values": [test_id]
                                                                         }])
        LOGGER.debug("Found Security Groups: %s", security_groups)
        existing_sgs = security_groups.get("SecurityGroups", [])
        if len(existing_sgs) == 0:
            return None
        assert len(existing_sgs) == 1, \
            f"More than 1 Security group with {name} found " \
            f"in {self.region_name}: {existing_sgs}!"
        return self.resource.SecurityGroup(existing_sgs[0]["GroupId"])

    def provide_sct_test_security_group(self, test_id: str):
        """
        Create a per test security group, that open all known ports
        publicly.
        should be used mainly for debugging, or for cases that must
        use public communication to/between nodes.
        """
        LOGGER.info("Creating Security Group for test %s...", test_id)
        name = self.SCT_TEST_SECURITY_GROUP_NAME_TMPL.format(test_id)
        if security_group := self.get_sct_test_security_group(test_id=test_id):
            LOGGER.warning("Security Group '%s' already exists! Id: '%s'.",
                           name, self.sct_security_group.group_id)
        else:

            result = self.client.create_security_group(Description=f'Security group that is used by test {test_id}',
                                                       GroupName=name,
                                                       VpcId=self.sct_vpc.vpc_id)
            sg_id = result["GroupId"]
            security_group = self.resource.SecurityGroup(sg_id)
            security_group.create_tags(Tags=[{"Key": "Name",
                                              "Value": name},
                                             {"Key": "RunByUser", "Value": get_username()},
                                             {"Key": "CreatedBy", "Value": "SCT"},
                                             {"Key": "TestId", "Value": test_id}])
            LOGGER.debug("'%s' with id '%s' created. ", name, self.sct_security_group.group_id)
            LOGGER.debug("Creating common ingress rules...")
            security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpProtocol": "tcp",
                        "IpRanges": [
                            {'CidrIp': '0.0.0.0/0', 'Description': 'SSH connectivity to the instances'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'SSH connectivity to the instances'}]
                    },
                    {
                        "FromPort": 3000,
                        "ToPort": 3000,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Grafana for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow Grafana for ALL'}]
                    },
                    {
                        "FromPort": 9042,
                        "ToPort": 9042,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow CQL for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow CQL for ALL'}]
                    },
                    {
                        "FromPort": 19042,
                        "ToPort": 19042,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow shard-aware CQL for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow shard-aware CQL for ALL'}]
                    },
                    {
                        "FromPort": 9142,
                        "ToPort": 9142,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow SSL CQL for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow SSL CQL for ALL'}]
                    },
                    {
                        "FromPort": 19142,
                        "ToPort": 19142,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow shard-aware SSL CQL for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow shard-aware SSL CQL for ALL'}]
                    },
                    {
                        "FromPort": 9100,
                        "ToPort": 9100,
                        "IpProtocol": "tcp",
                        "IpRanges": [
                            {'CidrIp': '0.0.0.0/0', 'Description': 'Allow node_exporter on Db nodes for ALL'}],
                        "Ipv6Ranges": [
                            {'CidrIpv6': '::/0', 'Description': 'Allow node_exporter on Db nodes for ALL'}]
                    },
                    {
                        "FromPort": 8080,
                        "ToPort": 8080,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Alternator for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow Alternator for ALL'}]
                    },
                    {
                        "FromPort": 9090,
                        "ToPort": 9090,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Prometheus for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow  Prometheus for ALL'}]
                    },
                    {
                        "FromPort": 9093,
                        "ToPort": 9093,
                        "IpProtocol": "tcp",
                        "IpRanges": [
                            {'CidrIp': '0.0.0.0/0', 'Description': 'Allow Prometheus Alert Manager For All'}],
                        "Ipv6Ranges": [
                            {'CidrIpv6': '::/0', 'Description': 'Allow Prometheus Alert Manager For All'}]
                    },
                    {
                        "FromPort": 9180,
                        "ToPort": 9180,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Prometheus API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow Prometheus API for ALL'}]
                    },
                    {
                        "FromPort": 7000,
                        "ToPort": 7000,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Inter-node communication (RPC) for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Inter-node communication (RPC) for ALL'}]
                    },
                    {
                        "FromPort": 7001,
                        "ToPort": 7001,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow SSL inter-node communication (RPC) for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow SSL inter-node communication (RPC) for ALL'}]
                    },
                    {
                        "FromPort": 7199,
                        "ToPort": 7199,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow JMX management for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow JMX management for ALL'}]
                    },
                    {
                        "FromPort": 10001,
                        "ToPort": 10001,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager Agent REST API  for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager Agent REST API for ALL'}]
                    },
                    {
                        "FromPort": 56090,
                        "ToPort": 56090,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager Agent version 2.1 Prometheus API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager Agent version 2.1 Prometheus API for ALL'}]
                    },
                    {
                        "FromPort": 15000,
                        "ToPort": 15000,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Vector log transport for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow Vector log transport for ALL'}]
                    },
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [{'CidrIp': '172.0.0.0/11',
                                      'Description': 'Allow traffic from Scylla Cloud lab while VPC peering for ALL'}],
                    },
                    {
                        "FromPort": 5080,
                        "ToPort": 5080,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager HTTP API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager HTTP API for ALL'}]
                    },
                    {
                        "FromPort": 5443,
                        "ToPort": 5443,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager HTTPS API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager HTTPS API for ALL'}]
                    },
                    {
                        "FromPort": 5090,
                        "ToPort": 5090,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager Agent Prometheus API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager Agent Prometheus API for ALL'}]
                    },
                    {
                        "FromPort": 5112,
                        "ToPort": 5112,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0',
                                      'Description': 'Allow Scylla Manager pprof Debug For ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0',
                                        'Description': 'Allow Scylla Manager pprof Debug For ALL'}]
                    },
                    {
                        "FromPort": 6080,
                        "ToPort": 6080,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'Allow Vector Store REST API for ALL'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'Allow Vector Store REST API for ALL'}]
                    }
                ]
            )
        return security_group

    @property
    def sct_ssh_security_group(self) -> EC2ServiceResource.SecurityGroup:
        security_groups = self.client.describe_security_groups(Filters=[{"Name": "tag:Name",
                                                                         "Values": [self.SCT_SSH_GROUP_NAME]}])
        existing_sgs = security_groups.get("SecurityGroups", [])
        if len(existing_sgs) == 0:
            return None
        assert len(existing_sgs) == 1, \
            f"More than 1 Security group with {self.SCT_SSH_GROUP_NAME} found " \
            f"in {self.region_name}: {existing_sgs}!"
        return self.resource.SecurityGroup(existing_sgs[0]["GroupId"])

    def create_sct_ssh_security_group(self):
        """

        Custom TCP	TCP	9093	0.0.0.0/0	Allow alert manager for all
        Custom TCP	TCP	9093	::/0	Allow alert manager for all

        """
        LOGGER.info("Creating Security Group...")
        if self.sct_ssh_security_group:
            LOGGER.warning("Security Group '%s' already exists! Id: '%s'.",
                           self.SCT_SSH_GROUP_NAME, self.sct_ssh_security_group.group_id)
        else:
            result = self.client.create_security_group(Description='Security group for builders '
                                                       'that is used by SCT',
                                                       GroupName=self.SCT_SSH_GROUP_NAME,
                                                       VpcId=self.sct_vpc.vpc_id)
            sg_id = result["GroupId"]
            security_group = self.resource.SecurityGroup(sg_id)
            security_group.create_tags(Tags=[{"Key": "Name", "Value": self.SCT_SSH_GROUP_NAME}])
            LOGGER.info("'%s' with id '%s' created. ", self.SCT_SSH_GROUP_NAME,
                        self.sct_ssh_security_group.group_id)
            LOGGER.info("Creating common ingress rules...")
            security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "FromPort": 22,
                        "ToPort": 22,
                        "IpProtocol": "tcp",
                        "IpRanges": [{'CidrIp': '0.0.0.0/0', 'Description': 'SSH connectivity to the builders'}],
                        "Ipv6Ranges": [{'CidrIpv6': '::/0', 'Description': 'SSH connectivity to the builders'}]
                    }]
            )

    @property
    def sct_keypair(self):
        try:
            key_pairs = self.client.describe_key_pairs(KeyNames=[self.SCT_KEY_PAIR_NAME])
        except botocore.exceptions.ClientError as ex:
            if "InvalidKeyPair.NotFound" in str(ex):
                return None
            raise
        LOGGER.debug("Found key pairs: %s", key_pairs)
        existing_key_pairs = key_pairs.get("KeyPairs", [])
        assert len(existing_key_pairs) == 1, \
            f"More than 1 Key Pair with {self.SCT_KEY_PAIR_NAME} found " \
            f"in {self.region_name}: {existing_key_pairs}!"
        return self.resource.KeyPair(existing_key_pairs[0]["KeyName"])

    def create_sct_key_pair(self):
        LOGGER.info("Creating SCT Key Pair...")
        if self.sct_keypair:
            LOGGER.warning("SCT Key Pair already exists in '%s'!", self.region_name)
        else:
            ks = KeyStore()
            sct_key_pair = ks.get_ec2_ssh_key_pair()
            self.resource.import_key_pair(KeyName=self.SCT_KEY_PAIR_NAME,
                                          PublicKeyMaterial=sct_key_pair.public_key)
            LOGGER.info("SCT Key Pair created.")

    def get_vpc_peering_routes(self) -> list[str]:
        """Discover all VPC peering routes in SCT route tables"""
        peering_routes = []
        for route_table in self.sct_route_tables:
            routes = route_table.routes_attribute
            for route in routes:
                if route.get('VpcPeeringConnectionId') and route.get('DestinationCidrBlock'):
                    dest_cidr = route.get('DestinationCidrBlock')
                    if dest_cidr != '0.0.0.0/0' and dest_cidr not in peering_routes:
                        peering_routes.append(dest_cidr)

        LOGGER.debug("Discovered %s VPC peering routes in %s", peering_routes, self.region_name)
        return peering_routes

    @cached_property
    def ssm(self):
        return boto3.client("ssm", region_name=self.region_name)

    @cached_property
    def sts(self):
        return boto3.client("sts", region_name=self.region_name)

    def configure_ssm(self, role_name=SCT_NODES_ROLE_ARN):
        """Ensure that SSM agent can work in the region by adding necessary IAM role and instance profile"""

        # Replace with the actual ARN of your IAM role created in step 1
        # Example: service-role/AWSSystemsManagerDefaultEC2InstanceManagementRole
        # Note: The 'service-role/' prefix is crucial when setting the value.
        iam_role_for_dhmc = f"service-role/{role_name}"

        account_id = self.sts.get_caller_identity()["Account"]
        region = self.ssm.meta.region_name

        setting_id = f"arn:aws:ssm:{region}:{account_id}:servicesetting/ssm/managed-instance/default-ec2-instance-management-role"

        try:
            response = self.ssm.update_service_setting(SettingId=setting_id, SettingValue=iam_role_for_dhmc)
            LOGGER.info("Default Host Management Configuration updated successfully.")
            LOGGER.debug(response)
        except botocore.exceptions.ClientError as e:
            LOGGER.error(f"Error updating Default Host Management Configuration: {e}")

    def configure(self):
        LOGGER.info("Configuring '%s' region...", self.region_name)
        self.create_sct_vpc()
        self.create_sct_internet_gateway()
        self.configure_sct_route_table()
        self.create_sct_subnets()
        self.create_sct_security_group()
        self.create_sct_ssh_security_group()
        self.create_sct_key_pair()
        self.configure_ssm()
        LOGGER.info("Region configured successfully.")

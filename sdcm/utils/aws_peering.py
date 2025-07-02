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
# Copyright (c) 2022 ScyllaDB

import logging
import itertools

from botocore.exceptions import ClientError

from sdcm.utils.aws_region import AwsRegion
from sdcm.sct_config import SCTConfiguration

LOG = logging.getLogger(__name__)


class AwsVpcPeering:
    def __init__(self, regions=None):
        regions = regions or SCTConfiguration.aws_supported_regions
        self.regions = [AwsRegion(r) for r in regions]

    @staticmethod
    def peering_connection(origin_region, target_region):
        peering_name = f"{origin_region.SCT_VPC_NAME}-{origin_region.region_name}-{target_region.region_name}"

        peering_connection = origin_region.client.describe_vpc_peering_connections(
            Filters=[{"Name": "tag:Name", "Values": [peering_name]}])
        if origin_vpx := peering_connection.get('VpcPeeringConnections'):
            origin_vpx = origin_region.resource.VpcPeeringConnection(origin_vpx[0].get('VpcPeeringConnectionId'))
            LOG.info("peering for %s, created already", origin_region.region_name)
        else:
            res = origin_region.client.create_vpc_peering_connection(
                PeerVpcId=target_region.sct_vpc.vpc_id,
                VpcId=origin_region.sct_vpc.vpc_id,
                PeerRegion=target_region.region_name,
                TagSpecifications=[
                    {
                        'ResourceType': 'vpc-peering-connection',
                        'Tags': [
                            {
                                'Key': 'Name',
                                'Value': peering_name
                            },
                        ]
                    },
                ]
            )
            origin_vpx = origin_region.resource.VpcPeeringConnection(
                res.get('VpcPeeringConnection').get('VpcPeeringConnectionId'))

        target_vpx = target_region.resource.VpcPeeringConnection(origin_vpx.vpc_peering_connection_id)
        try:
            target_vpx.wait_until_exists()
            target_vpx.accept()
        except ClientError as ex:
            if 'OperationNotPermitted' in str(ex) or 'VpcPeeringConnectionAlreadyExists' in str(ex):
                LOG.info("peering already exist for %s-%s", origin_region.region_name, target_region.region_name)
                LOG.info(str(ex))
            else:
                raise

        return origin_vpx, target_vpx

    @staticmethod
    def configure_route_tables(origin_region, target_region, vpc_peering_id):
        for index in range(AwsRegion.SCT_SUBNET_PER_AZ):
            route_table = origin_region.sct_route_table(index=index)
            try:
                route_table.create_route(DestinationCidrBlock=str(target_region.vpc_ipv4_cidr),
                                         VpcPeeringConnectionId=vpc_peering_id)
            except ClientError as ex:
                if 'already exists' not in str(ex):
                    raise
            try:
                route_table.create_route(DestinationIpv6CidrBlock=str(target_region.vpc_ipv6_cidr),
                                         VpcPeeringConnectionId=vpc_peering_id)
            except ClientError as ex:
                if 'already exists' not in str(ex):
                    raise

    @staticmethod
    def open_security_group(origin_region, target_region):
        try:
            origin_region.sct_security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [{'CidrIp': str(target_region.vpc_ipv4_cidr),
                                      'Description': f'VPC-peering for {target_region.region_name}'}],
                        "Ipv6Ranges": [{'CidrIpv6': str(target_region.vpc_ipv6_cidr),
                                        'Description': f'VPC-peering for {target_region.region_name}'}]
                    },
                ])
        except ClientError as ex:
            if 'InvalidPermission.Duplicate' not in str(ex):
                raise

    @staticmethod
    def open_security_group_to_local(origin_region):
        try:
            origin_region.sct_security_group.authorize_ingress(
                IpPermissions=[
                    {
                        "IpProtocol": "-1",
                        "IpRanges": [{'CidrIp': str(origin_region.vpc_ipv4_cidr),
                                      'Description': f'Local for {origin_region.region_name}'}],
                        "Ipv6Ranges": [{'CidrIpv6': str(origin_region.vpc_ipv6_cidr),
                                        'Description': f'Loca; for {origin_region.region_name}'}]
                    },
                ])
        except ClientError as ex:
            if 'InvalidPermission.Duplicate' not in str(ex):
                raise

    def configure(self):
        regions_pair: list[tuple[AwsRegion, AwsRegion]] = list(itertools.combinations(self.regions, 2))
        for origin_region, target_region in regions_pair:
            LOG.info("Configure peering for: %s %s, %s %s", origin_region.region_name, origin_region.vpc_ipv4_cidr,
                     target_region.region_name, target_region.vpc_ipv4_cidr)
            origin_vpx, target_vpx = self.peering_connection(origin_region, target_region)
            LOG.info("Configuring route tables")

            self.configure_route_tables(origin_region, target_region,
                                        origin_vpx.vpc_peering_connection_id)
            self.configure_route_tables(target_region, origin_region,
                                        target_vpx.vpc_peering_connection_id)

            LOG.info("Configuring security groups")
            self.open_security_group(origin_region, target_region)
            self.open_security_group(origin_region=target_region, target_region=origin_region)
            self.open_security_group_to_local(origin_region)

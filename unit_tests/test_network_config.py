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
import unittest
from typing import NamedTuple

from sdcm.utils.aws_utils import EC2NetworkConfiguration


class RegionAZSubnets(NamedTuple):
    region: str
    availability_zone: str
    subnets: list[str]


class RegionsData:
    UsEast1Region = [RegionAZSubnets(region="us-east-1", availability_zone="a", subnets=['subnet-0a09ba4421ec6aaa8',
                                                                                         'subnet-03d8900174e00a73d']),
                     RegionAZSubnets(region="us-east-1", availability_zone="b", subnets=['subnet-06604bf2840958461',
                                                                                         'subnet-094ed7c7c3bddd441']),
                     ]
    EuCentral1Region = [RegionAZSubnets(region="eu-central-1", availability_zone="a", subnets=['subnet-085db77751694e2a6']),
                        RegionAZSubnets(region="eu-central-1", availability_zone="b",
                                        subnets=['subnet-084b1d12f9974e61f'])
                        ]

    def subnets_per_region(self, regions: list[str], availability_zones: list[str], network_interfaces_count: int):
        subnets_per_region_dict = {}
        for region in regions:
            subnets_per_region_dict[region] = {}
            current_region = [raz for r in [self.UsEast1Region, self.EuCentral1Region]
                              for raz in r if raz.region == region]
            for availability_zone in availability_zones:
                current_az = [cr for cr in current_region if cr.availability_zone == availability_zone][0]
                az_subnet = {availability_zone: [current_az.subnets[i] for i in range(network_interfaces_count)]}
                subnets_per_region_dict[region].update(az_subnet)
        return subnets_per_region_dict


class FakeEC2NetworkConfiguration(EC2NetworkConfiguration):

    def __init__(self, regions: list[str], availability_zones: list[str], network_interfaces_count: int,
                 params: dict = None):
        self.regions = regions
        self.availability_zones = availability_zones
        self.params = params
        self.network_interfaces_count = network_interfaces_count

    @property
    def subnets_per_region(self):
        return RegionsData().subnets_per_region(regions=self.regions,
                                                availability_zones=self.availability_zones,
                                                network_interfaces_count=self.network_interfaces_count)


class AWSNetworkConfigurationTests(unittest.TestCase):
    def test_subnets_property_one_region_one_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["eu-central-1"], availability_zones=["a"], network_interfaces_count=1)
        subnets = net_config.subnets
        self.assertEqual(subnets, [[['subnet-085db77751694e2a6']]])

    def test_subnets_property_one_region_two_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["eu-central-1"], availability_zones=["a", "b"], network_interfaces_count=1)
        subnets = net_config.subnets
        self.assertEqual(subnets, [[['subnet-085db77751694e2a6'], ['subnet-084b1d12f9974e61f']]])

    def test_subnets_property_one_region_two_az_two_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["us-east-1"], availability_zones=["a", "b"], network_interfaces_count=2)
        subnets = net_config.subnets
        self.assertEqual(subnets, [[['subnet-0a09ba4421ec6aaa8', 'subnet-03d8900174e00a73d'],
                                    ['subnet-06604bf2840958461', 'subnet-094ed7c7c3bddd441']]])

    def test_subnets_property_two_region_two_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(regions=["us-east-1", 'eu-central-1'], availability_zones=["a", "b"],
                                                 network_interfaces_count=1)
        subnets = net_config.subnets
        self.assertEqual(subnets, [[['subnet-0a09ba4421ec6aaa8'], ['subnet-06604bf2840958461']],
                                   [['subnet-085db77751694e2a6'], ['subnet-084b1d12f9974e61f']]])

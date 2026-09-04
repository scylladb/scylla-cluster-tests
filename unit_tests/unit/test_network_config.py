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
from typing import NamedTuple

import pytest

from sdcm.provision.network_configuration import azure_ipv6_enabled, azure_network_interfaces
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.aws_utils import EC2NetworkConfiguration


class RegionAZSubnets(NamedTuple):
    region: str
    availability_zone: str
    subnets: list[str]


class RegionsData:
    UsEast1Region = [
        RegionAZSubnets(
            region="us-east-1", availability_zone="a", subnets=["subnet-0a09ba4421ec6aaa8", "subnet-03d8900174e00a73d"]
        ),
        RegionAZSubnets(
            region="us-east-1", availability_zone="b", subnets=["subnet-06604bf2840958461", "subnet-094ed7c7c3bddd441"]
        ),
    ]
    EuCentral1Region = [
        RegionAZSubnets(region="eu-central-1", availability_zone="a", subnets=["subnet-085db77751694e2a6"]),
        RegionAZSubnets(region="eu-central-1", availability_zone="b", subnets=["subnet-084b1d12f9974e61f"]),
    ]

    def subnets_per_region(self, regions: list[str], availability_zones: list[str], network_interfaces_count: int):
        subnets_per_region_dict = {}
        for region in regions:
            subnets_per_region_dict[region] = {}
            current_region = [
                raz for r in [self.UsEast1Region, self.EuCentral1Region] for raz in r if raz.region == region
            ]
            for availability_zone in availability_zones:
                current_az = [cr for cr in current_region if cr.availability_zone == availability_zone][0]
                az_subnet = {availability_zone: [current_az.subnets[i] for i in range(network_interfaces_count)]}
                subnets_per_region_dict[region].update(az_subnet)
        return subnets_per_region_dict


class FakeEC2NetworkConfiguration(EC2NetworkConfiguration):
    def __init__(
        self, regions: list[str], availability_zones: list[str], network_interfaces_count: int, params: dict = None
    ):
        self.regions = regions
        self.availability_zones = availability_zones
        self.params = params
        self.network_interfaces_count = network_interfaces_count

    @property
    def subnets_per_region(self):
        return RegionsData().subnets_per_region(
            regions=self.regions,
            availability_zones=self.availability_zones,
            network_interfaces_count=self.network_interfaces_count,
        )


class TestAWSNetworkConfiguration:
    def test_subnets_property_one_region_one_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["eu-central-1"], availability_zones=["a"], network_interfaces_count=1
        )
        subnets = net_config.subnets
        assert subnets == [[["subnet-085db77751694e2a6"]]]

    def test_subnets_property_one_region_two_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["eu-central-1"], availability_zones=["a", "b"], network_interfaces_count=1
        )
        subnets = net_config.subnets
        assert subnets == [[["subnet-085db77751694e2a6"], ["subnet-084b1d12f9974e61f"]]]

    def test_subnets_property_one_region_two_az_two_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["us-east-1"], availability_zones=["a", "b"], network_interfaces_count=2
        )
        subnets = net_config.subnets
        assert subnets == [
            [
                ["subnet-0a09ba4421ec6aaa8", "subnet-03d8900174e00a73d"],
                ["subnet-06604bf2840958461", "subnet-094ed7c7c3bddd441"],
            ]
        ]

    def test_subnets_property_two_region_two_az_one_interface(self):
        net_config = FakeEC2NetworkConfiguration(
            regions=["us-east-1", "eu-central-1"], availability_zones=["a", "b"], network_interfaces_count=1
        )
        subnets = net_config.subnets
        assert subnets == [
            [["subnet-0a09ba4421ec6aaa8"], ["subnet-06604bf2840958461"]],
            [["subnet-085db77751694e2a6"], ["subnet-084b1d12f9974e61f"]],
        ]


class FakeAzureParams(dict):
    """Minimal stand-in for SCTConfiguration: the Azure validator only reads params via get()."""

    def get(self, key, default=None):
        return super().get(key, default)


def azure_scylla_network_config(nic: int = 0, ip_type: str = "ipv4", public: bool = False) -> list[dict]:
    """A complete 'scylla_network_config' with every address on one NIC/address family."""
    return [
        {"address": address, "ip_type": ip_type, "public": public, "nic": nic, "use_dns": False}
        for address in (
            "listen_address",
            "rpc_address",
            "broadcast_rpc_address",
            "broadcast_address",
            "test_communication",
        )
    ]


def validate_azure(params: dict) -> None:
    SCTConfiguration._validate_azure_network_interfaces(FakeAzureParams(params))


class TestAzureNetworkInterfacesDefaults:
    def test_missing_option_yields_one_public_ipv4_nic(self):
        assert azure_network_interfaces({}) == [
            {"subnet": "default", "public_ip": True, "ipv6": False, "public_ipv6": False}
        ]

    def test_secondary_interfaces_default_to_their_own_private_subnet(self):
        interfaces = azure_network_interfaces({"azure_network_interfaces": [{}, {}, {}]})
        assert [interface["subnet"] for interface in interfaces] == ["default", "nic1", "nic2"]
        assert [interface["public_ip"] for interface in interfaces] == [True, False, False]

    def test_explicit_values_win_over_defaults(self):
        interfaces = azure_network_interfaces(
            {"azure_network_interfaces": [{"public_ip": False}, {"subnet": "rpc", "ipv6": True}]}
        )
        assert interfaces[0]["public_ip"] is False
        assert interfaces[1] == {"subnet": "rpc", "public_ip": False, "ipv6": True, "public_ipv6": False}


class TestAzureIpv6Enabled:
    def test_disabled_without_the_option(self):
        assert azure_ipv6_enabled({}) is False

    def test_disabled_when_no_interface_asks_for_ipv6(self):
        assert azure_ipv6_enabled({"azure_network_interfaces": [{}, {"subnet": "nic1"}]}) is False

    def test_enabled_when_any_interface_asks_for_ipv6(self):
        assert azure_ipv6_enabled({"azure_network_interfaces": [{}, {"ipv6": True}]}) is True


class TestAzureNetworkInterfacesValidation:
    def test_default_single_nic_config_is_valid(self):
        validate_azure({"scylla_network_config": azure_scylla_network_config()})

    def test_primary_interface_must_stay_on_the_default_subnet(self):
        with pytest.raises(ValueError, match="must stay on the 'default' subnet"):
            validate_azure({"azure_network_interfaces": [{"subnet": "nic1"}]})

    def test_nic_index_beyond_the_configured_interfaces_is_rejected(self):
        with pytest.raises(ValueError, match="defines only 1 interface"):
            validate_azure({"scylla_network_config": azure_scylla_network_config(nic=1)})

    def test_ipv6_address_on_an_ipv4_only_interface_is_rejected(self):
        with pytest.raises(ValueError, match="not configured for IPv6"):
            validate_azure({"scylla_network_config": azure_scylla_network_config(ip_type="ipv6")})

    def test_public_ipv6_address_without_a_public_ipv6_resource_is_rejected(self):
        with pytest.raises(ValueError, match="has no IPv6 Public IP"):
            validate_azure(
                {
                    "azure_network_interfaces": [{"ipv6": True}],
                    "scylla_network_config": azure_scylla_network_config(ip_type="ipv6", public=True),
                }
            )

    def test_public_ipv4_address_without_a_public_ip_resource_is_rejected(self):
        with pytest.raises(ValueError, match="has no IPv4 Public IP"):
            validate_azure(
                {
                    "azure_network_interfaces": [{"public_ip": False}],
                    "scylla_network_config": azure_scylla_network_config(public=True),
                }
            )

    def test_ipv6_ssh_needs_a_routable_address_on_the_primary_interface(self):
        with pytest.raises(ValueError, match="IPv6 SSH connections need a routable address"):
            validate_azure(
                {
                    "azure_network_interfaces": [{"ipv6": True}],
                    "scylla_network_config": azure_scylla_network_config(ip_type="ipv6"),
                }
            )

    def test_fully_configured_public_ipv6_setup_is_valid(self):
        validate_azure(
            {
                "azure_network_interfaces": [{"ipv6": True, "public_ipv6": True}],
                "scylla_network_config": azure_scylla_network_config(ip_type="ipv6", public=True),
            }
        )

    def test_two_interfaces_with_a_private_secondary_are_valid(self):
        validate_azure(
            {
                "azure_network_interfaces": [{}, {}],
                "scylla_network_config": azure_scylla_network_config(nic=1),
            }
        )

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
import json
from pathlib import Path
from typing import NamedTuple

import pytest
import yaml

from sdcm.provision.network_configuration import azure_ipv6_enabled, azure_network_interfaces
from sdcm.sct_config import SCTConfiguration
from sdcm.utils.aws_utils import EC2NetworkConfiguration
from sdcm.utils.lint.jenkins_parser import parse_jenkinsfile

# the Azure overrides, and the shared configurations/network_config/ profiles they extend
LAYOUT_DIR = Path("configurations/azure/network_config")
PROFILE_DIR = Path("configurations/network_config")


class RegionAZSubnets(NamedTuple):
    """One availability zone of a region with its subnet ids."""

    region: str
    availability_zone: str
    subnets: list[str]


class RegionsData:
    """Fixed subnet ids per region and availability zone, as the EC2 backend sees them."""

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
        """Map each region and zone to the first network_interfaces_count subnets of that zone."""
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
    """EC2NetworkConfiguration whose subnets come from RegionsData instead of AWS."""

    def __init__(
        self, regions: list[str], availability_zones: list[str], network_interfaces_count: int, params: dict = None
    ):
        self.regions = regions
        self.availability_zones = availability_zones
        self.params = params
        self.network_interfaces_count = network_interfaces_count

    @property
    def subnets_per_region(self):
        """Subnets of the configured regions and zones, from RegionsData."""
        return RegionsData().subnets_per_region(
            regions=self.regions,
            availability_zones=self.availability_zones,
            network_interfaces_count=self.network_interfaces_count,
        )


class FakeAzureParams(dict):
    """Minimal stand-in for SCTConfiguration: the Azure validator only reads params via get()."""

    def get(self, key, default=None):
        """Return the value of key, or default when it is missing."""
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
    """Run the SCTConfiguration Azure NIC validator on params."""
    SCTConfiguration._validate_azure_network_interfaces(FakeAzureParams(params))


def load_yaml(path: str) -> dict:
    """Parse the YAML file at path."""
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def shared_profiles() -> list[str]:
    """Names of the shared network_config profiles."""
    return sorted(path.stem for path in PROFILE_DIR.glob("*.yaml"))


def test_subnets_property_one_region_one_az_one_interface():
    """Subnets lists the single subnet of the one zone."""
    net_config = FakeEC2NetworkConfiguration(
        regions=["eu-central-1"], availability_zones=["a"], network_interfaces_count=1
    )
    subnets = net_config.subnets
    assert subnets == [[["subnet-085db77751694e2a6"]]]


def test_subnets_property_one_region_two_az_one_interface():
    """Subnets lists one subnet per zone of the region."""
    net_config = FakeEC2NetworkConfiguration(
        regions=["eu-central-1"], availability_zones=["a", "b"], network_interfaces_count=1
    )
    subnets = net_config.subnets
    assert subnets == [[["subnet-085db77751694e2a6"], ["subnet-084b1d12f9974e61f"]]]


def test_subnets_property_one_region_two_az_two_interface():
    """Subnets lists the NIC subnets of each zone."""
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


def test_subnets_property_two_region_two_az_one_interface():
    """Subnets nests the zones under each region, in the order requested."""
    net_config = FakeEC2NetworkConfiguration(
        regions=["us-east-1", "eu-central-1"], availability_zones=["a", "b"], network_interfaces_count=1
    )
    subnets = net_config.subnets
    assert subnets == [
        [["subnet-0a09ba4421ec6aaa8"], ["subnet-06604bf2840958461"]],
        [["subnet-085db77751694e2a6"], ["subnet-084b1d12f9974e61f"]],
    ]


def test_no_network_config_yields_one_public_ipv4_nic():
    """Without scylla_network_config Azure gets one public IPv4 NIC on the default subnet."""
    assert azure_network_interfaces({}) == [
        {"subnet": "default", "public_ip": True, "ipv6": False, "public_ipv6": False}
    ]


def test_secondary_interfaces_get_their_own_private_subnet():
    """Each extra NIC gets its own private subnet; only the primary NIC has a public IP."""
    interfaces = azure_network_interfaces(
        {
            "scylla_network_config": azure_scylla_network_config()
            + azure_scylla_network_config(nic=1)
            + azure_scylla_network_config(nic=2)
        }
    )
    assert [interface["subnet"] for interface in interfaces] == ["default", "nic1", "nic2"]
    assert [interface["public_ip"] for interface in interfaces] == [True, False, False]


def test_an_ipv6_address_makes_its_interface_dual_stack():
    """An IPv6 address on the primary NIC enables IPv6 without a public IPv6 address."""
    (interface,) = azure_network_interfaces({"scylla_network_config": azure_scylla_network_config(ip_type="ipv6")})
    assert interface == {"subnet": "default", "public_ip": True, "ipv6": True, "public_ipv6": False}


def test_a_public_ipv6_address_asks_for_the_billed_public_ip():
    """A public IPv6 address asks for the billed public IPv6 address on the NIC."""
    (interface,) = azure_network_interfaces(
        {"scylla_network_config": azure_scylla_network_config(ip_type="ipv6", public=True)}
    )
    assert interface["public_ipv6"] is True


def test_disabled_without_a_network_config():
    """IPv6 is off when there is no network config."""
    assert azure_ipv6_enabled({}) is False


def test_disabled_when_no_address_asks_for_ipv6():
    """IPv6 is off when no address of the network config is IPv6."""
    assert azure_ipv6_enabled({"scylla_network_config": azure_scylla_network_config(nic=1)}) is False


def test_enabled_when_any_address_asks_for_ipv6():
    """IPv6 is on as soon as one address of the network config is IPv6."""
    assert azure_ipv6_enabled({"scylla_network_config": azure_scylla_network_config(ip_type="ipv6")}) is True


def test_default_single_nic_config_is_valid():
    """The default single-NIC config passes Azure validation."""
    validate_azure({"scylla_network_config": azure_scylla_network_config()})


def test_a_gap_in_the_nic_indexes_is_rejected():
    """A secondary NIC without the interface before it is rejected."""
    with pytest.raises(ValueError, match="defines only 1 interface"):
        validate_azure({"scylla_network_config": azure_scylla_network_config(nic=1)})


def test_public_ipv4_outside_the_primary_interface_is_rejected():
    """Only the primary NIC may carry a public IPv4 address."""
    with pytest.raises(ValueError, match="IPv4 Public IP to the primary NIC only"):
        validate_azure(
            {"scylla_network_config": azure_scylla_network_config() + azure_scylla_network_config(nic=1, public=True)}
        )


def test_ipv6_ssh_needs_a_routable_address_on_the_primary_interface():
    """An IPv6-only primary NIC without a routable address is rejected, as SSH needs one."""
    with pytest.raises(ValueError, match="IPv6 SSH connections need a routable address"):
        validate_azure({"scylla_network_config": azure_scylla_network_config(ip_type="ipv6")})


def test_fully_configured_public_ipv6_setup_is_valid():
    """A public IPv6 setup on the primary NIC passes Azure validation."""
    validate_azure({"scylla_network_config": azure_scylla_network_config(ip_type="ipv6", public=True)})


def test_two_interfaces_with_a_private_secondary_are_valid():
    """A private secondary NIC next to the primary passes Azure validation."""
    validate_azure({"scylla_network_config": azure_scylla_network_config() + azure_scylla_network_config(nic=1)})


def test_every_azure_override_has_a_shared_profile_of_its_name():
    """An override is loaded after the profile of the same name, so it cannot stand alone."""
    assert set(path.stem for path in LAYOUT_DIR.glob("*.yaml")) <= set(shared_profiles())


def test_every_profile_is_usable_on_azure():
    """The NIC layout is derived from the profile, so none of them needs an Azure counterpart."""
    for profile in shared_profiles():
        params = load_yaml(f"{PROFILE_DIR}/{profile}.yaml")
        override = LAYOUT_DIR / f"{profile}.yaml"
        if override.exists():
            params |= load_yaml(str(override))

        validate_azure(params)


def test_only_the_ipv6_profile_enables_ipv6():
    """An Azure IPv6 is a billed Public IP, so no other profile may quietly turn it on."""
    enabled = {
        profile for profile in shared_profiles() if azure_ipv6_enabled(load_yaml(f"{PROFILE_DIR}/{profile}.yaml"))
    }
    assert enabled == {"all_addresses_ipv6_public"}


def test_multidc_topology_changes_job_config_validates():
    """The merged configs of the multidc topology-changes Azure job validate and keep their IPv6, region, zone and rack layout."""
    pipeline = parse_jenkinsfile(
        Path("jenkins-pipelines/oss/tier1/longevity-multidc-schema-topology-changes-12h-azure.jenkinsfile")
    )
    params = {}
    for config in pipeline.test_config:
        params |= load_yaml(config)

    validate_azure(params)
    # routable IPv6 on every Scylla address is the coverage this job owns
    assert azure_ipv6_enabled(params)
    assert azure_network_interfaces(params)[0]["public_ipv6"] is True
    assert {address["ip_type"] for address in params["scylla_network_config"]} == {"ipv6"}

    # the second DC is simulated, so every node has to land in the one real region
    assert json.loads(pipeline.params["azure_region_name"]) == ["eastus"]
    assert params["simulated_regions"] == 2

    # Azure resolves one zone letter per resource group, unlike the 'a,b,c' of the other clouds
    assert params["availability_zone"] == "a"

    # the racks are simulated too, and have to divide the node count of the base test case
    # evenly with at least two nodes each: a rack of one disappears with the node a nemesis
    # removes, leaving RF=3 over fewer racks than it needs
    assert params["simulated_racks"] == 3
    for count in params["n_db_nodes"]:
        assert count % params["simulated_racks"] == 0
        assert count // params["simulated_racks"] >= 2

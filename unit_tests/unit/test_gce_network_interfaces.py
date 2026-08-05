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
# Copyright (c) 2026 ScyllaDB

"""Unit tests for GCE multiple network interfaces support."""

import subprocess
from ipaddress import ip_network

import pytest

from sdcm.provision.gce.instance_provider import (
    VirtualMachineProvider,
    build_network_interfaces,
    max_network_interfaces,
)
from sdcm.provision.gce.network_provider import NetworkProvider
from sdcm.provision.provisioner import ProvisionError
from sdcm.utils.gce_region import GceRegion
from sdcm.utils.gce_utils import (
    SECONDARY_NIC_ROUTING_SCRIPT,
    gce_mac_address_for_ipv4,
)


PROJECT_ID = "sct-project"
NETWORK_NAME = "qa-vpc"
NETWORK_URL = f"projects/{PROJECT_ID}/global/networks/{NETWORK_NAME}"


@pytest.fixture(name="network_provider")
def fixture_network_provider() -> NetworkProvider:
    return NetworkProvider(project_id=PROJECT_ID, network_name=NETWORK_NAME)


def new_region(region_name: str | None = None) -> GceRegion:
    region = GceRegion.__new__(GceRegion)
    if region_name is not None:
        region.region_name = region_name
    return region


def test_single_nic_keeps_public_access_and_no_subnetwork(network_provider):
    interfaces = build_network_interfaces(network_provider=network_provider, region="us-east1", count=1)
    assert len(interfaces) == 1
    assert interfaces[0].network == NETWORK_URL
    assert [access.name for access in interfaces[0].access_configs] == ["External NAT"]
    assert not interfaces[0].subnetwork


def test_two_nics_attach_public_access_to_primary_only(network_provider):
    interfaces = build_network_interfaces(network_provider=network_provider, region="us-east1", count=2)
    assert len(interfaces) == 2
    assert len(interfaces[0].access_configs) == 1, "public IP must stay on the primary interface"
    assert not interfaces[1].access_configs, "secondary interfaces must not get a public IP"


def test_secondary_nic_uses_same_network_and_dedicated_subnetwork(network_provider):
    interfaces = build_network_interfaces(network_provider=network_provider, region="us-central1", count=2)
    assert interfaces[1].network == interfaces[0].network, "both NICs live in the same VPC network"
    assert interfaces[1].subnetwork == f"projects/{PROJECT_ID}/regions/us-central1/subnetworks/{NETWORK_NAME}-nic1"


def test_more_nics_than_prepared_subnets_are_rejected():
    """`prepare-regions` reserves a CIDR for one secondary subnet, so a third NIC has nowhere to attach."""
    provider = VirtualMachineProvider.__new__(VirtualMachineProvider)
    with pytest.raises(ProvisionError, match="at most 2"):
        provider._validate_network_interfaces_count(machine_type="n2-highmem-8", count=3)


@pytest.mark.parametrize(
    ("vcpus", "expected"),
    [(1, 2), (2, 2), (4, 4), (8, 8), (16, 8)],
)
def test_max_network_interfaces_follows_gce_rule(vcpus, expected):
    """GCE allows one NIC per vCPU, never fewer than 2 and never more than 8."""
    assert max_network_interfaces(vcpus) == expected


@pytest.mark.parametrize(
    ("ipv4", "expected_mac"),
    [
        ("10.128.0.2", "42:01:0a:80:00:02"),
        ("10.100.1.255", "42:01:0a:64:01:ff"),
        ("192.168.0.1", "42:01:c0:a8:00:01"),
    ],
)
def test_gce_mac_address_is_derived_from_private_ipv4(ipv4, expected_mac):
    """GCE derives NIC MACs deterministically from interface IPv4, allowing SCT to map cloud NICs to OS names."""
    assert gce_mac_address_for_ipv4(ipv4) == expected_mac


@pytest.mark.parametrize("ipv4", ["not-an-ip", "10.1.2.999", None])
def test_gce_mac_address_rejects_malformed_ipv4(ipv4):
    assert gce_mac_address_for_ipv4(ipv4) is None


@pytest.mark.parametrize(
    ("region_name", "expected_cidr"),
    [("us-east1", "10.100.0.0/24"), ("europe-west1", "10.100.6.0/24")],
)
def test_secondary_subnet_cidr_is_stable_per_region(region_name, expected_cidr):
    region = new_region(region_name)
    assert region.secondary_subnet_cidr == expected_cidr


def test_secondary_subnet_cidrs_do_not_overlap_or_clash_with_auto_mode():
    """Auto-mode subnets of `qa-vpc` come out of 10.128.0.0/9, so the secondary ones must not."""
    auto_mode_block = ip_network("10.128.0.0/9")
    internal_range = ip_network("10.0.0.0/8")
    seen = set()
    region = new_region()
    for region_name in GceRegion.SECONDARY_SUBNET_REGION_INDEXES:
        region.region_name = region_name
        cidr = ip_network(region.secondary_subnet_cidr)
        assert not cidr.overlaps(auto_mode_block), f"{region_name} overlaps the auto-mode range"
        assert cidr.subnet_of(internal_range), f"{region_name} falls outside the allowed internal range"
        assert cidr not in seen, f"{region_name} reuses a CIDR of another region"
        seen.add(cidr)


def test_secondary_subnet_cidr_rejects_unmapped_region():
    region = new_region("mars-north3")
    with pytest.raises(RuntimeError, match="no CIDR reserved"):
        _ = region.secondary_subnet_cidr


def test_configuring_an_unmapped_region_skips_the_subnet_instead_of_failing():
    """`prepare-regions` should skip regions that have no reserved secondary-subnet CIDR."""
    assert new_region("mars-north3").create_secondary_subnet() is None


def test_bootstrap_subnet_name_matches_what_the_provisioner_looks_up(network_provider):
    """Ensure NIC 1 uses the same subnet name that `GceRegion.configure()` creates."""
    assert network_provider.get_subnetwork_url(region="us-east1", index=1).endswith(
        f"/subnetworks/{new_region().secondary_subnet_name}"
    )


def test_secondary_nic_routing_script_is_valid_bash(tmp_path):
    script = tmp_path / "sct-secondary-nic-routing.sh"
    script.write_text(SECONDARY_NIC_ROUTING_SCRIPT)
    result = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True, check=False)
    assert result.returncode == 0, f"script has bash syntax errors: {result.stderr}"

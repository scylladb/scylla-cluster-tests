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

"""Unit tests for GCE networking: multiple network interfaces and the Jenkins builder firewall."""

import subprocess
from ipaddress import ip_network
from unittest.mock import MagicMock, patch

import google.api_core.exceptions
import pytest
from google.cloud import compute_v1
from google.cloud.compute_v1 import Firewall

from sdcm.provision.gce.instance_provider import (
    VirtualMachineProvider,
    build_network_interfaces,
    max_network_interfaces,
)
from sdcm.provision.gce.network_provider import NetworkProvider
from sdcm.provision.provisioner import ProvisionError
from sdcm.utils.gce_builder import GceBuilder
from sdcm.utils.gce_region import BUILDER_NETWORK_TAG, GceRegion, firewall_needs_patch
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


BUILDER_SOURCES = {"jenkins": ["192.0.2.10/32"], "office": ["198.51.100.10/32"]}


def _rule(**overrides):
    base = dict(
        name="r",
        direction="INGRESS",
        priority=100,
        source_ranges=["1.2.3.4/32"],
        allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["22"])],
        target_tags=["sct-builder"],
    )
    fields = base | overrides
    return Firewall(**{key: value for key, value in fields.items() if value is not None})


def test_firewall_needs_patch_detects_priority_and_target_tags():
    assert not firewall_needs_patch(_rule(), _rule())
    assert firewall_needs_patch(_rule(priority=1000), _rule())
    assert firewall_needs_patch(_rule(target_tags=[]), _rule())
    assert firewall_needs_patch(_rule(allowed=[], denied=[compute_v1.Denied(I_p_protocol="all")]), _rule())
    # a live rule at the API default must equal a desired rule that never set a priority
    assert not firewall_needs_patch(_rule(priority=1000), _rule(priority=None))


def _firewall_region():
    region = new_region()
    region.project = PROJECT_ID
    region.firewall_client = MagicMock()
    region.__dict__["network"] = MagicMock(self_link="net-link")
    return region


def test_builder_firewall_applies_allows_before_deny_and_waits():
    region = _firewall_region()
    with patch("sdcm.utils.gce_region.wait_for_extended_operation") as wait:
        region.configure_builder_firewall(BUILDER_SOURCES)

    inserted = [call.kwargs["firewall_resource"] for call in region.firewall_client.insert.call_args_list]
    # without the tag, the deny rule would cut off every host in the project
    assert all(list(rule.target_tags) == [BUILDER_NETWORK_TAG] for rule in inserted)
    # a deny rule without its allow rules would lock Jenkins out of new builders
    assert inserted[-1].name == "qa-vpc-builder-deny-all"
    waited = [call.args[0] for call in wait.call_args_list]
    assert waited.count(region.firewall_client.insert.return_value) == 3


def test_stale_ssh_rule_kept_when_its_replacement_is_missing():
    region = _firewall_region()
    region.firewall_client.get.side_effect = google.api_core.exceptions.NotFound("no such rule")

    region.delete_stale_firewall_rules()

    region.firewall_client.delete.assert_not_called()


def _builder_template(name, tags=(), image="projects/sct/global/images/runner-1"):
    template = compute_v1.InstanceTemplate(name=name)
    template.properties.tags = compute_v1.Tags(items=list(tags))
    template.properties.disks = [
        compute_v1.AttachedDisk(initialize_params=compute_v1.AttachedDiskInitializeParams(source_image=image))
    ]
    return template


def _get_image(project, image):
    if image == "gone":
        raise google.api_core.exceptions.NotFound("no such image")
    return compute_v1.Image(name=image)


def test_tag_builder_templates_covers_every_builder_version():
    builder = GceBuilder.__new__(GceBuilder)
    builder.region = MagicMock(project=PROJECT_ID, region_name="us-east1")
    builder.credentials = MagicMock()
    older = _builder_template(f"gce-{PROJECT_ID}-us-east1-qa-builder-v8")
    templates = [
        older,
        _builder_template(f"gce-{PROJECT_ID}-us-east1-qa-builder-v9", tags=[BUILDER_NETWORK_TAG]),
        # deleting a template whose image is gone would lose it for good
        _builder_template(f"gce-{PROJECT_ID}-us-east1-qa-builder-v2", image="projects/sct/global/images/gone"),
        _builder_template(f"gce-{PROJECT_ID}-us-east4-qa-builder-v8"),
    ]
    with (
        patch("sdcm.utils.gce_builder.compute_v1.InstanceTemplatesClient") as templates_client,
        patch("sdcm.utils.gce_builder.compute_v1.ImagesClient") as images_client,
        patch.object(GceBuilder, "_wait_for_extended_operation"),
    ):
        templates_client.return_value.list.return_value = templates
        images_client.return_value.get.side_effect = _get_image
        assert builder.tag_builder_templates() == 1

    deleted = [call.kwargs["instance_template"] for call in templates_client.return_value.delete.call_args_list]
    assert deleted == [older.name]
    inserted = templates_client.return_value.insert.call_args.kwargs["instance_template_resource"]
    assert inserted.name == older.name
    assert list(inserted.properties.tags.items) == [BUILDER_NETWORK_TAG]
    assert inserted.properties.disks[0].initialize_params.source_image == "projects/sct/global/images/runner-1"


def test_tag_running_builders_skips_builders_deleted_meanwhile():
    # one-shot builders are deleted after their job, often between the listing and the tagging
    builder = GceBuilder.__new__(GceBuilder)
    builder.region = MagicMock(project=PROJECT_ID, region_name="us-east1")
    builder.credentials = MagicMock()
    gone = compute_v1.Instance(name="builders-1", status="RUNNING")
    alive = compute_v1.Instance(name="builders-2", status="RUNNING")
    scoped_list = compute_v1.InstancesScopedList(instances=[gone, alive])
    with (
        patch("sdcm.utils.gce_builder.compute_v1.InstancesClient") as client,
        patch(
            "sdcm.utils.gce_builder.gce_set_tags",
            side_effect=[google.api_core.exceptions.NotFound("gone"), None],
        ) as set_tags,
    ):
        client.return_value.aggregated_list.return_value = [("zones/us-east1-b", scoped_list)]
        assert builder.tag_running_builders() == 1

    assert [call.args[1].name for call in set_tags.call_args_list] == ["builders-1", "builders-2"]

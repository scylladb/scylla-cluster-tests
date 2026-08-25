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
# Copyright (c) 2025 ScyllaDB

"""Unit tests for `sdcm.utils.oci_region.OciRegion`."""

from ipaddress import ip_network
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

from oci.core.models import EgressSecurityRule, IngressSecurityRule, PortRange, TcpOptions

from sdcm.utils.oci_region import OciRegion


def _make_wait_until_side_effect(expected_state="AVAILABLE"):
    def _fake_wait_until(
        client, response, resource_id=None, evaluate_response=None, max_wait_seconds=None, max_interval_seconds=None
    ):
        # The response is already the result from calling the getter
        # Just ensure lifecycle_state is set
        if hasattr(response, "data") and hasattr(response.data, "lifecycle_state"):
            response.data.lifecycle_state = expected_state
        return response

    return _fake_wait_until


def _assert_private_and_public_subnet_create_calls(network_client):
    assert network_client.create_subnet.call_count == 2
    create_details = [call.args[0] for call in network_client.create_subnet.call_args_list]

    expected_names = {
        "SCT-2-subnet-regional-private",
        "SCT-2-subnet-regional-public",
    }
    actual_names = {details.display_name for details in create_details}
    assert actual_names == expected_names

    prohibit_public_ip_by_subnet_name = {
        details.display_name: details.prohibit_public_ip_on_vnic for details in create_details
    }
    assert prohibit_public_ip_by_subnet_name["SCT-2-subnet-regional-private"] is True
    assert prohibit_public_ip_by_subnet_name["SCT-2-subnet-regional-public"] is False

    dns_labels_by_subnet_name = {details.display_name: details.dns_label for details in create_details}
    private_dns_label = dns_labels_by_subnet_name["SCT-2-subnet-regional-private"]
    public_dns_label = dns_labels_by_subnet_name["SCT-2-subnet-regional-public"]

    assert private_dns_label != public_dns_label
    for dns_label in (private_dns_label, public_dns_label):
        assert dns_label
        assert dns_label[0].isalpha()
        assert len(dns_label) <= 15


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch("sdcm.utils.oci_region.oci.wait_until")
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
@patch(
    "sdcm.utils.oci_region.OciRegion.compartment_id",
    new_callable=PropertyMock,
    return_value="ocid1.compartment.oc1..test",
)
@patch(
    "sdcm.utils.oci_region.OciRegion.availability_domains",
    new_callable=PropertyMock,
    return_value=["ocid.AD-1", "ocid.AD-2"],
)
@patch("oci.pagination.list_call_get_all_results_generator")
def test_configure_network_creates_resources(
    mock_page_iterator,
    mock_ads,
    mock_compartment,
    mock_compute,
    mock_identity,
    mock_network,
    mock_wait_until,
    mock_get_username,
):
    compute_client = MagicMock()
    identity_client = MagicMock()
    network_client = MagicMock()

    mock_compute.return_value = compute_client
    mock_identity.return_value = identity_client
    mock_network.return_value = network_client
    mock_wait_until.side_effect = _make_wait_until_side_effect()

    mock_vcn = MagicMock(
        spec_set=["id", "default_route_table_id", "default_dhcp_options_id", "lifecycle_state", "display_name"]
    )
    mock_vcn.id = "ocid1.vcn.oc1..123"
    mock_vcn.display_name = "sct-vpc-us-phoenix-1"
    mock_vcn.default_route_table_id = "ocid1.routetable.oc1..123"
    mock_vcn.default_dhcp_options_id = "ocid1.dhcp.oc1..123"
    mock_vcn.lifecycle_state = "AVAILABLE"
    # ipv6_cidr_blocks not in spec, so getattr will return the default (None)

    # list_vcns returns empty list first call so creation path is taken
    mock_page_iterator.return_value = iter([])

    # Configure VCN creation - must set both .data on create response and on get response
    vcn_response = MagicMock()
    vcn_response.data = mock_vcn
    network_client.create_vcn.return_value = vcn_response
    network_client.get_vcn.side_effect = lambda *args, **kwargs: vcn_response

    # Security list creation
    mock_sl = MagicMock()
    mock_sl.id = "ocid1.sl.oc1..123"
    mock_sl.lifecycle_state = "AVAILABLE"
    sl_response = MagicMock()
    sl_response.data = mock_sl
    network_client.create_security_list.return_value = sl_response
    network_client.get_security_list.side_effect = lambda *args, **kwargs: sl_response

    # Subnet creation
    mock_subnet = MagicMock()
    mock_subnet.id = "ocid1.subnet.oc1..123"
    mock_subnet.lifecycle_state = "AVAILABLE"
    subnet_response = MagicMock()
    subnet_response.data = mock_subnet
    network_client.create_subnet.return_value = subnet_response
    network_client.get_subnet.side_effect = lambda *args, **kwargs: subnet_response

    # Internet gateway creation
    mock_igw = MagicMock()
    mock_igw.id = "ocid1.internetgateway.oc1..123"
    mock_igw.lifecycle_state = "AVAILABLE"
    igw_response = MagicMock()
    igw_response.data = mock_igw
    network_client.create_internet_gateway.return_value = igw_response
    network_client.get_internet_gateway.side_effect = lambda *args, **kwargs: igw_response
    network_client.get_route_table.return_value.data = MagicMock(route_rules=[])
    mock_ngw = MagicMock()
    mock_ngw.id = "ocid1.natgateway.oc1..123"
    mock_ngw.lifecycle_state = "AVAILABLE"
    ngw_response = MagicMock()
    ngw_response.data = mock_ngw
    network_client.create_nat_gateway.return_value = ngw_response
    network_client.get_nat_gateway.side_effect = lambda *args, **kwargs: ngw_response

    region = OciRegion("us-ashburn-1")
    region.configure_network()

    # VCN created
    network_client.create_vcn.assert_called_once()
    # Security list created and attached
    network_client.create_security_list.assert_called_once()
    # Internet gateway created
    network_client.create_internet_gateway.assert_called_once()
    # NAT gateway created
    network_client.create_nat_gateway.assert_called_once()
    # Private and public subnets are created with expected names and visibility flags
    _assert_private_and_public_subnet_create_calls(network_client)


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch("sdcm.utils.oci_region.oci.wait_until")
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
@patch(
    "sdcm.utils.oci_region.OciRegion.compartment_id",
    new_callable=PropertyMock,
    return_value="ocid1.compartment.oc1..test",
)
@patch(
    "sdcm.utils.oci_region.OciRegion.availability_domains",
    new_callable=PropertyMock,
    return_value=["ocid.AD-1", "ocid.AD-2"],
)
@patch("oci.pagination.list_call_get_all_results_generator")
def test_configure_network_with_ipv6(
    mock_page_iterator,
    mock_ads,
    mock_compartment,
    mock_compute,
    mock_identity,
    mock_network,
    mock_wait_until,
    mock_get_username,
):
    """Test network configuration when VCN has IPv6 CIDR blocks."""
    compute_client = MagicMock()
    identity_client = MagicMock()
    network_client = MagicMock()

    mock_compute.return_value = compute_client
    mock_identity.return_value = identity_client
    mock_network.return_value = network_client
    mock_wait_until.side_effect = _make_wait_until_side_effect()

    # VCN with IPv6 CIDR blocks
    mock_vcn = MagicMock()
    mock_vcn.id = "ocid1.vcn.oc1..123"
    mock_vcn.default_route_table_id = "ocid1.routetable.oc1..123"
    mock_vcn.default_dhcp_options_id = "ocid1.dhcp.oc1..123"
    mock_vcn.lifecycle_state = "AVAILABLE"
    mock_vcn.ipv6_cidr_blocks = ["2603:c020:8000::/48"]  # Valid IPv6 CIDR block

    # list_vcns returns empty list first call so creation path is taken
    mock_page_iterator.return_value = iter([])

    # Configure VCN creation
    vcn_response = MagicMock()
    vcn_response.data = mock_vcn
    network_client.create_vcn.return_value = vcn_response
    network_client.get_vcn.side_effect = lambda *args, **kwargs: vcn_response

    # Security list creation
    mock_sl = MagicMock()
    mock_sl.id = "ocid1.sl.oc1..123"
    mock_sl.lifecycle_state = "AVAILABLE"
    sl_response = MagicMock()
    sl_response.data = mock_sl
    network_client.create_security_list.return_value = sl_response
    network_client.get_security_list.side_effect = lambda *args, **kwargs: sl_response

    # Subnet creation
    mock_subnet = MagicMock()
    mock_subnet.id = "ocid1.subnet.oc1..123"
    mock_subnet.lifecycle_state = "AVAILABLE"
    subnet_response = MagicMock()
    subnet_response.data = mock_subnet
    network_client.create_subnet.return_value = subnet_response
    network_client.get_subnet.side_effect = lambda *args, **kwargs: subnet_response

    # Internet gateway creation
    mock_igw = MagicMock()
    mock_igw.id = "ocid1.internetgateway.oc1..123"
    mock_igw.lifecycle_state = "AVAILABLE"
    igw_response = MagicMock()
    igw_response.data = mock_igw
    network_client.create_internet_gateway.return_value = igw_response
    network_client.get_internet_gateway.side_effect = lambda *args, **kwargs: igw_response
    network_client.get_route_table.return_value.data = MagicMock(route_rules=[])
    mock_ngw = MagicMock()
    mock_ngw.id = "ocid1.natgateway.oc1..123"
    mock_ngw.lifecycle_state = "AVAILABLE"
    ngw_response = MagicMock()
    ngw_response.data = mock_ngw
    network_client.create_nat_gateway.return_value = ngw_response
    network_client.get_nat_gateway.side_effect = lambda *args, **kwargs: ngw_response

    region = OciRegion("us-ashburn-1")
    region.configure_network()

    # VCN created
    network_client.create_vcn.assert_called_once()
    # Security list created with IPv6 rules
    network_client.create_security_list.assert_called_once()
    # Internet gateway created
    network_client.create_internet_gateway.assert_called_once()
    # NAT gateway created
    network_client.create_nat_gateway.assert_called_once()
    # Private and public subnets are created with expected names and visibility flags
    _assert_private_and_public_subnet_create_calls(network_client)

    # Verify that IPv6 CIDR was cached
    assert region._vcn_ipv6_cidr is not None
    assert str(region._vcn_ipv6_cidr) == "2603:c020:8000::/48"


@patch("sdcm.utils.oci_region.OciRegion.availability_domains", return_value=["ocid.AD-1"])
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", return_value="ocid1.compartment.oc1..test")
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
def test_subnet_name_suffix(mock_compute, mock_identity, mock_network, mock_compartment, mock_ads):
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = MagicMock()
    region = OciRegion("us-ashburn-1")

    assert region.subnet_name() == "SCT-2-subnet-regional-private"
    assert region.subnet_name(False) == "SCT-2-subnet-regional-private"
    assert region.subnet_name(True) == "SCT-2-subnet-regional-public"


@patch("sdcm.utils.oci_region.SUPPORTED_REGIONS", new=["us-ashburn-1"])
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", return_value="ocid1.compartment.oc1..test")
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
def test_region_validation(mock_compute, mock_identity, mock_network, mock_compartment):
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = MagicMock()

    with pytest.raises(ValueError):
        OciRegion("us-phoenix-1")


@patch(
    "sdcm.utils.oci_region.OciRegion.vcn",
    new_callable=PropertyMock,
    return_value=MagicMock(display_name="SCT-2-vcn", id="ocid1.vcn.oc1..no-dns", dns_label=None),
)
def test_validate_dns_infrastructure_fails_when_vcn_has_no_dns_label(mock_vcn):
    subnet = MagicMock(
        display_name="SCT-2-subnet-regional-private",
        id="ocid1.subnet.oc1..has-dns",
        dns_label="subnet1",
    )

    with pytest.raises(ValueError, match="VCN 'SCT-2-vcn'.*has no DNS label"):
        OciRegion("us-ashburn-1").validate_dns_infrastructure(subnet=subnet, public=False)

    mock_vcn.assert_called()


@patch(
    "sdcm.utils.oci_region.OciRegion.vcn",
    new_callable=PropertyMock,
    return_value=MagicMock(display_name="SCT-2-vcn", id="ocid1.vcn.oc1..has-dns", dns_label="vcn1"),
)
def test_validate_dns_infrastructure_fails_when_subnet_has_no_dns_label(mock_vcn):
    subnet = MagicMock(display_name="SCT-2-subnet-regional-private", id="ocid1.subnet.oc1..no-dns", dns_label=None)

    err_msg_template = "Private subnet 'SCT-2-subnet-regional-private'.*has no DNS label"
    with pytest.raises(ValueError, match=err_msg_template):
        OciRegion("us-ashburn-1").validate_dns_infrastructure(subnet=subnet, public=False)

    mock_vcn.assert_called()


def _make_subnet_mock(display_name, subnet_id, ipv6_cidr_blocks):
    subnet = MagicMock(spec_set=["id", "display_name", "lifecycle_state", "ipv6_cidr_blocks"])
    subnet.id = subnet_id
    subnet.display_name = display_name
    subnet.lifecycle_state = "AVAILABLE"
    subnet.ipv6_cidr_blocks = ipv6_cidr_blocks
    return subnet


@patch("sdcm.utils.oci_region.OciRegion.vcn", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
def test_subnet_adds_ipv6_prefix_to_legacy_subnet(
    mock_compute, mock_identity, mock_network, mock_compartment, mock_vcn
):
    """A subnet created before the IPv6 support must get an IPv6 prefix added in-place.

    All the VNICs are created with 'assign_ipv6_ip=True', which OCI rejects for
    subnets without an IPv6 prefix.
    """
    network_client = MagicMock()
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = network_client
    mock_compartment.return_value = "ocid1.compartment.oc1..test"
    mock_vcn.return_value = MagicMock(id="ocid1.vcn.oc1..123", display_name="SCT-2-vcn")

    legacy_subnet = _make_subnet_mock("SCT-2-subnet-regional-private", "ocid1.subnet.oc1..private", [])
    # NOTE: the public subnet already occupies the very first IPv6 prefix of the VCN CIDR
    public_subnet = _make_subnet_mock(
        "SCT-2-subnet-regional-public", "ocid1.subnet.oc1..public", ["2603:c020:8000:1c00::/64"]
    )
    upgraded_subnet = _make_subnet_mock(legacy_subnet.display_name, legacy_subnet.id, ["2603:c020:8000:1c01::/64"])
    network_client.list_subnets.return_value = MagicMock(data=[legacy_subnet, public_subnet])

    region = OciRegion("us-ashburn-1")
    region._vcn_ipv6_cidr = ip_network("2603:c020:8000:1c00::/56")

    with (
        patch("oci.wait_until", return_value=MagicMock(data=upgraded_subnet)),
        patch("oci.pagination.list_call_get_all_results", side_effect=lambda func, **kwargs: func(**kwargs)),
        patch(
            "oci.pagination.list_call_get_all_results_generator",
            side_effect=lambda func, yield_mode=None, **kwargs: iter([legacy_subnet]),
        ),
    ):
        subnet = region.subnet(public=False, _cache={})

    assert subnet is upgraded_subnet
    assert network_client.add_ipv6_subnet_cidr.call_count == 1
    call_args = network_client.add_ipv6_subnet_cidr.call_args.args
    assert call_args[0] == legacy_subnet.id
    # NOTE: the first free prefix is picked, the one taken by the public subnet gets skipped
    assert call_args[1].ipv6_cidr_block == "2603:c020:8000:1c01::/64"


@patch("sdcm.utils.oci_region.OciRegion.vcn", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
def test_subnet_keeps_ipv6_enabled_subnet_intact(mock_compute, mock_identity, mock_network, mock_compartment, mock_vcn):
    network_client = MagicMock()
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = network_client
    mock_compartment.return_value = "ocid1.compartment.oc1..test"
    mock_vcn.return_value = MagicMock(id="ocid1.vcn.oc1..123", display_name="SCT-2-vcn")

    existing_subnet = _make_subnet_mock(
        "SCT-2-subnet-regional-public", "ocid1.subnet.oc1..public", ["2603:c020:8000:1c00::/64"]
    )

    region = OciRegion("us-ashburn-1")
    region._vcn_ipv6_cidr = ip_network("2603:c020:8000:1c00::/56")

    with patch(
        "oci.pagination.list_call_get_all_results_generator",
        side_effect=lambda func, yield_mode=None, **kwargs: iter([existing_subnet]),
    ):
        subnet = region.subnet(public=True, _cache={})

    assert subnet is existing_subnet
    network_client.add_ipv6_subnet_cidr.assert_not_called()


@patch("sdcm.utils.oci_region.OciRegion.vcn", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock)
def test_subnet_skips_ipv6_upgrade_when_vcn_has_no_ipv6(
    mock_compute, mock_identity, mock_network, mock_compartment, mock_vcn
):
    network_client = MagicMock()
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = network_client
    mock_compartment.return_value = "ocid1.compartment.oc1..test"
    mock_vcn.return_value = MagicMock(id="ocid1.vcn.oc1..123", display_name="SCT-2-vcn", ipv6_cidr_blocks=[])

    legacy_subnet = _make_subnet_mock("SCT-2-subnet-regional-private", "ocid1.subnet.oc1..private", [])

    region = OciRegion("us-ashburn-1")

    with patch(
        "oci.pagination.list_call_get_all_results_generator",
        side_effect=lambda func, yield_mode=None, **kwargs: iter([legacy_subnet]),
    ):
        subnet = region.subnet(public=False, _cache={})

    assert subnet is legacy_subnet
    network_client.add_ipv6_subnet_cidr.assert_not_called()


def _make_ipv6_region(network_client):
    """Build an OciRegion with a mocked network client and a VCN that already has an IPv6 CIDR."""
    with (
        patch("sdcm.utils.oci_region.OciRegion.network_client", new_callable=PropertyMock) as mock_network,
        patch("sdcm.utils.oci_region.OciRegion.identity_client", new_callable=PropertyMock),
        patch("sdcm.utils.oci_region.OciRegion.compute_client", new_callable=PropertyMock),
        patch("sdcm.utils.oci_region.OciRegion.compartment_id", new_callable=PropertyMock) as mock_compartment,
    ):
        mock_network.return_value = network_client
        mock_compartment.return_value = "ocid1.compartment.oc1..test"
        region = OciRegion("us-ashburn-1")
        region.__dict__["network_client"] = network_client
        region.__dict__["compartment_id"] = "ocid1.compartment.oc1..test"
    region.__dict__["vcn"] = MagicMock(id="ocid1.vcn.oc1..123", display_name="SCT-2-vcn")
    region._vcn_ipv6_cidr = ip_network("2603:c020:8000:1c00::/56")
    return region


def _ipv6_rules(rules):
    """Keep only the IPv6 rules out of a security list rule set."""
    return [
        rule for rule in rules if ":" in (getattr(rule, "source", None) or getattr(rule, "destination", None) or "")
    ]


def test_ensure_security_list_ipv6_adds_ssh_egress_and_intra_vcn_rules():
    """An IPv4-only security list must gain all the IPv6 rules, SSH included."""
    network_client = MagicMock()
    region = _make_ipv6_region(network_client)
    legacy_sl = MagicMock()
    legacy_sl.id = "ocid1.securitylist.oc1..legacy"
    legacy_sl.display_name = "SCT-2-sl"
    legacy_sl.ingress_security_rules = [
        IngressSecurityRule(protocol="6", source="0.0.0.0/0", description="Allow SSH from anywhere"),
        IngressSecurityRule(protocol="all", source="10.0.0.0/16", description="Allow all traffic within VCN"),
    ]
    legacy_sl.egress_security_rules = [EgressSecurityRule(protocol="all", destination="0.0.0.0/0")]
    region._find_security_list = MagicMock(return_value=legacy_sl)

    region._ensure_security_list_ipv6()

    details = network_client.update_security_list.call_args.args[1]
    # the pre-existing IPv4 rules must be preserved
    assert len(details.ingress_security_rules) == 4
    ipv6_ingress = _ipv6_rules(details.ingress_security_rules)
    assert region._ipv6_ssh_ingress_rule() in ipv6_ingress
    assert region._ipv6_intra_vcn_ingress_rule() in ipv6_ingress
    assert _ipv6_rules(details.egress_security_rules) == [region._ipv6_egress_rule()]


def test_ensure_security_list_ipv6_matches_create_security_list():
    """The upgrade path must install exactly the IPv6 rules a freshly created list gets.

    This is what keeps '_ensure_security_list_ipv6' and '_create_security_list' from drifting
    apart again, the way the IPv6 SSH rule did.
    """
    create_client = MagicMock()
    create_region = _make_ipv6_region(create_client)
    with patch("oci.wait_until", return_value=MagicMock(data=MagicMock(lifecycle_state="AVAILABLE"))):
        create_region._create_security_list()
    created = create_client.create_security_list.call_args.args[0]

    upgrade_client = MagicMock()
    upgrade_region = _make_ipv6_region(upgrade_client)
    ipv4_only_sl = MagicMock()
    ipv4_only_sl.id = "ocid1.securitylist.oc1..legacy"
    ipv4_only_sl.ingress_security_rules = []
    ipv4_only_sl.egress_security_rules = []
    upgrade_region._find_security_list = MagicMock(return_value=ipv4_only_sl)
    upgrade_region._ensure_security_list_ipv6()
    upgraded = upgrade_client.update_security_list.call_args.args[1]

    assert _ipv6_rules(upgraded.ingress_security_rules) == _ipv6_rules(created.ingress_security_rules)
    assert _ipv6_rules(upgraded.egress_security_rules) == _ipv6_rules(created.egress_security_rules)


def test_ensure_security_list_ipv6_skips_update_when_already_configured():
    """A security list which already carries every IPv6 rule must not be updated again."""
    network_client = MagicMock()
    region = _make_ipv6_region(network_client)
    configured_sl = MagicMock()
    configured_sl.id = "ocid1.securitylist.oc1..configured"
    configured_sl.ingress_security_rules = [
        region._ipv6_ssh_ingress_rule(),
        region._ipv6_intra_vcn_ingress_rule(),
    ]
    configured_sl.egress_security_rules = [region._ipv6_egress_rule()]
    region._find_security_list = MagicMock(return_value=configured_sl)

    region._ensure_security_list_ipv6()

    network_client.update_security_list.assert_not_called()


@pytest.mark.parametrize(
    "rule, expected",
    [
        (IngressSecurityRule(protocol="6", source="0.0.0.0/0"), False),
        # NOTE: an unrelated rule sourced from '::/0' must not mask the missing SSH rule
        (IngressSecurityRule(protocol="17", source="::/0"), False),
        (
            IngressSecurityRule(
                protocol="6", source="::/0", tcp_options=TcpOptions(destination_port_range=PortRange(min=80, max=80))
            ),
            False,
        ),
        (IngressSecurityRule(protocol="all", source="::/0"), True),
        # NOTE: no 'tcp_options' means every TCP port, so port 22 is covered
        (IngressSecurityRule(protocol="6", source="::/0"), True),
        (
            IngressSecurityRule(
                protocol="6", source="::/0", tcp_options=TcpOptions(destination_port_range=PortRange(min=20, max=25))
            ),
            True,
        ),
    ],
)
def test_has_ipv6_ssh_ingress(rule, expected):
    assert OciRegion._has_ipv6_ssh_ingress([rule]) is expected

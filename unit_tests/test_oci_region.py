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

from unittest.mock import MagicMock, patch, PropertyMock

import pytest

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


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch("sdcm.utils.oci_region.oci.wait_until")
@patch("sdcm.utils.oci_region.OciRegion.network", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute", new_callable=PropertyMock)
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

    mock_vcn = MagicMock(spec_set=["id", "default_route_table_id", "default_dhcp_options_id", "lifecycle_state"])
    mock_vcn.id = "ocid1.vcn.oc1..123"
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
@patch("sdcm.utils.oci_region.OciRegion.network", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute", new_callable=PropertyMock)
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
@patch("sdcm.utils.oci_region.OciRegion.network", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute", new_callable=PropertyMock)
def test_subnet_name_suffix(mock_compute, mock_identity, mock_network, mock_compartment, mock_ads):
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = MagicMock()
    region = OciRegion("us-ashburn-1")

    assert region.subnet_name() == "SCT-2-subnet-regional-private"
    assert region.subnet_name(1) == "SCT-2-subnet-regional-1-private"
    assert region.subnet_name(5, False) == "SCT-2-subnet-regional-5-private"
    assert region.subnet_name(3, True) == "SCT-2-subnet-regional-3-public"


@patch("sdcm.utils.oci_region.SUPPORTED_REGIONS", new=["us-ashburn-1"])
@patch("sdcm.utils.oci_region.OciRegion.compartment_id", return_value="ocid1.compartment.oc1..test")
@patch("sdcm.utils.oci_region.OciRegion.network", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.identity", new_callable=PropertyMock)
@patch("sdcm.utils.oci_region.OciRegion.compute", new_callable=PropertyMock)
def test_region_validation(mock_compute, mock_identity, mock_network, mock_compartment):
    mock_compute.return_value = MagicMock()
    mock_identity.return_value = MagicMock()
    mock_network.return_value = MagicMock()

    with pytest.raises(ValueError):
        OciRegion("us-phoenix-1")

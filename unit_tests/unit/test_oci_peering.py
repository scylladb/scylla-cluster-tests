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

"""Unit tests for OCI VCN peering (sdcm.utils.oci_peering) and OciRegion DRG methods."""

from ipaddress import ip_network
from unittest.mock import MagicMock, PropertyMock, patch

from sdcm.utils.oci_peering import OciVcnPeering
from sdcm.utils.oci_region import OciRegion
from sdcm.utils.oci_utils import SUPPORTED_REGIONS


# ---------------------------------------------------------------------------
# OciVcnPeering.__init__
# ---------------------------------------------------------------------------


@patch("sdcm.utils.oci_peering.OciRegion", side_effect=lambda r: MagicMock(region_name=r))
def test_peering_init_default_regions(mock_region_cls):
    """Default constructor creates region objects (one per SUPPORTED_REGIONS entry)."""
    peering = OciVcnPeering()

    assert len(peering.regions) == len(SUPPORTED_REGIONS)
    assert mock_region_cls.call_count == len(SUPPORTED_REGIONS)
    for region_name in SUPPORTED_REGIONS:
        mock_region_cls.assert_any_call(region_name)


@patch("sdcm.utils.oci_peering.OciRegion", side_effect=lambda r: MagicMock(region_name=r))
def test_peering_init_custom_regions(mock_region_cls):
    """Custom region list creates only requested regions."""
    peering = OciVcnPeering(regions=["us-ashburn-1"])

    assert len(peering.regions) == 1
    mock_region_cls.assert_called_once_with("us-ashburn-1")
    assert peering.regions[0].region_name == "us-ashburn-1"


# ---------------------------------------------------------------------------
# OciVcnPeering.peering_connection
# ---------------------------------------------------------------------------


@patch(
    "sdcm.utils.oci_region.OciRegion.get_or_create_drg_attachment",
    return_value=MagicMock(id="ocid1.drgattachment.oc1..fake"),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.drg",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.drg.oc1..fake"),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_peering_connection_already_peered_skips_connect(
    mock_network,
    mock_drg,
    mock_drg_att,
):
    """When RPCs are already PEERED, no connect call is made."""
    origin = OciRegion("us-ashburn-1")
    target = OciRegion("us-phoenix-1")
    origin_rpc = MagicMock(id="ocid1.rpc.origin", peering_status="PEERED")
    target_rpc = MagicMock(id="ocid1.rpc.target", peering_status="PEERED")
    origin.get_or_create_rpc = MagicMock(return_value=origin_rpc)
    target.get_or_create_rpc = MagicMock(return_value=target_rpc)

    result_origin, result_target = OciVcnPeering.peering_connection(origin, target)

    assert result_origin is origin_rpc
    assert result_target is target_rpc
    origin.get_or_create_rpc.assert_called_once_with("us-phoenix-1")
    target.get_or_create_rpc.assert_called_once_with("us-ashburn-1")
    origin.network_client.connect_remote_peering_connections.assert_not_called()
    target.network_client.connect_remote_peering_connections.assert_not_called()
    assert mock_drg_att.call_count == 2


@patch("sdcm.utils.oci_peering.oci.wait_until")
@patch(
    "sdcm.utils.oci_region.OciRegion.get_or_create_drg_attachment",
    return_value=MagicMock(id="ocid1.drgattachment.oc1..fake"),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.drg",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.drg.oc1..fake"),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_peering_connection_connects_rpcs_when_not_peered(
    mock_network,
    mock_drg,
    mock_drg_att,
    mock_wait,
):
    """When RPCs have status NEW, a connect call is issued."""
    origin = OciRegion("us-ashburn-1")
    target = OciRegion("us-phoenix-1")
    origin_rpc = MagicMock(id="ocid1.rpc.origin", peering_status="NEW")
    target_rpc = MagicMock(id="ocid1.rpc.target", peering_status="NEW")
    origin.get_or_create_rpc = MagicMock(return_value=origin_rpc)
    target.get_or_create_rpc = MagicMock(return_value=target_rpc)

    OciVcnPeering.peering_connection(origin, target)

    origin.get_or_create_rpc.assert_called_once_with("us-phoenix-1")
    target.get_or_create_rpc.assert_called_once_with("us-ashburn-1")
    origin.network_client.connect_remote_peering_connections.assert_called_once()
    call_kwargs = origin.network_client.connect_remote_peering_connections.call_args
    assert call_kwargs.kwargs["remote_peering_connection_id"] == "ocid1.rpc.origin"
    connect_details = call_kwargs.kwargs["connect_remote_peering_connections_details"]
    assert connect_details.peer_id == "ocid1.rpc.target"
    assert connect_details.peer_region_name == "us-phoenix-1"
    mock_wait.assert_called_once()
    assert mock_drg_att.call_count == 2


# ---------------------------------------------------------------------------
# OciVcnPeering.configure_route_tables / open_security_list
# ---------------------------------------------------------------------------


@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_configure_route_tables_delegates_to_region(mock_network):
    """configure_route_tables calls add_drg_route_to_tables on origin with target CIDR."""
    origin = OciRegion("us-ashburn-1")
    target = OciRegion("us-phoenix-1")
    origin.add_drg_route_to_tables = MagicMock()
    target.add_drg_route_to_tables = MagicMock()

    OciVcnPeering.configure_route_tables(origin, target)

    origin.add_drg_route_to_tables.assert_called_once_with(peer_vcn_cidr=str(target.vcn_ipv4_cidr))
    target.add_drg_route_to_tables.assert_not_called()


@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_open_security_list_delegates_to_region(mock_network):
    """open_security_list calls allow_cross_vcn_traffic on origin with target CIDR."""
    origin = OciRegion("us-ashburn-1")
    target = OciRegion("us-phoenix-1")
    origin.allow_cross_vcn_traffic = MagicMock()
    target.allow_cross_vcn_traffic = MagicMock()

    OciVcnPeering.open_security_list(origin, target)

    origin.allow_cross_vcn_traffic.assert_called_once_with(peer_vcn_cidr=str(target.vcn_ipv4_cidr))
    target.allow_cross_vcn_traffic.assert_not_called()


# ---------------------------------------------------------------------------
# OciVcnPeering.configure
# ---------------------------------------------------------------------------


@patch("sdcm.utils.oci_peering.OciVcnPeering.peering_connection")
@patch("sdcm.utils.oci_peering.OciVcnPeering.configure_route_tables")
@patch("sdcm.utils.oci_peering.OciVcnPeering.open_security_list")
@patch("sdcm.utils.oci_peering.OciRegion", return_value=MagicMock(region_name="us-ashburn-1"))
def test_configure_single_region_skips_peering(mock_region_cls, mock_security, mock_routes, mock_peer):
    """With only one region, configure() does nothing."""
    peering = OciVcnPeering(regions=["us-ashburn-1"])

    peering.configure()

    mock_region_cls.assert_called_once_with("us-ashburn-1")
    mock_peer.assert_not_called()
    mock_routes.assert_not_called()
    mock_security.assert_not_called()


@patch("sdcm.utils.oci_peering.OciVcnPeering.open_security_list")
@patch("sdcm.utils.oci_peering.OciVcnPeering.configure_route_tables")
@patch("sdcm.utils.oci_peering.OciVcnPeering.peering_connection")
@patch(
    "sdcm.utils.oci_peering.OciRegion",
    side_effect=[
        MagicMock(region_name="us-ashburn-1", vcn_ipv4_cidr=ip_network("10.0.0.0/16")),
        MagicMock(region_name="us-phoenix-1", vcn_ipv4_cidr=ip_network("10.1.0.0/16")),
    ],
)
def test_configure_two_regions_configures_peering(mock_region_cls, mock_peer, mock_routes, mock_security):
    """With two regions, configure() establishes peering and updates routes+security in both directions."""
    peering = OciVcnPeering(regions=["us-ashburn-1", "us-phoenix-1"])

    peering.configure()

    assert mock_region_cls.call_count == 2
    mock_region_cls.assert_any_call("us-ashburn-1")
    mock_region_cls.assert_any_call("us-phoenix-1")
    r1, r2 = peering.regions
    mock_peer.assert_called_once_with(r1, r2)
    assert mock_routes.call_count == 2
    mock_routes.assert_any_call(r1, r2)
    mock_routes.assert_any_call(r2, r1)
    assert mock_security.call_count == 2
    mock_security.assert_any_call(r1, r2)
    mock_security.assert_any_call(r2, r1)


# ---------------------------------------------------------------------------
# OciRegion — DRG creation
# ---------------------------------------------------------------------------


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch("sdcm.utils.oci_region.oci.wait_until", side_effect=lambda client, resp, **kwargs: resp)
@patch("sdcm.utils.oci_region.oci.pagination.list_call_get_all_results_generator", return_value=iter([]))
@patch(
    "sdcm.utils.oci_region.OciRegion.compartment_id",
    new_callable=PropertyMock,
    return_value="ocid1.compartment.oc1..test",
)
@patch(
    "sdcm.utils.oci_utils.OciService.get_network_client",
    return_value=MagicMock(
        create_drg=MagicMock(
            return_value=MagicMock(data=MagicMock(id="ocid1.drg.oc1..new", lifecycle_state="AVAILABLE")),
        ),
        get_drg=MagicMock(
            return_value=MagicMock(data=MagicMock(id="ocid1.drg.oc1..new", lifecycle_state="AVAILABLE")),
        ),
    ),
)
def test_drg_create_when_none_exists(mock_network, mock_comp, mock_paginator, mock_wait, mock_user):
    """When no DRG exists, accessing .drg creates one via the network client."""
    region = OciRegion("us-ashburn-1")

    drg = region.drg

    assert drg.id == "ocid1.drg.oc1..new"
    assert drg.lifecycle_state == "AVAILABLE"
    region.network_client.create_drg.assert_called_once()
    region.network_client.get_drg.assert_called_once()
    mock_paginator.assert_called_once()
    mock_wait.assert_called_once()


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch(
    "sdcm.utils.oci_region.oci.pagination.list_call_get_all_results_generator",
    return_value=iter([MagicMock(display_name="SCT-2-drg", lifecycle_state="AVAILABLE", id="ocid1.drg.oc1..existing")]),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.compartment_id",
    new_callable=PropertyMock,
    return_value="ocid1.compartment.oc1..test",
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_drg_find_existing(mock_network, mock_comp, mock_paginator, mock_user):
    """When a matching DRG already exists, .drg returns it without creating."""
    region = OciRegion("us-ashburn-1")

    drg = region.drg

    assert drg.id == "ocid1.drg.oc1..existing"
    assert drg.display_name == "SCT-2-drg"
    region.network_client.create_drg.assert_not_called()
    mock_paginator.assert_called_once()


# ---------------------------------------------------------------------------
# OciRegion — RPC creation
# ---------------------------------------------------------------------------


@patch("sdcm.utils.oci_region.get_username", return_value="tester")
@patch("sdcm.utils.oci_region.oci.wait_until", side_effect=lambda client, resp, **kwargs: resp)
@patch("sdcm.utils.oci_region.oci.pagination.list_call_get_all_results_generator", return_value=iter([]))
@patch(
    "sdcm.utils.oci_region.OciRegion.compartment_id",
    new_callable=PropertyMock,
    return_value="ocid1.compartment.oc1..test",
)
@patch(
    "sdcm.utils.oci_region.OciRegion.drg",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.drg.oc1..existing"),
)
@patch(
    "sdcm.utils.oci_utils.OciService.get_network_client",
    return_value=MagicMock(
        create_remote_peering_connection=MagicMock(
            return_value=MagicMock(data=MagicMock(id="ocid1.rpc.oc1..new", lifecycle_state="AVAILABLE")),
        ),
        get_remote_peering_connection=MagicMock(
            return_value=MagicMock(data=MagicMock(id="ocid1.rpc.oc1..new", lifecycle_state="AVAILABLE")),
        ),
    ),
)
def test_rpc_create_when_none_exists(mock_network, mock_drg, mock_comp, mock_paginator, mock_wait, mock_user):
    """When no RPC exists for the peer, get_or_create_rpc creates one."""
    region = OciRegion("us-ashburn-1")

    rpc = region.get_or_create_rpc("us-phoenix-1")

    assert rpc.id == "ocid1.rpc.oc1..new"
    assert rpc.lifecycle_state == "AVAILABLE"
    region.network_client.create_remote_peering_connection.assert_called_once()
    create_details = region.network_client.create_remote_peering_connection.call_args.args[0]
    assert create_details.display_name == "SCT-2-rpc-us-phoenix-1"
    assert create_details.drg_id == "ocid1.drg.oc1..existing"
    assert create_details.compartment_id == "ocid1.compartment.oc1..test"
    mock_paginator.assert_called_once()
    mock_wait.assert_called_once()


# ---------------------------------------------------------------------------
# OciRegion — route table management
# ---------------------------------------------------------------------------


@patch(
    "sdcm.utils.oci_region.OciRegion.private_route_table",
    new_callable=PropertyMock,
    return_value=MagicMock(
        id="ocid1.rt.oc1..private",
        route_rules=[MagicMock(destination="10.1.0.0/16", network_entity_id="ocid1.drg.oc1..123")],
    ),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.public_route_table",
    new_callable=PropertyMock,
    return_value=MagicMock(
        id="ocid1.rt.oc1..public",
        route_rules=[MagicMock(destination="10.1.0.0/16", network_entity_id="ocid1.drg.oc1..123")],
    ),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.drg",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.drg.oc1..123"),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_add_drg_route_to_tables_skips_if_exists(mock_network, mock_drg, mock_pub_rt, mock_priv_rt):
    """When a route for the peer CIDR already exists, no update call is made."""
    region = OciRegion("us-ashburn-1")

    region.add_drg_route_to_tables("10.1.0.0/16")

    region.network_client.update_route_table.assert_not_called()


@patch(
    "sdcm.utils.oci_region.OciRegion.private_route_table",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.rt.oc1..private", route_rules=[]),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.public_route_table",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.rt.oc1..public", route_rules=[]),
)
@patch(
    "sdcm.utils.oci_region.OciRegion.drg",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.drg.oc1..123"),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_add_drg_route_to_tables_adds_new_rule(mock_network, mock_drg, mock_pub_rt, mock_priv_rt):
    """When no matching route exists, a new route rule is added to both tables."""
    region = OciRegion("us-ashburn-1")

    region.add_drg_route_to_tables("10.1.0.0/16")

    assert region.network_client.update_route_table.call_count == 2
    for call_args in region.network_client.update_route_table.call_args_list:
        rt_id, update_details = call_args.args
        assert rt_id in ("ocid1.rt.oc1..public", "ocid1.rt.oc1..private")
        assert len(update_details.route_rules) == 1
        rule = update_details.route_rules[0]
        assert rule.destination == "10.1.0.0/16"
        assert rule.destination_type == "CIDR_BLOCK"
        assert rule.network_entity_id == "ocid1.drg.oc1..123"


# ---------------------------------------------------------------------------
# OciRegion — security list management
# ---------------------------------------------------------------------------


@patch(
    "sdcm.utils.oci_region.OciRegion.security_list",
    new_callable=PropertyMock,
    return_value=MagicMock(
        id="ocid1.sl.oc1..fake",
        ingress_security_rules=[MagicMock(source="10.1.0.0/16", protocol="all")],
    ),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_allow_cross_vcn_traffic_skips_if_exists(mock_network, mock_sl):
    """When an ingress rule for the peer CIDR already exists, no update call is made."""
    region = OciRegion("us-ashburn-1")

    region.allow_cross_vcn_traffic("10.1.0.0/16")

    region.network_client.update_security_list.assert_not_called()


@patch(
    "sdcm.utils.oci_region.OciRegion.security_list",
    new_callable=PropertyMock,
    return_value=MagicMock(id="ocid1.sl.oc1..fake", ingress_security_rules=[]),
)
@patch("sdcm.utils.oci_utils.OciService.get_network_client", return_value=MagicMock())
def test_allow_cross_vcn_traffic_adds_rule(mock_network, mock_sl):
    """When no matching rule exists, a new ingress rule is added."""
    region = OciRegion("us-ashburn-1")

    region.allow_cross_vcn_traffic("10.1.0.0/16")

    region.network_client.update_security_list.assert_called_once()
    update_args = region.network_client.update_security_list.call_args
    assert update_args.args[0] == "ocid1.sl.oc1..fake"
    updated_details = update_args.args[1]
    assert len(updated_details.ingress_security_rules) == 1
    rule = updated_details.ingress_security_rules[0]
    assert rule.source == "10.1.0.0/16"
    assert rule.protocol == "all"
    assert rule.description == "Allow all traffic from peer VCN 10.1.0.0/16"

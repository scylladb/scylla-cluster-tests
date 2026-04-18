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

import itertools
import logging

import oci
from oci.core.models import ConnectRemotePeeringConnectionsDetails

from sdcm.utils.oci_region import OciRegion
from sdcm.utils.oci_utils import SUPPORTED_REGIONS

LOG = logging.getLogger(__name__)


class OciVcnPeering:
    """Configure cross-region VCN peering for OCI using DRG + Remote Peering Connections."""

    def __init__(self, regions=None):
        regions = regions or SUPPORTED_REGIONS
        self.regions = [OciRegion(r) for r in regions]

    @staticmethod
    def peering_connection(origin_region: OciRegion, target_region: OciRegion):
        """Establish a Remote Peering Connection between two OCI regions.

        Creates DRG + DRG-VCN attachment in each region if they don't exist,
        then creates RPCs and connects them.

        Returns (origin_rpc, target_rpc).
        """
        # Ensure DRG infrastructure exists in both regions
        origin_region.get_or_create_drg_attachment()
        target_region.get_or_create_drg_attachment()

        origin_rpc = origin_region.get_or_create_rpc(target_region.region_name)
        target_rpc = target_region.get_or_create_rpc(origin_region.region_name)

        if origin_rpc.peering_status == "PEERED":
            LOG.info(
                "RPC between %s and %s is already PEERED",
                origin_region.region_name,
                target_region.region_name,
            )
            return origin_rpc, target_rpc

        LOG.info(
            "Connecting RPC %s (%s) -> %s (%s)",
            origin_rpc.id,
            origin_region.region_name,
            target_rpc.id,
            target_region.region_name,
        )
        try:
            origin_region.network_client.connect_remote_peering_connections(
                remote_peering_connection_id=origin_rpc.id,
                connect_remote_peering_connections_details=ConnectRemotePeeringConnectionsDetails(
                    peer_id=target_rpc.id,
                    peer_region_name=target_region.region_name,
                ),
            )
        except oci.exceptions.ServiceError as exc:
            if "InvalidState" in str(exc) or "already peered" in str(exc).lower():
                LOG.info(
                    "Peering already established between %s and %s",
                    origin_region.region_name,
                    target_region.region_name,
                )
            else:
                raise

        # Wait for PEERED status
        oci.wait_until(
            origin_region.network_client,
            origin_region.network_client.get_remote_peering_connection(origin_rpc.id),
            evaluate_response=lambda r: r.data.peering_status == "PEERED",
            max_wait_seconds=300,
            max_interval_seconds=10,
        )

        return origin_rpc, target_rpc

    @staticmethod
    def configure_route_tables(origin_region: OciRegion, target_region: OciRegion):
        """Add routes in origin_region pointing to target_region's VCN CIDR via DRG."""
        origin_region.add_drg_route_to_tables(peer_vcn_cidr=str(target_region.vcn_ipv4_cidr))

    @staticmethod
    def open_security_list(origin_region: OciRegion, target_region: OciRegion):
        """Allow all traffic from target_region's VCN CIDR in origin_region's security list."""
        origin_region.allow_cross_vcn_traffic(peer_vcn_cidr=str(target_region.vcn_ipv4_cidr))

    def configure(self):
        """Configure full cross-region peering for all region pairs."""
        if len(self.regions) < 2:
            LOG.info("Only %d region(s) configured, skipping peering setup", len(self.regions))
            return

        regions_pairs: list[tuple[OciRegion, OciRegion]] = list(itertools.combinations(self.regions, 2))
        for origin_region, target_region in regions_pairs:
            LOG.info(
                "Configure peering for: %s %s <-> %s %s",
                origin_region.region_name,
                origin_region.vcn_ipv4_cidr,
                target_region.region_name,
                target_region.vcn_ipv4_cidr,
            )
            self.peering_connection(origin_region, target_region)

            LOG.info("Configuring route tables")
            self.configure_route_tables(origin_region, target_region)
            self.configure_route_tables(target_region, origin_region)

            LOG.info("Configuring security lists")
            self.open_security_list(origin_region, target_region)
            self.open_security_list(target_region, origin_region)

        LOG.info("OCI VCN peering configured for all region pairs")

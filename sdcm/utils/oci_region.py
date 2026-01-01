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

"""OCI region utilities for SCT network resources.

This helper mirrors the AwsRegion/GceRegion helpers and centralizes network
resource discovery/creation (VCN, subnets, security lists) for a single OCI
region. It intentionally keeps the surface small; higher-level builders can
layer automation on top of these primitives.
"""

from __future__ import annotations

import logging
from ipaddress import ip_network
from functools import cached_property
from typing import Optional

import oci
from oci.core.models import (
    CreateSubnetDetails,
    CreateVcnDetails,
    CreateSecurityListDetails,
    CreateInternetGatewayDetails,
    UpdateRouteTableDetails,
    RouteRule,
    EgressSecurityRule,
    IngressSecurityRule,
    PortRange,
    TcpOptions,
)

from sdcm.utils.get_username import get_username
from sdcm.utils.oci_utils import (
    SUPPORTED_REGIONS,
    get_availability_domains,
    get_oci_compartment_id,
    get_oci_compute_client,
    get_oci_identity_client,
    get_oci_network_client,
)


LOGGER = logging.getLogger(__name__)


class OciRegion:
    """Encapsulate OCI region-scoped resources used by SCT."""

    SCT_VCN_NAME = "SCT-2-vcn"
    SCT_SECURITY_LIST_NAME = "SCT-2-sl"
    SCT_SUBNET_NAME_TMPL = "SCT-2-subnet-{ad}{index}"
    SCT_SUBNETS_PER_AD = 1
    SCT_VCN_CIDR_TMPL = "10.{}.0.0/16"

    def __init__(self, region_name: str):
        self.region_name = region_name
        self.compute_client, self._config = get_oci_compute_client(region=region_name)
        self.network_client, _ = get_oci_network_client(region=region_name)
        self.identity_client, _ = get_oci_identity_client(region=region_name)
        self.compartment_id = get_oci_compartment_id()

        region_index = self._region_index()
        self.vcn_ipv4_cidr = ip_network(self.SCT_VCN_CIDR_TMPL.format(region_index))
        self._vcn_ipv6_cidr = None

    def _region_index(self) -> int:
        if self.region_name not in SUPPORTED_REGIONS:
            raise ValueError(f"Region '{self.region_name}' is not in supported list: {SUPPORTED_REGIONS}")
        return SUPPORTED_REGIONS.index(self.region_name)

    @cached_property
    def availability_domains(self) -> list[str]:
        return get_availability_domains(self.compartment_id, region=self.region_name)

    @cached_property
    def vcn(self):
        existing = self._find_vcn()
        if existing:
            self._cache_vcn_ipv6_cidr(existing)
            return existing
        return self._create_vcn()

    def _find_vcn(self):
        vcns = self.network_client.list_vcns(compartment_id=self.compartment_id, display_name=self.SCT_VCN_NAME).data
        for vcn in vcns:
            if vcn.lifecycle_state == "AVAILABLE":
                return vcn
        return None

    def _create_vcn(self):
        LOGGER.info("Creating VCN '%s' in %s", self.SCT_VCN_NAME, self.region_name)
        details = CreateVcnDetails(
            cidr_block=str(self.vcn_ipv4_cidr),
            compartment_id=self.compartment_id,
            display_name=self.SCT_VCN_NAME,
            dns_label=self._dns_label_from_name(self.SCT_VCN_NAME),
            freeform_tags=self._base_tags(self.SCT_VCN_NAME),
            is_ipv6_enabled=True,
        )
        response = self.network_client.create_vcn(details)
        vcn = self._wait_for_state(self.network_client, self.network_client.get_vcn, response.data.id, "AVAILABLE")
        self._cache_vcn_ipv6_cidr(vcn)
        return vcn

    def _cache_vcn_ipv6_cidr(self, vcn):
        """Cache the VCN's IPv6 CIDR block if available."""
        blocks = getattr(vcn, "ipv6_cidr_blocks", None) or []
        self._vcn_ipv6_cidr = ip_network(blocks[0]) if blocks else None
        if self._vcn_ipv6_cidr:
            LOGGER.info("VCN IPv6 CIDR: %s", self._vcn_ipv6_cidr)
        else:
            LOGGER.warning("No IPv6 CIDR block found for VCN %s", vcn.id)

    @cached_property
    def security_list(self):
        existing = self._find_security_list()
        if existing:
            return existing
        return self._create_security_list()

    def _find_security_list(self):
        lists = self.network_client.list_security_lists(
            compartment_id=self.compartment_id, vcn_id=self.vcn.id, display_name=self.SCT_SECURITY_LIST_NAME
        ).data
        for sec_list in lists:
            if sec_list.lifecycle_state == "AVAILABLE":
                return sec_list
        return None

    def _create_security_list(self):
        LOGGER.info("Creating security list '%s'", self.SCT_SECURITY_LIST_NAME)
        ssh_rule = IngressSecurityRule(
            protocol="6",  # TCP
            source="0.0.0.0/0",
            tcp_options=TcpOptions(destination_port_range=PortRange(min=22, max=22)),
            description="Allow SSH from anywhere",
        )
        ssh_rule_v6 = IngressSecurityRule(
            protocol="6",  # TCP
            source="::/0",
            source_type="CIDR_BLOCK",
            tcp_options=TcpOptions(destination_port_range=PortRange(min=22, max=22)),
            description="Allow SSH from anywhere (IPv6)",
        )
        intra_vcn_rule = IngressSecurityRule(
            protocol="all",
            source=str(self.vcn_ipv4_cidr),
            description="Allow all traffic within VCN",
        )
        intra_vcn_rule_v6 = None
        if self.vcn_ipv6_cidr:
            intra_vcn_rule_v6 = IngressSecurityRule(
                protocol="all",
                source=str(self.vcn_ipv6_cidr),
                source_type="CIDR_BLOCK",
                description="Allow all IPv6 traffic within VCN",
            )
        egress_rule = EgressSecurityRule(
            protocol="all",
            destination="0.0.0.0/0",
            description="Allow all outbound traffic",
        )
        egress_rule_v6 = None
        if self.vcn_ipv6_cidr:
            egress_rule_v6 = EgressSecurityRule(
                protocol="all",
                destination="::/0",
                destination_type="CIDR_BLOCK",
                description="Allow all outbound IPv6 traffic",
            )
        ingress_rules = [ssh_rule, ssh_rule_v6, intra_vcn_rule]
        if intra_vcn_rule_v6:
            ingress_rules.append(intra_vcn_rule_v6)
        egress_rules = [egress_rule]
        if egress_rule_v6:
            egress_rules.append(egress_rule_v6)
        details = CreateSecurityListDetails(
            compartment_id=self.compartment_id,
            display_name=self.SCT_SECURITY_LIST_NAME,
            vcn_id=self.vcn.id,
            ingress_security_rules=ingress_rules,
            egress_security_rules=egress_rules,
            freeform_tags=self._base_tags(self.SCT_SECURITY_LIST_NAME),
        )
        response = self.network_client.create_security_list(details)
        return self._wait_for_state(
            self.network_client, self.network_client.get_security_list, response.data.id, "AVAILABLE"
        )

    @property
    def vcn_ipv6_cidr(self):
        if self._vcn_ipv6_cidr is None and getattr(self.vcn, "ipv6_cidr_blocks", None):
            self._cache_vcn_ipv6_cidr(self.vcn)
        return self._vcn_ipv6_cidr

    @cached_property
    def internet_gateway(self):
        existing = self._find_internet_gateway()
        if existing:
            self._add_internet_gateway_route(existing)
            return existing
        return self._create_internet_gateway()

    def _find_internet_gateway(self):
        """Find existing internet gateway for this VCN."""
        igws = self.network_client.list_internet_gateways(compartment_id=self.compartment_id, vcn_id=self.vcn.id).data
        for igw in igws:
            if igw.lifecycle_state == "AVAILABLE":
                return igw
        return None

    def _create_internet_gateway(self):
        """Create internet gateway for the VCN."""
        LOGGER.info("Creating internet gateway for VCN '%s'", self.SCT_VCN_NAME)
        details = CreateInternetGatewayDetails(
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            is_enabled=True,
            display_name=f"{self.SCT_VCN_NAME}-igw",
            freeform_tags=self._base_tags(f"{self.SCT_VCN_NAME}-igw"),
        )
        response = self.network_client.create_internet_gateway(details)
        igw = self._wait_for_state(
            self.network_client, self.network_client.get_internet_gateway, response.data.id, "AVAILABLE"
        )

        # Add route to internet gateway in the VCN's default route table
        self._add_internet_gateway_route(igw)

        return igw

    def _add_internet_gateway_route(self, internet_gateway):
        """Add a route rule to the default route table for the internet gateway."""
        route_table = self.vcn.default_route_table_id
        LOGGER.info("Adding internet gateway route to default route table")

        # Get current route table
        rt = self.network_client.get_route_table(route_table).data

        # Check if route to internet gateway already exists
        existing_rules = {(route.network_entity_id, route.destination) for route in rt.route_rules}

        new_route_rules = list(rt.route_rules) if rt.route_rules else []
        if (internet_gateway.id, "0.0.0.0/0") not in existing_rules:
            new_route_rules.append(
                RouteRule(
                    destination="0.0.0.0/0",
                    destination_type="CIDR_BLOCK",
                    network_entity_id=internet_gateway.id,
                    description="Route to internet gateway",
                )
            )
        if self.vcn_ipv6_cidr and (internet_gateway.id, "::/0") not in existing_rules:
            new_route_rules.append(
                RouteRule(
                    destination="::/0",
                    destination_type="CIDR_BLOCK",
                    network_entity_id=internet_gateway.id,
                    description="Route to internet gateway (IPv6)",
                )
            )

        if len(new_route_rules) == len(rt.route_rules or []):
            LOGGER.info("Internet gateway routes already exist")
            return

        update_details = UpdateRouteTableDetails(route_rules=new_route_rules)
        self.network_client.update_route_table(route_table, update_details)
        LOGGER.info("Internet gateway route added successfully")

    def subnet_name(self, ad: str, subnet_index: Optional[int] = None) -> str:
        suffix = f"-{subnet_index}" if subnet_index is not None else ""
        return self.SCT_SUBNET_NAME_TMPL.format(ad=self._short_ad(ad), index=suffix)

    def subnet(self, ad: str, subnet_index: Optional[int] = None):
        name = self.subnet_name(ad, subnet_index=subnet_index)
        subnets = self.network_client.list_subnets(
            compartment_id=self.compartment_id, vcn_id=self.vcn.id, display_name=name
        ).data
        for subnet in subnets:
            if subnet.lifecycle_state == "AVAILABLE":
                return subnet
        return None

    def create_subnet(self, ad: str, ipv4_cidr, ipv6_cidr, subnet_index: Optional[int] = None):
        name = self.subnet_name(ad, subnet_index=subnet_index)
        if self.subnet(ad, subnet_index=subnet_index):
            LOGGER.info("Subnet '%s' already exists in %s", name, ad)
            return
        LOGGER.info("Creating subnet '%s' in %s", name, ad)
        details = CreateSubnetDetails(
            availability_domain=ad,
            cidr_block=str(ipv4_cidr),
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=name,
            prohibit_public_ip_on_vnic=False,
            security_list_ids=[self.security_list.id],
            route_table_id=self.vcn.default_route_table_id,
            dhcp_options_id=self.vcn.default_dhcp_options_id,
            freeform_tags=self._base_tags(name),
            ipv6_cidr_block=str(ipv6_cidr),
        )
        response = self.network_client.create_subnet(details)
        self._wait_for_state(self.network_client, self.network_client.get_subnet, response.data.id, "AVAILABLE")

    def ensure_subnets(self):
        cidr_iter = self.vcn_ipv4_cidr.subnets(prefixlen_diff=8)
        ipv6_iter = None
        if self.vcn_ipv6_cidr:
            ipv6_iter = self.vcn_ipv6_cidr.subnets(prefixlen_diff=8)
        for _ in range(self.SCT_SUBNETS_PER_AD):
            for ad in self.availability_domains:
                ipv6_cidr = next(ipv6_iter) if ipv6_iter else None
                self.create_subnet(ad=ad, ipv4_cidr=next(cidr_iter), ipv6_cidr=ipv6_cidr, subnet_index=None)

    def configure_network(self):
        # Ensure base resources exist
        _ = self.vcn
        _ = self.security_list
        _ = self.internet_gateway  # Create internet gateway for outbound connectivity
        self.ensure_subnets()

    def configure(self):
        """Configure all required resources for SCT in this OCI region."""
        LOGGER.info("Configuring '%s' region...", self.region_name)
        self.configure_network()
        LOGGER.info("Region configured successfully.")

    @staticmethod
    def _short_ad(ad_name: str) -> str:
        # OCI ADs typically end with "-AD-1", "-AD-2". Keep a compact suffix for naming.
        return ad_name.split("-AD-")[-1].lower()

    @staticmethod
    def _dns_label_from_name(name: str) -> str:
        return name.lower().replace("-", "")[:15]

    @staticmethod
    def _base_tags(name: str) -> dict:
        return {"Name": name, "Owner": get_username()}

    @staticmethod
    def _wait_for_state(client, getter, resource_id: str, target_state: str):
        response = oci.wait_until(
            client,
            getter(resource_id),
            evaluate_response=lambda r: getattr(r.data, "lifecycle_state", None) == target_state,
            max_wait_seconds=300,
            max_interval_seconds=10,
        )
        return response.data

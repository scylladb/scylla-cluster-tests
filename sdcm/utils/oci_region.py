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
import time
from ipaddress import ip_network
from functools import cached_property
from typing import Optional

import oci
from oci.core.models import (
    AddNetworkSecurityGroupSecurityRulesDetails,
    AddSecurityRuleDetails,
    CreateInternetGatewayDetails,
    CreateNatGatewayDetails,
    CreateNetworkSecurityGroupDetails,
    CreateRouteTableDetails,
    CreateSecurityListDetails,
    CreateSubnetDetails,
    CreateVcnDetails,
    EgressSecurityRule,
    IngressSecurityRule,
    PortRange,
    RouteRule,
    TcpOptions,
    UpdateRouteTableDetails,
)
from oci.identity.models import (
    CreateTagDetails,
    CreateTagNamespaceDetails,
)

from sdcm.provision.security import ScyllaOpenPorts
from sdcm.utils.get_username import get_username
from sdcm.utils.lock_utils import KeyBasedLock
from sdcm.utils.oci_utils import (
    OciService,
    SUPPORTED_REGIONS,
    get_availability_domains,
)
from sdcm.provision.oci.constants import (
    SCT_TAG_KEYS,
    TAG_NAMESPACE,
)


KEY_BASED_LOCKS = KeyBasedLock()
LOGGER = logging.getLogger(__name__)


class OciRegion:
    """Encapsulate OCI region-scoped resources used by SCT."""

    SCT_VCN_NAME = "SCT-2-vcn"
    SCT_SECURITY_LIST_NAME = "SCT-2-sl"
    SCT_SUBNET_NAME_TMPL = "SCT-2-subnet-regional{index}"
    SCT_VCN_CIDR_TMPL = "10.{}.0.0/16"

    def __init__(self, region_name: str):
        self.region_name = region_name
        region_index = self._region_index()
        self.vcn_ipv4_cidr = ip_network(self.SCT_VCN_CIDR_TMPL.format(region_index))
        self._vcn_ipv6_cidr = None

    @cached_property
    def _config(self):
        return self.oci_service.config

    @cached_property
    def oci_service(self) -> OciService:
        return OciService()

    @cached_property
    def compartment_id(self) -> str:
        return self.oci_service.compartment_id

    @cached_property
    def compute(self):
        return self.oci_service.get_compute_client(self.region_name)

    @cached_property
    def identity(self):
        return self.oci_service.get_identity_client(self.region_name)

    @cached_property
    def network(self):
        return self.oci_service.get_network_client(self.region_name)

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
        vcns = self.network.list_vcns(compartment_id=self.compartment_id, display_name=self.SCT_VCN_NAME).data
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
        response = self.network.create_vcn(details)
        vcn = self._wait_for_state(self.network, self.network.get_vcn, response.data.id, "AVAILABLE")
        self._cache_vcn_ipv6_cidr(vcn)
        return vcn

    @cached_property
    def public_route_table(self):
        """Route table for public subnets (IGW)."""
        with KEY_BASED_LOCKS.get_lock("oci--public_route_table"):
            existing = self._find_route_table(f"{self.SCT_VCN_NAME}-rt-public")
            if existing:
                return existing
            return self._create_public_route_table()

    @cached_property
    def private_route_table(self):
        """Route table for private subnets (NAT)."""
        with KEY_BASED_LOCKS.get_lock("oci--private_route_table"):
            existing = self._find_route_table(f"{self.SCT_VCN_NAME}-rt-private")
            if existing:
                return existing
            return self._create_private_route_table()

    def _create_private_route_table(self):
        display_name = f"{self.SCT_VCN_NAME}-rt-private"
        LOGGER.info("Creating private route table '%s'", display_name)

        # Ensure NAT Gateway exists
        ngw = self.nat_gateway
        rules = [
            RouteRule(
                destination="0.0.0.0/0",
                destination_type="CIDR_BLOCK",
                network_entity_id=ngw.id,
                description="Route to NAT Gateway",
            )
        ]
        details = CreateRouteTableDetails(
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=display_name,
            route_rules=rules,
            freeform_tags=self._base_tags(display_name),
        )
        response = self.network.create_route_table(details)
        return self._wait_for_state(self.network, self.network.get_route_table, response.data.id, "AVAILABLE")

    def _find_route_table(self, display_name):
        rts = oci.pagination.list_call_get_all_results_generator(
            self.network.list_route_tables,
            yield_mode="record",
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=display_name,
        )
        while current_rt := next(rts, None):
            if current_rt.lifecycle_state == "AVAILABLE":
                return current_rt
        return None

    def _create_public_route_table(self):
        display_name = f"{self.SCT_VCN_NAME}-rt-public"
        LOGGER.info("Creating public route table '%s'", display_name)

        # Ensure IGW exists
        igw = self.internet_gateway
        rules = [
            RouteRule(
                destination="0.0.0.0/0",
                destination_type="CIDR_BLOCK",
                network_entity_id=igw.id,
                description="Route to Internet Gateway",
            )
        ]
        if self.vcn_ipv6_cidr:
            rules.append(
                RouteRule(
                    destination="::/0",
                    destination_type="CIDR_BLOCK",
                    network_entity_id=igw.id,
                    description="Route to Internet Gateway (IPv6)",
                )
            )
        details = CreateRouteTableDetails(
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=display_name,
            route_rules=rules,
            freeform_tags=self._base_tags(display_name),
        )
        response = self.network.create_route_table(details)
        return self._wait_for_state(self.network, self.network.get_route_table, response.data.id, "AVAILABLE")

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
        with KEY_BASED_LOCKS.get_lock("oci--security-list"):
            existing = self._find_security_list()
            if existing:
                return existing
            return self._create_security_list()

    def _find_security_list(self):
        sec_lists = oci.pagination.list_call_get_all_results_generator(
            self.network.list_security_lists,
            yield_mode="record",
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=self.SCT_SECURITY_LIST_NAME,
        )
        while current_sec_list := next(sec_lists, None):
            if current_sec_list.lifecycle_state == "AVAILABLE":
                return current_sec_list
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
        response = self.network.create_security_list(details)
        return self._wait_for_state(self.network, self.network.get_security_list, response.data.id, "AVAILABLE")

    def get_or_create_nsg(self, node_type: str):
        suffix = "common"
        if "monitor" in node_type:
            suffix = "monitor"
        elif "loader" in node_type:
            suffix = "loader"
        elif "scylla-db" in node_type or "oracle-db" in node_type:
            suffix = "db"
        nsg_name = f"{self.SCT_VCN_NAME}-nsg-{suffix}"
        with KEY_BASED_LOCKS.get_lock(f"oci-nsg--{nsg_name}"):
            existing = self._find_nsg(nsg_name)
            if existing:
                return existing
            return self._create_nsg(nsg_name, suffix)

    def _find_nsg(self, display_name, _cache={}):  # noqa: B006
        if existing := _cache.get(display_name):
            return existing
        nsgs = oci.pagination.list_call_get_all_results_generator(
            self.network.list_network_security_groups,
            yield_mode="record",
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=display_name,
        )
        while current_nsg := next(nsgs, None):
            if current_nsg.lifecycle_state == "AVAILABLE":
                _cache[display_name] = current_nsg
                return current_nsg
        return None

    def _create_nsg(self, display_name, role):
        LOGGER.info("Creating NSG '%s' for role '%s'", display_name, role)
        details = CreateNetworkSecurityGroupDetails(
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=display_name,
            freeform_tags=self._base_tags(display_name),
        )
        nsg = self.network.create_network_security_group(details).data
        nsg = self._wait_for_state(self.network, self.network.get_network_security_group, nsg.id, "AVAILABLE")

        LOGGER.info("Adding security rules to NSG '%s'", display_name)
        ports_to_open = set()
        if role == "db":
            ports_to_open.update(
                [
                    ScyllaOpenPorts.CQL.value,
                    ScyllaOpenPorts.CQL_SSL.value,
                    ScyllaOpenPorts.CQL_SHARD_AWARE.value,
                    ScyllaOpenPorts.RPC.value,
                    ScyllaOpenPorts.RPC_SSL.value,
                    ScyllaOpenPorts.JMX_MGMT.value,
                    ScyllaOpenPorts.NODE_EXPORTER.value,
                    ScyllaOpenPorts.ALTERNATOR.value,
                    ScyllaOpenPorts.PROMETHEUS_API.value,
                    ScyllaOpenPorts.MANAGER_AGENT.value,
                    ScyllaOpenPorts.REST_API.value,
                    ScyllaOpenPorts.MANAGER_AGENT_PROMETHEUS.value,
                    10000,
                    9142,
                    19142,
                ]
            )
        elif role == "monitor":
            ports_to_open.update(
                [
                    ScyllaOpenPorts.GRAFANA.value,
                    ScyllaOpenPorts.PROMETHEUS.value,
                    ScyllaOpenPorts.NODE_EXPORTER.value,
                    9093,  # AlertManager
                ]
            )
        elif role == "loader":
            ports_to_open.update([ScyllaOpenPorts.NODE_EXPORTER.value])

        # Common ports
        ports_to_open.add(ScyllaOpenPorts.VECTOR.value)

        LOGGER.info("DEBUG: ports_to_open: %s", ports_to_open)
        ipv4_rules_details = []
        ipv6_rules_details = []
        for port in ports_to_open:
            ipv4_rules_details.append(
                AddSecurityRuleDetails(
                    direction="INGRESS",
                    protocol="6",  # TCP
                    source="0.0.0.0/0",
                    tcp_options=TcpOptions(destination_port_range=PortRange(min=port, max=port)),
                    description=f"Allow TCP {port} from anywhere",
                )
            )
            ipv6_rules_details.append(
                AddSecurityRuleDetails(
                    direction="INGRESS",
                    protocol="6",  # TCP
                    source="::/0",
                    source_type="CIDR_BLOCK",
                    tcp_options=TcpOptions(destination_port_range=PortRange(min=port, max=port)),
                    description=f"Allow TCP {port} from anywhere (IPv6)",
                )
            )

        if ipv4_rules_details:
            self.network.add_network_security_group_security_rules(
                network_security_group_id=nsg.id,
                add_network_security_group_security_rules_details=AddNetworkSecurityGroupSecurityRulesDetails(
                    security_rules=ipv4_rules_details
                ),
            )
        if ipv6_rules_details:
            self.network.add_network_security_group_security_rules(
                network_security_group_id=nsg.id,
                add_network_security_group_security_rules_details=AddNetworkSecurityGroupSecurityRulesDetails(
                    security_rules=ipv6_rules_details
                ),
            )

        return nsg

    @property
    def vcn_ipv6_cidr(self):
        if self._vcn_ipv6_cidr is None and getattr(self.vcn, "ipv6_cidr_blocks", None):
            self._cache_vcn_ipv6_cidr(self.vcn)
        return self._vcn_ipv6_cidr

    @cached_property
    def internet_gateway(self):
        with KEY_BASED_LOCKS.get_lock("oci--internet-gateway"):
            existing = self._find_internet_gateway()
            if existing:
                return existing
            return self._create_internet_gateway()

    def _find_internet_gateway(self):
        """Find existing internet gateway for the VCN."""
        inet_gateways = oci.pagination.list_call_get_all_results_generator(
            self.network.list_internet_gateways,
            yield_mode="record",
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
        )
        while current_inet_gateway := next(inet_gateways, None):
            if current_inet_gateway.lifecycle_state == "AVAILABLE":
                return current_inet_gateway
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
        response = self.network.create_internet_gateway(details)
        igw = self._wait_for_state(self.network, self.network.get_internet_gateway, response.data.id, "AVAILABLE")

        return igw

    @cached_property
    def nat_gateway(self):
        with KEY_BASED_LOCKS.get_lock("oci--nat-gateway"):
            existing = self._find_nat_gateway()
            if existing:
                return existing
            return self._create_nat_gateway()

    def _find_nat_gateway(self):
        """Find existing nat gateway for the VCN."""
        nat_gateways = oci.pagination.list_call_get_all_results_generator(
            self.network.list_nat_gateways,
            yield_mode="record",
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
        )
        while current_nat_gateway := next(nat_gateways, None):
            if current_nat_gateway.lifecycle_state == "AVAILABLE":
                return current_nat_gateway
        return None

    def _create_nat_gateway(self):
        """Create nat gateway for the VCN."""
        LOGGER.info("Creating nat gateway for VCN '%s'", self.SCT_VCN_NAME)
        details = CreateNatGatewayDetails(
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            block_traffic=False,
            display_name=f"{self.SCT_VCN_NAME}-ngw",
            freeform_tags=self._base_tags(f"{self.SCT_VCN_NAME}-ngw"),
        )
        response = self.network.create_nat_gateway(details)
        ngw = self._wait_for_state(self.network, self.network.get_nat_gateway, response.data.id, "AVAILABLE")
        return ngw

    def _add_nat_gateway_route(self, nat_gateway):
        """Add a route rule to the default route table for the nat gateway."""
        route_table = self.vcn.default_route_table_id
        LOGGER.info("Adding nat gateway route to default route table")

        # Get current route table
        rt = self.network.get_route_table(route_table).data

        # Check if route to nat gateway already exists
        existing_rules = {(route.network_entity_id, route.destination) for route in rt.route_rules}

        new_route_rules = list(rt.route_rules) if rt.route_rules else []
        if (nat_gateway.id, "0.0.0.0/0") not in existing_rules:
            new_route_rules.append(
                RouteRule(
                    destination="0.0.0.0/0",
                    destination_type="CIDR_BLOCK",
                    network_entity_id=nat_gateway.id,
                    description="Route to nat gateway",
                )
            )

        if len(new_route_rules) == len(rt.route_rules or []):
            LOGGER.info("Nat gateway routes already exist")
            return

        update_details = UpdateRouteTableDetails(route_rules=new_route_rules)
        self.network.update_route_table(route_table, update_details)
        LOGGER.info("Nat gateway route added successfully")

    def subnet_name(self, subnet_index: Optional[int] = None, public: bool = False) -> str:
        # We append "-public" or "-private" to distinguish subnets.
        # This allows having both public and private subnets in the same VCN.
        suffix = f"-{subnet_index}" if subnet_index is not None else ""
        type_suffix = "-public" if public else "-private"
        return self.SCT_SUBNET_NAME_TMPL.format(index=suffix) + type_suffix

    def subnet(
        self,
        subnet_index: Optional[int] = None,
        public: bool = False,
        _cache: dict = {},  # noqa: B006
    ):
        if existing := _cache.get((subnet_index, public)):
            return existing
        with KEY_BASED_LOCKS.get_lock(f"oci-subnet--i-{subnet_index}--public-{public}"):
            name = self.subnet_name(subnet_index=subnet_index, public=public)
            subnets = oci.pagination.list_call_get_all_results_generator(
                self.network.list_subnets,
                yield_mode="record",
                compartment_id=self.compartment_id,
                vcn_id=self.vcn.id,
                display_name=name,
            )
            while current_subnet := next(subnets, None):
                if current_subnet.lifecycle_state == "AVAILABLE":
                    _cache[(subnet_index, public)] = current_subnet
                    return current_subnet
        return None

    def create_subnet(
        self,
        ipv4_cidr=None,
        ipv6_cidr=None,
        subnet_index: Optional[int] = None,
        public: bool = False,
    ):
        name = self.subnet_name(subnet_index=subnet_index, public=public)
        if subnet := self.subnet(subnet_index=subnet_index, public=public):
            LOGGER.info("Subnet '%s' already exists", name)
            return subnet

        if ipv4_cidr is None:
            subnet_cidr4s = list(self.vcn_ipv4_cidr.subnets(prefixlen_diff=8))
            subnet_cidr6s = list(self.vcn_ipv6_cidr.subnets(prefixlen_diff=8)) if self.vcn_ipv6_cidr else []
            for i in range(len(subnet_cidr4s)):  # NOTE: must be 255 at max using /24 masks
                try:
                    subnet_cidr4 = subnet_cidr4s[i]
                    subnet_cidr6 = subnet_cidr6s[i] if i < len(subnet_cidr6s) else None
                    return self.create_subnet(
                        ipv4_cidr=subnet_cidr4,
                        ipv6_cidr=subnet_cidr6,
                        subnet_index=subnet_index,
                        public=public,
                    )
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Failed to create a %s subnet: %s", ("public" if public else "private"), exc)
                    time.sleep(2)
                    continue
            return self.subnet(subnet_index=subnet_index, public=public)

        LOGGER.info("Creating regional subnet '%s' (public=%s)", name, public)
        details_kwargs = dict(
            cidr_block=str(ipv4_cidr),
            compartment_id=self.compartment_id,
            vcn_id=self.vcn.id,
            display_name=name,
            prohibit_public_ip_on_vnic=not public,
            security_list_ids=[self.security_list.id],
            route_table_id=(self.public_route_table.id if public else self.private_route_table.id),
            dhcp_options_id=self.vcn.default_dhcp_options_id,
            freeform_tags=self._base_tags(name),
        )
        if ipv6_cidr:
            details_kwargs["ipv6_cidr_block"] = str(ipv6_cidr)
        details = CreateSubnetDetails(**details_kwargs)
        response = self.network.create_subnet(details)
        return self._wait_for_state(self.network, self.network.get_subnet, response.data.id, "AVAILABLE")

    def setup_defined_tags(self):
        """Set up OCI defined tags required by SCT.

        Creates the tag namespace and tag key definitions if they don't already exist.
        This allows SCT to use defined tags for resource tagging on OCI.

        Note: Tag operations (CREATE, UPDATE, DELETE) must be performed in the home region.
        """
        try:
            # Get the home region for tag operations
            # Tag namespace and tag operations must be done in the home region
            home_region = self._get_home_region()
            LOGGER.info("Using home region '%s' for tag operations", home_region)

            # Create identity client for home region if needed
            if self.region_name == home_region:
                home_identity_client = self.identity
            else:
                home_identity_client = self.oci_service.get_identity_client(region=home_region)

            # 1. Get or create the tag namespace
            LOGGER.info("Checking for tag namespace '%s'...", TAG_NAMESPACE)
            all_namespaces = oci.pagination.list_call_get_all_results(
                home_identity_client.list_tag_namespaces, self.compartment_id
            ).data
            namespace_summary = next((ns for ns in all_namespaces if ns.name == TAG_NAMESPACE), None)

            if namespace_summary:
                LOGGER.info("Tag namespace '%s' already exists.", TAG_NAMESPACE)
                namespace_id = namespace_summary.id
            else:
                LOGGER.info("Creating tag namespace '%s' in compartment '%s'...", TAG_NAMESPACE, self.compartment_id)
                namespace_details = CreateTagNamespaceDetails(
                    compartment_id=self.compartment_id,
                    name=TAG_NAMESPACE,
                    description="Tag namespace for scylla-cluster-tests",
                )
                namespace = home_identity_client.create_tag_namespace(namespace_details).data
                namespace_id = namespace.id
                LOGGER.info("Tag namespace '%s' created.", TAG_NAMESPACE)

            # 2. Get existing tags in the namespace
            existing_tags = oci.pagination.list_call_get_all_results(home_identity_client.list_tags, namespace_id).data
            existing_tag_names = {tag.name for tag in existing_tags}

            # 3. Create tag keys that don't exist
            for tag_key in SCT_TAG_KEYS:
                if tag_key in existing_tag_names:
                    LOGGER.debug("Tag definition for '%s' already exists in namespace '%s'.", tag_key, TAG_NAMESPACE)
                else:
                    try:
                        LOGGER.info("Creating tag definition for '%s' in namespace '%s'...", tag_key, TAG_NAMESPACE)
                        tag_details = CreateTagDetails(
                            name=tag_key,
                            description=f"SCT tag for {tag_key}",
                        )
                        home_identity_client.create_tag(namespace_id, tag_details)
                        LOGGER.info("Tag definition for '%s' created.", tag_key)
                    except oci.exceptions.ServiceError as tag_exc:
                        if tag_exc.code == "TagDefinitionAlreadyExists":
                            LOGGER.debug("Tag definition for '%s' already exists (caught during creation).", tag_key)
                        else:
                            raise

            LOGGER.info(
                "All defined tags for namespace '%s' are set up in compartment '%s'.",
                TAG_NAMESPACE,
                self.compartment_id,
            )

        except oci.exceptions.ServiceError as exc:
            LOGGER.error("OCI API error while setting up tags: %s - %s", exc.code, exc.message)
            if exc.code == "NotAllowed" and "home region" in exc.message:
                LOGGER.error("Tag operations must be performed in the home region.")
            LOGGER.error("Please ensure you have permissions to manage tags in the specified compartment.")
            raise
        except Exception as exc:
            LOGGER.error("An unexpected error occurred while setting up tags: %s", exc)
            raise

    def _get_home_region(self) -> str:
        """Get the home region for the tenancy.

        Returns:
            The home region name (e.g., 'us-ashburn-1')
        """
        try:
            region_subscriptions = oci.pagination.list_call_get_all_results_generator(
                self.identity.list_region_subscriptions, yield_mode="record", tenancy_id=self._config["tenancy"]
            )
            first_region = ""
            while current_region_subscription := next(region_subscriptions, None):
                if first_region == "":
                    first_region = current_region_subscription.region_name
                if current_region_subscription.is_home_region:
                    LOGGER.debug("Found home region: %s", current_region_subscription.region_name)
                    return current_region_subscription.region_name
            # Fallback: if no home region found, use the first region
            if first_region:
                LOGGER.warning("No home region found, using first region: %s", first_region)
                return first_region
            raise ValueError("No region subscriptions found for tenancy")
        except Exception as exc:
            LOGGER.error("Failed to determine home region: %s", exc)
            raise

    def configure_network(self):
        _ = self.vcn
        _ = self.security_list
        # NOTE: private subnet using nat gateway
        _ = self.nat_gateway
        self.create_subnet(public=False)
        # NOTE: public subnet using internet gateway
        _ = self.internet_gateway
        self.create_subnet(public=True)

    def configure(self):
        """Configure all required resources for SCT in this OCI region."""
        LOGGER.info("Configuring '%s' region...", self.region_name)
        self.setup_defined_tags()
        self.configure_network()
        LOGGER.info("Region configured successfully.")

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

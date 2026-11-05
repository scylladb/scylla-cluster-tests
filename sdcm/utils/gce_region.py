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
# Copyright (c) 2022 ScyllaDB

import logging
import os
import time
from functools import cached_property
from typing import Optional

import google.api_core.exceptions
import googleapiclient.errors
from google.cloud import compute_v1, storage
from google.cloud.compute_v1 import Firewall
from google.oauth2 import service_account
from googleapiclient.discovery import build

from sdcm.keystore import KeyStore
from sdcm.provision.gce.constants import SECONDARY_SUBNET_NAME_TMPL
from sdcm.utils.builder_access import BUILDER_SSH_PORT, flatten_sources, get_builder_ssh_sources
from sdcm.utils.gce_utils import _gce_client_options, wait_for_extended_operation

LOGGER = logging.getLogger(__name__)

BUILDER_NETWORK_TAG = "sct-builder"
# the API stores 1000 when a rule is created without a priority
DEFAULT_FIREWALL_PRIORITY = 1000


def _firewall_priority(rule: Firewall) -> int:
    return rule.priority if "priority" in rule else DEFAULT_FIREWALL_PRIORITY


def firewall_needs_patch(live: Firewall, desired: Firewall) -> bool:
    """Tell whether a live firewall rule differs from the desired one in what it lets through."""
    return (
        list(live.allowed) != list(desired.allowed)
        or list(live.denied) != list(desired.denied)
        or list(live.source_ranges) != list(desired.source_ranges)
        or list(live.target_tags) != list(desired.target_tags)
        or _firewall_priority(live) != _firewall_priority(desired)
    )


def builder_firewall_rules(network_name: str, network_link: str, sources: dict[str, list[str]]) -> list[Firewall]:
    """Return the builder firewall rules: allow SSH from `sources` and traffic from the SCT network, deny the rest.

    The deny rule is last, so it is never applied before the allow rules.
    """
    pairs = flatten_sources(sources)
    roles = ", ".join(sorted({role for _, role in pairs}))
    return [
        Firewall(
            name=f"{network_name}-builder-allow-ssh",
            description=f"SSH to SCT Jenkins builders from: {roles}",
            direction="INGRESS",
            allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=[str(BUILDER_SSH_PORT)])],
            source_ranges=sorted({cidr for cidr, _ in pairs}),
            network=network_link,
            target_tags=[BUILDER_NETWORK_TAG],
            priority=100,
        ),
        Firewall(
            name=f"{network_name}-builder-allow-internal",
            description="Allow ingress to SCT Jenkins builders from the SCT network",
            direction="INGRESS",
            allowed=[compute_v1.Allowed(I_p_protocol="all")],
            source_ranges=["10.0.0.0/8"],
            network=network_link,
            target_tags=[BUILDER_NETWORK_TAG],
            priority=100,
        ),
        Firewall(
            name=f"{network_name}-builder-deny-all",
            description="Deny all ingress to SCT Jenkins builders",
            direction="INGRESS",
            denied=[compute_v1.Denied(I_p_protocol="all")],
            source_ranges=["0.0.0.0/0"],
            network=network_link,
            target_tags=[BUILDER_NETWORK_TAG],
            priority=200,
        ),
    ]


class GceRegion:
    SCT_NETWORK_NAME = "qa-vpc"
    SCT_BACKUP_SERVICE_ACCOUNT = "sct-manager-backup"
    # old live rules to delete, mapped to the rules that replace them
    STALE_FIREWALL_RULES = {f"{SCT_NETWORK_NAME}-ssh": f"{SCT_NETWORK_NAME}-allow-ssh"}

    # Secondary subnets for extra network interfaces.
    # `qa-vpc` is auto-mode and already uses 10.128.0.0/9, so we keep these in 10.100.X.0/24.
    # Keep region order stable; append new regions to avoid shifting existing CIDRs.
    SECONDARY_SUBNET_REGIONS = (
        "us-east1",
        "us-east4",
        "us-central1",
        "us-west1",
        "us-west2",
        "us-west4",
        "europe-west1",
        "europe-west2",
        "europe-west3",
        "europe-west4",
        "asia-east1",
        "asia-northeast1",
        "asia-south1",
        "australia-southeast1",
        "southamerica-east1",
    )

    SECONDARY_SUBNET_REGION_INDEXES = {region: index for index, region in enumerate(SECONDARY_SUBNET_REGIONS)}
    SECONDARY_SUBNET_CIDR_TMPL = "10.100.{index}.0/24"

    def __init__(self, region_name):
        self.region_name = region_name
        info = KeyStore().get_gcp_credentials()
        self.project = info["project_id"]

        credentials = service_account.Credentials.from_service_account_info(info)

        # One shared endpoint override for every client here. Deliberate for now: point
        # everything at minicloud and implement/route the gaps one service at a time
        # (as done for AWS), rather than maintaining a per-service allowlist.
        endpoint_kwargs = _gce_client_options()

        self.iam = build("iam", "v1", credentials=credentials, cache_discovery=False, **endpoint_kwargs)

        self.network_client = compute_v1.NetworksClient(credentials=credentials, **endpoint_kwargs)
        self.firewall_client = compute_v1.FirewallsClient(credentials=credentials, **endpoint_kwargs)
        self.subnets_client = compute_v1.SubnetworksClient(credentials=credentials, **endpoint_kwargs)
        self.routes_client = compute_v1.RoutesClient(credentials=credentials, **endpoint_kwargs)
        self.storage_client = storage.Client(credentials=credentials, **endpoint_kwargs)

    @property
    def backup_storage_bucket_name(self):
        return f"manager-backup-tests-{self.project}-{self.region_name}"

    @property
    def _is_minicloud(self) -> bool:
        return bool(os.environ.get("GCE_ENDPOINT_URL"))

    @cached_property
    def network(self) -> compute_v1.Network:
        try:
            _network = self.network_client.get(project=self.project, network=self.SCT_NETWORK_NAME)
        except google.api_core.exceptions.NotFound:
            _network = self.create_network()
        return _network

    @cached_property
    def region_subnet(self) -> compute_v1.Subnetwork:
        for subnet in self.subnets_client.list(project=self.project, region=self.region_name):
            if subnet.network.endswith(f"networks/{self.SCT_NETWORK_NAME}"):
                return subnet
        raise RuntimeError(f"No subnet found for network {self.SCT_NETWORK_NAME} in region {self.region_name}")

    def create_network(self) -> compute_v1.Network:
        _network = compute_v1.Network()
        _network.name = self.SCT_NETWORK_NAME
        _network.auto_create_subnetworks = True
        self.network_client.insert(project=self.project, network_resource=_network)
        return self.network_client.get(project=self.project, network=self.SCT_NETWORK_NAME)

    def base_firewall_rules(self) -> list[Firewall]:
        return [
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-ssh",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["22"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-all-internal",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="all")],
                source_ranges=["10.0.0.0/8"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-icmp",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="icmp")],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-grafana",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["3000"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-node-gossip",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["7000", "7001"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-node-api",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["10000"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-node-cql",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["9042", "9142", "19042", "19142"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-prometheus",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["9090", "9100", "9103", "9180"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-http",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["80"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-alternator",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["8080"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-manager",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["10001", "5080", "5443", "5090", "5112"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-deny-all",
                direction="INGRESS",
                denied=[compute_v1.Denied(I_p_protocol="all")],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
                target_tags=["sct-network-only"],
                priority=200,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-only-sct-network",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="all")],
                source_ranges=["10.0.0.0/8"],
                network=self.network.self_link,
                target_tags=["sct-network-only"],
                priority=100,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-grafana-public",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="tcp", ports=["3000"])],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
                target_tags=["sct-network-only"],
                priority=100,
            ),
            Firewall(
                name=f"{self.SCT_NETWORK_NAME}-allow-public",
                direction="INGRESS",
                allowed=[compute_v1.Allowed(I_p_protocol="all")],
                source_ranges=["0.0.0.0/0"],
                network=self.network.self_link,
                target_tags=["sct-allow-public"],
                priority=100,
            ),
        ]

    def apply_firewall_rules(self, rules: list[Firewall]) -> None:
        for firewall_rule in rules:
            try:
                operation = self.firewall_client.insert(project=self.project, firewall_resource=firewall_rule)
                wait_for_extended_operation(operation, f"create firewall rule {firewall_rule.name}")
            except google.api_core.exceptions.Conflict:
                firewall = self.firewall_client.get(project=self.project, firewall=firewall_rule.name)
                if firewall_needs_patch(firewall, firewall_rule):
                    LOGGER.info("updating firewall %s", firewall_rule.name)
                    LOGGER.info(
                        "before: %s %s %s %s %s",
                        firewall.allowed,
                        firewall.denied,
                        firewall.source_ranges,
                        firewall.target_tags,
                        _firewall_priority(firewall),
                    )
                    LOGGER.info(
                        "after : %s %s %s %s %s",
                        firewall_rule.allowed,
                        firewall_rule.denied,
                        firewall_rule.source_ranges,
                        firewall_rule.target_tags,
                        _firewall_priority(firewall_rule),
                    )
                    operation = self.firewall_client.patch(
                        project=self.project, firewall=firewall_rule.name, firewall_resource=firewall_rule
                    )
                    wait_for_extended_operation(operation, f"update firewall rule {firewall_rule.name}")

    def delete_stale_firewall_rules(self) -> None:
        for name, replacement in self.STALE_FIREWALL_RULES.items():
            try:
                self.firewall_client.get(project=self.project, firewall=replacement)
            except google.api_core.exceptions.NotFound:
                LOGGER.warning(
                    "Keeping stale firewall rule '%s' in project '%s': its replacement '%s' does not exist",
                    name,
                    self.project,
                    replacement,
                )
                continue
            try:
                operation = self.firewall_client.delete(project=self.project, firewall=name)
            except google.api_core.exceptions.NotFound:
                continue
            wait_for_extended_operation(operation, f"delete firewall rule {name}")
            LOGGER.info("Deleted stale firewall rule '%s' in project '%s'", name, self.project)

    def configure_builder_firewall(self, sources: dict[str, list[str]]) -> None:
        self.apply_firewall_rules(builder_firewall_rules(self.SCT_NETWORK_NAME, self.network.self_link, sources))
        self.delete_stale_firewall_rules()

    def configure_firewall(self) -> None:
        self.apply_firewall_rules(self.base_firewall_rules())
        if not self._is_minicloud:
            self.configure_builder_firewall(get_builder_ssh_sources())

    def create_backup_service_account(self):
        """Creates a service account."""
        backup_service_account = None

        try:
            backup_service_account = (
                self.iam.projects()
                .serviceAccounts()
                .create(
                    name="projects/" + self.project,
                    body={
                        "accountId": self.SCT_BACKUP_SERVICE_ACCOUNT,
                        "serviceAccount": {
                            "displayName": "Account for having access to the gcs bucket for SCT backup tests"
                        },
                    },
                )
                .execute()
            )
            LOGGER.info("Created service account: %s", backup_service_account["email"])
        except googleapiclient.errors.HttpError as exc:
            if not exc.status_code == 409:
                raise
            service_accounts = (
                self.iam.projects().serviceAccounts().list(name=f"projects/{self.project}", pageSize=100).execute()
            )
            for service in service_accounts["accounts"]:
                if self.SCT_BACKUP_SERVICE_ACCOUNT in service["name"]:
                    backup_service_account = service
            assert backup_service_account, f"couldn't find {self.SCT_BACKUP_SERVICE_ACCOUNT} service"
        return backup_service_account

    def configure_backup_storage(self):
        try:
            bucket = self.storage_client.create_bucket(self.backup_storage_bucket_name, location=self.region_name)
        except google.api_core.exceptions.Conflict:
            bucket = self.storage_client.bucket(self.backup_storage_bucket_name)
        bucket.add_lifecycle_delete_rule(age=7)
        bucket.patch()

        service = self.create_backup_service_account()
        policy = bucket.get_iam_policy(requested_policy_version=3)
        role = "roles/storage.objectAdmin"
        policy.bindings.append({"role": role, "members": {f"serviceAccount:{service['email']}"}})
        bucket.set_iam_policy(policy)

    @property
    def secondary_subnet_name(self) -> str:
        return SECONDARY_SUBNET_NAME_TMPL.format(network_name=self.SCT_NETWORK_NAME, index=1)

    @property
    def secondary_subnet_cidr(self) -> str:
        """CIDR of this region's subnet for secondary network interfaces."""
        if (index := self.SECONDARY_SUBNET_REGION_INDEXES.get(self.region_name)) is None:
            raise RuntimeError(
                f"Region '{self.region_name}' has no CIDR reserved for secondary network interfaces. "
                f"Add it to {type(self).__name__}.SECONDARY_SUBNET_REGION_INDEXES, picking an index that is "
                f"not used by another region."
            )
        return self.SECONDARY_SUBNET_CIDR_TMPL.format(index=index)

    def create_secondary_subnet(self) -> None:
        """Create this region's subnet for secondary network interfaces.

        GCE attaches every network interface of an instance to a different subnet, so multi-NIC
        nodes need a subnet next to the auto-mode one. It lives in the same VPC network, which
        keeps the SCT runner and the monitor able to reach the secondary addresses without
        peering.

        Regions with no reserved CIDR are skipped rather than treated as an error: `prepare-regions`
        with no `-r` runs over every region the project has, while multi-NIC is used in a few.
        """
        if self.region_name not in self.SECONDARY_SUBNET_REGION_INDEXES:
            LOGGER.warning(
                "Region '%s' has no CIDR reserved for secondary network interfaces, skipping subnet creation",
                self.region_name,
            )
            return

        subnet_name = self.secondary_subnet_name
        try:
            subnet = self.subnets_client.get(
                project=self.project,
                region=self.region_name,
                subnetwork=subnet_name,
            )
        except google.api_core.exceptions.NotFound:
            subnet_cidr = self.secondary_subnet_cidr
            LOGGER.info("Creating subnet '%s' (%s)...", subnet_name, subnet_cidr)
            subnet_resource = compute_v1.Subnetwork(
                name=subnet_name,
                network=self.network.self_link,
                ip_cidr_range=subnet_cidr,
                region=self.region_name,
            )
            operation = self.subnets_client.insert(
                project=self.project,
                region=self.region_name,
                subnetwork_resource=subnet_resource,
            )
            wait_for_extended_operation(operation, f"create subnet {subnet_name}")
        else:
            LOGGER.info("Subnet '%s' already exists (%s)", subnet_name, subnet.ip_cidr_range)

    def configure(self):
        LOGGER.info("Configuring '%s' region...", self.region_name)
        self.configure_firewall()
        self.create_secondary_subnet()
        if not self._is_minicloud:
            # Backup service account + storage bucket require the IAM and GCS APIs,
            # which minicloud does not serve (everything is pointed at the emulator, and
            # manager-backup flows are not part of any minicloud test yet) — creating
            # them here would fail region preparation before a single instance runs.
            self.create_backup_service_account()
            self.configure_backup_storage()
        LOGGER.info("Region configured successfully.")

    def add_network_peering(
        self, peering_name: str, peer_project: str, peer_net: str, wait_for_active: bool = True
    ) -> None:
        peer_network_url = f"projects/{peer_project}/global/networks/{peer_net}"
        peering_request = compute_v1.AddPeeringNetworkRequest(
            network=self.SCT_NETWORK_NAME,
            project=self.project,
            networks_add_peering_request_resource=compute_v1.NetworksAddPeeringRequest(
                name=peering_name, peer_network=peer_network_url, auto_create_routes=True
            ),
        )
        self.network_client.add_peering(request=peering_request)
        if wait_for_active:
            self.wait_for_peering_status(peering_name, status="ACTIVE")

    def get_peering_status(self, peering_name: str) -> Optional[str]:
        network = self.network_client.get(project=self.project, network=self.SCT_NETWORK_NAME)
        for peering in network.peerings or []:
            if peering.name == peering_name:
                return peering.state
        return None

    def wait_for_peering_status(self, peering_name: str, status: str = "ACTIVE", timeout: int = 300) -> bool:
        end_time = time.time() + timeout
        while time.time() < end_time:
            if (current_status := self.get_peering_status(peering_name)) == status:
                LOGGER.debug("Peering %s status: %s", peering_name, current_status)
                return True
            time.sleep(5)

        LOGGER.error("Timeout waiting for GCP network peering %s to become %s", peering_name, status)
        return False

    def cleanup_vpc_peering_connection(self, peering_name: str) -> bool:
        peering_status = self.get_peering_status(peering_name)
        if not peering_status:
            LOGGER.debug("GCP network peering %s not found.", peering_name)
            return True

        remove_request = compute_v1.RemovePeeringNetworkRequest(
            network=self.SCT_NETWORK_NAME,
            project=self.project,
            networks_remove_peering_request_resource=compute_v1.NetworksRemovePeeringRequest(name=peering_name),
        )
        operation = self.network_client.remove_peering(request=remove_request)
        wait_for_extended_operation(operation, f"Remove peering {peering_name}", timeout=120)

        final_status = self.get_peering_status(peering_name)
        if final_status:
            LOGGER.warning(
                "GCP network peering %s still exists after deletion attempt (status: %s)", peering_name, final_status
            )
            return False
        return True

    def get_peering_routes(self) -> list[str]:
        """Discover all network peering routes in SCT VPC network"""
        peering_routes = []

        routes = self.routes_client.list(project=self.project)
        for route in routes:
            if (
                hasattr(route, "next_hop_peering") and route.next_hop_peering
            ):  # peering routes have `next_hop_peering` attribute
                if route.network and self.SCT_NETWORK_NAME in route.network:
                    if route.dest_range and route.dest_range not in peering_routes:
                        peering_routes.append(route.dest_range)

        LOGGER.debug("Discovered %s peering routes in %s", peering_routes, self.region_name)
        return peering_routes


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    a = GceRegion("us-east1")
    print(a.configure())

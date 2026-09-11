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
# Copyright (c) 2021 ScyllaDB

from __future__ import annotations

import logging
import time
import random
from typing import NamedTuple, TYPE_CHECKING
from functools import cached_property, lru_cache
from itertools import chain

from azure.identity import ClientSecretCredential
from azure.keyvault.keys import KeyClient
from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import VirtualMachine
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.storage.blob import BlobServiceClient
from azure.core.credentials import AzureNamedKeyCredential
from azure.mgmt.keyvault import KeyVaultManagementClient
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequestOptions, QueryRequest
from azure.core.exceptions import HttpResponseError

from sdcm.keystore import KeyStore
from sdcm.utils.metaclasses import Singleton

if TYPE_CHECKING:
    from typing import Optional, Callable, Iterator

    from azure.core.credentials import TokenCredential
    from azure.mgmt.resource.resources.models import Resource


API_VERSIONS = {
    "Microsoft.Compute/disks": "2020-12-01",
    "Microsoft.Compute/galleries": "2021-07-01",
    "Microsoft.Compute/galleries/images": "2021-07-01",
    "Microsoft.Compute/galleries/images/versions": "2021-07-01",
    "Microsoft.Compute/virtualMachines": "2021-07-01",
    "Microsoft.Network/networkInterfaces": "2021-02-01",
    "Microsoft.Network/networkSecurityGroups": "2021-02-01",
    "Microsoft.Network/publicIPAddresses": "2021-02-01",
    "Microsoft.Network/virtualNetworks": "2021-02-01",
    "Microsoft.Network/virtualNetworks/subnets": "2021-02-01",
    "Microsoft.Resources/resourceGroups": "2021-04-01",
}

LOGGER = logging.getLogger(__name__)


logging.getLogger("azure").setLevel(logging.ERROR)


class VirtualMachineIPs(NamedTuple):
    private_ip: str
    public_ip: Optional[str]


class AzureService(metaclass=Singleton):
    @cached_property
    def azure_credentials(self) -> dict[str, str]:
        return KeyStore().get_azure_credentials()

    @cached_property
    def blob_credentials(self) -> dict[str, str]:
        return KeyStore().get_backup_azure_blob_credentials()

    @cached_property
    def blob_account_url(self) -> str:
        return f"https://{self.blob_credentials['account']}.blob.core.windows.net/"

    @cached_property
    def subscription_id(self) -> str:
        return self.azure_credentials["subscription_id"]

    @cached_property
    def tenant_id(self) -> str:
        """Get tenant_id from Azure API to handle cases where credential config has invalid/placeholder values."""
        # Use tenants.list() to get the tenant_id associated with the current credentials
        tenants = list(self.subscription.tenants.list())
        if tenants:
            return tenants[0].tenant_id
        return self.azure_credentials["tenant_id"]

    @cached_property
    def credential(self) -> TokenCredential:
        return ClientSecretCredential(
            tenant_id=self.azure_credentials["tenant_id"],
            client_id=self.azure_credentials["client_id"],
            client_secret=self.azure_credentials["client_secret"],
        )

    @cached_property
    def compute(self) -> ComputeManagementClient:
        return ComputeManagementClient(credential=self.credential, subscription_id=self.subscription_id)

    @cached_property
    def network(self) -> NetworkManagementClient:
        return NetworkManagementClient(credential=self.credential, subscription_id=self.subscription_id)

    @cached_property
    def resource(self) -> ResourceManagementClient:
        return ResourceManagementClient(credential=self.credential, subscription_id=self.subscription_id)

    @cached_property
    def blob(self) -> BlobServiceClient:
        return BlobServiceClient(
            account_url=self.blob_account_url,
            credential=AzureNamedKeyCredential(name=self.blob_credentials["account"], key=self.blob_credentials["key"]),
        )

    @cached_property
    def subscription(self) -> SubscriptionClient:
        return SubscriptionClient(credential=self.credential)

    @cached_property
    def resource_graph(self) -> ResourceGraphClient:
        return ResourceGraphClient(credential=self.credential)

    @cached_property
    def all_regions(self) -> list[str]:
        locations = self.subscription.subscriptions.list_locations(subscription_id=self.subscription_id)
        return sorted(location.name for location in locations)

    @cached_property
    def get_by_id(self) -> Callable:
        return self.resource.resources.get_by_id

    @cached_property
    def keyvault(self) -> KeyVaultManagementClient:
        return KeyVaultManagementClient(credential=self.credential, subscription_id=self.subscription_id)

    def create_vault_key(self, vault_uri: str, key_name: str, key_size: int = 2048) -> str:
        key_client = KeyClient(vault_url=vault_uri, credential=self.credential)
        key = key_client.create_rsa_key(name=key_name, size=key_size)
        return key.id

    def get_vault_key(self, vault_uri: str, key_name: str):
        try:
            key_client = KeyClient(vault_url=vault_uri, credential=self.credential)
            key = key_client.get_key(name=key_name)
            return key
        except ResourceNotFoundError:
            return None

    def rotate_vault_key(self, key_uri: str) -> str:
        # Extract vault URI and key name from full key URI
        # Format: https://vault-name.vault.azure.net/scylla-key-N
        vault_uri = key_uri.split("scylla-key-", maxsplit=1)[0]
        key_name = key_uri.rsplit("/", maxsplit=1)[-1]

        key_client = KeyClient(vault_url=vault_uri, credential=self.credential)
        rotated_key = key_client.rotate_key(name=key_name)
        return rotated_key.id

    def _get_ip_configuration_dicts(self, network_interface_id: str) -> list[dict]:
        """Every ipConfiguration of a NIC.

        A dual-stack NIC has two, and only reading the first one would leave the IPv6 Public IP
        resource behind on cleanup.
        """
        return [
            configuration["properties"]
            for configuration in self.get_by_id(
                resource_id=network_interface_id,
                api_version=API_VERSIONS["Microsoft.Network/networkInterfaces"],
            ).properties["ipConfigurations"]
        ]

    def _get_ip_configuration_dict(self, network_interface_id: str) -> dict:
        return self._get_ip_configuration_dicts(network_interface_id)[0]

    def get_virtual_machine_ips(self, virtual_machine: VirtualMachine) -> VirtualMachineIPs:
        ip_configuration = self._get_ip_configuration_dict(
            network_interface_id=virtual_machine.network_profile.network_interfaces[0].id,
        )
        if "publicIPAddress" in ip_configuration:
            public_ip_address = self.get_by_id(
                resource_id=ip_configuration["publicIPAddress"]["id"],
                api_version=API_VERSIONS["Microsoft.Network/publicIPAddresses"],
            ).properties["ipAddress"]
        else:
            public_ip_address = None
        return VirtualMachineIPs(private_ip=ip_configuration["privateIPAddress"], public_ip=public_ip_address)

    # In Azure, when you delete Virtual Machine resource all other associated resources like disks, network interfaces,
    # and public IPs will not be deleted automatically.  Following method provide a list of resources we care about.
    def list_known_virtual_machine_resources(self, virtual_machine: VirtualMachine) -> list[Resource]:
        resources = [
            virtual_machine,
            self.get_by_id(
                resource_id=virtual_machine.storage_profile.os_disk.managed_disk.id,
                api_version=API_VERSIONS["Microsoft.Compute/disks"],
            ),
        ]
        for disk in virtual_machine.storage_profile.data_disks:
            resources.append(self.get_by_id(resource_id=disk.id, api_version=API_VERSIONS["Microsoft.Compute/disks"]))
        for iface in virtual_machine.network_profile.network_interfaces:
            resources.append(
                self.get_by_id(
                    resource_id=iface.id,
                    api_version=API_VERSIONS["Microsoft.Network/networkInterfaces"],
                )
            )
            for configuration in self._get_ip_configuration_dicts(network_interface_id=iface.id):
                if public_ip := configuration.get("publicIPAddress"):
                    resources.append(
                        self.get_by_id(
                            resource_id=public_ip["id"],
                            api_version=API_VERSIONS["Microsoft.Network/publicIPAddresses"],
                        )
                    )
        return resources

    def delete_resource(self, resource: Resource) -> None:
        if api_version := API_VERSIONS.get(resource.type):
            self.resource.resources.begin_delete_by_id(
                resource_id=resource.id,
                api_version=api_version,
            ).wait()
        else:
            LOGGER.error("Resource type `%s' is unknown, don't delete it", resource.type)

    def delete_virtual_machine(self, virtual_machine: VirtualMachine) -> None:
        for resource in self.list_known_virtual_machine_resources(virtual_machine=virtual_machine):
            self.delete_resource(resource=resource)

    # Azure Resource Graph is a service with extremely powerful query language for the resource exploration.
    # See https://docs.microsoft.com/en-us/azure/governance/resource-graph/overview for more details.
    def resource_graph_query(self, query: str) -> Iterator:
        LOGGER.debug("query=%r", query)
        request = QueryRequest(
            subscriptions=[self.subscription_id],
            query=query,
            options=QueryRequestOptions(result_format="objectArray"),
        )

        def paged_query() -> Iterator[list]:
            retry_count = 0
            max_retries = 5
            base_delay = 2  # Start with 2 seconds

            while True:
                try:
                    response = self.resource_graph.resources(request)
                    retry_count = 0  # Reset retry count on successful request
                    yield response.data
                    if not response.skip_token:
                        # See https://docs.microsoft.com/en-us/azure/governance/resource-graph/concepts/work-with-data#paging-results
                        assert response.result_truncated == "false", (
                            "paging is not possible because you missed id column"
                        )
                        break
                    LOGGER.debug("get next page of query=%r", query)
                    request.options.skip_token = response.skip_token
                except HttpResponseError as e:
                    if "RateLimiting" in str(e) and retry_count < max_retries:
                        retry_count += 1
                        # Exponential backoff with jitter: 2, 4, 8, 16, 32 seconds (with random jitter)
                        delay = (base_delay**retry_count) + random.uniform(0.5, 1.5)
                        LOGGER.warning(
                            "Azure Resource Graph rate limiting encountered. "
                            "Retrying in %d seconds (attempt %d/%d). Query: %s",
                            delay,
                            retry_count,
                            max_retries,
                            query,
                        )
                        time.sleep(delay)
                        continue
                    else:
                        # Re-raise if not rate limiting or max retries exceeded
                        raise

        return chain.from_iterable(paged_query())


def list_instances_azure(
    tags_dict: Optional[dict[str, str]] = None, running: bool = False, verbose: bool = False
) -> list[VirtualMachine]:
    query_bits = [
        "Resources",
        "where resourceGroup startswith 'SCT-'",  # look in `SCT-*' resource groups only
        "where type =~ 'Microsoft.Compute/virtualMachines'",
    ]
    if tags_dict:
        tags = [f"tags['{key}'] == '{value}'" for key, value in tags_dict.items()]
        query_bits.append(f"where {' and '.join(tags)}")
    if running:
        query_bits.append("where tostring(properties.extended.instanceView.powerState.code) == 'PowerState/running'")
    query_bits.append("project id, resourceGroup, name")  # id column is required for the paging

    if verbose:
        LOGGER.info("Going to list Azure instances")
    azure_service = AzureService()
    res = azure_service.resource_graph_query(query=" | ".join(query_bits))
    get_virtual_machine = azure_service.compute.virtual_machines.get
    instances = [get_virtual_machine(resource_group_name=vm["resourceGroup"], vm_name=vm["name"]) for vm in res]
    if verbose:
        LOGGER.info("Done. Found total of %s instances.", len(instances))

    return instances


@lru_cache
def max_network_interfaces(instance_type: str, location: str, azure_service: AzureService = None) -> int | None:
    """Number of NICs an Azure VM size accepts, or None when the size is unknown in the location.

    Azure publishes it as the 'MaxNetworkInterfaces' capability of the VM size SKU. Cached because
    listing the SKUs of a region is a slow call and the answer never changes during a run.
    """
    azure_service = azure_service or AzureService()
    skus = azure_service.compute.resource_skus.list(filter=f"location eq '{location}'")
    for sku in skus:
        if sku.resource_type != "virtualMachines" or sku.name != instance_type:
            continue
        for capability in sku.capabilities or []:
            if capability.name == "MaxNetworkInterfaces":
                return int(capability.value)
        # the SKU exists but does not publish the capability, so SCT must not guess a limit
        return None
    return None


def azure_check_instance_type_available(instance_type: str, location: str) -> bool:
    """
    Check if instance type is available in the given location.
    """
    azure_service = AzureService()
    return any(
        instance_type in size.name for size in azure_service.compute.virtual_machine_sizes.list(location=location)
    )


SECONDARY_NICS_SCRIPT_PATH = "/usr/local/sbin/sct-configure-secondary-nics.sh"
SECONDARY_NICS_SERVICE = "sct-secondary-nics"

# Azure hands a secondary NIC an address over DHCP but installs no routing policy for it, so a reply
# sourced from a secondary NIC address leaves through the primary NIC's default route and is dropped
# by the platform as asymmetric. Source-based policy routing is what Azure documents for multi-NIC
# VMs: https://learn.microsoft.com/en-us/azure/virtual-network/virtual-network-multiple-ip-addresses-portal
#
# Everything is resolved from IMDS at run time, so one script serves every node and can safely re-run
# after a reboot or an interface restart.
#
# Arguments: $1 - number of NICs to expect in IMDS.
SECONDARY_NICS_SCRIPT = r"""#!/bin/bash
# auto-generated by SCT - addresses and policy routing for secondary Azure NICs
set -euo pipefail

EXPECTED_NICS="${1:?number of expected NICs is required}"

# --retry-all-errors also retries connection failures during early boot, but needs curl >= 7.71
RETRY_ALL_ERRORS=$(curl --retry-all-errors --version >/dev/null 2>&1 && echo --retry-all-errors || true)

imds_fetch() {
    curl -sf --connect-timeout 10 --retry 5 --retry-max-time 60 $RETRY_ALL_ERRORS \
        -H "Metadata: true" \
        "http://169.254.169.254/metadata/instance/network?api-version=2021-02-01"
}

# IMDS can lag behind the VM create call, so wait until every NIC shows up. A failing fetch and an
# unparsable payload are both retried, but neither may be swallowed forever: configuring only a
# subset of the NICs leaves the node half-broken in a way that surfaces much later, as a confusing
# connectivity or streaming failure.
METADATA=""
for _attempt in $(seq 1 30); do
    if ! METADATA=$(imds_fetch); then
        echo "IMDS query failed, retrying" >&2
    elif ! NIC_COUNT=$(echo "$METADATA" | python3 -c "import json,sys; print(len(json.load(sys.stdin)['interface']))"); then
        echo "IMDS returned an unparsable interface list, retrying" >&2
    elif [ "$NIC_COUNT" -ge "$EXPECTED_NICS" ]; then
        break
    else
        echo "IMDS reports $NIC_COUNT of $EXPECTED_NICS NIC(s), retrying" >&2
    fi
    METADATA=""
    sleep 2
done
if [ -z "$METADATA" ]; then
    echo "IMDS did not return a usable interface list for all the $EXPECTED_NICS NIC(s) after 30 attempts" >&2
    exit 1
fi

echo "$METADATA" | python3 -c '
import ipaddress, json, subprocess, sys

expected_nics = int(sys.argv[1])
interfaces = json.load(sys.stdin)["interface"]
failures = []


def run(*command):
    # Run an "ip" command, recording anything but a success as a failure.
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode:
        failures.append(" ".join(command) + ": " + (result.stderr.strip() or str(result.returncode)))


def replace_rule(family, selector, table):
    # "ip rule" has no "replace" counterpart, and every "del" removes a single match, so drain
    # whatever a previous run of this script stacked up before adding the rule back exactly once.
    for _ in range(16):
        drained = subprocess.run(family + ["rule", "del"] + selector + ["lookup", table], capture_output=True, check=False)
        if drained.returncode:
            break
    else:
        failures.append("failed to drain the duplicate ip rules for " + " ".join(selector))
    run(*(family + ["rule", "add"] + selector + ["lookup", table, "priority", table]))


def resolve_iface(mac):
    # IMDS reports a MAC without separators ("000D3A123456"), ip-link with colons and lowercase.
    normalized = ":".join(mac[i : i + 2] for i in range(0, len(mac), 2)).lower()
    # "check=True" on purpose: a broken "ip" binary must fail the whole script, not skip a NIC.
    output = subprocess.run(["ip", "-o", "link"], capture_output=True, text=True, check=True).stdout
    for line in output.splitlines():
        if normalized in line.lower():
            return line.split(": ")[1].rstrip()
    return ""


configured = 0
# IMDS lists the interfaces in device-index order, so index 0 is the primary NIC and is left alone:
# it already owns the default route SCT reaches the node through.
for idx, interface in enumerate(interfaces):
    if idx == 0:
        continue
    ipv4 = interface.get("ipv4", {})
    addresses, subnets = ipv4.get("ipAddress", []), ipv4.get("subnet", [])
    mac = interface.get("macAddress", "")
    if not addresses or not subnets or not mac:
        failures.append(f"NIC #{idx} metadata is incomplete: {interface}")
        continue
    private_ip = addresses[0].get("privateIpAddress", "")
    subnet = subnets[0]
    cidr = subnet.get("address", "") + "/" + str(subnet.get("prefix", ""))
    if not private_ip or not subnet.get("address") or not subnet.get("prefix"):
        failures.append(f"NIC #{idx} metadata is incomplete: {interface}")
        continue
    if not (iface := resolve_iface(mac)):
        failures.append(f"no OS device with MAC {mac} (NIC #{idx})")
        continue

    # Azure reserves the first usable address of every subnet as its default gateway
    gateway = str(ipaddress.ip_network(cidr, strict=False)[1])
    # table 100+N keeps clear of the reserved tables and of the primary NIC routes
    table = str(100 + idx)
    # the device must be up before any address or route referencing it gets installed
    run("ip", "link", "set", "dev", iface, "up")
    run("ip", "addr", "replace", private_ip + "/" + str(subnet["prefix"]), "dev", iface)
    replace_rule(["ip"], ["from", private_ip], table)
    run("ip", "route", "replace", cidr, "dev", iface, "table", table)
    run("ip", "route", "replace", "default", "via", gateway, "dev", iface, "table", table)

    # IPv6, when the NIC has a dual-stack ipConfiguration. Azure IMDS publishes no IPv6 gateway
    # field, unlike the OCI one, so it is derived the same way as the IPv4 one: Azure reserves the
    # first usable address of every subnet prefix as the gateway.
    ipv6 = interface.get("ipv6", {})
    ipv6_subnets = ipv6.get("subnet", [])
    for address in ipv6.get("ipAddress", []):
        if not (ipv6_address := address.get("privateIpAddress", "")):
            continue
        run("ip", "-6", "addr", "replace", ipv6_address + "/128", "dev", iface)
        replace_rule(["ip", "-6"], ["from", ipv6_address], table)
    for ipv6_subnet in ipv6_subnets:
        if not (ipv6_subnet.get("address") and ipv6_subnet.get("prefix")):
            continue
        ipv6_cidr = ipv6_subnet["address"] + "/" + str(ipv6_subnet["prefix"])
        ipv6_gateway = str(ipaddress.ip_network(ipv6_cidr, strict=False)[1])
        run("ip", "-6", "route", "replace", ipv6_cidr, "dev", iface, "table", table)
        run("ip", "-6", "route", "replace", "default", "via", ipv6_gateway, "dev", iface, "table", table)

    configured += 1

for failure in failures:
    print(failure, file=sys.stderr)
if configured < expected_nics - 1:
    print(f"configured only {configured} of the {expected_nics - 1} secondary NIC(s)", file=sys.stderr)
    sys.exit(1)
if failures:
    sys.exit(1)
print(f"configured addresses and policy routing for {configured} secondary NIC(s)")
' "$EXPECTED_NICS"
"""

SECONDARY_NICS_SERVICE_UNIT_TMPL = """\
[Unit]
Description=SCT secondary NIC configuration
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart={script_path} {nic_count}
RemainAfterExit=yes

[Install]
WantedBy=multi-user.target
"""

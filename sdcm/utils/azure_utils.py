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
from functools import cached_property
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
    'Microsoft.Network/virtualNetworks/subnets': "2021-02-01",
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
        return BlobServiceClient(account_url=self.blob_account_url,
                                 credential=AzureNamedKeyCredential(name=self.blob_credentials['account'],
                                                                    key=self.blob_credentials['key']))

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
        vault_uri = key_uri.split('scylla-key-')[0]
        key_name = key_uri.split('/')[-1]

        key_client = KeyClient(vault_url=vault_uri, credential=self.credential)
        rotated_key = key_client.rotate_key(name=key_name)
        return rotated_key.id

    def _get_ip_configuration_dict(self, network_interface_id: str) -> dict:
        return self.get_by_id(
            resource_id=network_interface_id,
            api_version=API_VERSIONS["Microsoft.Network/networkInterfaces"],
        ).properties["ipConfigurations"][0]["properties"]

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
            resources.append(self.get_by_id(
                resource_id=iface.id,
                api_version=API_VERSIONS["Microsoft.Network/networkInterfaces"],
            ))
            if public_ip := self._get_ip_configuration_dict(network_interface_id=iface.id).get("publicIPAddress"):
                resources.append(self.get_by_id(
                    resource_id=public_ip["id"],
                    api_version=API_VERSIONS["Microsoft.Network/publicIPAddresses"],
                ))
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
                        assert response.result_truncated == "false", "paging is not possible because you missed id column"
                        break
                    LOGGER.debug("get next page of query=%r", query)
                    request.options.skip_token = response.skip_token
                except HttpResponseError as e:
                    if "RateLimiting" in str(e) and retry_count < max_retries:
                        retry_count += 1
                        # Exponential backoff with jitter: 2, 4, 8, 16, 32 seconds (with random jitter)
                        delay = (base_delay ** retry_count) + random.uniform(0.5, 1.5)
                        LOGGER.warning("Azure Resource Graph rate limiting encountered. "
                                       "Retrying in %d seconds (attempt %d/%d). Query: %s",
                                       delay, retry_count, max_retries, query)
                        time.sleep(delay)
                        continue
                    else:
                        # Re-raise if not rate limiting or max retries exceeded
                        raise

        return chain.from_iterable(paged_query())


def list_instances_azure(tags_dict: Optional[dict[str, str]] = None,
                         running: bool = False,
                         verbose: bool = False) -> list[VirtualMachine]:
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
    res = azure_service.resource_graph_query(query=' | '.join(query_bits))
    get_virtual_machine = azure_service.compute.virtual_machines.get
    instances = [get_virtual_machine(resource_group_name=vm["resourceGroup"], vm_name=vm["name"]) for vm in res]
    if verbose:
        LOGGER.info("Done. Found total of %s instances.", len(instances))

    return instances


def azure_check_instance_type_available(instance_type: str, location: str) -> bool:
    """
    Check if instance type is available in the given location.
    """
    azure_service = AzureService()
    return any(instance_type in size.name for size in azure_service.compute.virtual_machine_sizes.list(location=location))

from functools import cached_property

from azure.identity import ClientSecretCredential
from azure.mgmt.compute.v2021_07_01 import ComputeManagementClient
from azure.mgmt.network.v2021_02_01 import NetworkManagementClient
from azure.mgmt.resource.resources.v2021_04_01 import ResourceManagementClient

from sdcm.keystore import KeyStore


class AzureMixing:
    @cached_property
    def _credential(self) -> 'TokenCredential':
        return ClientSecretCredential(
            tenant_id=self._azure_credentials["tenant_id"],
            client_id=self._azure_credentials["client_id"],
            client_secret=self._azure_credentials["client_secret"],
        )

    @cached_property
    def _azure_credentials(self):
        return KeyStore().get_azure_credentials()

    @cached_property
    def _subscription_id(self) -> str:
        return self._azure_credentials["subscription_id"]

    @cached_property
    def _compute(self) -> ComputeManagementClient:
        return ComputeManagementClient(credential=self._credential, subscription_id=self._subscription_id)

    @cached_property
    def _network(self) -> NetworkManagementClient:
        return NetworkManagementClient(credential=self._credential, subscription_id=self._subscription_id)

    @cached_property
    def _resource(self) -> ResourceManagementClient:
        return ResourceManagementClient(credential=self._credential, subscription_id=self._subscription_id)


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
import logging
from dataclasses import dataclass

from azure.core.exceptions import AzureError, HttpResponseError
from sdcm.utils.azure_utils import AzureService
from sdcm.keystore import KeyStore
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


@dataclass
class AzureKmsProvider:
    _resource_group_name: str
    _region: str
    _az: str
    _azure_service: AzureService = AzureService()

    def __post_init__(self):
        self._kms_config = KeyStore().get_azure_kms_config()

    @property
    def managed_identity_config(self):
        return {
            "resource_group": self._kms_config["resource_group"],
            "identity_name": self._kms_config["identity_name"],
            "principal_id": self._kms_config["managed_identity_principal_id"],
        }

    @property
    def sct_service_principal_id(self):
        return self._kms_config["sct_service_principal_id"]

    @classmethod
    def _get_vault_name(cls, region: str) -> str:
        """Generate vault name for the given region"""
        kms_config = KeyStore().get_azure_kms_config()
        return f"{kms_config['shared_vault_name']}-{region}"

    def _get_managed_identity_id(self) -> str:
        return (
            f"/subscriptions/{self._azure_service.subscription_id}"
            f"/resourcegroups/{self.managed_identity_config['resource_group']}"
            "/providers/Microsoft.ManagedIdentity"
            f"/userAssignedIdentities/{self.managed_identity_config['identity_name']}"
        )

    @classmethod
    def get_key_uri_for_test(cls, region: str, test_id: str) -> str:
        vault_name = cls._get_vault_name(region)
        vault_uri = f"https://{vault_name}.vault.azure.net/"
        kms_config = KeyStore().get_azure_kms_config()
        num_of_keys = kms_config["num_of_keys"]
        key_number = (hash(test_id) % num_of_keys) + 1
        return f"{vault_uri}scylla-key-{key_number}"

    @staticmethod
    def _is_conflict_error(error: AzureError) -> bool:
        """Check if the error is a ConflictError from Azure Key Vault."""
        if isinstance(error, HttpResponseError):
            # Check error code in the response
            if hasattr(error, 'error') and hasattr(error.error, 'code'):
                return error.error.code == 'ConflictError'
            # Fallback: check message for ConflictError indication
            error_msg = str(error)
            return 'ConflictError' in error_msg or 'conflict occurred' in error_msg.lower()
        return False

    @retrying(n=5, sleep_time=10, allowed_exceptions=(AzureError,), message="Retrying Key Vault setup due to conflict")
    def _create_or_update_keyvault(self, vault_name: str):
        """Create or update Azure Key Vault with retry logic for conflict errors."""
        return self._azure_service.keyvault.vaults.begin_create_or_update(
            resource_group_name=self._kms_config["resource_group"],
            vault_name=vault_name,
            parameters={
                "location": self._region,
                "properties": {
                    "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                    "sku": {"name": "standard", "family": "A"},
                    "enabled_for_disk_encryption": True,
                    "enable_rbac_authorization": False,
                    "access_policies": [
                        {
                            "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                            "object_id": self.managed_identity_config["principal_id"],
                            "permissions": {
                                "keys": ["get", "encrypt", "decrypt", "wrapKey", "unwrapKey"],
                                "secrets": ["get"],
                                "certificates": ["get"],
                            },
                        },
                        {
                            # SCT service principal
                            "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                            "object_id": self.sct_service_principal_id,
                            "permissions": {
                                "keys": ["create", "get", "list", "update", "import", "delete", "rotate"],
                                "secrets": ["get"],
                                "certificates": ["get"],
                            },
                        },
                    ],
                },
            },
        ).result()

    def get_or_create_keyvault_and_identity(self, test_id: str):
        """Use fixed vault with keys.

        Returns dict with vault info on success, None on failure.
        The vault creation is retried automatically if Azure returns a ConflictError
        due to parallel operations on the same Key Vault.
        """
        vault_name = self._get_vault_name(self._region)
        try:
            vault = self._create_or_update_keyvault(vault_name)
            vault_uri = vault.properties.vault_uri

            # Pick one key, if required create keys.
            num_of_keys = self._kms_config["num_of_keys"]
            for i in range(1, num_of_keys + 1):
                key_name = f"scylla-key-{i}"
                if not self._azure_service.get_vault_key(vault_uri, key_name):
                    self._azure_service.create_vault_key(vault_uri, key_name)
                    LOGGER.info(f"Created key: {key_name}")

            key_number = (hash(test_id) % num_of_keys) + 1
            key_uri = f"{vault_uri}scylla-key-{key_number}"
            vault_info = {"identity_id": self._get_managed_identity_id(), "vault_uri": vault_uri, "key_uri": key_uri}
            return vault_info
        except AzureError as e:
            LOGGER.error(f"Failed to setup Azure KMS after retries: {e}")
            return None


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

from sdcm.utils.azure_utils import AzureService

LOGGER = logging.getLogger(__name__)


@dataclass
class KmsProvider:
    _resource_group_name: str
    _region: str
    _az: str
    _azure_service: AzureService = AzureService()
    MANAGED_IDENTITY_CONFIG = {
        'resource_group': 'qa_azure_key_vault',
        'identity_name': 'myManagedIdentity',
        'principal_id': '0b5c8c2a-faa1-41d8-ab2d-e9605c2b8654'
    }
    SCT_SERVICE_PRINCIPAL_ID = "6c580092-c65c-4527-93d5-25177e10f54e"

    def _get_managed_identity_id(self) -> str:
        return f"/subscriptions/{self._azure_service.subscription_id}/resourcegroups/{self.MANAGED_IDENTITY_CONFIG['resource_group']}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{self.MANAGED_IDENTITY_CONFIG['identity_name']}"

    def get_or_create_keyvault_and_identity(self):
        """Create dynamic vault with existing identity"""
        vault_name = f"scylla{self._resource_group_name.split('-')[1][:8]}kv"[:24]
        try:
            # Create vault
            vault = self._azure_service.keyvault.vaults.begin_create_or_update(
                resource_group_name=self._resource_group_name, vault_name=vault_name,
                parameters={
                    "location": self._region,
                    "properties": {
                        "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                        "sku": {"name": "standard", "family": "A"},
                        "enabled_for_disk_encryption": True,
                        "enable_rbac_authorization": False,
                        "access_policies": [{
                            "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                            "object_id": self.MANAGED_IDENTITY_CONFIG['principal_id'],
                            "permissions": {
                                "keys": ["get", "encrypt", "decrypt", "wrapKey", "unwrapKey"],
                                "secrets": ["get"],
                                "certificates": ["get"]
                            }
                        },
                            {
                            # SCT service principal (for key creation)
                            "tenant_id": self._azure_service.azure_credentials["tenant_id"],
                            "object_id": self.SCT_SERVICE_PRINCIPAL_ID,
                            "permissions": {
                                "keys": ["create", "get", "list"],
                                "secrets": ["get"],
                                "certificates": ["get"]
                            }
                        }],
                    }
                }
            ).result()
            LOGGER.info(f"Created vault with existing identity access: {vault_name}")

            # Create key
            key_id = self._azure_service.create_vault_key(vault_uri=vault.properties.vault_uri, key_name="scylla-key")
            vault_info = {'identity_id': self._get_managed_identity_id(), 'vault_uri': vault.properties.vault_uri,
                          'key_name': 'scylla-key', 'key_uri': key_id}
            LOGGER.info(f"vault_uri item : {vault_info}")
            return vault_info
        except Exception as e:  # noqa: BLE001
            LOGGER.warning(f"Failed to Setup Keyvault: {e}")
            return None

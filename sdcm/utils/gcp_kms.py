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

from google.cloud import kms
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class GcpKms:

    def __init__(self, project_id: str, location: str, key_name: str):
        self._gcp_kms_config = KeyStore().get_gcp_kms_config()
        self.key_name = key_name
        self.project_id = project_id
        self.location = location
        keyring_name = self._gcp_kms_config['keyring_name']
        self.keyring_name = keyring_name
        self.keyring_path = (
            f"projects/{project_id}/locations/{location}"
            f"/keyRings/{keyring_name}"
        )
        self.key_path = f"{self.keyring_path}/cryptoKeys/{key_name}"

        # Create credentials from service account info
        credentials = service_account.Credentials.from_service_account_info(
            KeyStore().get_gcp_credentials())
        self.client = kms.KeyManagementServiceClient(credentials=credentials)

    def create_test_key(self):
        self.client.create_crypto_key(
            parent=self.keyring_path,
            crypto_key_id=self.key_name,
            crypto_key={'purpose': kms.CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT}
        )
        LOGGER.info("Created key: %s", self.key_name)

    def get_or_create_key(self):
        try:
            self.client.get_crypto_key(name=self.key_path)
            LOGGER.info("Using existing GCP KMS key: %s", self.key_path)
        except GoogleCloudError:
            self.create_test_key()

    def rotate_key(self):
        self.client.create_crypto_key_version(parent=self.key_path, crypto_key_version={})
        LOGGER.info("Rotated GCP KMS key '%s'", self.key_path)

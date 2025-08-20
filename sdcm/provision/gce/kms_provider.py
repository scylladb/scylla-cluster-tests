import logging
from dataclasses import dataclass

from google.cloud.exceptions import GoogleCloudError
from sdcm.utils.gcp_kms import GcpKms
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


@dataclass
class GcpKmsProvider:

    def __post_init__(self):
        self._gcp_kms_config = KeyStore().get_gcp_kms_config()
        gcp_credentials = KeyStore().get_gcp_credentials()
        self._project_id = gcp_credentials['project_id']
        self._location = self._gcp_kms_config['keyring_location']

    @classmethod
    def get_key_name_for_test(cls, test_id: str) -> str:
        gcp_kms_config = KeyStore().get_gcp_kms_config()
        num_of_keys = gcp_kms_config['num_of_keys']
        key_number = (hash(test_id) % num_of_keys) + 1
        return f"scylla-key-{key_number}"

    @classmethod
    def get_key_uri_for_test(cls, test_id: str) -> str:
        gcp_kms_config = KeyStore().get_gcp_kms_config()
        keyring_name = gcp_kms_config['keyring_name']
        key_name = cls.get_key_name_for_test(test_id)
        return f"{keyring_name}/{key_name}"

    def get_or_create_keys_pool(self, test_id: str):
        try:
            num_of_keys = self._gcp_kms_config['num_of_keys']
            for i in range(1, num_of_keys + 1):
                key_name = f"scylla-key-{i}"
                gcp_kms = GcpKms(self._project_id, self._location, key_name)
                gcp_kms.get_or_create_key()
                LOGGER.info("GCP KMS key ready in pool: %s", key_name)

            key_name = self.get_key_name_for_test(test_id)
            key_uri = self.get_key_uri_for_test(test_id)

            key_info = {
                'project_id': self._project_id,
                'location': self._location,
                'keyring_name': self._gcp_kms_config['keyring_name'],
                'key_name': key_name,
                'key_uri': key_uri
            }
            return key_info
        except GoogleCloudError as e:
            LOGGER.warning("Failed to setup GCP KMS key pool: %s", e)
            return None

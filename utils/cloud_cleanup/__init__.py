import os
import sys
from functools import partial
from typing import Literal, Callable

from argus.client.sct.client import ArgusSCTClient

from sdcm.keystore import KeyStore
from sdcm.utils.log import setup_stdout_logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
LOGGER = setup_stdout_logger()


def argus_client_factory() -> Callable[[str], ArgusSCTClient]:
    """Factory function to create ArgusSCTClient instances with pre-configured credentials."""
    creds = KeyStore().get_argus_rest_credentials()
    return partial(ArgusSCTClient, auth_token=creds["token"], base_url=creds["baseUrl"], extra_headers=creds.get("extra_headers"))


argus_client = argus_client_factory()


def update_argus_resource_status(test_id: str, resource_name: str, action: Literal['terminate', 'stop']):
    if not test_id and not resource_name:
        LOGGER.error("Skip update Argus due missing test_id and resource_name")
        return
    try:
        client = argus_client(test_id)
        client.terminate_resource(name=resource_name, reason=f'cloud-cleanup: {action} resource due to expiration')
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error("Failed to update Argus resource status: %s", exc)
        return

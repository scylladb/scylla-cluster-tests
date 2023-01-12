import logging
from uuid import UUID

from argus.client.sct.client import ArgusSCTClient
from argus.client.base import ArgusClientError
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


def get_argus_client(run_id: UUID | str) -> ArgusSCTClient:
    creds = KeyStore().get_argus_rest_credentials()
    argus_client = ArgusSCTClient(run_id=run_id, auth_token=creds["token"], base_url=creds["baseUrl"])

    return argus_client


def terminate_resource_in_argus(client: ArgusSCTClient, resource_name: str):
    try:
        client.terminate_resource(name=resource_name, reason="clean-resources: Graceful Termination")
    except ArgusClientError as exc:
        if len(exc.args) >= 3 and exc.args[2] == 404:
            LOGGER.warning("%s doesn't exist in Argus", resource_name)
        else:
            LOGGER.error("Failure to communicate resource deletion to Argus", exc_info=True)

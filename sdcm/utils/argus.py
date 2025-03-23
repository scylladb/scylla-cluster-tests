import logging
from uuid import UUID

from argus.client.sct.client import ArgusSCTClient
from argus.client.base import ArgusClientError
from sdcm.keystore import KeyStore

LOGGER = logging.getLogger(__name__)


class ArgusError(Exception):

    def __init__(self, message: str, *args: list) -> None:
        self._message = message
        super().__init__(*args)

    @property
    def message(self):
        return self._message


def is_uuid(uuid) -> bool:
    if isinstance(uuid, UUID):
        return True

    try:
        UUID(uuid)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def get_argus_client(run_id: UUID | str) -> ArgusSCTClient:
    if not is_uuid(run_id):
        raise ArgusError("Malformed UUID provided")
    creds = KeyStore().get_argus_rest_credentials()
    argus_client = ArgusSCTClient(
        run_id=run_id, auth_token=creds["token"], base_url=creds["baseUrl"], extra_headers=creds.get("extra_headers"))

    return argus_client


def terminate_resource_in_argus(client: ArgusSCTClient, resource_name: str):
    try:
        client.terminate_resource(name=resource_name, reason="clean-resources: Graceful Termination")
    except ArgusClientError as exc:
        if len(exc.args) >= 3 and exc.args[2] == 404:
            LOGGER.warning("%s doesn't exist in Argus", resource_name)
        else:
            LOGGER.error("Failure to communicate resource deletion to Argus", exc_info=True)

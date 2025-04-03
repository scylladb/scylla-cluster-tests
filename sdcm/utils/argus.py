import logging
import os
from pathlib import Path
from uuid import UUID

from argus.client.sct.client import ArgusSCTClient
from argus.client.base import ArgusClientError
from argus.client.sct.types import EventsInfo
from sdcm.keystore import KeyStore
from sdcm.sct_events.events_processes import EventsProcessesRegistry
from sdcm.sct_events.events_device import start_events_main_device
from sdcm.sct_events.file_logger import get_logger_event_summary, start_events_logger, get_events_grouped_by_category

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


def argus_offline_collect_events(client: ArgusSCTClient) -> None:
    base_results_dir = Path(os.environ.get("HOME")) / "sct-results"
    latest_log = os.readlink(str(base_results_dir / "latest"))
    log_dir = base_results_dir / latest_log
    r = EventsProcessesRegistry(log_dir=log_dir)
    start_events_main_device(_registry=r)
    start_events_logger(_registry=r)
    last_events = get_events_grouped_by_category(limit=100, _registry=r)
    events_sorted = []
    events_summary = get_logger_event_summary(_registry=r)
    for severity, messages in last_events.items():
        event_category = EventsInfo(severity=severity, total_events=events_summary.get(severity, 0), messages=messages)
        events_sorted.append(event_category)
    client.submit_events(events_sorted)

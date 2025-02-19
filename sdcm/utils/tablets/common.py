import logging
import time
from dataclasses import dataclass
from typing import Optional

from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)


@dataclass
class TabletsConfiguration:
    enabled: Optional[bool] = None
    initial: Optional[int] = None

    def __str__(self):
        items = []
        for k, v in self.__dict__.items():
            if v is not None:
                value = str(v).lower() if isinstance(v, bool) else v
                items.append(f"'{k}': {value}")
        return '{' + ', '.join(items) + '}'


def wait_no_tablets_migration_running(node, timeout: int = 3600):
    """
    Waiting for having no ongoing tablets topology operations using REST API.
    !!! It does not guarantee that tablets are balanced !!!
    Keep in mind that it is good only to find when ongoing tablets topology operations is done.
    Very next second another topology operation can be started.

    doing it several times as there's a risk of:
    "currently a small time window after adding nodes and before load balancing starts during which
    topology may appear as quiesced because the state machine goes through an idle state before it enters load balancing state"
    """
    if not is_tablets_feature_enabled(node):
        LOGGER.info("Tablets are disabled, skipping wait for balance")
        return
    time.sleep(60)  # one minute gap before checking, just to give some time to the state machine
    client = RemoteCurlClient(host="127.0.0.1:10000", endpoint="", node=node)
    LOGGER.info("Waiting for having no ongoing tablets topology operations")
    try:
        with adaptive_timeout(Operations.TABLET_MIGRATION, node, timeout=timeout):
            client.run_remoter_curl(method="POST", path="storage_service/quiesce_topology",
                                    params={}, timeout=4*3600)
        LOGGER.info("All ongoing tablets topology operations are done")
    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        InfoEvent(f"Failed to wait for having no ongoing tablets topology operations. Exception: {exc.__repr__()}",
                  severity=Severity.ERROR).publish()

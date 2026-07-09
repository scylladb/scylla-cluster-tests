import logging
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
        return "{" + ", ".join(items) + "}"


def wait_tablets_balanced(node, timeout: int = 3600):
    """
    Wait for tablets to be balanced, no more pending splits/merges and no ongoing tablets topology operations using REST API.
    A single request is enough as it is submitted as a global topology request and completes only after fresh tablet load stats produce an empty balance plan and tablets are idle."""
    if not is_tablets_feature_enabled(node):
        LOGGER.info("Tablets are disabled, skipping wait for balance")
        return
    client = RemoteCurlClient(host="127.0.0.1:10000", endpoint="", node=node)
    LOGGER.info("Waiting for tablets balancing (no pending splits/merges, no ongoing topology operations)")
    try:
        with adaptive_timeout(Operations.TABLET_MIGRATION, node, timeout=timeout) as adaptive_timeout_value:
            client.run_remoter_curl(
                method="POST", path="storage_service/quiesce_topology", params={}, timeout=adaptive_timeout_value
            )
        LOGGER.info("Tablets are balanced")
    except Exception as exc:  # noqa: BLE001
        InfoEvent(
            f"Failed to wait for tablets to be balanced. Exception: {exc.__repr__()}",
            severity=Severity.ERROR,
        ).publish()

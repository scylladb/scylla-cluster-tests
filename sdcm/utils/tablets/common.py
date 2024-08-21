import logging
import time
from dataclasses import dataclass
from typing import Optional

from sdcm.rest.remote_curl_client import RemoteCurlClient
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


def wait_for_tablets_balanced(node):
    """
    Waiting for tablets to be balanced using REST API.

    doing it several times as there's a risk of:
    "currently a small time window after adding nodes and before load balancing starts during which
    topology may appear as quiesced because the state machine goes through an idle state before it enters load balancing state"
    """
    if not node.raft.is_enabled:
        LOGGER.info("Raft is disabled, skipping wait for balance")
        return
    with node.parent_cluster.cql_connection_patient(node=node) as session:
        if not is_tablets_feature_enabled(session):
            LOGGER.info("Tablets are disabled, skipping wait for balance")
            return
    time.sleep(60)  # one minute gap before checking, just to give some time to the state machine
    client = RemoteCurlClient(host="127.0.0.1:10000", endpoint="", node=node)
    LOGGER.info("Waiting for tablets to be balanced")
    for _ in range(3):
        client.run_remoter_curl(method="POST", path="storage_service/quiesce_topology",
                                params={}, timeout=3600, retry=3)
        time.sleep(5)
    LOGGER.info("Tablets are balanced")

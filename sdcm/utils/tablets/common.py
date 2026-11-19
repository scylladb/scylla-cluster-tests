import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

from cassandra import InvalidRequest

from sdcm import wait
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.rest.task_manager_client import TaskManagerClient
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.adaptive_timeouts import Operations, adaptive_timeout
from sdcm.utils.cluster_config import get_cluster_config, set_cluster_config
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)

# CQL cluster configuration option of Automatic Repair MS2 (SCYLLADB-100)
AUTO_REPAIR_OPTION = "auto_repair_enabled"
# In-flight tablet auto-repair sessions were observed running up to ~17 minutes (SCT-905)
AUTO_REPAIR_DRAIN_TIMEOUT = 30 * 60


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
    A single request is enough as it is submitted as a global topology request and completes only after fresh tablet load stats produce an empty balance plan and tablets are idle.
    """
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


def wait_no_active_repair_tasks(nodes: list, timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT, step: int = 30) -> bool:
    """Wait until no node reports active repair-module tasks. Returns False on timeout instead of raising."""

    def no_active_repair_tasks():
        for node in nodes:
            try:
                if active_tasks := TaskManagerClient(node).get_active_repair_tasks():
                    LOGGER.debug("Node %s still runs %d repair tasks", node.name, len(active_tasks))
                    return False
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Could not list repair tasks on node %s, skipping it: %s", node.name, exc)
        return True

    return bool(
        wait.wait_for(
            func=no_active_repair_tasks,
            timeout=timeout,
            step=step,
            throw_exc=False,
            text="Waiting for in-flight repair tasks to finish",
        )
    )


@contextmanager
def temporarily_disable_auto_repair(node, drain_timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT):
    """Disable tablet auto-repair and drain in-flight repair tasks before yielding, so a
    user-driven repair can run without colliding with it (SCT-905). Restores it on exit.

    Runs ALTER CLUSTER WITH auto_repair_enabled = false on `node`, which the caller picks as a node
    that stays up, and restores the previously stored value on exit. A failed restore is retried for
    5 minutes, otherwise auto-repair would stay off for the rest of the test. Raises without yielding
    when the disable fails. The restore also runs when the disable or the drain is interrupted.
    A keyspace or table that stores its own auto_repair_enabled = true keeps auto-repair.

    No-op when auto-repair is off, which also makes nested use safe: an inner use sees auto-repair
    already off. Also a no-op on Scylla releases without CQL cluster configuration.
    Not guarded against parallel nemeses: a sibling nemesis exiting first re-enables it.
    """
    try:
        stored_value = get_cluster_config(node, AUTO_REPAIR_OPTION)
    except InvalidRequest:
        LOGGER.info("No CQL cluster configuration on this Scylla release, leaving auto-repair as is")
        stored_value = None
    was_enabled = stored_value == "true"

    def restore_auto_repair():
        set_cluster_config(node, AUTO_REPAIR_OPTION, stored_value)
        return True

    try:
        if was_enabled:
            set_cluster_config(node, AUTO_REPAIR_OPTION, False)
            if not wait_no_active_repair_tasks(node.parent_cluster.data_nodes, timeout=drain_timeout):
                LOGGER.warning(
                    "In-flight repair tasks did not finish within %s seconds, proceeding anyway", drain_timeout
                )
        yield
    finally:
        if was_enabled and not wait.wait_for(
            func=restore_auto_repair,
            step=15,
            timeout=5 * 60,
            throw_exc=False,
            text=f"Restoring the cluster-scope {AUTO_REPAIR_OPTION}",
        ):
            LOGGER.error("Failed to restore the cluster-scope %s to %s", AUTO_REPAIR_OPTION, stored_value)

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

from sdcm import wait
from sdcm.rest.remote_curl_client import RemoteCurlClient
from sdcm.rest.task_manager_client import TaskManagerClient
from sdcm.sct_events import Severity
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)

AUTO_REPAIR_PARAM = "auto_repair_enabled_default"
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


def set_node_auto_repair(node, enabled: bool) -> bool:
    """Set the auto-repair flag on one node: live-update the running scylla via CQL and persist
    it to scylla.yaml (no restart), so restarts and config reloads keep the value.
    """
    if not node.set_scylla_config_param(AUTO_REPAIR_PARAM, str(enabled).lower()):
        return False
    try:
        with node.remote_scylla_yaml() as scylla_yaml:
            setattr(scylla_yaml, AUTO_REPAIR_PARAM, enabled)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning(
            "Set %s=%s live on node %s but failed to persist it to scylla.yaml: %s",
            AUTO_REPAIR_PARAM,
            enabled,
            node.name,
            exc,
        )
    return True


# Nested and parallel-nemesis safety: only the first entrant disables auto-repair and only the
# last one out re-enables it, so a sibling nemesis exiting cannot re-enable it mid-repair
AUTO_REPAIR_GUARD = threading.Lock()
AUTO_REPAIR_DISABLE_STATE = {"depth": 0, "disabled_nodes": []}


@contextmanager
def temporarily_disable_auto_repair(db_cluster, drain_timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT):
    """Disable tablet auto-repair on all data nodes and drain in-flight repair tasks before
    yielding, so a user-driven repair can run without colliding with it (SCT-905).
    Re-enables it on exit. No-op when auto-repair is off or unsupported.
    Reentrant and thread-safe: nested or concurrent uses toggle only once, on the outermost.
    """
    disabled_nodes = AUTO_REPAIR_DISABLE_STATE["disabled_nodes"]
    try:
        with AUTO_REPAIR_GUARD:
            AUTO_REPAIR_DISABLE_STATE["depth"] += 1
            if AUTO_REPAIR_DISABLE_STATE["depth"] == 1:
                for node in db_cluster.data_nodes:
                    prior_value = node.get_scylla_config_param(AUTO_REPAIR_PARAM, verbose=False)
                    if prior_value is None or prior_value.strip() != "true":
                        continue
                    if set_node_auto_repair(node, enabled=False):
                        disabled_nodes.append(node)
                    else:
                        LOGGER.warning("Could not disable auto-repair on node %s, skipping it", node.name)
                if disabled_nodes:
                    LOGGER.info("Temporarily disabled tablet auto-repair on %d nodes", len(disabled_nodes))
                    # Drain all data nodes, not only the just-disabled ones: a node that was already
                    # disabled or unreachable can still be running an in-flight repair session
                    if not wait_no_active_repair_tasks(db_cluster.data_nodes, timeout=drain_timeout):
                        LOGGER.warning(
                            "In-flight repair tasks did not finish within %s seconds, proceeding anyway",
                            drain_timeout,
                        )
        yield
    finally:
        with AUTO_REPAIR_GUARD:
            AUTO_REPAIR_DISABLE_STATE["depth"] -= 1
            if AUTO_REPAIR_DISABLE_STATE["depth"] == 0:
                for node in disabled_nodes:
                    if set_node_auto_repair(node, enabled=True):
                        LOGGER.info("Re-enabled tablet auto-repair on node %s", node.name)
                    else:
                        LOGGER.warning("Failed to re-enable auto-repair on node %s", node.name)
                disabled_nodes.clear()

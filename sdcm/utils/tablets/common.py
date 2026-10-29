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
from sdcm.utils.adaptive_timeouts import adaptive_timeout, Operations
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)

AUTO_REPAIR_PARAM = "auto_repair_enabled_default"
# CQL cluster configuration option that supersedes AUTO_REPAIR_PARAM (Automatic Repair MS2, SCYLLADB-100)
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


def get_cluster_config(db_cluster, option: str) -> Optional[str]:
    """Return the value stored for a CQL cluster configuration option at CLUSTER scope, or None
    when nothing is stored there. Raises InvalidRequest on Scylla releases without CQL cluster
    configuration, where system_schema.scylla_clusters does not exist.
    """
    with db_cluster.cql_connection_patient(db_cluster.data_nodes[0], verbose=False) as session:
        row = session.execute("SELECT configs FROM system_schema.scylla_clusters").one()
    return (row.configs or {}).get(option) if row else None


def set_cluster_config(db_cluster, option: str, value: Optional[str]) -> bool:
    """Store a CQL cluster configuration option at CLUSTER scope; None removes the stored value."""
    statement = f"ALTER CLUSTER WITH {option} = {'NULL' if value is None else value}"
    try:
        with db_cluster.cql_connection_patient(db_cluster.data_nodes[0], verbose=False) as session:
            session.execute(statement)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to run '%s': %s", statement, exc)
        return False
    LOGGER.info("Ran '%s'", statement)
    return True


def drain_repair_tasks(db_cluster, drain_timeout: int) -> None:
    # Drain all data nodes, not only the ones auto-repair was disabled on: a node that was
    # already disabled or unreachable can still be running an in-flight repair session
    if not wait_no_active_repair_tasks(db_cluster.data_nodes, timeout=drain_timeout):
        LOGGER.warning("In-flight repair tasks did not finish within %s seconds, proceeding anyway", drain_timeout)


@contextmanager
def temporarily_disable_auto_repair(db_cluster, drain_timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT):
    """Disable tablet auto-repair and drain in-flight repair tasks before yielding, so a
    user-driven repair can run without colliding with it (SCT-905). Restores it on exit.

    With CQL cluster configuration, runs ALTER CLUSTER WITH auto_repair_enabled = false, which also
    overrides the deprecated scylla.yaml option on every node, and restores the previously stored
    value (or NULL) on exit. A keyspace or table that stores its own auto_repair_enabled = true keeps
    auto-repair. Older releases fall back to toggling the scylla.yaml option on each node.

    No-op when auto-repair is off or unsupported, which also makes nested use safe: an inner use
    sees auto-repair already off.
    Not guarded against parallel nemeses: a sibling nemesis exiting first re-enables it.
    """
    try:
        stored_value = get_cluster_config(db_cluster, AUTO_REPAIR_OPTION)
    except InvalidRequest:
        disable_auto_repair = temporarily_disable_node_auto_repair(db_cluster, drain_timeout)
    else:
        disable_auto_repair = temporarily_disable_cluster_auto_repair(db_cluster, stored_value, drain_timeout)
    with disable_auto_repair:
        yield


@contextmanager
def temporarily_disable_cluster_auto_repair(db_cluster, stored_value: Optional[str], drain_timeout: int):
    """Variant of temporarily_disable_auto_repair for Scylla releases with CQL cluster configuration.
    stored_value is the auto_repair_enabled value stored at CLUSTER scope, restored on exit.
    """
    if stored_value is None:
        # Nothing is stored at CLUSTER scope, so the deprecated scylla.yaml option decides on each node
        enabled = any(
            (node.get_scylla_config_param(AUTO_REPAIR_PARAM, verbose=False) or "").strip() == "true"
            for node in db_cluster.data_nodes
        )
    else:
        enabled = stored_value == "true"
    disabled = enabled and set_cluster_config(db_cluster, AUTO_REPAIR_OPTION, "false")
    if disabled:
        drain_repair_tasks(db_cluster, drain_timeout)
    try:
        yield
    finally:
        if disabled and not set_cluster_config(db_cluster, AUTO_REPAIR_OPTION, stored_value):
            LOGGER.error("Failed to restore the cluster-scope %s to %s", AUTO_REPAIR_OPTION, stored_value)


@contextmanager
def temporarily_disable_node_auto_repair(db_cluster, drain_timeout: int = AUTO_REPAIR_DRAIN_TIMEOUT):
    """Variant of temporarily_disable_auto_repair for Scylla releases without CQL cluster
    configuration: toggles the scylla.yaml option on all data nodes, and re-enables it on exit on
    the nodes it disabled.
    """
    disabled_nodes = []
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
        drain_repair_tasks(db_cluster, drain_timeout)
    try:
        yield
    finally:
        for node in disabled_nodes:
            if set_node_auto_repair(node, enabled=True):
                LOGGER.info("Re-enabled tablet auto-repair on node %s", node.name)
            else:
                LOGGER.error("Failed to re-enable auto-repair on node %s", node.name)

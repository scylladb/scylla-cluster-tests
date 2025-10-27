import logging
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any

from sdcm.sct_events.system import SoftTimeoutEvent
from sdcm.utils.adaptive_timeouts.load_info_store import NodeLoadInfoService, AdaptiveTimeoutStore, ESAdaptiveTimeoutStore, \
    NodeLoadInfoServices

LOGGER = logging.getLogger(__name__)


def _get_decommission_timeout(node_info_service: NodeLoadInfoService) -> tuple[int, dict[str, Any]]:
    """Calculate timeout for decommission operation based on node load info. Still experimental, used to gather historical data."""
    try:
        # rough estimation from previous runs almost 9h for 1TB
        timeout = int(node_info_service.node_data_size_mb * 0.03)
        timeout = max(timeout, 7200)  # 2 hours minimum
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.warning("Failed to calculate decommission timeout: \n%s \nDefaulting to 6 hours", exc)
        return 6*60*60, {}


def _get_soft_timeout(node_info_service: NodeLoadInfoService, timeout: int | float = None) -> tuple[int | float, dict[str, Any]]:
    # no timeout calculation - just return the timeout passed as argument along with node load info
    try:
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.warning("Failed to get node info for timeout: \n%s", exc)
        return timeout, {}


def _get_query_timeout(node_info_service: NodeLoadInfoService, timeout: int | float = None, query: str = None) -> \
        tuple[int | float, dict[str, Any]]:
    timeout, stats = _get_soft_timeout(node_info_service=node_info_service, timeout=timeout)
    stats["query"] = query
    return timeout, stats


def _get_service_level_propagation_timeout(node_info_service: NodeLoadInfoService, timeout: int | float = None,
                                           service_level_for_test_step: str = None) -> \
        tuple[int | float, dict[str, Any]]:
    """service_level_for_test_step will report in ES on which step of test the timeout happened"""
    timeout, stats = _get_soft_timeout(node_info_service=node_info_service, timeout=timeout)
    stats["service_level_for_test_step"] = service_level_for_test_step
    return timeout, stats


class Operations(Enum):
    """Available operations for adaptive timeouts. Each operation maps to a function to calculate timeout based on node load info.

    maps to (function, (required arguments))"""
    DECOMMISSION = ("decommission", _get_decommission_timeout, ())
    NEW_NODE = ("new_node", _get_soft_timeout, ("timeout",))
    CREATE_INDEX = ("create_index", _get_soft_timeout, ("timeout",))
    START_SCYLLA = ("start_scylla", _get_soft_timeout, ("timeout",))
    RESTART_SCYLLA = ("restart_scylla", _get_soft_timeout, ("timeout",))
    CREATE_MV = ("create_mv", _get_soft_timeout, ("timeout",))
    MAJOR_COMPACT = ("major_compact", _get_soft_timeout, ("timeout",))
    REPAIR = ("repair", _get_soft_timeout, ("timeout",))
    MGMT_REPAIR = ("mgmt_repair", _get_soft_timeout, ("timeout",))
    BACKUP = ("backup", _get_soft_timeout, ("timeout",))
    RESTORE = ("restore", _get_soft_timeout, ("timeout",))
    REBUILD = ("rebuild", _get_soft_timeout, ("timeout",))
    CLEANUP = ("cleanup", _get_soft_timeout, ("timeout",))
    REMOVE_NODE = ("remove_node", _get_soft_timeout, ("timeout",))
    SCRUB = ("scrub", _get_soft_timeout, ("timeout",))
    SOFT_TIMEOUT = ("soft_timeout", _get_soft_timeout, ("timeout",))
    FLUSH = ("flush", _get_soft_timeout, ("timeout",))
    QUERY = ("query", _get_query_timeout, ("timeout", "query"))
    SERVICE_LEVEL_PROPAGATION = ("service_level_propagation", _get_service_level_propagation_timeout,
                                 ("timeout", "service_level_for_test_step"))
<<<<<<< HEAD
||||||| parent of 68b70fa88 (fix(wait_ssh_up): use adaptive_timeout and triple the hard timeout)
    TABLET_MIGRATION = ("tablet_migration", _get_soft_timeout, ("timeout",))
    HEALTHCHECK = ("healthcheck", _get_soft_timeout, ("timeout",))
=======
    TABLET_MIGRATION = ("tablet_migration", _get_soft_timeout, ("timeout",))
    HEALTHCHECK = ("healthcheck", _get_soft_timeout, ("timeout",))
    SSH_CONNECTIVITY = ("ssh_connectivity", _get_soft_timeout, ("timeout",))
>>>>>>> 68b70fa88 (fix(wait_ssh_up): use adaptive_timeout and triple the hard timeout)


class TestInfoServices:  # pylint: disable=too-few-public-methods
    @staticmethod
    def get(node: "BaseNode") -> dict:
        return dict(
            n_db_nodes=len(node.parent_cluster.nodes),
        )


@contextmanager
def adaptive_timeout(operation: Operations, node: "BaseNode", stats_storage: AdaptiveTimeoutStore = ESAdaptiveTimeoutStore(), **kwargs):
    """
    Calculate timeout in seconds for given operation based on node load info and return its value.
    Upon exit, verify if timeout occurred and publish SoftTimeoutEvent if happened.
    Also store node load info and timeout value in AdaptiveTimeoutStore (ES by default) for future reference.
    Use Operation.SOFT_TIMEOUT to set timeout explicitly without calculations.
    """
<<<<<<< HEAD
    args = {}
    for arg in operation.value[2]:
        assert arg in kwargs, f"Argument '{arg}' is required for operation {operation.name}"
        args[arg] = kwargs[arg]
    timeout, load_metrics = operation.value[1](node_info_service=NodeLoadInfoServices().get(node), **args)
    load_metrics = load_metrics | TestInfoServices.get(node)
    start_time = time.time()
||||||| parent of 68b70fa88 (fix(wait_ssh_up): use adaptive_timeout and triple the hard timeout)
    tablet_sensitive_op = operation in {Operations.DECOMMISSION, Operations.NEW_NODE}
    tablets_enabled = is_tablets_feature_enabled(node)

    _, timeout_func, required_arg_names = operation.value
    args = {arg: kwargs[arg] for arg in required_arg_names}
    store_metrics = node.parent_cluster.params.get("adaptive_timeout_store_metrics")
    if store_metrics:
        metrics = NodeLoadInfoServices().get(node)
    else:
        metrics = {}
    if tablet_sensitive_op:
        args['tablets_enabled'] = tablets_enabled
    result = timeout_func(node_info_service=metrics, **args)
    if tablet_sensitive_op:
        (soft_timeout, hard_timeout), load_metrics = result
    else:
        soft_timeout, load_metrics = result
        hard_timeout = None
    if store_metrics:
        load_metrics.update(TestInfoServices.get(node))

    start_time = time.monotonic()
=======
    tablet_sensitive_op = operation in {Operations.DECOMMISSION, Operations.NEW_NODE}
    kwargs.setdefault('node_available', True)

    # in some situations we may want to skip tablet check, since it depends on ssh connectivity
    tablets_enabled = kwargs['node_available'] and is_tablets_feature_enabled(node)

    _, timeout_func, required_arg_names = operation.value
    args = {arg: kwargs[arg] for arg in required_arg_names}

    # if node is known to be not available, skip metrics gathering and use default timeouts
    store_metrics = node.parent_cluster.params.get(
        "adaptive_timeout_store_metrics") and kwargs['node_available']
    if store_metrics:
        metrics = NodeLoadInfoServices().get(node)
    else:
        metrics = {}
    if tablet_sensitive_op:
        args['tablets_enabled'] = tablets_enabled
    result = timeout_func(node_info_service=metrics, **args)
    if tablet_sensitive_op:
        (soft_timeout, hard_timeout), load_metrics = result
    else:
        soft_timeout, load_metrics = result
        hard_timeout = None
    if store_metrics:
        load_metrics.update(TestInfoServices.get(node))

    start_time = time.monotonic()
>>>>>>> 68b70fa88 (fix(wait_ssh_up): use adaptive_timeout and triple the hard timeout)
    timeout_occurred = False
    try:
        yield timeout
    except Exception as exc:  # pylint: disable=broad-except
        exc_name = exc.__class__.__name__
        if "timeout" in exc_name.lower() or "timed out" in str(exc):
            timeout_occurred = True
        raise
    finally:
        duration = time.time() - start_time
        if duration > timeout:
            timeout_occurred = True
            SoftTimeoutEvent(operation=operation.name, soft_timeout=timeout, duration=duration).publish_or_dump()
        try:
            if load_metrics:
                stats_storage.store(metrics=load_metrics, operation=operation.name, duration=duration,
                                    timeout=timeout, timeout_occurred=timeout_occurred)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.warning("Failed to store adaptive timeout stats: \n%s", exc)

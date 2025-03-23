import logging
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any

from sdcm.sct_events.system import SoftTimeoutEvent
from sdcm.utils.adaptive_timeouts.load_info_store import NodeLoadInfoService, AdaptiveTimeoutStore, ArgusAdaptiveTimeoutStore, \
    NodeLoadInfoServices

LOGGER = logging.getLogger(__name__)


def _get_decommission_timeout(node_info_service: NodeLoadInfoService) -> tuple[int, dict[str, Any]]:
    """Calculate timeout for decommission operation based on node load info. Still experimental, used to gather historical data."""
    try:
        # rough estimation from previous runs almost 9h for 1TB
        timeout = int(node_info_service.node_data_size_mb * 0.03)
        timeout = max(timeout, 7200)  # 2 hours minimum
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to calculate decommission timeout: \n%s \nDefaulting to 6 hours", exc)
        return 6*60*60, {}


def _get_soft_timeout(node_info_service: NodeLoadInfoService, timeout: int | float = None) -> tuple[int | float, dict[str, Any]]:
    # no timeout calculation - just return the timeout passed as argument along with node load info
    try:
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
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
    TABLET_MIGRATION = ("tablet_migration", _get_soft_timeout, ("timeout",))


class TestInfoServices:
    @staticmethod
    def get(node: "BaseNode") -> dict:  # noqa: F821
        return dict(
            n_db_nodes=len(node.parent_cluster.nodes),
        )


@contextmanager
def adaptive_timeout(operation: Operations, node: "BaseNode",  # noqa: PLR0914, F821
                     stats_storage: AdaptiveTimeoutStore = ArgusAdaptiveTimeoutStore(), **kwargs):
    """
    Calculate timeout in seconds for given operation based on node load info and return its value.
    Upon exit, verify if timeout occurred and publish SoftTimeoutEvent if happened.
    Also store node load info and timeout value in AdaptiveTimeoutStore (ES by default) for future reference.
    Use Operation.SOFT_TIMEOUT to set timeout explicitly without calculations.
    """
    args = {}
    for arg in operation.value[2]:
        assert arg in kwargs, f"Argument '{arg}' is required for operation {operation.name}"
        args[arg] = kwargs[arg]
    timeout, load_metrics = operation.value[1](node_info_service=NodeLoadInfoServices().get(node), **args)
    load_metrics = load_metrics | TestInfoServices.get(node)
    start_time = time.time()
    timeout_occurred = False
    try:
        yield timeout
    except Exception as exc:
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
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to store adaptive timeout stats: \n%s", exc, exc_info=True)

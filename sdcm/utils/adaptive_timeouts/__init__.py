import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from enum import Enum
from typing import Any

from sdcm.sct_events.system import SoftTimeoutEvent, HardTimeoutEvent
from sdcm.utils.adaptive_timeouts.load_info_store import NodeLoadInfoService, AdaptiveTimeoutStore, ArgusAdaptiveTimeoutStore, \
    NodeLoadInfoServices
from sdcm.utils.features import is_tablets_feature_enabled

LOGGER = logging.getLogger(__name__)

TABLETS_SOFT_TIMEOUT = 1 * 60 * 60
TABLETS_HARD_TIMEOUT = 3 * 60 * 60


def _get_decommission_timeout(node_info_service: NodeLoadInfoService,
                              tablets_enabled: bool = False) -> tuple[tuple[int, int | None], dict[str, Any]]:
    """Calculate timeout for decommission operation based on node load info. Still experimental, used to gather historical data."""
    try:
        node_info = node_info_service.as_dict()
        if tablets_enabled:
            node_info["tablets_enabled"] = True
            return (TABLETS_SOFT_TIMEOUT, TABLETS_HARD_TIMEOUT), node_info

        # For non-tablet cases, calculate based on data size
        # rough estimation from previous runs almost 9h for 1TB
        timeout = max(int(node_info_service.node_data_size_mb * 0.03), 7200)  # 2 hours minimum
        return (timeout, None), node_info
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to calculate decommission timeout: \n%s \nDefaulting to 6 hours", exc)
        return (6*60*60, None), {}


def _get_new_node_timeout(node_info_service: NodeLoadInfoService,
                          timeout: int | float = None,
                          tablets_enabled: bool = False,) -> tuple[tuple[int, int | None], dict[str, Any]]:
    """Calculate timeout for adding a new node operation"""
    try:
        node_info = node_info_service.as_dict()
        if tablets_enabled:
            node_info["tablets_enabled"] = True
            return (TABLETS_SOFT_TIMEOUT, TABLETS_HARD_TIMEOUT), node_info
        # For non-tablet cases, use the passed timeout
        return (timeout, None), node_info
    except Exception as exc:  # pylint: disable=broad-except  # noqa: BLE001
        LOGGER.warning("Failed to get node info for timeout: \n%s", exc)
        return (timeout, None), {}


def _get_soft_timeout(node_info_service: NodeLoadInfoService, timeout: int | float = None) -> tuple[int | float, dict[str, Any]]:
    # no timeout calculation - just return the timeout passed as argument along with node load info
    try:
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to get node info for timeout: \n%s", exc)
        return timeout, {}


def _get_soft_timeout_no_node_info(node_info_service: NodeLoadInfoService, timeout: int | float = None) -> tuple[int | float, dict[str, Any]]:
    # no timeout calculation - just return the timeout passed as argument without node load info
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
    NEW_NODE = ("new_node", _get_new_node_timeout, ("timeout",))
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
    HEALTHCHECK = ("healthcheck", _get_soft_timeout, ("timeout",))
    SSH_CONNECTIVITY = ("ssh_connectivity", _get_soft_timeout_no_node_info, ("timeout",))


class TestInfoServices:
    @staticmethod
    def get(node: "BaseNode") -> dict:  # noqa: F821
        return dict(
            n_db_nodes=len(node.parent_cluster.nodes),
        )


class TimeoutMonitor:
    """
    Monitors an operation and publishes error or critical event, when soft or hard timeouts is reached, correspondingly
    """

    def __init__(self, operation: str, soft_timeout: int | float, hard_timeout: int | float = None, start_time: float | None = None):
        self.operation = operation
        self.soft_timeout = soft_timeout
        self.hard_timeout = hard_timeout
        self.start_time = start_time or time.monotonic()
        self.soft_timeout_triggered = False
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="timeout_monitor")
        self.running = False

    def __enter__(self):
        if self.soft_timeout or self.hard_timeout:
            self.running = True
            self.executor.submit(self._monitor_timeouts)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.running = False
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except Exception as exc:
            LOGGER.exception("Error shutting down timeout monitor executor for %s: %s", self.operation, exc)

    def _monitor_timeouts(self):
        while self.running:
            duration = time.monotonic() - self.start_time

            if self.hard_timeout and duration > self.hard_timeout:
                HardTimeoutEvent(
                    operation=self.operation, duration=duration, hard_timeout=self.hard_timeout).publish_or_dump()
                self.running = False
                return
            if not self.soft_timeout_triggered and self.soft_timeout and duration > self.soft_timeout:
                SoftTimeoutEvent(
                    operation=self.operation, duration=duration, soft_timeout=self.soft_timeout, still_running=True
                ).publish_or_dump()
                self.soft_timeout_triggered = True

            time.sleep(5.0)


@contextmanager
def adaptive_timeout(operation: Operations, node: "BaseNode",  # noqa: PLR0914, F821
                     stats_storage: AdaptiveTimeoutStore = ArgusAdaptiveTimeoutStore(), **kwargs):
    """
    Calculate timeout in seconds for given operation based on node load info and return its value.
    Upon exit, verify if timeout occurred and publish SoftTimeoutEvent if happened.
    Also store node load info and timeout value in AdaptiveTimeoutStore (ES by default) for future reference.
    Use Operation.SOFT_TIMEOUT to set timeout explicitly without calculations.
    """
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
    timeout_occurred = False
    monitor_ctx = (TimeoutMonitor(operation.name, soft_timeout, hard_timeout, start_time=start_time)
                   if tablet_sensitive_op and tablets_enabled and hard_timeout
                   else nullcontext())

    try:
        with monitor_ctx:
            yield hard_timeout or soft_timeout
    except Exception as exc:
        exc_name = exc.__class__.__name__
        if "timeout" in exc_name.lower() or "timed out" in str(exc):
            timeout_occurred = True
        raise
    finally:
        duration = time.monotonic() - start_time
        soft_timeout_exceeded = soft_timeout and duration > soft_timeout
        if soft_timeout_exceeded:
            timeout_occurred = True
        if timeout_occurred:
            SoftTimeoutEvent(operation=operation.name, soft_timeout=soft_timeout, duration=duration).publish_or_dump()

        try:
            if load_metrics and store_metrics:
                stats_storage.store(metrics=load_metrics, operation=operation.name, duration=duration,
                                    timeout=soft_timeout, timeout_occurred=timeout_occurred)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to store adaptive timeout stats: \n%s", exc, exc_info=True)

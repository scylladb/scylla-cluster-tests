import logging
import time
from contextlib import contextmanager
from enum import Enum
from typing import Any

from sdcm.sct_events.system import SoftTimeoutEvent
from sdcm.utils.adaptive_timeouts.load_info_store import (
    NodeLoadInfoService,
    AdaptiveTimeoutStore,
    ArgusAdaptiveTimeoutStore,
    NodeLoadInfoServices,
)

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
        return 6 * 60 * 60, {}


def _get_soft_timeout(
    node_info_service: NodeLoadInfoService, timeout: int | float = None
) -> tuple[int | float, dict[str, Any]]:
    # no timeout calculation - just return the timeout passed as argument along with node load info
    try:
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to get node info for timeout: \n%s", exc)
        return timeout, {}


def _get_query_timeout(
    node_info_service: NodeLoadInfoService, timeout: int | float = None, query: str = None
) -> tuple[int | float, dict[str, Any]]:
    timeout, stats = _get_soft_timeout(node_info_service=node_info_service, timeout=timeout)
    stats["query"] = query
    return timeout, stats


def _get_service_level_propagation_timeout(
    node_info_service: NodeLoadInfoService, timeout: int | float = None, service_level_for_test_step: str = None
) -> tuple[int | float, dict[str, Any]]:
    """service_level_for_test_step will report in ES on which step of test the timeout happened"""
    timeout, stats = _get_soft_timeout(node_info_service=node_info_service, timeout=timeout)
    stats["service_level_for_test_step"] = service_level_for_test_step
    return timeout, stats


def _get_compaction_timeout(
    node_info_service: NodeLoadInfoService, timeout: int | float = None
) -> tuple[int | float, dict[str, Any]]:
    """
    Experimentally got these timings
    Using the estimate = (MB / shards) / 25 + 120
    we ensure we always overshoot the actual duration

    Shards | MB     | Time     | Actual(s) | Est(s) | Diff(s) | Diff(%)
    -------+--------+----------+-----------+--------+---------+--------
    2      | 53084  | 00:18:05 |      1085 |   1182 |      97 |   8.9%
    4      | 51814  | 00:08:47 |       527 |    638 |     111 |  21.1%
    7      | 50698  | 00:04:25 |       265 |    410 |     145 |  54.7%

    2      | 217528 | 00:59:36 |      3576 |   4471 |     895 |  25.0%
    4      | 204093 | 00:30:53 |      1853 |   2161 |     308 |  16.6%
    7      | 187566 | 00:14:27 |       867 |   1192 |     325 |  37.5%

    2      | 340346 | 01:43:14 |      6194 |   6927 |     733 |  11.8%
    4      | 335093 | 00:49:18 |      2958 |   3471 |     513 |  17.3%
    7      | 272496 | 00:21:07 |      1267 |   1677 |     410 |  32.4%
    """
    COMPACTION_SPEED = 25  # MB/s per shard
    FIXED_OVERHEAD = 120  # seconds
    SAFETY_FACTOR = 2  # timeout is doubled to account for load, instance type, etc. variations
    try:
        data_size = node_info_service.node_data_size_mb  # MB
        shards = node_info_service.shards_count
        if data_size and shards:
            estimated = int((data_size / shards) / COMPACTION_SPEED) + FIXED_OVERHEAD
            timeout = estimated * SAFETY_FACTOR
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to get node info for timeout: \n%s", exc)
        return timeout, {}


def _get_cleanup_timeout(node_info_service, timeout=None):
    """Calculate cleanup timeout based on node data size.
    After decommission/topology change, cleanup removes data that no longer
    belongs to a node. The amount of data to process is proportional to total
    node data size divided by the number of nodes (since only the reshuffled
    portion needs cleanup).

    Ref: https://scylladb.atlassian.net/browse/SCT-247

    Formula:
        timeout = max(MIN_CLEANUP_TIMEOUT, data_size_mb / throughput_mb_s * safety_factor)
    Where:
        - data_size_mb: total data on the node (from node_info_service)
        - throughput_mb_s: expected I/O throughput for the instance type
        - safety_factor: 2.0 (accounts for compaction contention, I/O variability)
        - MIN_CLEANUP_TIMEOUT: 120 seconds (floor for small datasets)
    """
    MIN_CLEANUP_TIMEOUT = 120  # seconds
    SAFETY_FACTOR = 2.0
    try:
        data_size_mb = node_info_service.node_data_size_mb
        throughput = node_info_service.expected_throughput  # MB/s
        if data_size_mb and throughput:
            calculated = (data_size_mb / throughput) * SAFETY_FACTOR
            timeout = max(MIN_CLEANUP_TIMEOUT, calculated)
        else:
            timeout = timeout or MIN_CLEANUP_TIMEOUT
        return timeout, node_info_service.as_dict()
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Failed to calculate cleanup timeout: \n%s \nDefaulting to %s seconds", exc, MIN_CLEANUP_TIMEOUT)
        return timeout or MIN_CLEANUP_TIMEOUT, {}


class Operations(Enum):
    """Available operations for adaptive timeouts. Each operation maps to a function to calculate timeout based on node load info.

    maps to (function, (required arguments))"""

    DECOMMISSION = ("decommission", _get_decommission_timeout, ())
    NEW_NODE = ("new_node", _get_soft_timeout, ("timeout",))
    CREATE_INDEX = ("create_index", _get_soft_timeout, ("timeout",))
    START_SCYLLA = ("start_scylla", _get_soft_timeout, ("timeout",))
    RESTART_SCYLLA = ("restart_scylla", _get_soft_timeout, ("timeout",))
    CREATE_MV = ("create_mv", _get_soft_timeout, ("timeout",))
    MAJOR_COMPACT = ("major_compact", _get_compaction_timeout, ("timeout",))
    REPAIR = ("repair", _get_soft_timeout, ("timeout",))
    MGMT_REPAIR = ("mgmt_repair", _get_soft_timeout, ("timeout",))
    BACKUP = ("backup", _get_soft_timeout, ("timeout",))
    RESTORE = ("restore", _get_soft_timeout, ("timeout",))
    REBUILD = ("rebuild", _get_soft_timeout, ("timeout",))
    CLEANUP = ("cleanup", _get_cleanup_timeout, ("timeout",))
    REMOVE_NODE = ("remove_node", _get_soft_timeout, ("timeout",))
    SCRUB = ("scrub", _get_soft_timeout, ("timeout",))
    SOFT_TIMEOUT = ("soft_timeout", _get_soft_timeout, ("timeout",))
    FLUSH = ("flush", _get_soft_timeout, ("timeout",))
    QUERY = ("query", _get_query_timeout, ("timeout", "query"))
    SERVICE_LEVEL_PROPAGATION = (
        "service_level_propagation",
        _get_service_level_propagation_timeout,
        ("timeout", "service_level_for_test_step"),
    )
    TABLET_MIGRATION = ("tablet_migration", _get_soft_timeout, ("timeout",))


class TestInfoServices:
    @staticmethod
    def get(node: "BaseNode") -> dict:  # noqa: F821
        return dict(
            n_db_nodes=len(node.parent_cluster.nodes),
        )


@contextmanager
def adaptive_timeout(
    operation: Operations,
    node: "BaseNode",  # noqa: PLR0914, F821
    stats_storage: AdaptiveTimeoutStore = ArgusAdaptiveTimeoutStore(),
    **kwargs,
):
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

    store_metrics = node.parent_cluster.params.get("adaptive_timeout_store_metrics")

    if store_metrics:
        metrics = NodeLoadInfoServices().get(node)
    else:
        metrics = {}

    timeout, load_metrics = operation.value[1](node_info_service=metrics, **args)
    if store_metrics:
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
            if load_metrics and store_metrics:
                stats_storage.store(
                    metrics=load_metrics,
                    operation=operation.name,
                    duration=duration,
                    timeout=timeout,
                    timeout_occurred=timeout_occurred,
                )
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to store adaptive timeout stats: \n%s", exc, exc_info=True)

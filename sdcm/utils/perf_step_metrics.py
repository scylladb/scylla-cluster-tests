# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB
"""Per-step resource and disk usage of the perf steps tests, from the monitor's Prometheus, for Argus.

For each step, every query below gives each node's average over the step's window, and every node
role (DB, loader) reports the mean of its nodes' averages and the highest one: the hottest node,
not a single-sample spike. That is enough to tell a step capped by the loaders (loader CPU near
100%) from one capped by Scylla (reactor utilization near 100%), and the disk table tells whether
Scylla's data disk is the limit.
"""

import logging
import statistics
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from argus.client.generic_result import ColumnMetadata, ResultType, StaticGenericResultTable, Status
from sdcm.db_stats import PrometheusDBStats

LOGGER = logging.getLogger(__name__)

# The end of every step is cut by this much: the loaders end their load at different moments.
STEP_TAIL_SEC = 60

# Each Prometheus request, with its retries, ends within a few minutes: the report runs between steps.
PROMETHEUS_REQUEST_TIMEOUT_SEC = 30

# Every query returns one series per node, labeled by 'instance', with the node's average over the
# whole window: it is evaluated once, at the window's end, over a range of '${window}' seconds. That is
# exact whatever the scrape interval, while a rate() over a fixed short range is empty when the job is
# scraped less often than half of it: scylla-monitoring scrapes node_exporter every minute.
CPU_QUERY = '100 * (1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[${window}s])))'
REACTOR_UTILIZATION_QUERY = "avg by (instance) (avg_over_time(scylla_reactor_utilization[${window}s]))"

# Scylla's data disk: the device mounted at /var/lib/scylla, as node_exporter's diskstats names it
# ('nvme0n1', 'md0'). Device names alone can't pick it: on AWS the instance-store disk is 'nvme0n1'
# on some nodes and the EBS root on others, and the root filesystem is reported as '/dev/root'.
SCYLLA_DATA_DEVICE = (
    'label_replace(max by (instance, device) (node_filesystem_size_bytes{mountpoint="/var/lib/scylla"}), '
    '"device", "$1", "device", "/dev/(.+)")'
)


def data_disk_rate(counter: str) -> str:
    """Each node's rate of a node_disk counter over the window, on Scylla's data device only."""
    return (
        f"sum by (instance) (rate({counter}[${{window}}s]) "
        f"* on (instance, device) group_left () (0 * {SCYLLA_DATA_DEVICE} + 1))"
    )


DISK_READ_MBPS_QUERY = f"{data_disk_rate('node_disk_read_bytes_total')} / 1e6"
DISK_WRITE_MBPS_QUERY = f"{data_disk_rate('node_disk_written_bytes_total')} / 1e6"
DISK_READ_IOPS_QUERY = data_disk_rate("node_disk_reads_completed_total")
DISK_WRITE_IOPS_QUERY = data_disk_rate("node_disk_writes_completed_total")
# The average number of requests in flight: the time all of them spent, per second
DISK_QUEUE_DEPTH_QUERY = data_disk_rate("node_disk_io_time_weighted_seconds_total")
# The average time of one request: the time they spent over their count
DISK_READ_LATENCY_QUERY = (
    f"1000 * {data_disk_rate('node_disk_read_time_seconds_total')} "
    f"/ {data_disk_rate('node_disk_reads_completed_total')}"
)
DISK_WRITE_LATENCY_QUERY = (
    f"1000 * {data_disk_rate('node_disk_write_time_seconds_total')} "
    f"/ {data_disk_rate('node_disk_writes_completed_total')}"
)


@dataclass(frozen=True)
class StepMetric:
    """One value per node role: 'role' picks the nodes, and its columns are '<column> avg' and '<column> max'."""

    column: str
    query: str
    role: str
    unit: str = "%"
    digits: int = 1  # rounding of the values


STEP_METRICS = (
    # Not Scylla's load: Seastar busy-polls on its idle shards, so the DB nodes' CPU stays near 100%
    # at any rate under load (97% at reactor utilization 70% and 94% in a write run). Scylla's load
    # is the reactor utilization below.
    StepMetric(column="DB CPU", query=CPU_QUERY, role="db"),
    StepMetric(column="DB reactor utilization", query=REACTOR_UTILIZATION_QUERY, role="db"),
    StepMetric(column="Loader CPU", query=CPU_QUERY, role="loader"),
)

STEP_DISK_METRICS = (
    StepMetric(column="Disk read", query=DISK_READ_MBPS_QUERY, role="db", unit="MB/s"),
    StepMetric(column="Disk write", query=DISK_WRITE_MBPS_QUERY, role="db", unit="MB/s"),
    StepMetric(column="Disk read IOPS", query=DISK_READ_IOPS_QUERY, role="db", unit="ops/s", digits=0),
    StepMetric(column="Disk write IOPS", query=DISK_WRITE_IOPS_QUERY, role="db", unit="ops/s", digits=0),
    StepMetric(column="Disk queue depth", query=DISK_QUEUE_DEPTH_QUERY, role="db", unit="", digits=2),
    StepMetric(column="Disk read latency", query=DISK_READ_LATENCY_QUERY, role="db", unit="ms", digits=3),
    StepMetric(column="Disk write latency", query=DISK_WRITE_LATENCY_QUERY, role="db", unit="ms", digits=3),
)


@dataclass(frozen=True)
class StepWindow:
    start: float
    end: float
    trimmed: bool

    @property
    def duration(self) -> int:
        return int(self.end - self.start)


def step_window(start: float, end: float, warmup_sec: float) -> StepWindow:
    """The part of a step to average: without its first 'warmup_sec' and its last STEP_TAIL_SEC.

    The stress tools start every step without a warm-up and need the first minutes to settle, and
    a step's start is taken before the loaders launch their load. A step too short to leave at least
    'warmup_sec' after the cut is used whole, rather than averaged over a few samples. 0 disables
    the cut.
    """
    if warmup_sec <= 0 or end - start <= 2 * warmup_sec + STEP_TAIL_SEC:
        return StepWindow(start=start, end=end, trimmed=False)
    return StepWindow(start=start + warmup_sec, end=end - STEP_TAIL_SEC, trimmed=True)


def instance_host(instance: str) -> str:
    """The address of a Prometheus 'instance' label: '10.0.0.1:9100', '[fd00::1]:9100', '10.0.0.1' or 'fd00::1'."""
    if instance.startswith("["):
        return instance[1:].split("]", maxsplit=1)[0]
    if instance.count(":") == 1:
        return instance.split(":", maxsplit=1)[0]
    return instance


def node_addresses(node) -> set[str]:
    """Every address the monitor may scrape a node by. Each is looked up separately: one that fails is skipped."""
    addresses = set()
    for attr in ("ip_address", "private_ip_address", "public_ip_address", "external_address"):
        try:
            if address := getattr(node, attr, None):
                addresses.add(str(address).strip("[]"))
        except Exception:  # noqa: BLE001
            LOGGER.debug("Node %s has no %s", getattr(node, "name", node), attr)
    return addresses


def roles_by_address(db_nodes: Iterable, loader_nodes: Iterable) -> dict[str, str]:
    roles = {}
    for role, nodes in (("db", db_nodes), ("loader", loader_nodes)):
        for node in nodes:
            for address in node_addresses(node):
                roles[address] = role
    return roles


def series_average(values: list) -> float | None:
    """The time average of one Prometheus range series, [[timestamp, "value"], ...], without NaN samples."""
    numbers = [float(value) for _, value in values if value.lower() not in ("nan", "+inf", "-inf")]
    return statistics.fmean(numbers) if numbers else None


def summarize_by_role(series: list[dict], roles: dict[str, str], role: str) -> tuple[float, float] | None:
    """The mean and the highest of the per-node averages of the nodes with 'role'; None without data."""
    node_averages = []
    for entry in series:
        if roles.get(instance_host(entry.get("metric", {}).get("instance", ""))) != role:
            continue
        if (average := series_average(entry.get("values", []))) is not None:
            node_averages.append(average)
    if not node_averages:
        return None
    return statistics.fmean(node_averages), max(node_averages)


def collect_step_metrics(
    prometheus: PrometheusDBStats,
    window: StepWindow,
    roles: dict[str, str],
    metrics: Iterable[StepMetric] = STEP_METRICS,
) -> tuple[dict[str, float], dict[str, Exception]]:
    """Run the query of each of 'metrics' over the window.

    Returns the values, {'<column> avg': value, '<column> max': value}, and the error of each metric
    whose query failed, by column, for the caller to report. A query that fails or returns no series
    for its role leaves its columns out, so one missing metric does not hide the others. A window
    with fewer than two node_exporter scrapes (about two minutes) has no CPU series.
    """
    results = {}
    failures = {}
    series_by_query = {}
    errors_by_query = {}
    for metric in metrics:
        if metric.query not in series_by_query:
            # PrometheusDBStats puts the query in the URL as is: unencoded, a '+' would be read as a space
            query = urllib.parse.quote(metric.query.replace("${window}", str(window.duration)))
            try:
                # A range query from the window's end to itself: one sample, the average over the window
                series_by_query[metric.query] = prometheus.query(query, window.end, window.end) or []
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Step metrics: query '%s' failed: %s", metric.query, exc)
                series_by_query[metric.query] = []
                errors_by_query[metric.query] = exc
        if metric.query in errors_by_query:
            failures[metric.column] = errors_by_query[metric.query]
            continue
        summary = summarize_by_role(series_by_query[metric.query], roles, metric.role)
        if summary is None:
            LOGGER.warning("Step metrics: no '%s' data for the %s nodes", metric.column, metric.role)
            continue
        results[f"{metric.column} avg"], results[f"{metric.column} max"] = (
            round(value, metric.digits) for value in summary
        )
    return results, failures


def metric_columns(metrics: Iterable[StepMetric]) -> list[ColumnMetadata]:
    return [
        ColumnMetadata(name=f"{metric.column} {stat}", unit=metric.unit, type=ResultType.FLOAT)
        for metric in metrics
        for stat in ("avg", "max")
    ] + [
        ColumnMetadata(name="window start", unit="", type=ResultType.TEXT),
        ColumnMetadata(name="window duration", unit="HH:MM:SS", type=ResultType.DURATION),
    ]


class PerfStepResourceUsageResult(StaticGenericResultTable):
    class Meta:
        name = ""  # set per workload, one table per run with one row per step
        description = ""
        Columns = metric_columns(STEP_METRICS)


class PerfStepDiskUsageResult(StaticGenericResultTable):
    class Meta:
        name = ""  # set per workload, one table per run with one row per step
        description = ""
        Columns = metric_columns(STEP_DISK_METRICS)


@dataclass(frozen=True)
class StepResourceUsage:
    """The values of one step, of the metrics of both tables."""

    step: str
    window: StepWindow
    metrics: dict[str, float]


def _steps_table(table: StaticGenericResultTable, steps: Iterable[StepResourceUsage]):
    """Fill a table with one row per step that has values of the table's own columns.

    Argus overwrites the cells sent again and shows a run's tables in the order of their cells' write
    time, so sending the table with all its rows after every step keeps it below the steps' tables.
    """
    columns = {column.name for column in table.columns}
    for usage in steps:
        values = {column: value for column, value in usage.metrics.items() if column in columns}
        if not values:  # no row for a step without data, e.g. a backend whose nodes have no data mount
            continue
        for column, value in values.items():
            table.add_result(column=column, row=usage.step, value=value, status=Status.UNSET)
        window_start = datetime.fromtimestamp(usage.window.start, tz=timezone.utc).strftime("%H:%M:%S")
        table.add_result(column="window start", row=usage.step, value=window_start, status=Status.UNSET)
        table.add_result(column="window duration", row=usage.step, value=usage.window.duration, status=Status.UNSET)
    return table


def step_resource_usage_table(workload: str, steps: Iterable[StepResourceUsage]):
    """The DB and loader CPU and reactor utilization, one row per step."""
    table = PerfStepResourceUsageResult()
    table.name = f"{workload} - Step resource usage"
    table.description = (
        f"{workload} workload - per-step DB and loader CPU and reactor utilization: the mean and the hottest "
        f"node's average over each step, without its first minutes and its last {STEP_TAIL_SEC}s"
    )
    return _steps_table(table, steps)


def step_disk_usage_table(workload: str, steps: Iterable[StepResourceUsage]):
    """The DB nodes' Scylla data disk throughput, IOPS, queue depth and latency, one row per step."""
    table = PerfStepDiskUsageResult()
    table.name = f"{workload} - Step disk usage"
    table.description = (
        f"{workload} workload - per-step throughput, IOPS, queue depth and request latency of the DB nodes' "
        f"Scylla data disk (mounted at /var/lib/scylla): the mean and the busiest node's average over each "
        f"step, without its first minutes and its last {STEP_TAIL_SEC}s"
    )
    return _steps_table(table, steps)

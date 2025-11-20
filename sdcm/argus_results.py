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
# Copyright (c) 2024 ScyllaDB
import json
import logging
import time
from datetime import timezone, datetime

from argus.client import ArgusClient
from argus.client.base import ArgusClientError
from argus.client.generic_result import GenericResultTable, ColumnMetadata, ResultType, Status, ValidationRule, \
    StaticGenericResultTable

from sdcm.sct_events.event_counter import STALL_INTERVALS
from sdcm.sct_events.system import FailedResultEvent


LOGGER = logging.getLogger(__name__)

LATENCY_ERROR_THRESHOLDS = {
    "replace_node": {
        "percentile_90": 5,
        "percentile_99": 10
    },
    "default": {
        "percentile_90": 5,
        "percentile_99": 10
    }
}


class LatencyCalculatorMixedResult(StaticGenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 write", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P90 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P99 write", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P99 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="Throughput write", unit="op/s", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="Throughput read", unit="op/s", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION, higher_is_better=False),
            # help jump into proper place in logs/monitoring
            ColumnMetadata(name="start time", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="Overview", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="QA dashboard", unit="", type=ResultType.TEXT),
        ]


class LatencyCalculatorWriteResult(StaticGenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 write", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P99 write", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="Throughput write", unit="op/s", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION, higher_is_better=False),
            # help jump into proper place in logs/monitoring
            ColumnMetadata(name="start time", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="Overview", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="QA dashboard", unit="", type=ResultType.TEXT),
        ]


class LatencyCalculatorReadResult(StaticGenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P99 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="Throughput read", unit="op/s", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION, higher_is_better=False),
            # help jump into proper place in logs/monitoring
            ColumnMetadata(name="start time", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="Overview", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="QA dashboard", unit="", type=ResultType.TEXT),
        ]


class LatencyCalculatorReadDiskOnlyResult(StaticGenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="P99 read", unit="ms", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(name="Throughput read", unit="op/s", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION, higher_is_better=False),
            # help jump into proper place in logs/monitoring
            ColumnMetadata(name="start time", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="Overview", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="QA dashboard", unit="", type=ResultType.TEXT),
        ]


class ReactorStallStatsResult(StaticGenericResultTable):
    class Meta:
        name = ""
        description = ""
        Columns = [
            ColumnMetadata(name="total", unit="count", type=ResultType.INTEGER),
            *[
                ColumnMetadata(name=f"{interval}ms", unit="count", type=ResultType.INTEGER)
                for interval in STALL_INTERVALS
            ]
        ]


class ManagerRestoreBenchmarkResult(StaticGenericResultTable):
    class Meta:
        name = "Regular L&S restore benchmark"
        description = "Regular L&S restore benchmark"
        Columns = [
            ColumnMetadata(name="restore time", unit="s", type=ResultType.DURATION, higher_is_better=False),
            ColumnMetadata(name="download bandwidth", unit="MiB/s/shard", type=ResultType.FLOAT, higher_is_better=True),
            ColumnMetadata(name="l&s bandwidth", unit="MiB/s/shard", type=ResultType.FLOAT, higher_is_better=True),
            ColumnMetadata(name="repair time", unit="s", type=ResultType.DURATION, higher_is_better=False),
            ColumnMetadata(name="total", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]
        ValidationRules = {
            "restore time": ValidationRule(best_pct=10),
            "download bandwidth": ValidationRule(best_pct=10),
            "l&s bandwidth": ValidationRule(best_pct=10),
            "repair time": ValidationRule(best_pct=10),
            "total": ValidationRule(best_pct=10),
        }


class ManagerOneOneRestoreBenchmarkResult(StaticGenericResultTable):
    class Meta:
        name = "1-1 restore benchmark"
        description = "1-1 restore benchmark"
        Columns = [
            ColumnMetadata(name="bootstrap time", unit="s", type=ResultType.DURATION),
            ColumnMetadata(name="restore time", unit="s", type=ResultType.DURATION),
            ColumnMetadata(name="total", unit="s", type=ResultType.DURATION),
        ]


class ManagerBackupBenchmarkResult(StaticGenericResultTable):
    class Meta:
        name = "Backup benchmark"
        description = "Backup benchmark"
        Columns = [
            ColumnMetadata(name="Size", unit="bytes", type=ResultType.TEXT, higher_is_better=False),
            ColumnMetadata(name="Time", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]


class ManagerBackupReadResult(StaticGenericResultTable):
    class Meta:
        name = "Read timing"
        description = "Read timing"
        Columns = [
            ColumnMetadata(name="read time", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]


class ManagerSnapshotDetails(StaticGenericResultTable):
    class Meta:
        name = "Snapshot details"
        description = "Manager snapshots (pre-created for next utilization in restore tests) details"
        Columns = [
            ColumnMetadata(name="tag", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="size", unit="GB", type=ResultType.INTEGER),
            ColumnMetadata(name="locations", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="ks_name", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="cluster_id", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="scylla_version", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="ear_key_id", unit="", type=ResultType.TEXT),
            ColumnMetadata(name="manager_cluster_id", unit="", type=ResultType.TEXT),
        ]


class PerfSimpleQueryResult(StaticGenericResultTable):
    def __init__(self, workload: str, parameters: dict):
        super().__init__(name=f"{workload} - Perf Simple Query", description=json.dumps(parameters))

    class Meta:
        Columns = [ColumnMetadata(name="allocs_per_op", unit="", type=ResultType.FLOAT, higher_is_better=False),
                   ColumnMetadata(name="cpu_cycles_per_op", unit="", type=ResultType.FLOAT, higher_is_better=False),
                   ColumnMetadata(name="instructions_per_op", unit="",
                                  type=ResultType.FLOAT, higher_is_better=False),
                   ColumnMetadata(name="logallocs_per_op", unit="", type=ResultType.FLOAT, higher_is_better=False),
                   ColumnMetadata(name="mad tps", unit="", type=ResultType.FLOAT, higher_is_better=True),
                   ColumnMetadata(name="max tps", unit="", type=ResultType.FLOAT, higher_is_better=True),
                   ColumnMetadata(name="median tps", unit="", type=ResultType.FLOAT, higher_is_better=True),
                   ColumnMetadata(name="min tps", unit="", type=ResultType.FLOAT, higher_is_better=True),
                   ColumnMetadata(name="tasks_per_op", unit="", type=ResultType.FLOAT, higher_is_better=False),
                   ]

        ValidationRules = dict()


class IOPropertiesResultsTable(StaticGenericResultTable):
    def __init__(self, cluster_backend: str, instance_type: str):
        super().__init__(name=f"{cluster_backend} - {instance_type} - Disk Performance")

    class Meta:
        description = "io_properties.yaml generated from live measured disk"
        Columns = [
            ColumnMetadata(name="read_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="read_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="write_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
            ColumnMetadata(name="write_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
        ]


class IOPropertiesDeviationResultsTable(StaticGenericResultTable):
    def __init__(self, cluster_backend: str, instance_type: str):
        super().__init__(name=f"{cluster_backend} - {instance_type} - Disk Performance Percent deviation")

    class Meta:
        description = "io_properties.yaml percent deviation from pre-configured disk"
        Columns = [
            ColumnMetadata(name="read_iops_pct_deviation", unit="%",
                           type=ResultType.INTEGER, higher_is_better=False),
            ColumnMetadata(name="read_bandwidth_pct_deviation", unit="%",
                           type=ResultType.INTEGER, higher_is_better=False),
            ColumnMetadata(name="write_iops_pct_deviation", unit="%",
                           type=ResultType.INTEGER, higher_is_better=False),
            ColumnMetadata(name="write_bandwidth_pct_deviation", unit="%",
                           type=ResultType.INTEGER, higher_is_better=False),
        ]

        ValidationRules = {
            "read_iops_pct_deviation": ValidationRule(fixed_limit=15),
            "read_bandwidth_pct_deviation": ValidationRule(fixed_limit=15),
            "write_iops_pct_deviation": ValidationRule(fixed_limit=15),
            "write_bandwidth_pct_deviation": ValidationRule(fixed_limit=15),
        }


class LatteStressLatencyComparison(StaticGenericResultTable):
    class Meta:
        name = "Latency comparison"
        description = "Compares identical sets of latte commands ran twice (i.e before and after upgrade)"
        Columns = [
            ColumnMetadata(name="before_ops", unit="", type=ResultType.INTEGER),
            ColumnMetadata(name="before_mean", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="before_p99", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="after_p99", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="after_mean", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="after_ops", unit="", type=ResultType.INTEGER),
        ]


workload_to_table = {
    "mixed": LatencyCalculatorMixedResult,
    "write": LatencyCalculatorWriteResult,
    "read": LatencyCalculatorReadResult,
    "read_disk_only": LatencyCalculatorReadDiskOnlyResult,
    "throughput": LatencyCalculatorMixedResult,
}


def submit_results_to_argus(argus_client: ArgusClient, result_table: GenericResultTable):
    try:
        argus_client.submit_results(result_table)
    except ArgusClientError as exc:
        if exc.args[1] == "DataValidationError":
            FailedResultEvent(f"Argus validation failed for the result in {result_table.name}."
                              f" Please check the 'Results' tab for more details.").publish()
        else:
            raise


def send_result_to_argus(argus_client: ArgusClient, workload: str, name: str, description: str,  # noqa: PLR0914
                         cycle: int | str, result: dict, start_time: float = 0, error_thresholds: dict = None):
    """Sends results to Argus service.

    This function creates following data tables in Argus:
    - Stress commands latency results table is registered always
      and it stores parsed values taken from the HDR histograms.
    - Summary table for above results is registered
      when "result['hdr_summary']" has more than 2 rows with unique HDR tags.
      It is useful for cases when we run multiple stress commands of different types in parallel
      like customer-scenarios covered by latte stress commands.
      It stores the worst P90 and P99 latencies among all of the rows/results
      and summary throughput for all of them even if workload types are different.
    - Reactor stalls table is registered
      when relevant SCT events occured (result['reactor_stalls_stats'])
      during the measured time range.
    """
    result_table, result_table_summary = workload_to_table[workload](), workload_to_table[workload]()
    if type(cycle) is int:
        cycle = f"Cycle #{cycle}"
    result_table.name = f"{workload} - {name} - latencies"
    result_table.description = f"{workload} workload - {description}"
    result_table_summary.name = f"{workload} - {name} - Summary latencies"
    result_table_summary.description = f"{workload} workload summary - {description}"
    if error_thresholds:
        error_thresholds = error_thresholds[workload]["default"] | error_thresholds[workload].get(name, {})
        result_table.validation_rules = {metric: ValidationRule(**rules) for metric, rules in error_thresholds.items()}
        result_table_summary.validation_rules = result_table.validation_rules
    try:
        start_time = datetime.fromtimestamp(start_time or time.time(), tz=timezone.utc).strftime('%H:%M:%S')
    except ValueError:
        start_time = "N/A"

    summary_throughput, summary_worst_lat = 0, {}
    summary_row_name = f"{cycle} (Summary of all HDR tags)"
    overview_screenshot = [s for s in result["screenshots"] if "overview" in s]
    qa_screenshot = [s for s in result["screenshots"] if "scylla-per-server-metrics-nemesis" in s]
    hdr_summary = result.get("hdr_summary", {})
    hdr_summary_len = len(hdr_summary)
    skip_hdr_tag = hdr_summary_len == 1 or (workload == "mixed" and hdr_summary_len == 2)
    for i, (workload_type_and_hdr_tag, hdr_data) in enumerate(hdr_summary.items()):
        (workload_type, hdr_tag) = workload_type_and_hdr_tag.split("--", maxsplit=1)
        row_name = f"{cycle}" + "" if skip_hdr_tag else f" (HDR tag: {hdr_tag})"
        for percentile in ("90", "99"):
            if (workload_type, percentile) not in summary_worst_lat:
                summary_worst_lat[(workload_type, percentile)] = 0.0
            column_name = f"P{percentile} {workload_type.lower()}"
            value = hdr_data[f"percentile_{percentile}"]
            result_table.add_result(
                column=column_name,
                row=row_name,
                value=value,
                status=Status.UNSET,
            )
            if summary_worst_lat[(workload_type, percentile)] < value:
                summary_worst_lat[(workload_type, percentile)] = value
                result_table_summary.add_result(
                    column=column_name,
                    row=summary_row_name,
                    value=value,
                    status=Status.UNSET,
                )
        if value := hdr_data.get("throughput", None):
            summary_throughput += value
            result_table.add_result(
                column=f"Throughput {workload_type.lower()}",
                row=row_name,
                value=value,
                status=Status.UNSET,
            )
        if (i > 0 and skip_hdr_tag) or not skip_hdr_tag:
            continue
        result_table.add_result(column="duration", row=row_name, value=result["duration_in_sec"], status=Status.UNSET)
        result_table.add_result(column="start time", row=row_name, value=start_time, status=Status.UNSET)
        if overview_screenshot:
            result_table.add_result(column="Overview", row=row_name, value=overview_screenshot[0], status=Status.UNSET)
        if qa_screenshot:
            result_table.add_result(column="QA dashboard", row=row_name, value=qa_screenshot[0], status=Status.UNSET)

    if hdr_summary_len > 2:
        result_table_summary.add_result(
            column=f"Throughput {workload_type.lower()}",
            row=summary_row_name,
            value=summary_throughput,
            status=Status.UNSET,
        )
        result_table_summary.add_result(
            column="duration", row=summary_row_name, value=result["duration_in_sec"], status=Status.UNSET)
        result_table_summary.add_result(
            column="start time", row=summary_row_name, value=start_time, status=Status.UNSET)
        if overview_screenshot:
            result_table_summary.add_result(
                column="Overview", row=summary_row_name, value=overview_screenshot[0], status=Status.UNSET)
        if qa_screenshot:
            result_table_summary.add_result(
                column="QA dashboard", row=summary_row_name, value=qa_screenshot[0], status=Status.UNSET)
        submit_results_to_argus(argus_client, result_table_summary)
    submit_results_to_argus(argus_client, result_table)
    for event in result["reactor_stalls_stats"]:  # each stall event has own table
        event_name = event.split(".")[-1]
        stall_stats = result["reactor_stalls_stats"][event]
        result_table = ReactorStallStatsResult()
        result_table.name = f"{workload} - {name} - stalls - {event_name}"
        result_table.description = f"{event_name} event counts"
        result_table.add_result(column="total", row=cycle,
                                value=stall_stats["counter"], status=Status.UNSET)
        for interval, value in stall_stats["ms"].items():
            result_table.add_result(column=f"{interval}ms", row=cycle,
                                    value=value, status=Status.UNSET)
        submit_results_to_argus(argus_client, result_table)


def send_perf_simple_query_result_to_argus(argus_client: ArgusClient, result: dict, error_thresholds: dict):
    def set_validation_rules(column_metadata):
        if column_threshold := error_thresholds.get(workload, {}).get(column_metadata, {}):
            LOGGER.debug("%s_threshold result: %s", column_metadata, column_threshold)
            return ValidationRule(**column_threshold)
        else:
            return ValidationRule(best_pct=5)

    stats = result["stats"]
    workload = result["test_properties"]["type"]
    validation_rules = dict()
    validation_rules["instructions_per_op"] = set_validation_rules("instructions_per_op")
    validation_rules["allocs_per_op"] = set_validation_rules("allocs_per_op")

    result_table = PerfSimpleQueryResult(workload=workload, parameters=result["parameters"])
    result_table.validation_rules = validation_rules
    LOGGER.debug("result_table.validation_rules result: %s", result_table.validation_rules)
    for key, value in stats.items():
        result_table.add_result(column=key, row="#1", value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)


def send_manager_benchmark_results_to_argus(argus_client: ArgusClient,
                                            result: dict,
                                            result_table: StaticGenericResultTable,
                                            row_name: str = None) -> None:
    if not row_name:
        row_name = "#1"

    for key, value in result.items():
        result_table.add_result(column=key, row=row_name, value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)


def send_manager_snapshot_details_to_argus(argus_client: ArgusClient, snapshot_details: dict) -> None:
    result_table = ManagerSnapshotDetails()
    for key, value in snapshot_details.items():
        result_table.add_result(column=key, row="#1", value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)


def send_iotune_results_to_argus(argus_client: ArgusClient, results: dict, node, params):
    if not argus_client:
        LOGGER.warning("Will not submit to argus - no client initialized")
        return

    run = argus_client.get_run()
    if not run["test_id"]:
        LOGGER.warning("No test exists for this run, skipping submitting results")
        return

    table = IOPropertiesResultsTable(cluster_backend=params.get('cluster_backend'),
                                     instance_type=node.db_node_instance_type)
    for key, value in results["active"].items():
        table.add_result(column=key, row="measured", value=value, status=Status.UNSET)
        table.add_result(column=key, row="pre-configured", value=results["preset"][key], status=Status.UNSET)
    submit_results_to_argus(argus_client, table)

    table = IOPropertiesDeviationResultsTable(cluster_backend=params.get('cluster_backend'),
                                              instance_type=node.db_node_instance_type)
    for key, value in results["deviation_pct"].items():
        table.add_result(column=f"{key}_pct_deviation", row="deviation_percent",
                         value=value, status=Status.PASS if value < 15 else Status.WARNING)

    submit_results_to_argus(argus_client, table)

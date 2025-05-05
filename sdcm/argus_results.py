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
from argus.client.generic_result import GenericResultTable, ColumnMetadata, ResultType, Status, ValidationRule

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


class LatencyCalculatorMixedResult(GenericResultTable):
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


class LatencyCalculatorWriteResult(GenericResultTable):
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


class LatencyCalculatorReadResult(GenericResultTable):
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


class ReactorStallStatsResult(GenericResultTable):
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


class ManagerRestoreBanchmarkResult(GenericResultTable):
    class Meta:
        name = "Restore benchmark"
        description = "Restore benchmark"
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


class ManagerBackupBenchmarkResult(GenericResultTable):
    class Meta:
        name = "Backup benchmark"
        description = "Backup benchmark"
        Columns = [
            ColumnMetadata(name="backup time", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]


class ManagerBackupReadResult(GenericResultTable):
    class Meta:
        name = "Read timing"
        description = "Read timing"
        Columns = [
            ColumnMetadata(name="read time", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]


workload_to_table = {
    "mixed": LatencyCalculatorMixedResult,
    "write": LatencyCalculatorWriteResult,
    "read": LatencyCalculatorReadResult
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
                         cycle: int, result: dict, start_time: float = 0, error_thresholds: dict = None):
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
    summary_row_name = f"Cycle #{cycle} (Summary of all HDR tags)"
    overview_screenshot = [s for s in result["screenshots"] if "overview" in s]
    qa_screenshot = [s for s in result["screenshots"] if "scylla-per-server-metrics-nemesis" in s]
    hdr_summary = result.get("hdr_summary", {})
    hdr_summary_len = len(hdr_summary)
    skip_hdr_tag = hdr_summary_len == 1 or (workload == "mixed" and hdr_summary_len == 2)
    for i, (workload_type_and_hdr_tag, hdr_data) in enumerate(hdr_summary.items()):
        (workload_type, hdr_tag) = workload_type_and_hdr_tag.split("--", maxsplit=1)
        row_name = f"Cycle #{cycle}" + "" if skip_hdr_tag else f" (HDR tag: {hdr_tag})"
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
        result_table.add_result(column="total", row=f"Cycle #{cycle}",
                                value=stall_stats["counter"], status=Status.UNSET)
        for interval, value in stall_stats["ms"].items():
            result_table.add_result(column=f"{interval}ms", row=f"Cycle #{cycle}",
                                    value=value, status=Status.UNSET)
        submit_results_to_argus(argus_client, result_table)


def send_perf_simple_query_result_to_argus(argus_client: ArgusClient, result: dict):
    stats = result["stats"]
    workload = result["test_properties"]["type"]
    parameters = result["parameters"]

    class PerfSimpleQueryResult(GenericResultTable):
        class Meta:
            name = f"{workload} - Perf Simple Query"
            description = json.dumps(parameters)
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

            ValidationRules = {
                "allocs_per_op": ValidationRule(best_pct=5),
                "cpu_cycles_per_op": ValidationRule(best_pct=5),
                "instructions_per_op": ValidationRule(best_pct=5),
            }

    result_table = PerfSimpleQueryResult()
    for key, value in stats.items():
        result_table.add_result(column=key, row="#1", value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)


def send_manager_benchmark_results_to_argus(argus_client: ArgusClient, result: dict, sut_timestamp: int,
                                            row_name: str = None) -> None:
    if not row_name:
        row_name = "#1"

    result_table = ManagerRestoreBanchmarkResult(sut_timestamp=sut_timestamp)
    for key, value in result.items():
        result_table.add_result(column=key, row=row_name, value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)


def send_iotune_results_to_argus(argus_client: ArgusClient, results: dict, node, params):
    if not argus_client:
        LOGGER.warning("Will not submit to argus - no client initialized")
        return

    run = argus_client.get_run()
    if not run["test_id"]:
        LOGGER.warning("No test exists for this run, skipping submitting results")
        return

    class IOPropertiesResultsTable(GenericResultTable):
        class Meta:
            name = f"{params.get('cluster_backend')} - {node.db_node_instance_type} - Disk Performance"
            description = "io_properties.yaml generated from live measured disk"
            Columns = [
                ColumnMetadata(name="read_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
                ColumnMetadata(name="read_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
                ColumnMetadata(name="write_iops", unit="iops", type=ResultType.INTEGER, higher_is_better=True),
                ColumnMetadata(name="write_bandwidth", unit="bps", type=ResultType.INTEGER, higher_is_better=True),
            ]

    class IOPropertiesDeviationResultsTable(GenericResultTable):
        class Meta:
            name = f"{params.get('cluster_backend')} - {node.db_node_instance_type} - Disk Performance Percent deviation"
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

    table = IOPropertiesResultsTable()
    for key, value in results["active"].items():
        table.add_result(column=key, row="measured", value=value, status=Status.UNSET)
        table.add_result(column=key, row="pre-configured", value=results["preset"][key], status=Status.UNSET)
    submit_results_to_argus(argus_client, table)

    table = IOPropertiesDeviationResultsTable()
    for key, value in results["deviation_pct"].items():
        table.add_result(column=f"{key}_pct_deviation", row="deviation_percent",
                         value=value, status=Status.PASS if value < 15 else Status.WARNING)

    submit_results_to_argus(argus_client, table)

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
import time
from datetime import timezone, datetime

from argus.client import ArgusClient
from argus.client.base import ArgusClientError
from argus.client.generic_result import GenericResultTable, ColumnMetadata, ResultType, Status, ValidationRule

from sdcm.sct_events.event_counter import STALL_INTERVALS
from sdcm.sct_events.system import FailedResultEvent

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
            ColumnMetadata(name="repair time", unit="s", type=ResultType.DURATION, higher_is_better=False),
            ColumnMetadata(name="total", unit="s", type=ResultType.DURATION, higher_is_better=False),
        ]
        ValidationRules = {
            "restore time": ValidationRule(best_pct=10),
            "repair time": ValidationRule(best_pct=10),
            "total": ValidationRule(best_pct=10)
        }


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


def send_result_to_argus(argus_client: ArgusClient, workload: str, name: str, description: str, cycle: int, result: dict,
                         start_time: float = 0):
    result_table = workload_to_table[workload]()
    result_table.name = f"{workload} - {name} - latencies"
    result_table.description = f"{workload} workload - {description}"
    operation_error_thresholds = LATENCY_ERROR_THRESHOLDS.get(name, LATENCY_ERROR_THRESHOLDS["default"])
    try:
        start_time = datetime.fromtimestamp(start_time or time.time(), tz=timezone.utc).strftime('%H:%M:%S')
    except ValueError:
        start_time = "N/A"
    for operation in ["write", "read"]:
        summary = result["hdr_summary"]
        if operation.upper() not in summary:
            continue
        for percentile in ["90", "99"]:
            value = summary[operation.upper()][f"percentile_{percentile}"]
            result_table.add_result(column=f"P{percentile} {operation}",
                                    row=f"Cycle #{cycle}",
                                    value=value,
                                    status=Status.PASS if value < operation_error_thresholds[f"percentile_{percentile}"] else Status.ERROR)
        if value := summary[operation.upper()].get("throughput", None):
            # TODO: This column will be validated in the gradual test. `PASS` is temporary status. Should be handled later
            result_table.add_result(column=f"Throughput {operation.lower()}",
                                    row=f"Cycle #{cycle}",
                                    value=value,
                                    status=Status.UNSET)

    result_table.add_result(column="duration", row=f"Cycle #{cycle}",
                            value=result["duration_in_sec"], status=Status.PASS)
    try:
        overview_screenshot = [screenshot for screenshot in result["screenshots"] if "overview" in screenshot][0]
        result_table.add_result(column="Overview", row=f"Cycle #{cycle}",
                                value=overview_screenshot, status=Status.UNSET)
    except IndexError:
        pass
    try:
        qa_screenshot = [screenshot for screenshot in result["screenshots"]
                         if "scylla-per-server-metrics-nemesis" in screenshot][0]
        result_table.add_result(column="QA dashboard", row=f"Cycle #{cycle}",
                                value=qa_screenshot, status=Status.UNSET)
    except IndexError:
        pass
    result_table.add_result(column="start time", row=f"Cycle #{cycle}",
                            value=start_time, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)
    for event in result["reactor_stalls_stats"]:  # each stall event has own table
        event_name = event.split(".")[-1]
        stall_stats = result["reactor_stalls_stats"][event]
        result_table = ReactorStallStatsResult()
        result_table.name = f"{workload} - {name} - stalls - {event_name}"
        result_table.description = f"{event_name} event counts"
        result_table.add_result(column=f"total", row=f"Cycle #{cycle}",
                                value=stall_stats["counter"], status=Status.PASS)
        for interval, value in stall_stats["ms"].items():
            result_table.add_result(column=f"{interval}ms", row=f"Cycle #{cycle}",
                                    value=value, status=Status.PASS)
        submit_results_to_argus(argus_client, result_table)


def send_perf_simple_query_result_to_argus(argus_client: ArgusClient, result: dict, previous_results: list = None):
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

    def _get_status_based_on_previous_results(metric: str):
        if previous_results is None:
            return Status.PASS
        if all((result.get(f"is_{metric}_within_limits", True) for result in previous_results)):
            return Status.PASS
        else:
            return Status.ERROR

    result_table = PerfSimpleQueryResult()
    for key, value in stats.items():
        result_table.add_result(column=key, row="#1", value=value, status=_get_status_based_on_previous_results(key))
    submit_results_to_argus(argus_client, result_table)


def send_manager_benchmark_results_to_argus(argus_client: ArgusClient, result: dict, sut_timestamp: int,
                                            row_name: str = None) -> None:
    if not row_name:
        row_name = "#1"

    result_table = ManagerRestoreBanchmarkResult(sut_timestamp=sut_timestamp)
    for key, value in result.items():
        result_table.add_result(column=key, row=row_name, value=value, status=Status.UNSET)
    submit_results_to_argus(argus_client, result_table)

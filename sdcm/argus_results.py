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

from argus.client import ArgusClient
from argus.client.generic_result import GenericResultTable, ColumnMetadata, ResultType, Status

from sdcm.sct_events.event_counter import STALL_INTERVALS


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
            ColumnMetadata(name="P90 write", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="P90 read", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="P99 write", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="P99 read", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION)
        ]


class LatencyCalculatorWriteResult(GenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 write", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="P99 write", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION)
        ]


class LatencyCalculatorReadResult(GenericResultTable):
    class Meta:
        name = ""  # to be set by the decorator to differentiate different operations
        description = ""
        Columns = [
            ColumnMetadata(name="P90 read", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="P99 read", unit="ms", type=ResultType.FLOAT),
            ColumnMetadata(name="duration", unit="HH:MM:SS", type=ResultType.DURATION)
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


workload_to_table = {
    "mixed": LatencyCalculatorMixedResult,
    "write": LatencyCalculatorWriteResult,
    "read": LatencyCalculatorReadResult
}


def send_result_to_argus(argus_client: ArgusClient, workload: str, name: str, description: str, cycle: int, result: dict):
    result_table = workload_to_table[workload]()
    result_table.name = f"{name} - {workload} - latencies"
    result_table.description = f"{description} - {workload} workload"
    operation_error_thresholds = LATENCY_ERROR_THRESHOLDS.get(name, LATENCY_ERROR_THRESHOLDS["default"])
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
    result_table.add_result(column="duration", row=f"Cycle #{cycle}",
                            value=result["duration_in_sec"], status=Status.PASS)
    argus_client.submit_results(result_table)
    for event in result["reactor_stalls_stats"]:  # each stall event has own table
        event_name = event.split(".")[-1]
        stall_stats = result["reactor_stalls_stats"][event]
        result_table = ReactorStallStatsResult()
        result_table.name = f"{name} - {workload} - stalls count - {event_name}"
        result_table.description = f"{event_name} event counts"
        result_table.add_result(column=f"total", row=f"Cycle #{cycle}",
                                value=stall_stats["counter"], status=Status.PASS)
        for interval, value in stall_stats["ms"].items():
            result_table.add_result(column=f"{interval}ms", row=f"Cycle #{cycle}",
                                    value=value, status=Status.PASS)
        argus_client.submit_results(result_table)


def send_perf_simple_query_result_to_argus(argus_client: ArgusClient, result: dict, previous_results: list = None):
    stats = result["stats"]
    workload = result["test_properties"]["type"]
    parameters = result["parameters"]

    class PerfSimpleQueryResult(GenericResultTable):
        class Meta:
            name = f"{workload} - Perf Simple Query"
            description = json.dumps(parameters)
            Columns = [ColumnMetadata(name=param, unit="", type=ResultType.FLOAT) for param in stats.keys()]

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
    argus_client.submit_results(result_table)

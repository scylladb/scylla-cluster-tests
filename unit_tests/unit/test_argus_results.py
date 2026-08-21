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
from unittest.mock import MagicMock, call

import pytest
from argus.client.generic_result import Cell, ColumnMetadata, ResultType, Status

from sdcm.argus_results import (
    ReactorStallStatsResult,
    send_iotune_results_to_argus,
    send_result_to_argus,
    LatencyCalculatorMixedResult,
)


def test_send_latency_decorator_result_to_argus(test_data_dir):
    argus_mock = MagicMock()
    argus_mock.submit_results = MagicMock()
    result = json.loads((test_data_dir / "latency_decorator_result.json").read_text())
    cycle_num = 1
    send_result_to_argus(
        argus_client=argus_mock,
        workload="mixed",
        name="test",
        description="test",
        cycle=cycle_num,
        result=result,
        start_time=1721564063.4528425,
    )
    row_name = f"Cycle #{cycle_num}"
    expected_calls = [
        call(
            LatencyCalculatorMixedResult(
                name="mixed - test - latencies",
                description="mixed workload - test",
                sut_timestamp=0,
                results=[
                    Cell(column="P90 write", row=row_name, value=2.15, status=Status.UNSET),
                    Cell(column="P99 write", row=row_name, value=3.62, status=Status.UNSET),
                    Cell(column="duration", row=row_name, value=2654, status=Status.UNSET),
                    Cell(column="start time", row=row_name, value="12:14:23", status=Status.UNSET),
                    Cell(
                        column="Overview",
                        row=row_name,
                        value="https://cloudius-jenkins-test.s3.amazonaws.com/a9b9a308-6ff8-4cc8-b33d-c439f75c9949/20240721_125838/"
                        "grafana-screenshot-overview-20240721_125838-perf-latency-grow-shrink-ubuntu-monitor-node-a9b9a308-1.png",
                        status=Status.UNSET,
                    ),
                    Cell(
                        column="QA dashboard",
                        row=row_name,
                        value="https://cloudius-jenkins-test.s3.amazonaws.com/a9b9a308-6ff8-4cc8-b33d-c439f75c9949/20240721_125838/"
                        "grafana-screenshot-scylla-master-perf-regression-latency-650gb-grow-shrink-scylla-per-server-metrics-nemesis"
                        "-20240721_125845-perf-latency-grow-shrink-ubuntu-monitor-node-a9b9a308-1.png",
                        status=Status.UNSET,
                    ),
                    Cell(column="P90 read", row=row_name, value=2.86, status=Status.UNSET),
                    Cell(column="P99 read", row=row_name, value=5.36, status=Status.UNSET),
                ],
            )
        ),
        call(
            ReactorStallStatsResult(
                name="mixed - test - stalls - REACTOR_STALLED",
                description="REACTOR_STALLED event counts",
                sut_timestamp=0,
                results=[
                    Cell(column="total", row=row_name, value=18, status=Status.UNSET),
                    Cell(column="10ms", row=row_name, value=18, status=Status.UNSET),
                ],
            )
        ),
    ]
    argus_mock.submit_results.assert_has_calls(expected_calls, any_order=True)


@pytest.mark.parametrize("run", [{}, {"test_id": None}], ids=["no-run-registered", "empty-test-id"])
def test_send_iotune_results_to_argus_skips_when_no_run(run):
    """In replay-log-only mode get_run() returns an empty payload, it should be skipped, not raise."""
    argus_mock = MagicMock()
    argus_mock.get_run = MagicMock(return_value=run)

    send_iotune_results_to_argus(argus_client=argus_mock, results={}, node=MagicMock(), params={})

    argus_mock.submit_results.assert_not_called()


def test_extra_columns_and_extra_values_are_reported_once_per_row():
    """FTS-only usage: caller-supplied columns/values, without touching any existing table schema."""
    argus_mock = MagicMock()
    result = {
        "screenshots": [],
        "duration_in_sec": 30,
        "reactor_stalls_stats": {},
        "hdr_summary": {
            "READ--fn--search": {
                "percentile_90": 2.1,
                "percentile_99": 4.5,
                "throughput": 320,
            }
        },
    }
    extra_columns = [ColumnMetadata(name="query_example", unit="", type=ResultType.TEXT)]
    extra_values = {"query_example": "hello world"}

    send_result_to_argus(
        argus_client=argus_mock,
        workload="read",
        name="fts_search_p99_10ms",
        description="FTS BM25 full-text search query latency. Expected P99 read <= 10 ms.",
        cycle="ds | 900 docs | term_common",
        result=result,
        start_time=1721564063.4528425,
        extra_columns=extra_columns,
        extra_values=extra_values,
    )

    (submitted_table,) = (c.args[0] for c in argus_mock.submit_results.call_args_list)
    assert submitted_table.name == "read - fts_search_p99_10ms - latencies"
    assert any(col.name == "query_example" for col in submitted_table.columns)
    example_cells = [cell for cell in submitted_table.results if cell.column == "query_example"]
    assert len(example_cells) == 1
    assert example_cells[0].value == "hello world"
    assert example_cells[0].row == "ds | 900 docs | term_common"


def test_no_extra_columns_means_no_extra_columns_or_cells():
    """Existing callers (extra_columns/extra_values omitted) must see the unmodified table schema."""
    argus_mock = MagicMock()
    result = {
        "screenshots": [],
        "duration_in_sec": 30,
        "reactor_stalls_stats": {},
        "hdr_summary": {"READ--fn--search": {"percentile_90": 2.1, "percentile_99": 4.5, "throughput": 320}},
    }

    send_result_to_argus(
        argus_client=argus_mock,
        workload="read",
        name="some_cycle",
        description="",
        cycle="row-1",
        result=result,
    )

    # NOTE: against a literal, not against a fresh 'LatencyCalculatorReadResult()'. Had the extra
    #       columns leaked onto the shared class attribute, both sides would carry them and the
    #       comparison would pass while asserting nothing.
    (submitted_table,) = (c.args[0] for c in argus_mock.submit_results.call_args_list)
    assert [col.name for col in submitted_table.columns] == [
        "P90 read",
        "P99 read",
        "Throughput read",
        "duration",
        "start time",
        "Overview",
        "QA dashboard",
    ]

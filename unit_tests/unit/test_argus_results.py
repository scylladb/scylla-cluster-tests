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

from argus.client.generic_result import Cell, Status

from sdcm.argus_results import ReactorStallStatsResult, send_result_to_argus, LatencyCalculatorMixedResult


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


def test_send_result_to_argus_per_hdr_tag_rows_keep_cycle_prefix():
    """With more than two HDR tags every tag gets its own row, named '<cycle> (HDR tag: <tag>)'."""
    argus_mock = MagicMock()
    hdr_data = {"percentile_90": 1.0, "percentile_99": 2.0, "throughput": 100}
    tags = ("fn--logstor_write", "fn--logstor_read", "fn--lsm_write", "fn--lsm_read")
    result = {
        "screenshots": [],
        "duration_in_sec": 60,
        "reactor_stalls_stats": {},
        "hdr_summary": {f"{'WRITE' if 'write' in tag else 'READ'}--{tag}": dict(hdr_data) for tag in tags},
    }
    send_result_to_argus(argus_client=argus_mock, workload="mixed", name="test", description="", cycle=3, result=result)

    latency_table = next(
        c.args[0] for c in argus_mock.submit_results.call_args_list if c.args[0].name == "mixed - test - latencies"
    )
    assert {cell.row for cell in latency_table.results} == {f"Cycle #3 (HDR tag: {tag})" for tag in tags}

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
from pathlib import Path
from unittest.mock import MagicMock, call

from argus.client.generic_result import Cell, Status

from sdcm.argus_results import ReactorStallStatsResult, send_result_to_argus, LatencyCalculatorMixedResult


def test_send_latency_decorator_result_to_argus():
    argus_mock = MagicMock()
    argus_mock.submit_results = MagicMock()
    result = json.loads(Path(__file__).parent.joinpath("test_data/latency_decorator_result.json").read_text())
    send_result_to_argus(
        argus_client=argus_mock,
        workload="mixed",
        name="test",
        description="test",
        cycle=1,
        result=result,
        start_time=1721564063.4528425
    )
    expected_calls = [
        call(LatencyCalculatorMixedResult(
            sut_timestamp=0,
            results=[
                Cell(column='P90 write', row='Cycle #1', value=2.15, status=Status.PASS),
                Cell(column='P99 write', row='Cycle #1', value=3.62, status=Status.PASS),
                Cell(column='P90 read', row='Cycle #1', value=2.86, status=Status.PASS),
                Cell(column='P99 read', row='Cycle #1', value=5.36, status=Status.PASS),
                Cell(column='duration', row='Cycle #1', value=2654, status=Status.PASS),
                Cell(column='Overview', row='Cycle #1',
                     value='https://cloudius-jenkins-test.s3.amazonaws.com/a9b9a308-6ff8-4cc8-b33d-c439f75c9949/20240721_125838/'
                           'grafana-screenshot-overview-20240721_125838-perf-latency-grow-shrink-ubuntu-monitor-node-a9b9a308-1.png',
                     status=Status.UNSET),
                Cell(column='QA dashboard', row='Cycle #1',
                     value='https://cloudius-jenkins-test.s3.amazonaws.com/a9b9a308-6ff8-4cc8-b33d-c439f75c9949/20240721_125838/'
                           'grafana-screenshot-scylla-master-perf-regression-latency-650gb-grow-shrink-scylla-per-server-metrics-nemesis'
                           '-20240721_125845-perf-latency-grow-shrink-ubuntu-monitor-node-a9b9a308-1.png',
                     status=Status.UNSET),
                Cell(column='start time', row='Cycle #1', value='12:14:23', status=Status.UNSET)
            ]
        )),
        call(ReactorStallStatsResult(
            sut_timestamp=0,
            results=[
                Cell(column='total', row='Cycle #1', value=18, status=Status.PASS),
                Cell(column='10ms', row='Cycle #1', value=18, status=Status.PASS)
            ]
        ))
    ]
    argus_mock.submit_results.assert_has_calls(expected_calls, any_order=True)

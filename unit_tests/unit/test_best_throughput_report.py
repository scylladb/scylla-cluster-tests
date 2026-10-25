"""Unit tests for reporting the best gradual step to Argus and to the results log."""

import json
import logging
from functools import partial
from types import SimpleNamespace

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.argus_results import send_best_throughput_result_to_argus
from sdcm.utils.latency import BestThroughputStep


class FakeArgusClient:
    def __init__(self):
        self.tables = []

    def submit_results(self, table):
        self.tables.append(table)


def _cells(table):
    return {cell.column: cell.value for cell in table.results}


def _step(threads, throughput, p99):
    return {
        "threads": threads,
        "cycles": [
            {
                "hdr_summary": {"READ--READ-st": {"percentile_90": p99 / 2, "percentile_99": p99}},
                "cycle_hdr_throughput": throughput,
            }
        ],
    }


def test_best_step_is_sent_as_one_row():
    client = FakeArgusClient()
    best_step = BestThroughputStep(
        name="unthrottled_400_threads",
        throughput=850000,
        p99={"READ--READ-st": 7.5, "WRITE--WRITE-st": 4.2},
        threads=400,
    )
    send_best_throughput_result_to_argus(client, workload="mixed", best_step=best_step, p99_limit_ms=10)

    (table,) = client.tables
    assert table.name == "mixed - best throughput under P99 limit"
    assert {cell.row for cell in table.results} == {"best"}
    assert _cells(table) == {
        "P99 limit": 10,
        "Throughput": 850000,
        "Step": "unthrottled_400_threads",
        "Threads": 400,
        "P99 read": 7.5,
        "P99 write": 4.2,
    }


def test_nothing_passed_is_sent_as_zero_throughput():
    client = FakeArgusClient()
    send_best_throughput_result_to_argus(client, workload="read", best_step=None, p99_limit_ms=10)

    (table,) = client.tables
    assert _cells(table) == {"P99 limit": 10, "Throughput": 0, "Step": "none passed"}


def test_worst_p99_per_operation_across_hdr_tags():
    client = FakeArgusClient()
    best_step = BestThroughputStep(
        name="unthrottled_200_threads",
        throughput=500000,
        p99={"WRITE--WRITE-st": 3.0, "WRITE--WRITE-st-2": 5.5, "SCAN--fn--scan": 9.0},
    )
    send_best_throughput_result_to_argus(client, workload="write", best_step=best_step, p99_limit_ms=10)

    cells = _cells(client.tables[0])
    assert cells["P99 write"] == 5.5
    # The table has no column for other operations, and a step without a thread count reports none.
    assert "P99 scan" not in cells
    assert "Threads" not in cells


def _report_best_throughput(latency_limit, workload_type, total_summary):
    client = FakeArgusClient()
    instance = SimpleNamespace(
        params={"perf_gradual_latency_limit": latency_limit},
        log=logging.getLogger(__name__),
        test_config=SimpleNamespace(argus_client=lambda: client),
    )
    instance.p99_limit_ms = partial(gradual_grow_module.PerformanceRegressionPredefinedStepsTest.p99_limit_ms, instance)
    best_step = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.report_best_throughput(
        instance, workload=SimpleNamespace(workload_type=workload_type), total_summary=total_summary
    )
    return best_step, _cells(client.tables[0])


SUMMARY = {
    "unthrottled_100_threads": _step(100, 300000, 4.0),
    "unthrottled_200_threads": _step(200, 450000, 12.0),
}


@pytest.mark.parametrize(
    "latency_limit, workload_type, expected_limit, expected_threads",
    [
        pytest.param({"default": {"percentile_99": 10}}, "read", 10, 100, id="default"),
        pytest.param(
            {"default": {"percentile_99": 10}, "read_disk_only": {"percentile_99": 15}},
            "read_disk_only",
            15,
            200,
            id="workload_override",
        ),
        pytest.param(None, "read", 10, 100, id="not_configured"),
    ],
)
def test_report_uses_the_workload_latency_limit(latency_limit, workload_type, expected_limit, expected_threads):
    best_step, cells = _report_best_throughput(latency_limit, workload_type, SUMMARY)
    assert best_step.threads == expected_threads
    assert cells["P99 limit"] == expected_limit
    assert cells["Threads"] == expected_threads


def test_report_sends_nothing_passed():
    best_step, cells = _report_best_throughput({"default": {"percentile_99": 1}}, "read", SUMMARY)
    assert best_step is None
    assert cells["Step"] == "none passed"


@pytest.mark.parametrize(
    "best_step, expected",
    [
        pytest.param(
            BestThroughputStep(
                name="unthrottled_100_threads", throughput=300000, p99={"READ--READ-st": 4.0}, threads=100
            ),
            {"name": "unthrottled_100_threads", "throughput": 300000, "p99": {"READ--READ-st": 4.0}, "threads": 100},
            id="best_step",
        ),
        pytest.param(None, None, id="nothing_passed"),
    ],
)
def test_results_log_contains_the_best_step(tmp_path, best_step, expected):
    instance = SimpleNamespace(logdir=str(tmp_path), log=logging.getLogger(__name__))
    gradual_grow_module.PerformanceRegressionPredefinedStepsTest.save_total_summary_in_file(
        instance, {"unthrottled_100_threads": {}}, best_step=best_step
    )

    saved = json.loads((tmp_path / "result_gradual_increase.log").read_text(encoding="utf-8"))
    assert saved == {"unthrottled_100_threads": {}, "best_throughput": expected}

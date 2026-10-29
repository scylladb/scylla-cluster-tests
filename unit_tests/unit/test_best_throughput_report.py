"""Unit tests for reporting the best gradual step to Argus and to the results log."""

import json
import logging
from functools import partial
from types import SimpleNamespace

import pytest
import requests
from argus.client.base import ArgusClientError

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


@pytest.mark.parametrize("stop_reason", ["narrowed", "thread cap"])
def test_search_stop_reason_is_sent(stop_reason):
    client = FakeArgusClient()
    best_step = BestThroughputStep(name="unthrottled_4096_threads", throughput=900000, p99={}, threads=4096)
    send_best_throughput_result_to_argus(
        client, workload="read", best_step=best_step, p99_limit_ms=10, stop_reason=stop_reason
    )

    assert _cells(client.tables[0])["Stop reason"] == stop_reason


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


def _report_best_throughput(latency_limit, workload_type, total_summary, client=None):
    client = client or FakeArgusClient()
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = SimpleNamespace(
        params={"perf_gradual_latency_limit": latency_limit},
        log=logging.getLogger(__name__),
        test_config=SimpleNamespace(argus_client=lambda: client),
    )
    instance.p99_limit_ms = partial(test_class.p99_limit_ms, instance)
    instance.send_to_argus = partial(test_class.send_to_argus, instance)
    workload = SimpleNamespace(workload_type=workload_type)
    best_step = test_class.find_best_step(instance, workload=workload, total_summary=total_summary)
    test_class.report_best_throughput(instance, workload=workload, best_step=best_step)
    return best_step, _cells(client.tables[0]) if client.tables else None


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


class FailingArgusClient:
    tables = ()

    def __init__(self, error):
        self.error = error

    def submit_results(self, table):
        raise self.error


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ArgusClientError("Internal Server Error", "APIException"), id="argus_5xx"),
        pytest.param(requests.ConnectionError("Connection refused"), id="argus_unreachable"),
    ],
)
def test_argus_failure_does_not_fail_the_test(error, caplog):
    with caplog.at_level(logging.WARNING):
        best_step, _ = _report_best_throughput(
            {"default": {"percentile_99": 10}}, "read", SUMMARY, client=FailingArgusClient(error)
        )
    assert best_step.threads == 100
    assert "Could not send the 'read' best throughput to Argus" in caplog.text


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

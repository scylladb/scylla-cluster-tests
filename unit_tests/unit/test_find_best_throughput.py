"""Unit tests for picking the best unthrottled gradual step under a latency limit."""

import pytest

from sdcm.utils.latency import BestThroughputStep, find_best_throughput

P99_LIMIT_MS = 10


def _step(threads, throughput, **p99_per_operation):
    """Build one unthrottled step of the gradual test summary with a single cycle."""
    return {
        "threads": threads,
        "cycles": [
            {
                "hdr_summary": {
                    operation: {"percentile_90": p99 / 2, "percentile_99": p99}
                    for operation, p99 in p99_per_operation.items()
                },
                "cycle_hdr_throughput": throughput,
            }
        ],
    }


def test_picks_highest_passing_throughput():
    summary = {
        "unthrottled_100_threads": _step(100, 300000, WRITE=2.1),
        "unthrottled_200_threads": _step(200, 450000, WRITE=6.4),
        "unthrottled_400_threads": _step(400, 520000, WRITE=14.2),
    }
    assert find_best_throughput(summary, P99_LIMIT_MS) == BestThroughputStep(
        name="unthrottled_200_threads", throughput=450000, p99={"WRITE": 6.4}, threads=200
    )


def test_limit_is_inclusive():
    summary = {"unthrottled_100_threads": _step(100, 300000, READ=P99_LIMIT_MS)}
    assert find_best_throughput(summary, P99_LIMIT_MS).name == "unthrottled_100_threads"


def test_mixed_fails_when_only_one_operation_is_over_the_limit():
    summary = {
        "unthrottled_500_threads": _step(500, 400000, READ=3.0, WRITE=2.0),
        "unthrottled_1000_threads": _step(1000, 600000, READ=12.0, WRITE=2.5),
    }
    assert find_best_throughput(summary, P99_LIMIT_MS).name == "unthrottled_500_threads"


def test_ranks_by_measured_throughput_not_thread_count():
    # Past saturation more threads only queue up: lower throughput, still within the limit.
    summary = {
        "unthrottled_400_threads": _step(400, 700000, READ=6.0),
        "unthrottled_600_threads": _step(600, 680000, READ=9.5),
    }
    assert find_best_throughput(summary, P99_LIMIT_MS).threads == 400


def test_step_without_threads_entry_is_reported_without_threads():
    step = _step(100, 300000, WRITE=3.0)
    del step["threads"]
    best = find_best_throughput({"unthrottled": step}, P99_LIMIT_MS)
    assert best.name == "unthrottled"
    assert best.threads is None


@pytest.mark.parametrize(
    "broken_step",
    [
        pytest.param({"threads": 200, "cycles": []}, id="no_cycles"),
        pytest.param({"threads": 200, "legend": "", "cycles": [{"hdr_summary": {}}]}, id="empty_hdr"),
        pytest.param(
            {"threads": 200, "cycles": [{"hdr_summary": {"READ": {"percentile_99": 2.0}}}]},
            id="no_throughput",
        ),
        pytest.param(
            {"threads": 200, "cycles": [{"hdr_summary": {"READ": {}}, "cycle_hdr_throughput": 900000}]},
            id="no_percentile_99",
        ),
    ],
)
def test_step_with_failed_latency_results_is_skipped(broken_step):
    summary = {"unthrottled_100_threads": _step(100, 300000, READ=3.0), "unthrottled_200_threads": broken_step}
    assert find_best_throughput(summary, P99_LIMIT_MS).name == "unthrottled_100_threads"


def _two_cycle_step(second_cycle_p99):
    step = _step(100, 300000, READ=3.0)
    step["cycles"].append(_step(100, 290000, READ=second_cycle_p99)["cycles"][0])
    return step


def test_multiple_cycles_fail_on_the_worst_p99():
    assert find_best_throughput({"unthrottled_100_threads": _two_cycle_step(11.0)}, P99_LIMIT_MS) is None


def test_multiple_cycles_use_worst_p99_and_average_throughput():
    assert find_best_throughput({"unthrottled_100_threads": _two_cycle_step(4.0)}, P99_LIMIT_MS) == BestThroughputStep(
        name="unthrottled_100_threads", throughput=295000, p99={"READ": 4.0}, threads=100
    )


def test_nothing_passed():
    summary = {
        "unthrottled_100_threads": _step(100, 300000, WRITE=11.0),
        "unthrottled_200_threads": _step(200, 400000, WRITE=40.0),
    }
    assert find_best_throughput(summary, P99_LIMIT_MS) is None


def test_empty_summary():
    assert find_best_throughput({}, P99_LIMIT_MS) is None


def test_operation_reported_only_by_a_later_cycle_is_checked():
    step = _step(100, 300000, READ=3.0)
    step["cycles"].append(_step(100, 300000, READ=3.0, WRITE=12.0)["cycles"][0])
    assert find_best_throughput({"unthrottled_100_threads": step}, P99_LIMIT_MS) is None

    step["cycles"][1]["hdr_summary"]["WRITE"]["percentile_99"] = 5.0
    assert find_best_throughput({"unthrottled_100_threads": step}, P99_LIMIT_MS).p99 == {"READ": 3.0, "WRITE": 5.0}

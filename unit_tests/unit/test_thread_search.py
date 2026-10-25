"""Unit tests for the adaptive thread search of the best-throughput run."""

import logging
from types import SimpleNamespace

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.thread_search import ThreadSearch, ThreadSearchConfig


def _run(search: ThreadSearch, curve) -> list[int]:
    """Drive the search with `curve(threads) -> (passed, throughput)` and return the thread counts it ran."""
    ran = []
    while (threads := search.next_threads()) is not None:
        assert search.is_retry or threads not in ran, f"thread count {threads} ran twice"
        ran.append(threads)
        passed, throughput = curve(threads)
        search.record(threads=threads, passed=passed, throughput=throughput)
    return ran


def _limit_at(max_passing_threads, throughput_per_thread=1000):
    """Throughput grows with threads; P99 stays within the limit up to `max_passing_threads`."""
    return lambda threads: (threads <= max_passing_threads, threads * throughput_per_thread)


CONFIG = ThreadSearchConfig(max_steps=20, precision_pct=5)


def test_doubles_then_bisects_to_the_precision():
    search = ThreadSearch(start_threads=100, config=CONFIG)
    ran = _run(search, _limit_at(530))

    assert ran[:4] == [100, 200, 400, 800]
    assert ran[4:] == [600, 500, 550, 525]
    # 550 - 525 = 25 threads, within 5% of 525.
    assert (search.highest_passing, search.lowest_failing) == (525, 550)
    assert "narrowed to 525-550" in search.stop_reason


def test_halves_when_the_first_step_fails():
    search = ThreadSearch(start_threads=400, config=CONFIG)
    ran = _run(search, _limit_at(120))

    assert ran[:3] == [400, 200, 100]
    assert search.highest_passing >= 114
    assert search.lowest_failing <= 126


def test_stops_doubling_when_throughput_stops_growing():
    def saturated(threads):
        # Throughput flattens at 300k from 200 threads on, while latency stays within the limit.
        return True, min(threads, 200) * 1500

    search = ThreadSearch(start_threads=100, config=CONFIG)
    assert _run(search, saturated) == [100, 200, 400]
    assert "throughput grew less than 5%" in search.stop_reason


def test_nothing_passes_down_to_one_thread():
    search = ThreadSearch(start_threads=8, config=CONFIG)
    assert _run(search, lambda threads: (False, threads * 1000)) == [8, 4, 2, 1]
    assert search.stop_reason == "no thread count kept P99 within the limit"
    assert search.highest_passing is None


def test_step_without_results_twice_counts_as_failed():
    search = ThreadSearch(start_threads=100, config=CONFIG)
    ran = _run(search, lambda threads: (threads <= 200, None if threads == 400 else threads * 1000))
    assert ran[:4] == [100, 200, 400, 400]
    assert search.lowest_failing <= 210


def test_step_without_results_is_run_once_more():
    """One collection error must not count as the latency limit and end doubling early."""
    collection_errors = {400}

    def curve(threads):
        if threads in collection_errors:
            collection_errors.discard(threads)
            return False, None
        return _limit_at(530)(threads)

    search = ThreadSearch(start_threads=100, config=CONFIG)
    ran = _run(search, curve)

    assert ran[:5] == [100, 200, 400, 400, 800]
    assert (search.highest_passing, search.lowest_failing) == (525, 550)


def test_retry_is_flagged_and_does_not_use_the_budget():
    search = ThreadSearch(start_threads=100, config=ThreadSearchConfig(max_steps=5, precision_pct=1))
    retries = []
    collection_errors = {100}

    def curve(threads):
        retries.append(search.is_retry)
        if threads in collection_errors:
            collection_errors.discard(threads)
            return False, None
        return _limit_at(530)(threads)

    ran = _run(search, curve)

    assert ran[:2] == [100, 100]
    assert retries[:3] == [False, True, False]
    # 5 steps with results, plus the rerun of the first one.
    assert len(ran) == 6
    assert search.stop_reason == "used the step budget of 5"


def test_doubling_stops_at_the_thread_cap():
    search = ThreadSearch(start_threads=1500, config=ThreadSearchConfig(max_threads_per_command=4096))
    assert _run(search, _limit_at(10**6)) == [1500, 3000, 4096]
    assert search.stop_reason == "reached the cap of 4096 threads per command"


def test_start_threads_above_the_cap_start_at_the_cap():
    search = ThreadSearch(start_threads=10_000, config=ThreadSearchConfig(max_threads_per_command=4096))
    assert search.next_threads() == 4096


def test_stops_at_the_step_budget():
    search = ThreadSearch(start_threads=100, config=ThreadSearchConfig(max_steps=5, precision_pct=1))
    assert len(_run(search, _limit_at(530))) == 5
    assert search.stop_reason == "used the step budget of 5"


@pytest.mark.parametrize(
    "start_threads, max_passing_threads, expected_steps",
    [
        pytest.param(100, 530, [100, 200, 400, 800], id="doubling"),
        pytest.param(800, 100, [800, 400, 200, 100], id="halving"),
    ],
)
def test_budget_does_not_stop_the_search_before_the_limit_is_found(start_threads, max_passing_threads, expected_steps):
    search = ThreadSearch(start_threads=start_threads, config=ThreadSearchConfig(max_steps=2))
    assert _run(search, _limit_at(max_passing_threads)) == expected_steps
    assert search.stop_reason == "used the step budget of 2"


def test_gap_of_one_thread_ends_the_search():
    search = ThreadSearch(start_threads=1, config=ThreadSearchConfig(max_steps=20, precision_pct=0.1))
    _run(search, _limit_at(5))
    assert (search.highest_passing, search.lowest_failing) == (5, 6)


@pytest.mark.parametrize(
    "search_params, workload_type, expected",
    [
        pytest.param(None, "read", None, id="not_set"),
        pytest.param({"write": {"max_steps": 6}}, "read", None, id="other_workload_only"),
        pytest.param({"default": {}}, "read", ThreadSearchConfig(), id="built_in_defaults"),
        pytest.param(
            {"default": {"start_threads_per_shard": 8, "max_steps": 6}, "read": {"max_steps": 10}},
            "read",
            ThreadSearchConfig(start_threads_per_shard=8, max_steps=10),
            id="workload_overrides_default_key_by_key",
        ),
        pytest.param(
            {"read_disk_only": {"start_threads_per_shard": 2}},
            "read_disk_only",
            ThreadSearchConfig(start_threads_per_shard=2),
            id="workload_only",
        ),
    ],
)
def test_config_from_params(search_params, workload_type, expected):
    assert ThreadSearchConfig.from_params(search_params, workload_type) == expected


@pytest.mark.parametrize(
    "per_shard, total_shards, commands, expected",
    [
        pytest.param(16, 42, 4, 168, id="i4i_3_nodes_4_loaders"),
        pytest.param(16, 42, 1, 672, id="one_command"),
        pytest.param(1, 3, 4, 1, id="rounds_up_to_one"),
        pytest.param(16, 0, 4, 1, id="no_shards_reported"),
    ],
)
def test_start_threads(per_shard, total_shards, commands, expected):
    config = ThreadSearchConfig(start_threads_per_shard=per_shard)
    assert config.start_threads(total_shards=total_shards, num_stress_commands=commands) == expected


def _step_summary(threads, p99, throughput):
    return {
        "threads": threads,
        "cycles": [
            {
                "hdr_summary": {"READ--READ-st": {"percentile_90": p99 / 2, "percentile_99": p99}},
                "cycle_hdr_throughput": throughput,
            }
        ],
    }


def test_test_class_runs_unthrottled_steps_at_the_searched_thread_counts():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append((step_name, throttle_step_dict, num_threads))
        # P99 crosses 10 ms between 400 and 800 threads.
        return {step_name: _step_summary(num_threads, p99=num_threads / 50, throughput=num_threads * 1000)}

    instance = SimpleNamespace(
        log=logging.getLogger(__name__), run_gradual_step=run_gradual_step, p99_limit_ms=lambda workload: 10
    )
    total_summary = {}
    search = ThreadSearch(start_threads=100, config=ThreadSearchConfig(max_steps=6))
    test_class.run_thread_search(
        instance,
        workload=SimpleNamespace(workload_type="read"),
        search=search,
        total_summary=total_summary,
        stress_num=1,
        num_loaders=4,
    )

    assert [name for name, _, _ in run_steps] == [
        "unthrottled_100_threads",
        "unthrottled_200_threads",
        "unthrottled_400_threads",
        "unthrottled_800_threads",
        "unthrottled_600_threads",
        "unthrottled_500_threads",
    ]
    assert all(step == {"rate": "unthrottled"} for _, step, _ in run_steps)
    assert list(total_summary) == [name for name, _, _ in run_steps]
    assert (search.highest_passing, search.lowest_failing) == (500, 600)


def test_test_class_reruns_a_step_without_results_under_its_own_name():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(step_name)
        if step_name == "unthrottled_200_threads":
            return {step_name: {"threads": num_threads, "cycles": []}}  # the collection failed
        return {step_name: _step_summary(num_threads, p99=num_threads / 50, throughput=num_threads * 1000)}

    instance = SimpleNamespace(
        log=logging.getLogger(__name__), run_gradual_step=run_gradual_step, p99_limit_ms=lambda workload: 10
    )
    total_summary = {}
    search = ThreadSearch(start_threads=100, config=ThreadSearchConfig(max_steps=4))
    test_class.run_thread_search(
        instance,
        workload=SimpleNamespace(workload_type="read"),
        search=search,
        total_summary=total_summary,
        stress_num=1,
        num_loaders=4,
    )

    assert run_steps[:4] == [
        "unthrottled_100_threads",
        "unthrottled_200_threads",
        "unthrottled_200_threads_retry",
        "unthrottled_400_threads",
    ]
    assert search.highest_passing >= 400


@pytest.mark.parametrize("helper", ["throttle_steps", "get_num_threads_for_workload"])
def test_fixed_steps_are_not_required_when_the_search_is_set(helper):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = SimpleNamespace(params={"perf_best_throughput_search": {"default": {}}})
    instance.thread_search_config = lambda workload_type: test_class.thread_search_config(instance, workload_type)
    assert getattr(test_class, helper)(instance, "read") == []

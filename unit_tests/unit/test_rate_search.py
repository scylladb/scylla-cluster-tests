"""Unit tests for the rate search of the best-throughput run."""

import logging
import math
import re
from types import SimpleNamespace
from urllib.parse import unquote

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.rate_search import RateRepeat, RateSearch, RateSearchConfig, baseline_for, in_flight_for

LATTE_READ = (
    "latte run --function read --consistency QUORUM --duration $duration "
    "--threads $threads --concurrency $concurrency $throttle data_dir/latte/latte_cs_alike.rn"
)
CS_READ = "cassandra-stress read no-warmup cl=QUORUM duration=$duration -rate threads=$threads $throttle"


def _run(search: RateSearch, curve) -> list[int]:
    """Drive the search with `curve(rate) -> (passed, throughput)`; return the rates it ran."""
    ran = []
    while (rate := search.next_rate()) is not None:
        assert search.is_retry or rate not in ran, f"rate {rate} ran twice"
        ran.append(rate)
        passed, throughput = curve(rate)
        search.record(rate=rate, passed=passed, throughput=throughput)
    return ran


def _limit_at(max_passing_rate):
    """Every step sends its rate; P99 stays within the limit up to `max_passing_rate`."""
    return lambda rate: (rate <= max_passing_rate, rate)


CONFIG = RateSearchConfig(min_rate_per_shard=1000, max_rate_per_shard=60000, max_steps=20, precision_pct=5)


def test_starts_at_the_log_middle_of_the_bounds_and_bisects():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(25000))

    assert ran[0] == round(math.sqrt(1000 * 60000))
    # Every step is the log middle of the passing and failing rates known before it; the minimum rate stands in as
    # passing until a step passes, and the maximum rate as failing until a step fails.
    passing, failing = 1000, 60000
    for rate in ran:
        assert rate == round(math.sqrt(passing * failing))
        passing, failing = (rate, failing) if rate <= 25000 else (passing, rate)
    assert search.highest_passing <= 25000 < search.lowest_failing
    assert search.lowest_failing <= search.highest_passing * 1.05
    assert search.stop_kind == "narrowed"
    assert f"narrowed to {search.highest_passing}-{search.lowest_failing} op/s" in search.stop_reason
    assert len(ran) == 7


@pytest.mark.parametrize("limit", [1500, 25000, 55000])
def test_bounds_are_not_run_when_steps_between_them_pass_and_fail(limit):
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(limit))
    assert 1000 not in ran
    assert 60000 not in ran
    assert 1000 < search.highest_passing
    assert search.lowest_failing < 60000
    assert search.stop_kind == "narrowed"


def test_default_search_takes_seven_steps():
    search = RateSearch(config=RateSearchConfig(), shards=42)
    ran = _run(search, _limit_at(840000))
    # 7 bisections narrow a ratio of 60 to within 5%.
    assert ran == [325331, 905447, 542743, 701017, 796702, 849336, 822598]
    assert search.stop_kind == "narrowed"


def test_bounds_are_per_shard():
    search = RateSearch(config=CONFIG, shards=42)
    ran = _run(search, _limit_at(10**9))
    assert ran[0] == round(math.sqrt(42000 * 2520000))
    assert ran[-1] == 2520000


def test_min_rate_that_fails_stops_with_an_error():
    search = RateSearch(config=CONFIG, shards=1)
    # The minimum rate runs only once the search has narrowed down to it.
    assert _run(search, _limit_at(500)) == [7746, 2783, 1668, 1292, 1137, 1066, 1032, 1000]
    assert search.stop_kind == "min rate fails"
    assert "lower min_rate_per_shard" in search.stop_reason
    assert search.highest_passing is None


def test_min_rate_that_passes_at_the_bottom_narrows_the_search():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(1020))
    assert ran == [7746, 2783, 1668, 1292, 1137, 1066, 1032, 1000]
    assert search.stop_kind == "narrowed"
    assert (search.highest_passing, search.lowest_failing) == (1000, 1032)


def test_min_rate_runs_as_the_last_step_of_the_budget_when_nothing_passed():
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=1)
    assert _run(search, _limit_at(500)) == [7746, 2783, 1668, 1000]
    assert search.stop_kind == "min rate fails"


def test_max_rate_that_passes_stops_with_a_lower_bound():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(100000))
    # The maximum rate runs only once the search has narrowed up to it.
    assert ran == [7746, 21558, 35965, 46453, 52794, 56282, 58111, 60000]
    assert search.stop_kind == "max rate passes"
    assert "raise max_rate_per_shard" in search.stop_reason
    assert search.highest_passing == 60000


def test_max_rate_that_fails_at_the_top_narrows_the_search():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(59000))
    assert ran[-2:] == [58111, 60000]
    assert search.stop_kind == "narrowed"
    assert (search.highest_passing, search.lowest_failing) == (58111, 60000)


def test_stops_at_the_step_budget():
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=1)
    ran = _run(search, _limit_at(25000))
    assert len(ran) == 4
    assert search.stop_kind == "budget"
    assert "step budget of 4" in search.stop_reason


def test_loaders_out_of_cpu_stop_the_search_without_recording_the_step():
    search = RateSearch(config=CONFIG, shards=1)
    search.record(rate=7746, passed=True, throughput=7746)
    search.stop_on_loader_cpu(rate=21558, loaders_cpu_pct={"loader-2": 97.4, "loader-1": 93.0})

    assert search.next_rate() is None
    assert search.stop_kind == "loader cpu"
    assert search.highest_passing == 7746
    assert [rate for rate, _, _ in search.steps] == [7746]
    assert "loader-1 93%, loader-2 97%" in search.stop_reason
    assert "raise n_loaders" in search.stop_reason


def test_step_without_results_is_run_once_more_outside_the_budget():
    results = iter([(True, None), (True, 7746), (False, 21558)])
    search = RateSearch(config=RateSearchConfig(max_steps=2), shards=1)
    ran = _run(search, lambda rate: next(results))

    assert ran == [7746, 7746, 21558]
    assert search.highest_passing == 7746
    assert search.stop_kind == "budget"


def test_step_without_results_twice_stops_with_no_result():
    search = RateSearch(config=CONFIG, shards=1)
    assert _run(search, lambda rate: (True, None)) == [7746, 7746]
    assert search.stop_kind == "no results"
    assert search.highest_passing is None


BASELINE = RateSearchConfig(baseline_per_shard=20000, max_steps=20)


def test_baseline_search_starts_at_the_baseline_and_bisects_its_range():
    search = RateSearch(config=BASELINE, shards=2)
    ran = _run(search, _limit_at(42000))
    # 40k op/s, within the bounds 30k and 50k, which run only when the search narrows to them. Around a baseline,
    # the search takes the plain middle.
    assert ran == [40000, 45000, 42500, 41250]
    assert (search.highest_passing, search.lowest_failing) == (41250, 42500)
    assert search.stop_kind == "narrowed"


def test_baseline_search_moves_the_upper_bound_out_when_it_passes():
    search = RateSearch(config=BASELINE, shards=1)
    ran = _run(search, _limit_at(27000))
    assert ran[:5] == [20000, 22500, 23750, 24375, 25000]
    # 25k, the upper bound, passed: the next one is a range above it.
    assert search.high_bound == 31250
    assert search.highest_passing <= 27000 < search.lowest_failing
    assert search.stop_kind == "narrowed"


def test_baseline_search_moves_the_lower_bound_out_when_it_fails():
    search = RateSearch(config=BASELINE, shards=1)
    ran = _run(search, _limit_at(13000))
    assert ran[:5] == [20000, 17500, 16250, 15625, 15000]
    assert search.low_bound == 11250
    assert search.highest_passing <= 13000 < search.lowest_failing
    assert search.stop_kind == "narrowed"


@pytest.mark.parametrize("limit, stop_kind", [(500, "min rate fails"), (10**9, "max rate passes")])
def test_baseline_search_stops_at_the_min_and_max_rates(limit, stop_kind):
    config = RateSearchConfig(
        baseline_per_shard=20000, min_rate_per_shard=10000, max_rate_per_shard=30000, max_steps=20
    )
    search = RateSearch(config=config, shards=1)
    ran = _run(search, _limit_at(limit))
    assert ran[-1] == (10000 if stop_kind == "min rate fails" else 30000)
    assert search.stop_kind == stop_kind


@pytest.mark.parametrize(
    "baselines, instance_type, expected",
    [
        pytest.param({"i8g": {"read": 40000}}, "i8g.4xlarge", 40000, id="family"),
        pytest.param({"i8g": {"read": 40000}, "i8g.4xlarge": {"read": 38000}}, "i8g.4xlarge", 38000, id="type wins"),
        pytest.param({"i8g": {"write": 15000}}, "i8g.4xlarge", None, id="other workload"),
        pytest.param({"i8g": {"read": 40000}}, "i4i.4xlarge", None, id="other family"),
        pytest.param(None, "i8g.4xlarge", None, id="not set"),
    ],
)
def test_baseline_for(baselines, instance_type, expected):
    assert baseline_for(baselines, instance_type, "read") == expected


def test_config_from_params_takes_the_baseline():
    config = RateSearchConfig.from_params(
        {"default": {}}, "read", baselines={"i8g": {"read": 40000}}, instance_type="i8g.4xlarge"
    )
    assert config == RateSearchConfig(baseline_per_shard=40000)


def test_retry_is_flagged():
    search = RateSearch(config=CONFIG, shards=1)
    search.record(rate=7746, passed=False, throughput=None)
    assert search.is_retry
    assert search.next_rate() == 7746
    search.record(rate=7746, passed=True, throughput=7746)
    assert not search.is_retry
    assert search.next_rate() == 21558


@pytest.mark.parametrize(
    "rate, p99_limit_ms, expected",
    [
        pytest.param(831000, 10, 8310, id="Little's law"),
        pytest.param(1001, 10, 11, id="rounded up"),
        pytest.param(1, 10, 1, id="at least one"),
    ],
)
def test_in_flight_for(rate, p99_limit_ms, expected):
    assert in_flight_for(rate, p99_limit_ms) == expected


@pytest.mark.parametrize(
    "search_params, workload_type, expected",
    [
        pytest.param(None, "read", None, id="not set"),
        pytest.param({"write": {}}, "read", None, id="other workload only"),
        pytest.param({"default": {}}, "read", RateSearchConfig(), id="defaults"),
        pytest.param(
            {"default": {"max_steps": 8, "min_rate_per_shard": 500}, "read": {"min_rate_per_shard": 2000}},
            "read",
            RateSearchConfig(max_steps=8, min_rate_per_shard=2000),
            id="workload overrides default key by key",
        ),
        pytest.param(
            {"default": {"repeat_runs": 2}, "read": {"max_repeat_steps": 4}},
            "read",
            RateSearchConfig(repeat_runs=2, max_repeat_steps=4),
            id="repeat runs",
        ),
        pytest.param(
            {"default": {}, "read": {"max_busiest_shard_util_pct": 98.5}},
            "read",
            RateSearchConfig(max_busiest_shard_util_pct=98.5),
            id="busiest shard limit",
        ),
    ],
)
def test_config_from_params(search_params, workload_type, expected):
    assert RateSearchConfig.from_params(search_params, workload_type) == expected


def _step_summary(rate, p99, throughput):
    return {
        "rate": rate,
        "cycles": [
            {
                "hdr_summary": {"READ--fn--read": {"percentile_90": p99 / 2, "percentile_99": p99}},
                "cycle_hdr_throughput": throughput,
            }
        ],
    }


def _test_instance(run_gradual_step, threads=32, stress_cmd=LATTE_READ):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = SimpleNamespace(
        log=logging.getLogger(__name__),
        run_gradual_step=run_gradual_step,
        p99_limit_ms=lambda workload: 10,
        busy_loaders=lambda start, end, max_cpu_pct: {},
        busiest_shard_utilization=lambda start, end: None,
        params={"perf_gradual_throttle_steps": {"read": [{"threads": threads, "concurrency": 55, "rate": "300000"}]}},
    )
    instance.configured_step_params = lambda workload_type: test_class.configured_step_params(instance, workload_type)
    instance.search_step_params = lambda **kwargs: test_class.search_step_params(instance, **kwargs)
    instance.run_search_step = lambda *args: test_class.run_search_step(instance, *args)
    instance.repeat_best_rate = lambda *args: test_class.repeat_best_rate(instance, *args)
    workload = SimpleNamespace(workload_type="read", cs_cmd_tmpl=[stress_cmd] * 4)
    return test_class, instance, workload


def test_test_class_runs_fixed_rate_steps_at_the_searched_rates(monkeypatch):
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append((step_name, throttle_step_dict))
        rate = int(throttle_step_dict["rate"])
        # P99 crosses 10 ms at 500k op/s.
        return {step_name: _step_summary(rate, p99=rate / 50000, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    total_summary = {}
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary=total_summary, stress_num=1, num_loaders=4
    )

    assert [name for name, _ in run_steps] == ["rate_309839", "rate_862330", "rate_516898", "rate_400194"]
    assert list(total_summary) == [name for name, _ in run_steps]
    # 309,839 op/s x 10 ms = 3,099 in flight, 775 per command: 32 latte threads of 25.
    assert run_steps[0][1] == {"rate": "309839", "threads": 32, "concurrency": 25}
    assert search.highest_passing == 400194


def test_test_class_reruns_a_step_without_results_under_its_own_name():
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(step_name)
        rate = int(throttle_step_dict["rate"])
        if step_name == "rate_309839":
            return {step_name: {"rate": rate, "cycles": []}}  # the collection failed
        return {step_name: _step_summary(rate, p99=rate / 50000, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=2), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert run_steps == ["rate_309839", "rate_309839_retry", "rate_862330"]


def test_test_class_fails_a_step_that_did_not_send_its_rate(monkeypatch):
    events = []
    monkeypatch.setattr(
        gradual_grow_module,
        "TestFrameworkEvent",
        lambda **kwargs: SimpleNamespace(publish=lambda: events.append(kwargs)),
    )

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        rate = int(throttle_step_dict["rate"])
        # Low P99 up to 400k op/s, but above it only 90% of the rate is sent.
        return {step_name: _step_summary(rate, p99=1.0, throughput=rate if rate <= 400000 else round(rate * 0.9))}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    # Every step above 400k sent 90% of its rate with a low P99: each fails, and the search goes on below it.
    assert [(rate, passed) for rate, passed, _ in search.steps] == [
        (309839, True),
        (862330, False),
        (516898, False),
        (400194, False),
    ]
    assert search.stop_kind == "budget"
    assert not events


def test_test_class_stops_when_the_loaders_run_out_of_cpu(monkeypatch):
    events = []
    monkeypatch.setattr(
        gradual_grow_module,
        "TestFrameworkEvent",
        lambda **kwargs: SimpleNamespace(publish=lambda: events.append(kwargs)),
    )

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        rate = int(throttle_step_dict["rate"])
        return {step_name: _step_summary(rate, p99=1.0, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    instance.busy_loaders = lambda start, end, max_cpu_pct: {"loader-1": 95.0} if max_cpu_pct == 90 else {}
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=40)
    total_summary = {}
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary=total_summary, stress_num=1, num_loaders=4
    )
    assert list(total_summary) == ["rate_309839"]
    assert search.stop_kind == "loader cpu"
    assert search.steps == []
    assert [event["severity"] for event in events] == [gradual_grow_module.Severity.ERROR]
    assert "loader-1 95%" in events[0]["message"]


def _busiest_shard_step(step_name, throttle_step_dict):
    rate = int(throttle_step_dict["rate"])
    # P99 stays low up to 600k op/s; the busiest shard reaches 98.5% at 450k.
    summary = _step_summary(rate, p99=1.0 if rate <= 600000 else 50.0, throughput=rate)
    return {step_name: summary | {"stress_start": float(rate), "stress_end": rate + 600.0}}


def test_test_class_fails_a_step_over_the_busiest_shard_limit():
    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        return _busiest_shard_step(step_name, throttle_step_dict)

    test_class, instance, workload = _test_instance(run_gradual_step)
    windows = []

    def busiest_shard_utilization(start, end):
        windows.append((start, end))
        return 90 + 8.5 * start / 450000

    instance.busiest_shard_utilization = busiest_shard_utilization
    search = RateSearch(config=RateSearchConfig(max_steps=6, max_busiest_shard_util_pct=98.5), shards=40)
    total_summary = {}
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary=total_summary, stress_num=1, num_loaders=4
    )

    # Every step is measured over its stress window, and the value is kept in its results.
    assert windows == [(float(rate), rate + 600.0) for rate, _, _ in search.steps]
    assert all("busiest_shard_util_pct" in step for step in total_summary.values())
    # P99 passes up to 600k, but the steps whose busiest shard went over 98.5% fail.
    assert all(passed == (rate <= 450000) for rate, passed, _ in search.steps)
    assert 400000 < search.highest_passing <= 450000


def test_test_class_measures_the_busiest_shard_without_deciding_by_it_when_no_limit_is_set():
    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        return _busiest_shard_step(step_name, throttle_step_dict)

    test_class, instance, workload = _test_instance(run_gradual_step)
    instance.busiest_shard_utilization = lambda start, end: 99.9
    search = RateSearch(config=RateSearchConfig(max_steps=6), shards=40)
    total_summary = {}
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary=total_summary, stress_num=1, num_loaders=4
    )

    assert {step["busiest_shard_util_pct"] for step in total_summary.values()} == {99.9}
    assert all(passed == (rate <= 600000) for rate, passed, _ in search.steps)


def test_test_class_keeps_the_p99_result_when_the_busiest_shard_is_not_measured():
    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        return _busiest_shard_step(step_name, throttle_step_dict)

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=6, max_busiest_shard_util_pct=98.5), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )

    assert all(passed == (rate <= 600000) for rate, passed, _ in search.steps)


def _prometheus_util(values, queries):
    def query(query, start, end, scrap_metrics_step):
        queries.append((unquote(query), start, end))
        return [{"metric": {}, "values": [[end, str(value)]]} for value in values]

    return SimpleNamespace(query=query)


def _util_instance(prometheus_db):
    return SimpleNamespace(log=logging.getLogger(__name__), prometheus_db=prometheus_db)


def test_busiest_shard_utilization_averages_each_shard_over_the_step_without_its_edges():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    queries = []
    instance = _util_instance(_prometheus_util([99.2349], queries))

    assert test_class.busiest_shard_utilization(instance, 1000, 1600) == 99.23
    # The first minute and the last 30 seconds are left out: 1060 to 1570, evaluated at 1570.
    assert queries == [("max(avg_over_time(scylla_reactor_utilization[510s]))", 1570, 1570)]


def test_busiest_shard_utilization_without_the_monitor_or_its_data():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    assert test_class.busiest_shard_utilization(_util_instance(None), 1000, 1600) is None
    assert test_class.busiest_shard_utilization(_util_instance(_prometheus_util([], [])), 1000, 1600) is None


def _prometheus_cpu(*series, queries=None):
    def query(query, start, end, scrap_metrics_step):
        if queries is not None:
            queries.append(unquote(query))
        return [
            {"metric": {"instance": instance}, "values": [[start + 60 * i, str(cpu)] for i, cpu in enumerate(values)]}
            for instance, values in series
        ]

    return SimpleNamespace(query=query)


LOADERS = [
    SimpleNamespace(name="loader-1", ip_address="10.0.0.1", private_ip_address="10.0.0.1", public_ip_address=None),
    SimpleNamespace(name="loader-2", ip_address="fd00::2", private_ip_address="fd00::2", public_ip_address=None),
    SimpleNamespace(name="loader-3", ip_address="10.0.0.3", private_ip_address="10.0.0.3", public_ip_address=None),
]


def _cpu_instance(prometheus_db):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    return SimpleNamespace(
        log=logging.getLogger(__name__),
        loaders=SimpleNamespace(nodes=LOADERS),
        prometheus_db=prometheus_db,
        _instance_address=test_class._instance_address,
    )


def test_busy_loaders_reads_each_loader_cpu_from_node_exporter():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = _cpu_instance(
        _prometheus_cpu(
            ("10.0.0.1:9100", [40, 93.5, 60]),
            ("[fd00::2]:9100", [70, 91]),
            ("10.0.0.3:9100", [89.9]),
            ("10.0.0.9:9100", [100]),  # a DB node
        )
    )
    assert test_class.busy_loaders(instance, 0, 600, max_cpu_pct=90) == {"loader-1": 93.5, "loader-2": 91.0}


def test_busy_loaders_reads_instance_labels_without_a_port():
    """SCT's monitor labels node_exporter targets with the bare address."""
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = _cpu_instance(_prometheus_cpu(("10.0.0.1", [95]), ("fd00::2", [92]), ("10.0.0.3", [10])))
    assert test_class.busy_loaders(instance, 0, 600, max_cpu_pct=90) == {"loader-1": 95.0, "loader-2": 92.0}


def test_busy_loaders_rate_window_holds_two_scrapes():
    """The monitor scrapes node_exporter once a minute, and rate() over a window with one sample is empty."""
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    queries = []
    instance = _cpu_instance(_prometheus_cpu(queries=queries))
    test_class.busy_loaders(instance, 0, 600, max_cpu_pct=90)

    window_minutes = int(re.search(r"\[(\d+)m\]", queries[0]).group(1))
    assert window_minutes >= 2


def test_busy_loaders_warns_about_loaders_the_monitor_has_no_cpu_for(caplog):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = _cpu_instance(_prometheus_cpu(("10.0.0.1", [20])))
    with caplog.at_level(logging.WARNING):
        assert test_class.busy_loaders(instance, 0, 600, max_cpu_pct=90) == {}
    assert "no CPU use of loader-2, loader-3" in caplog.text


@pytest.mark.parametrize(
    "instance, address",
    [
        pytest.param("10.0.0.1", "10.0.0.1", id="ipv4"),
        pytest.param("10.0.0.1:9100", "10.0.0.1", id="ipv4 with port"),
        pytest.param("fd00::1", "fd00::1", id="ipv6"),
        pytest.param("[fd00::1]:9100", "fd00::1", id="ipv6 with port"),
    ],
)
def test_instance_address(instance, address):
    assert gradual_grow_module.PerformanceRegressionPredefinedStepsTest._instance_address(instance) == address


def test_busy_loaders_skips_the_check_without_the_monitor():
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = SimpleNamespace(log=logging.getLogger(__name__), prometheus_db=None)
    assert test_class.busy_loaders(instance, 0, 600, max_cpu_pct=90) == {}


@pytest.mark.parametrize("limit, stop_kind", [(10000, "min rate fails"), (10**9, "max rate passes")])
def test_test_class_reports_wrong_bounds_as_an_error(monkeypatch, limit, stop_kind):
    events = []
    monkeypatch.setattr(
        gradual_grow_module,
        "TestFrameworkEvent",
        lambda **kwargs: SimpleNamespace(publish=lambda: events.append(kwargs)),
    )

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        rate = int(throttle_step_dict["rate"])
        return {step_name: _step_summary(rate, p99=rate / limit * 10, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert search.stop_kind == stop_kind
    assert [event["severity"] for event in events] == [gradual_grow_module.Severity.ERROR]
    assert search.stop_reason in events[0]["message"]


@pytest.mark.parametrize(
    "stress_cmd, threads, expected",
    [
        pytest.param(LATTE_READ, 32, {"rate": "800000", "threads": 32, "concurrency": 63}, id="latte"),
        pytest.param(
            LATTE_READ, 4000, {"rate": "800000", "threads": 2000, "concurrency": 1}, id="latte, few in flight"
        ),
        pytest.param(CS_READ, 32, {"rate": "800000", "threads": 2000}, id="cassandra-stress"),
    ],
)
def test_search_step_params_give_enough_requests_in_flight(stress_cmd, threads, expected):
    test_class, instance, workload = _test_instance(None, threads=threads, stress_cmd=stress_cmd)
    # 800k op/s x 10 ms = 8,000 in flight, 2,000 per command.
    assert test_class.search_step_params(instance, workload=workload, rate=800000, p99_limit_ms=10) == expected


@pytest.mark.parametrize(
    "params, expected",
    [
        pytest.param(
            {"perf_gradual_throttle_steps": {"read": [{"threads": 32, "concurrency": 55, "rate": "300000"}]}},
            {"threads": 32, "concurrency": 55},
            id="latte steps",
        ),
        pytest.param(
            {"perf_gradual_throttle_steps": {"read": ["100000"]}, "perf_gradual_threads": {"read": [620, 700]}},
            {"threads": 620},
            id="rate-only steps and perf_gradual_threads",
        ),
        pytest.param({}, {}, id="nothing configured"),
    ],
)
def test_configured_step_params(params, expected):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    assert test_class.configured_step_params(SimpleNamespace(params=params), "read") == expected


@pytest.mark.parametrize("helper", ["throttle_steps", "get_num_threads_for_workload"])
def test_fixed_steps_are_not_required_when_the_search_is_set(helper):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    instance = SimpleNamespace(params={"perf_best_throughput_search": {"default": {}}}, db_cluster=None)
    instance.rate_search_config = lambda workload_type: test_class.rate_search_config(instance, workload_type)
    assert getattr(test_class, helper)(instance, "read") == []


def _repeat(repeat: RateRepeat, outcomes) -> list[int]:
    """Drive the repeat runs with `outcomes`, a list of (passed, throughput) in run order; return the rates run."""
    ran, outcomes = [], iter(outcomes)
    while (rate := repeat.next_rate()) is not None:
        ran.append(rate)
        passed, throughput = next(outcomes)
        repeat.record(rate=rate, passed=passed, throughput=throughput)
    return ran


def test_repeat_confirms_the_best_rate_when_every_run_passes():
    repeat = RateRepeat([1287562, 1425516, 1379531], repeat_runs=2, max_steps=6)
    assert _repeat(repeat, [(True, 1425000)] * 2) == [1425516, 1425516]
    assert repeat.confirmed_rate == 1425516
    assert repeat.stop_kind is None


def test_repeat_steps_down_to_the_next_lower_passing_rate_when_a_run_fails():
    repeat = RateRepeat([1287562, 1425516, 1379531], repeat_runs=2, max_steps=6)
    # 1,425,516 passes its first repeat and fails the second; 1,379,531 passes both.
    ran = _repeat(repeat, [(True, 1425000), (False, 1400000), (True, 1379000), (True, 1379000)])
    assert ran == [1425516, 1425516, 1379531, 1379531]
    assert repeat.confirmed_rate == 1379531


def test_repeat_is_not_repeatable_when_every_passing_rate_fails_a_run():
    repeat = RateRepeat([900000, 800000], repeat_runs=2, max_steps=6)
    assert _repeat(repeat, [(False, 890000), (True, 800000), (False, 790000)]) == [900000, 800000, 800000]
    assert repeat.confirmed_rate is None
    assert repeat.stop_kind == "not repeatable"
    assert "800000 op/s, failed" in repeat.stop_reason


def test_repeat_stops_at_its_budget():
    repeat = RateRepeat([900000, 800000, 700000], repeat_runs=2, max_steps=3)
    assert _repeat(repeat, [(True, 900000), (False, 850000), (True, 800000)]) == [900000, 900000, 800000]
    assert repeat.confirmed_rate is None
    assert repeat.stop_kind == "not repeatable"
    assert "budget of 3" in repeat.stop_reason


def test_repeat_reruns_a_run_without_results_outside_the_budget():
    repeat = RateRepeat([900000], repeat_runs=2, max_steps=2)
    assert _repeat(repeat, [(False, None), (True, 900000), (True, 900000)]) == [900000] * 3
    assert repeat.confirmed_rate == 900000


def test_repeat_stops_when_a_run_has_no_results_twice():
    repeat = RateRepeat([900000], repeat_runs=2, max_steps=6)
    assert _repeat(repeat, [(False, None), (False, None)]) == [900000, 900000]
    assert repeat.stop_kind == "no results"


def _flaky_step(fail_steps):
    """P99 crosses 10 ms at 500k op/s; the steps named in `fail_steps` go over the limit at any rate."""
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(step_name)
        rate = int(throttle_step_dict["rate"])
        return {step_name: _step_summary(rate, p99=50.0 if step_name in fail_steps else rate / 50000, throughput=rate)}

    return run_steps, run_gradual_step


def test_test_class_repeats_the_best_rate_and_steps_down_when_a_repeat_fails():
    run_steps, run_gradual_step = _flaky_step({"rate_400194_repeat_2"})
    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=4, repeat_runs=2), shards=40)
    total_summary = {}
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary=total_summary, stress_num=1, num_loaders=4
    )
    assert run_steps == [
        "rate_309839",
        "rate_862330",
        "rate_516898",
        "rate_400194",
        "rate_400194_repeat_1",
        "rate_400194_repeat_2",
        "rate_309839_repeat_1",
        "rate_309839_repeat_2",
    ]
    assert search.repeated
    assert search.confirmed_rate == 309839
    assert search.stop_kind == "budget"  # the search's own stop stands


def test_test_class_reports_an_error_when_no_rate_passes_all_its_runs(monkeypatch):
    events = []
    monkeypatch.setattr(
        gradual_grow_module,
        "TestFrameworkEvent",
        lambda **kwargs: SimpleNamespace(publish=lambda: events.append(kwargs)),
    )
    run_steps, run_gradual_step = _flaky_step({"rate_400194_repeat_1", "rate_309839_repeat_1"})
    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=4, repeat_runs=2), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert run_steps[4:] == ["rate_400194_repeat_1", "rate_309839_repeat_1"]
    assert search.confirmed_rate is None
    assert search.stop_kind == "not repeatable"
    assert [event["severity"] for event in events] == [gradual_grow_module.Severity.ERROR]


def test_test_class_does_not_repeat_without_repeat_runs():
    run_steps, run_gradual_step = _flaky_step(set())
    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=4), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert not any("repeat" in name for name in run_steps)
    assert not search.repeated


def test_test_class_does_not_repeat_after_a_search_error(monkeypatch):
    monkeypatch.setattr(
        gradual_grow_module, "TestFrameworkEvent", lambda **kwargs: SimpleNamespace(publish=lambda: None)
    )
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(step_name)
        rate = int(throttle_step_dict["rate"])
        return {step_name: _step_summary(rate, p99=1.0, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=10, repeat_runs=2, max_rate_per_shard=10000), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert search.stop_kind == "max rate passes"
    assert not any("repeat" in name for name in run_steps)


def test_best_step_candidates_after_repeat_runs():
    pick = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.best_step_candidates
    total_summary = {
        "rate_1425516": {"rate": 1425516},
        "rate_1425516_repeat_1": {"rate": 1425516},
        "rate_1425516_repeat_2": {"rate": 1425516},
        "rate_1379531": {"rate": 1379531},
        "rate_1379531_repeat_1": {"rate": 1379531},
        "rate_1379531_repeat_2": {"rate": 1379531},
    }
    search = RateSearch(config=RateSearchConfig(repeat_runs=2), shards=45)
    # No repeat runs: every step.
    assert pick(total_summary, search) is total_summary
    assert pick(total_summary, None) is total_summary
    # 1,425,516 failed a repeat run and 1,379,531 passed all: only its steps, though 1,425,516 measured more.
    search.repeated, search.confirmed_rate = True, 1379531
    assert list(pick(total_summary, search)) == ["rate_1379531", "rate_1379531_repeat_1", "rate_1379531_repeat_2"]
    # No rate passed all its runs: no step.
    search.confirmed_rate = None
    assert pick(total_summary, search) == {}

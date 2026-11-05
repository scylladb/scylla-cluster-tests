"""Unit tests for the rate search of the best-throughput run."""

import logging
import math
from types import SimpleNamespace

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.rate_search import RateSearch, RateSearchConfig, in_flight_for

LATTE_READ = (
    "latte run --function read --consistency QUORUM --duration $duration "
    "--threads $threads --concurrency $concurrency $throttle data_dir/latte/latte_cs_alike.rn"
)
CS_READ = "cassandra-stress read no-warmup cl=QUORUM duration=$duration -rate threads=$threads $throttle"


def _run(search: RateSearch, curve) -> list[int]:
    """Drive the search with `curve(rate) -> (passed, throughput)` and return the rates it ran."""
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


def test_runs_the_min_rate_then_bisects_on_a_log_scale():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(25000))

    assert ran[:2] == [1000, round(math.sqrt(1000 * 60000))]
    # Every bisection step is the log middle of the passing and failing rates known before it; the maximum rate
    # stands in as failing until a step fails.
    passing, failing = 1000, 60000
    for rate in ran[1:]:
        assert rate == round(math.sqrt(passing * failing))
        passing, failing = (rate, failing) if rate <= 25000 else (passing, rate)
    assert search.highest_passing <= 25000 < search.lowest_failing
    assert search.lowest_failing <= search.highest_passing * 1.05
    assert search.stop_kind == "narrowed"
    assert f"narrowed to {search.highest_passing}-{search.lowest_failing} op/s" in search.stop_reason
    assert len(ran) == 8


@pytest.mark.parametrize("limit", [1500, 25000, 55000])
def test_max_rate_is_not_run_when_a_step_below_it_fails(limit):
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(limit))
    assert 60000 not in ran
    assert search.lowest_failing < 60000
    assert search.stop_kind == "narrowed"


def test_default_search_takes_eight_steps():
    search = RateSearch(config=RateSearchConfig(), shards=42)
    ran = _run(search, _limit_at(840000))
    # The minimum rate and 7 bisections narrow a ratio of 60 to within 5%.
    assert ran == [42000, 325331, 905447, 542743, 701017, 796702, 849336, 822598]
    assert search.stop_kind == "narrowed"


def test_bounds_are_per_shard():
    search = RateSearch(config=CONFIG, shards=42)
    ran = _run(search, _limit_at(10**9))
    assert ran[:2] == [42000, round(math.sqrt(42000 * 2520000))]
    assert ran[-1] == 2520000


def test_min_rate_that_fails_stops_with_an_error():
    search = RateSearch(config=CONFIG, shards=1)
    assert _run(search, _limit_at(500)) == [1000]
    assert search.stop_kind == "min rate fails"
    assert "lower min_rate_per_shard" in search.stop_reason
    assert search.highest_passing is None


def test_max_rate_that_passes_stops_with_a_lower_bound():
    search = RateSearch(config=CONFIG, shards=1)
    ran = _run(search, _limit_at(100000))
    # The maximum rate runs only once the search has narrowed up to it.
    assert ran == [1000, 7746, 21558, 35965, 46453, 52794, 56282, 58111, 60000]
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


def test_a_step_that_did_not_send_its_rate_counts_as_failed():
    def loader_bound(rate):
        # The cluster keeps P99 within the limit, but the loaders send at most 30k op/s.
        return min(rate, 30000) >= rate * 0.95, min(rate, 30000)

    search = RateSearch(config=CONFIG, shards=1)
    _run(search, loader_bound)
    assert search.highest_passing <= 30000 / 0.95
    assert search.stop_kind == "narrowed"


def test_step_without_results_is_run_once_more_outside_the_budget():
    results = iter([(True, None), (True, 1000), (False, 50000)])
    search = RateSearch(config=RateSearchConfig(max_steps=2), shards=1)
    ran = _run(search, lambda rate: next(results))

    assert ran == [1000, 1000, 7746]
    assert search.highest_passing == 1000
    assert search.stop_kind == "budget"


def test_step_without_results_twice_counts_as_failed():
    search = RateSearch(config=CONFIG, shards=1)
    assert _run(search, lambda rate: (True, None)) == [1000, 1000]
    assert search.stop_kind == "min rate fails"


def test_retry_is_flagged():
    search = RateSearch(config=CONFIG, shards=1)
    search.record(rate=1000, passed=False, throughput=None)
    assert search.is_retry
    assert search.next_rate() == 1000
    search.record(rate=1000, passed=True, throughput=1000)
    assert not search.is_retry
    assert search.next_rate() == 7746


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
        params={"perf_gradual_throttle_steps": {"read": [{"threads": threads, "concurrency": 55, "rate": "300000"}]}},
    )
    instance.configured_step_params = lambda workload_type: test_class.configured_step_params(instance, workload_type)
    instance.search_step_params = lambda **kwargs: test_class.search_step_params(instance, **kwargs)
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

    assert [name for name, _ in run_steps] == ["rate_40000", "rate_309839", "rate_862330", "rate_516898"]
    assert list(total_summary) == [name for name, _ in run_steps]
    # 309,839 op/s x 10 ms = 3,099 in flight, 775 per command: 32 latte threads of 25.
    assert run_steps[1][1] == {"rate": "309839", "threads": 32, "concurrency": 25}
    assert search.highest_passing == 309839


def test_test_class_reruns_a_step_without_results_under_its_own_name():
    run_steps = []

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(step_name)
        rate = int(throttle_step_dict["rate"])
        if step_name == "rate_40000":
            return {step_name: {"rate": rate, "cycles": []}}  # the collection failed
        return {step_name: _step_summary(rate, p99=rate / 50000, throughput=rate)}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=2), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert run_steps == ["rate_40000", "rate_40000_retry", "rate_309839"]


def test_test_class_fails_a_step_that_did_not_send_its_rate(monkeypatch):
    monkeypatch.setattr(
        gradual_grow_module, "TestFrameworkEvent", lambda **kwargs: SimpleNamespace(publish=lambda: None)
    )

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        rate = int(throttle_step_dict["rate"])
        # Low P99, but the loaders send only 90% of the rate.
        return {step_name: _step_summary(rate, p99=1.0, throughput=round(rate * 0.9))}

    test_class, instance, workload = _test_instance(run_gradual_step)
    search = RateSearch(config=RateSearchConfig(max_steps=2), shards=40)
    test_class.run_rate_search(
        instance, workload=workload, search=search, total_summary={}, stress_num=1, num_loaders=4
    )
    assert search.stop_kind == "min rate fails"


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
    instance = SimpleNamespace(params={"perf_best_throughput_search": {"default": {}}})
    instance.rate_search_config = lambda workload_type: test_class.rate_search_config(instance, workload_type)
    assert getattr(test_class, helper)(instance, "read") == []

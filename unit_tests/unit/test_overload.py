"""Unit tests for goodput under overload (SCYLLADB-3267)."""

import json
import logging
from functools import partial
from types import SimpleNamespace

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.argus_results import send_goodput_results_to_argus
from sdcm.utils.latency import BestThroughputStep
from sdcm.utils.overload import GoodputResult, OverloadConfig, OverloadStep, goodput_result

BEST = BestThroughputStep(name="unthrottled_100_threads", throughput=500000, p99={"READ--READ-st": 6.0}, threads=100)


class FakeArgusClient:
    def __init__(self):
        self.tables = []

    def submit_results(self, table):
        self.tables.append(table)


def _step_summary(throughput, p99=8.0, failed_ops=None):
    return {
        "failed_ops": failed_ops,
        "cycles": [
            {
                "hdr_summary": {"READ--READ-st": {"percentile_90": p99 / 2, "percentile_99": p99}},
                "cycle_hdr_throughput": throughput,
            }
        ],
    }


@pytest.mark.parametrize(
    "overload_params, workload_type, expected",
    [
        pytest.param(None, "read", None, id="not_set"),
        pytest.param({"write": {"factors": [2]}}, "read", None, id="other_workload_only"),
        pytest.param({"default": {}}, "read", OverloadConfig(), id="built_in_defaults"),
        pytest.param(
            {"default": {"factors": [1.5], "min_goodput_ratio": 0.8}, "read": {"factors": [1.2, 3]}},
            "read",
            OverloadConfig(factors=(1.2, 3), min_goodput_ratio=0.8),
            id="workload_overrides_default_key_by_key",
        ),
    ],
)
def test_config_from_params(overload_params, workload_type, expected):
    assert OverloadConfig.from_params(overload_params, workload_type) == expected


def test_steps_follow_the_best_throughput():
    steps = OverloadConfig(factors=(1.3, 2)).steps(best_threads=100, best_throughput=500000)
    assert steps == [
        OverloadStep(factor=1.3, rate=650000, threads=130),
        OverloadStep(factor=2, rate=1000000, threads=200),
    ]
    assert [step.name for step in steps] == ["overload_1.3x", "overload_2x"]


def test_threads_are_capped():
    steps = OverloadConfig(factors=(1.3, 2), max_threads_multiplier=1.5).steps(best_threads=100, best_throughput=1)
    assert [step.threads for step in steps] == [130, 150]


def test_goodput_result():
    step = OverloadStep(factor=2, rate=1000000, threads=200)
    result = goodput_result(step, _step_summary(470000, p99=40.0), best_throughput=500000, failed_ops=1234)
    assert result == GoodputResult(
        step="overload_2x",
        factor=2,
        offered_rate=1000000,
        threads=200,
        goodput=470000,
        goodput_ratio=0.94,
        p99={"READ--READ-st": 40.0},
        failed_ops=1234,
    )


def test_step_without_results_has_no_goodput():
    step = OverloadStep(factor=1.3, rate=650000, threads=130)
    result = goodput_result(step, {"cycles": []}, best_throughput=500000)
    assert (result.goodput, result.goodput_ratio) == (0, 0.0)


def test_argus_table_has_a_row_per_factor_and_the_ratio_rule():
    client = FakeArgusClient()
    results = [
        GoodputResult("overload_1.3x", 1.3, 650000, 130, 640000, 1.28, {"READ--READ-st": 9.0}, 0),
        GoodputResult(
            "overload_2x", 2, 1000000, 200, 430000, 0.86, {"READ--READ-st": 80.0, "WRITE--WRITE-st": 60.0}, None
        ),
    ]
    send_goodput_results_to_argus(client, "mixed", results, best_throughput=500000, min_goodput_ratio=0.9)

    (table,) = client.tables
    assert table.name == "mixed - goodput under overload"
    assert table.validation_rules["Goodput ratio"].fixed_limit == 0.9
    cells = {(cell.row, cell.column): cell.value for cell in table.results}
    assert cells[("1.3x", "Goodput ratio")] == 1.28
    assert cells[("2x", "Goodput")] == 430000
    assert cells[("2x", "Best throughput")] == 500000
    assert cells[("2x", "P99 write")] == 60.0
    assert cells[("1.3x", "Failed ops")] == 0
    assert ("2x", "Failed ops") not in cells, "a tool that reports no error count leaves the cell empty"


@pytest.mark.parametrize(
    "results, expected",
    [
        pytest.param([{"total errors": "12"}, {"total errors": "3"}], 15, id="cassandra_stress"),
        pytest.param([{"op rate": "1000"}], None, id="not_reported"),
        pytest.param([{"total errors": "12"}, {"total errors": None}], 12, id="partly_reported"),
        pytest.param([], None, id="no_results"),
    ],
)
def test_failed_ops(results, expected):
    assert gradual_grow_module.PerformanceRegressionPredefinedStepsTest._failed_ops(results) == expected


def _test_instance(run_steps, client):
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest

    def run_gradual_step(workload, throttle_step_dict, num_threads, step_name, stress_num, num_loaders):
        run_steps.append(
            {"name": step_name, "step": throttle_step_dict, "threads": num_threads, "cmds": workload.cs_cmd_tmpl}
        )
        # Goodput drops as the offered rate grows past the best throughput.
        offered = int(throttle_step_dict["rate"])
        return {step_name: _step_summary(min(offered, 1_000_000 - offered // 2), failed_ops=offered // 1000)}

    instance = SimpleNamespace(
        log=logging.getLogger(__name__),
        run_gradual_step=run_gradual_step,
        test_config=SimpleNamespace(argus_client=lambda: client),
    )
    instance.send_to_argus = partial(test_class.send_to_argus, instance)
    return instance, test_class


WORKLOAD = gradual_grow_module.Workload(
    workload_type="read",
    cs_cmd_tmpl=[
        "cassandra-stress read no-warmup cl=QUORUM duration=$duration -rate 'threads=$threads $throttle'",
        "latte run --duration $duration workload.rn",
    ],
    cs_cmd_warm_up=None,
    num_threads=[],
    throttle_steps=[],
    preload_data=False,
    drop_keyspace=False,
    wait_no_compactions=False,
    step_duration="30m",
    prepare_schema=False,
)


def test_test_class_runs_a_throttled_step_per_factor():
    run_steps, client = [], FakeArgusClient()
    instance, test_class = _test_instance(run_steps, client)

    results = test_class.run_overload_steps(
        instance, workload=WORKLOAD, overload_config=OverloadConfig(), best_step=BEST, stress_num=1, num_loaders=4
    )

    assert [(step["name"], step["step"], step["threads"]) for step in run_steps] == [
        ("overload_1.3x", {"rate": "650000"}, 130),
        ("overload_2x", {"rate": "1000000"}, 200),
    ]
    # cassandra-stress keeps going through errors; other tools are left as they are.
    assert all(step["cmds"][0].endswith(" -errors ignore") for step in run_steps)
    assert all(step["cmds"][1] == WORKLOAD.cs_cmd_tmpl[1] for step in run_steps)
    assert [result.goodput_ratio for result in results] == [1.3, 1.0]
    assert [result.failed_ops for result in results] == [650, 1000]
    # Sent by report_goodput, after the results log has them.
    assert not client.tables

    test_class.report_goodput(
        instance, workload=WORKLOAD, overload_config=OverloadConfig(), best_step=BEST, results=results
    )
    assert len(client.tables) == 1


@pytest.mark.parametrize(
    "best_step",
    [
        pytest.param(None, id="nothing_passed"),
        pytest.param(BestThroughputStep(name="150000", throughput=150000, p99={}, threads=None), id="no_threads"),
    ],
)
def test_no_overload_steps_without_a_best_step(best_step):
    run_steps, client = [], FakeArgusClient()
    instance, test_class = _test_instance(run_steps, client)

    assert (
        test_class.run_overload_steps(
            instance,
            workload=WORKLOAD,
            overload_config=OverloadConfig(),
            best_step=best_step,
            stress_num=1,
            num_loaders=4,
        )
        is None
    )
    assert not run_steps
    assert not client.tables


def test_results_log_contains_the_goodput(tmp_path):
    instance = SimpleNamespace(logdir=str(tmp_path), log=logging.getLogger(__name__))
    result = GoodputResult("overload_2x", 2, 1000000, 200, 470000, 0.94, {"READ--READ-st": 40.0}, 12)
    gradual_grow_module.PerformanceRegressionPredefinedStepsTest.save_total_summary_in_file(
        instance, {}, best_step=BEST, overload_results=[result]
    )
    saved = json.loads((tmp_path / "result_gradual_increase.log").read_text(encoding="utf-8"))
    assert saved["goodput_under_overload"][0]["goodput_ratio"] == 0.94

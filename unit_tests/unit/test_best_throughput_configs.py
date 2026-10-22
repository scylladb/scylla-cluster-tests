"""Unit tests for the best-throughput job configurations (unthrottled steps that differ by thread count)."""

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.sct_config import SCTConfiguration

BASE_CONFIGS = [
    "test-cases/performance/perf-regression-predefined-throughput-steps.yaml",
    "configurations/performance/best_throughput_unthrottled_steps.yaml",
]
WORKLOADS = ("read", "read_disk_only", "write", "mixed")


@pytest.fixture(params=["i4i", "i8g"])
def best_throughput_config(request, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    config_files = [
        *BASE_CONFIGS,
        f"configurations/performance/cassandra_stress_best_throughput_steps_{request.param}.yaml",
    ]
    monkeypatch.setenv("SCT_CONFIG_FILES", str(config_files).replace("'", '"'))
    config = SCTConfiguration()
    config.verify_configuration()
    return config


@pytest.mark.parametrize("workload_type", WORKLOADS)
def test_every_step_is_unthrottled_with_increasing_threads(best_throughput_config, workload_type):
    threads = best_throughput_config.get("perf_gradual_threads")[workload_type]
    steps = best_throughput_config.get("perf_gradual_throttle_steps")[workload_type]

    assert set(steps) == {"unthrottled"}
    assert len(threads) == len(steps)
    assert threads == sorted(set(threads)), "thread counts must be unique and increasing"


@pytest.mark.parametrize("workload_type", WORKLOADS)
def test_step_names_carry_the_thread_count(best_throughput_config, workload_type):
    threads = best_throughput_config.get("perf_gradual_threads")[workload_type]
    workload = gradual_grow_module.Workload(
        workload_type=workload_type,
        cs_cmd_tmpl=[],
        cs_cmd_warm_up=None,
        num_threads=threads,
        throttle_steps=best_throughput_config.get("perf_gradual_throttle_steps")[workload_type],
        preload_data=False,
        drop_keyspace=False,
        wait_no_compactions=False,
        step_duration="30m",
        prepare_schema=False,
    )
    test_class = gradual_grow_module.PerformanceRegressionPredefinedStepsTest
    step_names = test_class.get_sequential_throttle_steps(test_class, workload)
    assert step_names == [f"unthrottled_{count}_threads" for count in threads]


def test_steps_are_time_bound(best_throughput_config):
    assert all(best_throughput_config.get("perf_gradual_step_duration")[workload] for workload in WORKLOADS)
    assert all("duration=$duration" in cmd for cmd in best_throughput_config.get("stress_cmd_w"))


def test_steps_over_the_latency_limit_do_not_fail_the_run(best_throughput_config):
    thresholds = best_throughput_config.get("latency_decorator_error_thresholds")
    for workload in WORKLOADS:
        p99_rules = {
            column: rule for column, rule in thresholds[workload]["default"].items() if column.startswith("P99")
        }
        assert p99_rules
        assert all(rule["fixed_limit"] is None for rule in p99_rules.values())

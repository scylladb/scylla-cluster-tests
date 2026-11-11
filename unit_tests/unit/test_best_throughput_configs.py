"""Unit tests for the best-throughput overlays on top of the predefined-steps base configuration."""

import json
import re

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.utils.best_throughput_launcher import DEFAULT_TEST_CONFIG
from sdcm.utils.dataset_sizing import DatasetSizingConfig
from sdcm.utils.rate_search import RateSearchConfig

PREDEFINED_STEPS_BASE = list(DEFAULT_TEST_CONFIG)
BEST_THROUGHPUT_OVERLAYS = [
    "configurations/performance/best-throughput/sized_dataset.yaml",
    "configurations/performance/best-throughput/latte_time_bound_write.yaml",
    "configurations/performance/best-throughput/best_throughput.yaml",
]
WORKLOADS = ("read", "read_disk_only", "write", "mixed")


@pytest.fixture(params=["i4i.4xlarge", "i8g.4xlarge"])
def best_throughput_config(request, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", request.param)
    monkeypatch.setenv("SCT_CONFIG_FILES", json.dumps([*PREDEFINED_STEPS_BASE, *BEST_THROUGHPUT_OVERLAYS]))
    config = SCTConfiguration()
    config.verify_configuration()
    return config


@pytest.mark.parametrize("workload_type", WORKLOADS)
def test_search_is_on_for_every_workload(best_throughput_config, workload_type):
    search = RateSearchConfig.from_params(best_throughput_config.get("perf_best_throughput_search"), workload_type)
    assert search is not None
    assert search.min_rate_per_shard < search.max_rate_per_shard
    assert search.max_steps >= 2


def test_every_step_is_time_bound(best_throughput_config):
    step_durations = best_throughput_config.get("perf_gradual_step_duration")
    assert all(step_durations[workload] for workload in WORKLOADS)


def test_write_steps_run_for_the_step_duration(best_throughput_config):
    assert all("--duration $duration" in cmd for cmd in best_throughput_config.get("stress_cmd_w"))
    assert not any(re.search(r"--duration \d", cmd) for cmd in best_throughput_config.get("stress_cmd_w"))


@pytest.mark.parametrize("param", ["stress_cmd_w", "stress_cmd_r", "stress_cmd_m", "stress_cmd_read_disk"])
def test_measured_commands_take_the_step_rate_and_concurrency(best_throughput_config, param):
    assert all(
        "$throttle" in cmd and "$threads" in cmd and "$concurrency" in cmd for cmd in best_throughput_config.get(param)
    )


@pytest.mark.parametrize("workload_type", WORKLOADS)
def test_every_workload_has_latte_threads_to_keep(best_throughput_config, workload_type):
    first_step = best_throughput_config.get("perf_gradual_throttle_steps")[workload_type][0]
    assert first_step["threads"] > 0


def test_steps_over_the_latency_limit_do_not_fail_the_run(best_throughput_config):
    thresholds = best_throughput_config.get("latency_decorator_error_thresholds")
    for workload in WORKLOADS:
        p99_rules = {
            column: rule for column, rule in thresholds[workload]["default"].items() if column.startswith("P99")
        }
        assert p99_rules
        assert all(rule["fixed_limit"] is None for rule in p99_rules.values())


def test_latency_limit_defaults_to_10_ms(best_throughput_config):
    assert best_throughput_config.get("perf_gradual_latency_limit") == {"default": {"percentile_99": 10}}


def test_dataset_is_sized_from_the_reference_node(best_throughput_config):
    assert DatasetSizingConfig.from_params(best_throughput_config.get("perf_dataset_sizing")) == DatasetSizingConfig()

"""Unit tests for the best-throughput overlays on top of the predefined-steps base configuration."""

import json
import re

import pytest

from sdcm.sct_config import SCTConfiguration
from sdcm.utils.dataset_sizing import DatasetSizingConfig
from sdcm.utils.thread_search import ThreadSearchConfig

PREDEFINED_STEPS_BASE = [
    "test-cases/performance/perf-regression-predefined-throughput-steps.yaml",
    "configurations/disable_kms.yaml",
    "configurations/disable_speculative_retry.yaml",
]
BEST_THROUGHPUT_OVERLAYS = [
    "configurations/performance/best-throughput/cassandra_stress_time_bound_write.yaml",
    "configurations/performance/best-throughput/cassandra_stress_sized_dataset.yaml",
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
    search = ThreadSearchConfig.from_params(best_throughput_config.get("perf_best_throughput_search"), workload_type)
    assert search is not None
    assert search.max_steps >= 2


def test_every_step_is_time_bound(best_throughput_config):
    assert all(best_throughput_config.get("perf_gradual_step_duration")[workload] for workload in WORKLOADS)
    assert all("duration=$duration" in cmd for cmd in best_throughput_config.get("stress_cmd_w"))
    assert not any(re.search(r" n=\d", cmd) for cmd in best_throughput_config.get("stress_cmd_w"))


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

"""Unit tests for perf_gradual_throttle_steps dict format validation."""

import pytest

from sdcm.sct_config import SCTConfiguration


@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
    monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")


def test_string_format_backward_compatibility():
    """Test that string format (cassandra-stress) still works."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {"read": ["100000", "150000", "200000"], "write": ["50000", "100000"]},
            "perf_gradual_threads": {"read": 100, "write": 50},
        }
    )
    config.verify_configuration()


def test_dict_format_with_all_fields():
    """Test dict format with threads, concurrency, and rate."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "read": [
                    {"threads": 10, "concurrency": 128, "rate": "100000"},
                    {"threads": 20, "concurrency": 256, "rate": "200000"},
                ]
            }
        }
    )
    config.verify_configuration()


def test_dict_format_with_only_threads_and_concurrency():
    """Test dict format with threads and concurrency but no rate (unthrottled)."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "mixed": [{"threads": 30, "concurrency": 40}, {"threads": 35, "concurrency": 40}]
            }
        }
    )
    config.verify_configuration()


def test_dict_format_requires_perf_gradual_threads_when_missing_threads():
    """Test that dict without threads field requires perf_gradual_threads."""
    config = SCTConfiguration()
    config.update({"perf_gradual_throttle_steps": {"write": [{"concurrency": 128, "rate": "100000"}]}})
    with pytest.raises(ValueError, match="perf_gradual_threads should be defined"):
        config.verify_configuration()


def test_mixed_threads_without_fallback():
    """Test that mixing dict steps with and without threads, but no perf_gradual_threads fallback, raises."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "read": [
                    {"threads": 10, "concurrency": 128, "rate": "100000"},
                    {"concurrency": 256, "rate": "200000"},  # missing threads
                ]
            }
        }
    )
    with pytest.raises(ValueError, match="perf_gradual_threads should be defined"):
        config.verify_configuration()


def test_mixed_format_same_workload():
    """Test that mixing string and dict in same workload works when fallback is provided."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "read": [
                    "100000",  # string
                    {"threads": 20, "concurrency": 256, "rate": "200000"},  # dict
                ]
            },
            "perf_gradual_threads": {"read": 100},
        }
    )
    config.verify_configuration()


def test_mixed_format_different_workloads():
    """Test using string format for one workload, dict for another."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "read": ["100000", "150000"],  # string format
                "write": [
                    {"threads": 15, "concurrency": 128, "rate": "50000"},
                    {"threads": 20, "concurrency": 256, "rate": "100000"},
                ],  # dict format
            },
            "perf_gradual_threads": {"read": 100},  # only needed for read (string format)
        }
    )
    config.verify_configuration()


@pytest.mark.parametrize(
    "steps,match",
    [
        pytest.param(
            [{"threads": "10", "concurrency": 128, "rate": "100000"}],
            "'threads' must be a positive integer",
            id="threads_string",
        ),
        pytest.param(
            [{"threads": -10, "concurrency": 128, "rate": "100000"}],
            "'threads' must be a positive integer",
            id="threads_negative",
        ),
        pytest.param(
            [{"threads": 10, "concurrency": "128", "rate": "100000"}],
            "'concurrency' must be a positive integer",
            id="concurrency_string",
        ),
        pytest.param(
            [{"threads": 10, "concurrency": 0, "rate": "100000"}],
            "'concurrency' must be a positive integer",
            id="concurrency_zero",
        ),
        pytest.param(
            [{"threads": 10, "concurrency": 128, "rate": 100000}],
            "'rate' must be a string",
            id="rate_int",
        ),
        pytest.param(
            [{}],
            "dict must have at least one key",
            id="empty_dict",
        ),
        pytest.param(
            [[100000]],
            "each step must be a string, int, or dict",
            id="list_step",
        ),
    ],
)
def test_invalid_step_values(steps, match):
    """Test that invalid step values are rejected with clear error messages."""
    config = SCTConfiguration()
    extra = {}
    # list step type needs perf_gradual_threads to avoid hitting that error first
    if any(isinstance(s, list) for s in steps):
        extra["perf_gradual_threads"] = {"read": 100}
    config.update({"perf_gradual_throttle_steps": {"read": steps}, **extra})
    with pytest.raises(ValueError, match=match):
        config.verify_configuration()


def test_rate_unthrottled_string():
    """Test that 'unthrottled' is a valid rate value."""
    config = SCTConfiguration()
    config.update(
        {
            "perf_gradual_throttle_steps": {
                "read": [
                    {"threads": 10, "concurrency": 128, "rate": "100000"},
                    {"threads": 20, "concurrency": 256, "rate": "unthrottled"},
                ]
            }
        }
    )
    config.verify_configuration()

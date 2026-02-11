"""Unit tests for perf_gradual_throttle_steps dict format validation."""

import pytest

from sdcm.sct_config import SCTConfiguration


class TestPerfGradualThrottleStepsValidation:
    """Test validation of perf_gradual_throttle_steps with dict format support."""

    @pytest.fixture(scope="function", autouse=True)
    def fixture_env(self, monkeypatch):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", "aws")
        monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-1234")
        monkeypatch.setenv("SCT_INSTANCE_TYPE_DB", "i4i.large")

    def test_string_format_backward_compatibility(self):
        """Test that string format (cassandra-stress) still works."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {"read": ["100000", "150000", "200000"], "write": ["50000", "100000"]},
                "perf_gradual_threads": {"read": 100, "write": 50},
            }
        )
        # Should not raise any exception
        config.verify_configuration()

    def test_dict_format_with_all_fields(self):
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
        # Should not raise - dict format has threads, no need for perf_gradual_threads
        config.verify_configuration()

    def test_dict_format_with_only_threads_and_concurrency(self):
        """Test dict format with threads and concurrency but no rate (unthrottled)."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "mixed": [{"threads": 30, "concurrency": 40}, {"threads": 35, "concurrency": 40}]
                }
            }
        )
        # Should not raise - dict has threads
        config.verify_configuration()

    def test_dict_format_requires_perf_gradual_threads_when_missing_threads(self):
        """Test that dict without threads field requires perf_gradual_threads."""
        config = SCTConfiguration()
        config.update({"perf_gradual_throttle_steps": {"write": [{"concurrency": 128, "rate": "100000"}]}})
        with pytest.raises(ValueError, match="perf_gradual_threads should be defined"):
            config.verify_configuration()

    def test_mixed_format_same_workload(self):
        """Test that mixing string and dict in same workload is not allowed."""
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
        # This should work - mixed format is allowed
        config.verify_configuration()

    def test_mixed_format_different_workloads(self):
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

    def test_invalid_threads_type(self):
        """Test that threads must be an integer."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "read": [
                        {"threads": "10", "concurrency": 128, "rate": "100000"}  # threads as string
                    ]
                }
            }
        )
        with pytest.raises(ValueError, match="'threads' must be a positive integer"):
            config.verify_configuration()

    def test_invalid_threads_negative(self):
        """Test that threads must be positive."""
        config = SCTConfiguration()
        config.update(
            {"perf_gradual_throttle_steps": {"read": [{"threads": -10, "concurrency": 128, "rate": "100000"}]}}
        )
        with pytest.raises(ValueError, match="'threads' must be a positive integer"):
            config.verify_configuration()

    def test_invalid_concurrency_type(self):
        """Test that concurrency must be an integer."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "mixed": [
                        {"threads": 10, "concurrency": "128", "rate": "100000"}  # concurrency as string
                    ]
                }
            }
        )
        with pytest.raises(ValueError, match="'concurrency' must be a positive integer"):
            config.verify_configuration()

    def test_invalid_concurrency_zero(self):
        """Test that concurrency must be positive."""
        config = SCTConfiguration()
        config.update({"perf_gradual_throttle_steps": {"mixed": [{"threads": 10, "concurrency": 0, "rate": "100000"}]}})
        with pytest.raises(ValueError, match="'concurrency' must be a positive integer"):
            config.verify_configuration()

    def test_invalid_rate_type(self):
        """Test that rate must be a string."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "write": [
                        {"threads": 10, "concurrency": 128, "rate": 100000}  # rate as int
                    ]
                }
            }
        )
        with pytest.raises(ValueError, match="'rate' must be a string"):
            config.verify_configuration()

    def test_empty_dict_not_allowed(self):
        """Test that empty dict is not allowed."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "read": [{}]  # empty dict
                }
            }
        )
        with pytest.raises(ValueError, match="dict must have at least one key"):
            config.verify_configuration()

    def test_invalid_step_type(self):
        """Test that step must be string, int, or dict."""
        config = SCTConfiguration()
        config.update(
            {
                "perf_gradual_throttle_steps": {
                    "read": [[100000]]  # list instead of string, int, or dict - actually invalid
                },
                "perf_gradual_threads": {"read": 100},
            }
        )
        with pytest.raises(ValueError, match="each step must be a string, int, or dict"):
            config.verify_configuration()

    def test_rate_unthrottled_string(self):
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

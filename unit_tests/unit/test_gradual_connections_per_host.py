# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Tests for PerformanceRegressionPredefinedStepsTest.connections_per_host resolution."""

from unittest.mock import patch

import pytest

# Import the module (not the class) so pytest does not collect the imported
# unittest.TestCase subclass and try to run its real test_* methods.
import performance_regression_gradual_grow_throughput as perf_mod


def _make_test(connections_per_host):
    """Build an instance without running the heavy setUp; only ``.params`` is needed."""
    test = object.__new__(perf_mod.PerformanceRegressionPredefinedStepsTest)
    test.params = {"perf_gradual_connections_per_host": connections_per_host}
    return test


def test_connections_per_host_int_global():
    """A single int applies to every workload type."""
    test = _make_test(8)
    assert test.connections_per_host("read") == 8
    assert test.connections_per_host("write") == 8
    assert test.connections_per_host("read_disk_only") == 8


def test_connections_per_host_per_workload_dict():
    """A dict resolves the value per workload type."""
    test = _make_test({"read": 8, "write": 16, "mixed": 32, "read_disk_only": 8})
    assert test.connections_per_host("read") == 8
    assert test.connections_per_host("write") == 16
    assert test.connections_per_host("mixed") == 32
    assert test.connections_per_host("read_disk_only") == 8


def test_connections_per_host_missing_workload_key_publishes_critical():
    """A workload type missing from the dict publishes a CRITICAL event."""
    test = _make_test({"read": 8})
    with patch("performance_regression_gradual_grow_throughput.TestFrameworkEvent") as mock_event:
        # current behaviour (mirrors throttle_steps/step_duration): publish CRITICAL then KeyError
        with pytest.raises(KeyError):
            test.connections_per_host("write")
    mock_event.assert_called_once()
    assert mock_event.call_args.kwargs["severity"].name == "CRITICAL"


def test_connections_per_host_unset_returns_none():
    """When perf_gradual_connections_per_host is not configured, resolves to None
    (opt-out of $connections_per_host substitution) instead of raising."""
    test = _make_test(None)
    assert test.connections_per_host("read") is None
    assert test.connections_per_host("write") is None
    assert test.connections_per_host("mixed") is None
    assert test.connections_per_host("read_disk_only") is None

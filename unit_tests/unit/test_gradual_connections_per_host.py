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

"""Tests for per-throttle-step ``connections_per_host`` resolution and expansion."""

from unittest.mock import patch

import pytest

import performance_regression_gradual_grow_throughput as gradual_grow_module


def _make_test(connections_per_host):
    """Build an instance without running the heavy setUp; only ``.params`` is needed."""
    test = object.__new__(gradual_grow_module.PerformanceRegressionPredefinedStepsTest)
    test.params = {"perf_gradual_connections_per_host": connections_per_host}
    return test


def _make_workload(connections_per_host, num_threads=620, num_steps=4):
    return gradual_grow_module.Workload(
        workload_type="read",
        cs_cmd_tmpl=["cassandra-stress read ... -rate 'threads=$threads $throttle'"],
        cs_cmd_warm_up=None,
        connections_per_host=connections_per_host,
        num_threads=num_threads,
        throttle_steps=[str(100000 * (idx + 1)) for idx in range(num_steps)],
        preload_data=False,
        drop_keyspace=False,
        wait_no_compactions=False,
        step_duration="30m",
        prepare_schema=False,
    )


# ---------------------------------------------------------------------------
# connections_per_host() resolution
# ---------------------------------------------------------------------------


def test_connections_per_host_scalar_per_workload():
    """A scalar per workload type is returned as-is (expanded to steps later)."""
    test = _make_test({"read": 8, "write": 16, "mixed": 32, "read_disk_only": 8})
    assert test.connections_per_host("read") == 8
    assert test.connections_per_host("write") == 16
    assert test.connections_per_host("mixed") == 32
    assert test.connections_per_host("read_disk_only") == 8


def test_connections_per_host_per_step_list():
    """A list holds one value per throttle step and is returned untouched."""
    test = _make_test({"read": [8, 100, 1000, 3750]})
    assert test.connections_per_host("read") == [8, 100, 1000, 3750]


@pytest.mark.parametrize("param_value", [None, {}])
def test_connections_per_host_unset_returns_none(param_value):
    """When the parameter is not configured every workload resolves to None.

    Stress tools other than cassandra-stress (latte, scylla-bench, cql-stress, logstor)
    have no '$connections_per_host' placeholder and never set the parameter.
    """
    test = _make_test(param_value)
    for workload_type in ("read", "write", "mixed", "read_disk_only"):
        assert test.connections_per_host(workload_type) is None


def test_connections_per_host_missing_workload_returns_none():
    """A workload absent from the dict opts out of the substitution rather than failing."""
    test = _make_test({"read": 8})
    with patch("performance_regression_gradual_grow_throughput.TestFrameworkEvent") as mock_event:
        assert test.connections_per_host("write") is None
    mock_event.assert_not_called()


# ---------------------------------------------------------------------------
# Workload normalisation
# ---------------------------------------------------------------------------


def test_workload_normalizes_int_to_list():
    assert _make_workload(connections_per_host=8).connections_per_host == [8]


def test_workload_keeps_list():
    assert _make_workload(connections_per_host=[8, 100, 1000, 3750]).connections_per_host == [8, 100, 1000, 3750]


def test_workload_keeps_none():
    assert _make_workload(connections_per_host=None).connections_per_host is None


# ---------------------------------------------------------------------------
# update_workload_for_steps() expansion
# ---------------------------------------------------------------------------


def test_update_workload_broadcasts_single_value():
    """A single value is repeated for every throttle step, for both per-step lists."""
    workload = _make_workload(connections_per_host=8, num_threads=620, num_steps=4)
    updated = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.update_workload_for_steps(workload)
    assert updated.connections_per_host == [8, 8, 8, 8]
    assert updated.num_threads == [620, 620, 620, 620]


def test_update_workload_keeps_per_step_list():
    workload = _make_workload(connections_per_host=[8, 100, 1000, 3750], num_steps=4)
    updated = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.update_workload_for_steps(workload)
    assert updated.connections_per_host == [8, 100, 1000, 3750]


def test_update_workload_handles_none():
    """An unset connections_per_host survives expansion untouched."""
    workload = _make_workload(connections_per_host=None, num_steps=4)
    updated = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.update_workload_for_steps(workload)
    assert updated.connections_per_host is None
    assert updated.num_threads == [620, 620, 620, 620]


def test_update_workload_single_step_is_noop():
    workload = _make_workload(connections_per_host=8, num_steps=1)
    updated = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.update_workload_for_steps(workload)
    assert updated is workload

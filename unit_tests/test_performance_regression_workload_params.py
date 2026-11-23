"""Unit tests for the stress command parameters of PerformanceRegressionTest.run_workload."""

from types import SimpleNamespace

import pytest

import performance_regression_test as perf_module

SINGLE_CMD = "cassandra-stress read cl=QUORUM duration=800m -pop seq=1..20000000"
LOADER_CMDS = [
    "cassandra-stress read cl=QUORUM duration=800m -pop seq=1..5000000",
    "cassandra-stress read cl=QUORUM duration=800m -pop seq=5000001..10000000",
]


def _params(stress_cmd, round_robin):
    tester = SimpleNamespace(params={"round_robin": round_robin})
    return perf_module.PerformanceRegressionTest._workload_stress_params(tester, stress_cmd)


@pytest.mark.parametrize("stress_cmd", [SINGLE_CMD, [SINGLE_CMD]], ids=["string", "list-of-one"])
def test_single_command_runs_on_all_loaders(stress_cmd):
    """The config turns a single command into a list of one, and it must not go to one loader."""
    params = _params(stress_cmd, round_robin=True)
    assert "round_robin" not in params
    assert params["stress_cmd"] == stress_cmd
    assert params["stress_num"] == 1


def test_command_per_loader_runs_round_robin():
    params = _params(LOADER_CMDS, round_robin=True)
    assert params["round_robin"] is True
    assert params["stress_cmd"] == LOADER_CMDS


def test_command_list_without_round_robin_runs_on_all_loaders():
    assert "round_robin" not in _params(LOADER_CMDS, round_robin=False)

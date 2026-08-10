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
# Copyright (c) 2024 ScyllaDB

"""Unit tests for dual-engine (logstor + LSM) concurrent workload orchestration.

Covers:
- _dispatch_stress_cmds: placeholder substitution, correct number of launched threads.
- _await_engine_results: awaits pre-dispatched queues, returns (results, queues).
- _partition_dual_engine_cmds: partitions by function-name prefix, raises ValueError
  when either engine's command set is empty.
- _aggregate_ops_rate: round-robin slice semantics (avg×N) vs additive semantics (sum×N).
- check_latency_during_steps_dual_engine: reads both step keys; deletes file exactly once.
- logstor_lsm_dual_60_40.yaml: required config keys are present and correctly shaped.
- test_dual_engine_mixed_gradual_increase_load: schema is created before preload_data
  (exercised via the real production method, not a re-implementation of its logic).
"""

import json
import pathlib

import pytest
import yaml

from sdcm import sct_abs_path

# Import only the module; do NOT expose the class at module-level so pytest's
# collector doesn't pick up its test_* methods as test cases.
import performance_regression_gradual_grow_throughput as _perf_module


def _predefined_steps_cls():
    return _perf_module.PerformanceRegressionPredefinedStepsTest


# ─── Stubs ────────────────────────────────────────────────────────────────────


class _FakeQueue:
    """Minimal stress-queue stub: carries hdr_tags and canned results."""

    def __init__(self, hdr_tags, results=None):
        self.hdr_tags = hdr_tags
        self._results = results or []


class _FakeTest:
    """Minimal stub of PerformanceRegressionPredefinedStepsTest for unit tests.

    Only the methods under test are real; everything else is stubbed.
    """

    log = type(
        "log",
        (),
        {
            "debug": staticmethod(lambda *a, **kw: None),
            "info": staticmethod(lambda *a, **kw: None),
            "error": staticmethod(lambda *a, **kw: None),
        },
    )()

    # Populated by monkeypatch in latency-file tests
    latency_results_file = ""

    def __init__(self):
        self._dispatch_calls = []

    def run_stress_thread(self, stress_cmd, **_kwargs):
        """Record the dispatched command and return a fake queue."""
        fn_name = stress_cmd.split("--function ")[1].split()[0] if "--function " in stress_cmd else "unknown"
        queue = _FakeQueue(
            hdr_tags=[f"fn--{fn_name}"],
            results=[{"op rate": 100.0, "latency mean": 1.0, "latency 99th percentile": 5.0}],
        )
        self._dispatch_calls.append(stress_cmd)
        return queue

    def get_stress_results(self, queue, store_results=False):
        return queue._results

    # Bind the real static/instance methods from the class under test.
    # These are bound at class-definition time via the module reference (not _predefined_steps_cls())
    # so they are plain unbound method references, not live instances.
    _dispatch_stress_cmds = _perf_module.PerformanceRegressionPredefinedStepsTest._dispatch_stress_cmds
    _await_stress_queues = _perf_module.PerformanceRegressionPredefinedStepsTest._await_stress_queues
    _await_engine_results = _perf_module.PerformanceRegressionPredefinedStepsTest._await_engine_results
    _aggregate_ops_rate = staticmethod(_perf_module.PerformanceRegressionPredefinedStepsTest._aggregate_ops_rate)
    _partition_dual_engine_cmds = staticmethod(
        _perf_module.PerformanceRegressionPredefinedStepsTest._partition_dual_engine_cmds
    )
    _extract_step_latency = staticmethod(_perf_module.PerformanceRegressionPredefinedStepsTest._extract_step_latency)
    check_latency_during_steps_dual_engine = (
        _perf_module.PerformanceRegressionPredefinedStepsTest.check_latency_during_steps_dual_engine
    )


@pytest.fixture()
def fake_test():
    return _FakeTest()


# ─── Tests: _dispatch_stress_cmds ─────────────────────────────────────────────


def test_dispatch_stress_cmds_substitutes_placeholders(fake_test):
    """Placeholder tokens ($threads, $rate, $duration) are replaced before dispatch."""
    cmds = [
        "latte run --function lsm_write --threads $threads --rate $lsm_write_rate --duration $duration "
        "data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    step_params = {"threads": 16, "lsm_write_rate": "72000"}
    queues = fake_test._dispatch_stress_cmds(cmds, step_params, step_duration="3600")

    assert len(queues) == 1
    dispatched_cmd = fake_test._dispatch_calls[0]
    assert "--threads 16" in dispatched_cmd
    assert "--rate 72000" in dispatched_cmd
    assert "--duration 3600" in dispatched_cmd
    assert "$threads" not in dispatched_cmd
    assert "$lsm_write_rate" not in dispatched_cmd
    assert "$duration" not in dispatched_cmd


def test_dispatch_stress_cmds_creates_one_queue_per_cmd(fake_test):
    """Each command in the list produces an independent queue object."""
    cmds = [
        "latte run --function logstor_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function logstor_read data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    queues = fake_test._dispatch_stress_cmds(cmds, step_params={}, step_duration=None)
    assert len(queues) == 2
    assert queues[0] is not queues[1]


def test_dispatch_stress_cmds_none_duration_skipped(fake_test):
    """When step_duration is None the $duration placeholder is left as-is."""
    cmds = ["latte run --function lsm_read --duration $duration data_dir/latte/latte_cs_alike_dual_engine.rn"]
    fake_test._dispatch_stress_cmds(cmds, step_params={}, step_duration=None)
    assert "$duration" in fake_test._dispatch_calls[0]


def test_dispatch_stress_cmds_longer_placeholder_wins(fake_test):
    """Longer placeholder names are substituted before shorter ones to avoid partial matches.

    e.g. $logstor_write_rate must be replaced before $logstor_write to prevent
    '$logstor_write_rate' being partially mangled to '<value>_rate'.
    """
    cmds = [
        "latte run --function logstor_write --rate $logstor_write_rate data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    step_params = {"logstor_write_rate": "108000", "logstor_write": "WRONG"}
    fake_test._dispatch_stress_cmds(cmds, step_params, step_duration=None)
    dispatched = fake_test._dispatch_calls[0]
    assert "--rate 108000" in dispatched
    assert "WRONG" not in dispatched


# ─── Tests: _await_engine_results ────────────────────────────────────────────


def test_await_engine_results_collects_from_pre_dispatched_queues(fake_test):
    """_await_engine_results collects results from already-dispatched queues.

    The queues are created first (simulating non-blocking dispatch), then
    _await_engine_results is called — mirroring the production concurrency model
    where both engines' threads are dispatched before either is awaited.
    """
    cmds = [
        "latte run --function logstor_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function logstor_read data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    # Dispatch non-blocking first (no results collected yet)
    queues = fake_test._dispatch_stress_cmds(cmds, step_params={}, step_duration=None)
    assert len(fake_test._dispatch_calls) == 2

    # Now await: _await_engine_results must not re-dispatch
    results, returned_queues = fake_test._await_engine_results(
        engine_name="logstor",
        queues=queues,
        hdr_tags=["fn--logstor_write", "fn--logstor_read"],
    )
    # Still 2 dispatch calls — _await_engine_results does not dispatch anything
    assert len(fake_test._dispatch_calls) == 2
    assert returned_queues is queues
    assert len(results) == 2  # one result dict per fake queue


# ─── Tests: _partition_dual_engine_cmds ───────────────────────────────────────


def test_partition_dual_engine_cmds_by_prefix():
    """Commands in stress_cmd_m are correctly split into logstor_* and lsm_* groups."""
    cmds = [
        "latte run --function logstor_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function logstor_read data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function lsm_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function lsm_read data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    logstor_cmds, lsm_cmds = _predefined_steps_cls()._partition_dual_engine_cmds(cmds)

    assert len(logstor_cmds) == 2
    assert all("logstor_" in c for c in logstor_cmds)
    assert len(lsm_cmds) == 2
    assert all("lsm_" in c for c in lsm_cmds)
    # No overlap between the two sets
    assert not set(logstor_cmds) & set(lsm_cmds)


def test_partition_dual_engine_cmds_raises_on_missing_logstor():
    """_partition_dual_engine_cmds raises ValueError when no logstor_* commands are present."""
    cmds = [
        "latte run --function lsm_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function lsm_read data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    with pytest.raises(ValueError, match="logstor_\\*"):
        _predefined_steps_cls()._partition_dual_engine_cmds(cmds)


def test_partition_dual_engine_cmds_raises_on_missing_lsm():
    """_partition_dual_engine_cmds raises ValueError when no lsm_* commands are present."""
    cmds = [
        "latte run --function logstor_write data_dir/latte/latte_cs_alike_dual_engine.rn",
        "latte run --function logstor_read data_dir/latte/latte_cs_alike_dual_engine.rn",
    ]
    with pytest.raises(ValueError, match="lsm_\\*"):
        _predefined_steps_cls()._partition_dual_engine_cmds(cmds)


def test_partition_dual_engine_cmds_raises_on_empty_list():
    """_partition_dual_engine_cmds raises ValueError when the command list is empty."""
    with pytest.raises(ValueError):
        _predefined_steps_cls()._partition_dual_engine_cmds([])


# ─── Tests: _aggregate_ops_rate ───────────────────────────────────────────────


def test_aggregate_ops_rate_round_robin_slices():
    """Round-robin slice commands (num_commands == num_loaders): avg × num_loaders.

    Base predefined config: 4 slice commands, 4 loaders (one command per loader).
    Each result is one loader's measured throughput.  The historical formula
    avg × num_loaders must be preserved exactly to avoid Argus series drift.
    """
    # 4 loaders, each measuring ~100k op/s: total = 400k
    results = [{"op rate": 100000.0}] * 4
    total = _predefined_steps_cls()._aggregate_ops_rate(results, num_loaders=4, num_commands=4)
    assert total == pytest.approx(400000.0)


def test_aggregate_ops_rate_round_robin_equals_avg_times_loaders():
    """Round-robin path: result matches avg × num_loaders exactly (historical formula)."""
    results = [{"op rate": r} for r in [90000.0, 110000.0, 95000.0, 105000.0]]
    cls = _predefined_steps_cls()
    expected = (sum(r["op rate"] for r in results) / len(results)) * 4
    assert cls._aggregate_ops_rate(results, num_loaders=4, num_commands=4) == pytest.approx(expected)


def test_aggregate_ops_rate_additive_commands():
    """Additive commands (num_commands != num_loaders): sum × num_loaders.

    Logstor config: 2 commands (write + read) on 1 loader.
    num_commands(2) != num_loaders(1) → use sum path → 108k + 252k = 360k.
    """
    results = [{"op rate": 108000.0}, {"op rate": 252000.0}]
    total = _predefined_steps_cls()._aggregate_ops_rate(results, num_loaders=1, num_commands=2)
    assert total == pytest.approx(360000.0)


def test_aggregate_ops_rate_additive_does_not_average():
    """Additive path must NOT average: (108k + 252k) / 2 = 180k ≠ 360k."""
    results = [{"op rate": 108000.0}, {"op rate": 252000.0}]
    total = _predefined_steps_cls()._aggregate_ops_rate(results, num_loaders=1, num_commands=2)
    # Must be 360k (sum), not 180k (average)
    assert total != pytest.approx(180000.0)
    assert total == pytest.approx(360000.0)


def test_aggregate_ops_rate_empty_results():
    """Empty result list returns 0.0 without raising."""
    assert _predefined_steps_cls()._aggregate_ops_rate([], num_loaders=4, num_commands=4) == pytest.approx(0.0)


def test_aggregate_ops_rate_dual_engine_logstor():
    """Dual-engine logstor (2 cmds, 1 loader): write 108k + read 252k = 360k."""
    results = [{"op rate": 108000.0}, {"op rate": 252000.0}]
    total = _predefined_steps_cls()._aggregate_ops_rate(results, num_loaders=1, num_commands=2)
    assert total == pytest.approx(360000.0)


def test_aggregate_ops_rate_dual_engine_lsm():
    """Dual-engine LSM (2 cmds, 1 loader): write 72k + read 168k = 240k."""
    results = [{"op rate": 72000.0}, {"op rate": 168000.0}]
    total = _predefined_steps_cls()._aggregate_ops_rate(results, num_loaders=1, num_commands=2)
    assert total == pytest.approx(240000.0)


# ─── Tests: check_latency_during_steps_dual_engine ───────────────────────────


def test_check_latency_during_steps_dual_engine_reads_both_keys(fake_test, tmp_path, monkeypatch):
    """Both logstor and lsm step keys are extracted; the file is deleted exactly once."""
    latency_data = {
        "logstor_600000": {"legend": "Logstor step", "cycles": []},
        "lsm_600000": {"legend": "LSM step", "cycles": []},
    }
    results_file = tmp_path / "latency_results.json"
    results_file.write_text(json.dumps(latency_data))
    monkeypatch.setattr(fake_test, "latency_results_file", str(results_file))

    logstor_summary, lsm_summary = fake_test.check_latency_during_steps_dual_engine(
        logstor_step="logstor_600000", lsm_step="lsm_600000"
    )

    assert "logstor_600000" in logstor_summary
    assert "lsm_600000" in lsm_summary
    # File must have been deleted exactly once
    assert not results_file.exists()


def test_check_latency_during_steps_dual_engine_missing_key_returns_empty(fake_test, tmp_path, monkeypatch):
    """A missing step key returns the fallback empty structure without raising."""
    latency_data = {
        "logstor_600000": {"legend": "Logstor step", "cycles": []},
        # lsm key intentionally absent
    }
    results_file = tmp_path / "latency_results.json"
    results_file.write_text(json.dumps(latency_data))
    monkeypatch.setattr(fake_test, "latency_results_file", str(results_file))

    logstor_summary, lsm_summary = fake_test.check_latency_during_steps_dual_engine(
        logstor_step="logstor_600000", lsm_step="lsm_600000"
    )

    assert "logstor_600000" in logstor_summary
    assert lsm_summary == {"lsm_600000": {"step": "lsm_600000", "legend": "", "cycles": []}}


# ─── Tests: YAML config logstor_lsm_dual_60_40.yaml ──────────────────────────


@pytest.fixture(scope="module")
def dual_engine_config():
    config_path = pathlib.Path(sct_abs_path("configurations/performance/logstor_lsm_dual_60_40.yaml"))
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def test_dual_engine_config_has_stress_cmd_m(dual_engine_config):
    """stress_cmd_m must be present and contain all four engine commands."""
    assert "stress_cmd_m" in dual_engine_config, "stress_cmd_m is missing"
    assert isinstance(dual_engine_config["stress_cmd_m"], list)
    assert len(dual_engine_config["stress_cmd_m"]) >= 4, (
        "stress_cmd_m must have at least 4 commands (logstor_write, logstor_read, lsm_write, lsm_read)"
    )


def test_dual_engine_config_stress_cmd_m_has_all_four_functions(dual_engine_config):
    """stress_cmd_m must contain logstor_write, logstor_read, lsm_write, lsm_read functions."""
    cmds = dual_engine_config["stress_cmd_m"]
    fn_names = {cmd.split("--function ")[1].split()[0] for cmd in cmds if "--function " in cmd}
    for expected in ("logstor_write", "logstor_read", "lsm_write", "lsm_read"):
        assert expected in fn_names, f"'{expected}' not found in stress_cmd_m functions: {fn_names}"


def test_dual_engine_config_throttle_steps_have_all_rate_keys(dual_engine_config):
    """Each throttle step must carry the four per-engine rate keys and an aggregate rate."""
    steps = dual_engine_config["perf_gradual_throttle_steps"]["dual_engine_mixed"]
    for step in steps:
        for key in ("rate", "logstor_write_rate", "logstor_read_rate", "lsm_write_rate", "lsm_read_rate"):
            assert key in step, f"Throttle step missing '{key}': {step}"


def test_dual_engine_config_ops_split_is_60_40(dual_engine_config):
    """Total logstor op/s should be 60% and lsm 40% for each throttle step."""
    steps = dual_engine_config["perf_gradual_throttle_steps"]["dual_engine_mixed"]
    for step in steps:
        logstor_total = int(step["logstor_write_rate"]) + int(step["logstor_read_rate"])
        lsm_total = int(step["lsm_write_rate"]) + int(step["lsm_read_rate"])
        combined = logstor_total + lsm_total
        assert combined > 0
        logstor_pct = logstor_total / combined
        # Allow ±2% tolerance around 60%
        assert 0.58 <= logstor_pct <= 0.62, f"Logstor share {logstor_pct:.2%} is not within 58-62% (step={step})"


def test_dual_engine_config_no_global_storage_engine(dual_engine_config):
    """latte_schema_parameters must NOT set a global storage_engine key."""
    schema_params = dual_engine_config.get("latte_schema_parameters", {})
    assert "storage_engine" not in schema_params, (
        "Global storage_engine must not be set; engine is embedded per-command via inline -P flags."
    )


def test_dual_engine_config_prepare_cmd_uses_dual_engine_script(dual_engine_config):
    """prepare_stress_cmd must reference latte_cs_alike_dual_engine.rn."""
    prepare_cmd = dual_engine_config["prepare_stress_cmd"]
    assert "latte_cs_alike_dual_engine.rn" in prepare_cmd, (
        f"prepare_stress_cmd does not use dual-engine script: {prepare_cmd}"
    )


def test_dual_engine_config_prepare_write_cmd_populates_both_engines(dual_engine_config):
    """prepare_write_cmd must contain commands for both lsm_write and logstor_write."""
    cmds = dual_engine_config["prepare_write_cmd"]
    fn_names = set()
    for cmd in cmds:
        if "--function " in cmd:
            fn_names.add(cmd.split("--function ")[1].split()[0])
    assert "lsm_write" in fn_names, f"lsm_write not found in prepare_write_cmd: {fn_names}"
    assert "logstor_write" in fn_names, f"logstor_write not found in prepare_write_cmd: {fn_names}"


def test_dual_engine_config_stress_cmd_m_partitions_cleanly(dual_engine_config):
    """_partition_dual_engine_cmds succeeds on the real config's stress_cmd_m."""
    cmds = dual_engine_config["stress_cmd_m"]
    logstor_cmds, lsm_cmds = _predefined_steps_cls()._partition_dual_engine_cmds(cmds)
    assert len(logstor_cmds) >= 1
    assert len(lsm_cmds) >= 1


def test_dual_engine_config_step_rate_matches_sum(dual_engine_config):
    """The aggregate 'rate' field must equal the sum of all four per-engine rates."""
    steps = dual_engine_config["perf_gradual_throttle_steps"]["dual_engine_mixed"]
    for step in steps:
        expected_total = (
            int(step["logstor_write_rate"])
            + int(step["logstor_read_rate"])
            + int(step["lsm_write_rate"])
            + int(step["lsm_read_rate"])
        )
        assert int(step["rate"]) == expected_total, (
            f"Step 'rate' {step['rate']} != sum of engine rates {expected_total}"
        )


# ─── Tests: schema-before-preload contract ────────────────────────────────────


def test_dual_engine_config_has_prepare_stress_cmd(dual_engine_config):
    """prepare_stress_cmd must be present so the dual-engine test can create the schema.

    test_dual_engine_mixed_gradual_increase_load runs prepare_stress_cmd before
    preload_data().  Without this key the latte schema command is never executed
    and populate hits non-existent tables.
    """
    assert "prepare_stress_cmd" in dual_engine_config, (
        "prepare_stress_cmd is missing — schema will never be created before populate"
    )
    cmd = dual_engine_config["prepare_stress_cmd"]
    assert "latte schema" in cmd, f"prepare_stress_cmd does not contain 'latte schema': {cmd}"


def test_dual_engine_test_runs_schema_before_preload(monkeypatch):
    """Schema (prepare_stress_cmd) is dispatched before preload_data() is called.

    Calls the real test_dual_engine_mixed_gradual_increase_load production method on a
    stub (rather than re-implementing its schema-dispatch logic in the test body), so a
    regression in the actual method's ordering would be caught here.
    """
    call_order = []

    class _StubTest(_FakeTest):
        def __init__(self):
            super().__init__()
            self.params = {
                "prepare_stress_cmd": "latte schema -P col=1 data_dir/latte/latte_cs_alike_dual_engine.rn",
                "stress_cmd_m": [
                    "latte run --function logstor_write data_dir/latte/latte_cs_alike_dual_engine.rn",
                    "latte run --function lsm_write data_dir/latte/latte_cs_alike_dual_engine.rn",
                ],
            }
            self.loaders = type("L", (), {"nodes": ["loader1"]})()

        def run_stress_thread(self, stress_cmd, **kwargs):
            call_order.append(("schema_dispatch", stress_cmd))
            return _FakeQueue(hdr_tags=["fn--schema"], results=[])

        def preload_data(self, compaction_strategy=None):
            call_order.append(("preload_data",))

        def wait_no_compactions_running(self, **kwargs):
            return (0,)

        def wait_for_no_tablets_splits(self):
            pass

        def run_fstrim_on_all_db_nodes(self):
            pass

        def throttle_steps(self, workload_type):
            return ["100000"]

        def step_duration(self, workload_type):
            return "3600"

        def get_num_threads_for_workload(self, workload_type):
            return 16

        def run_dual_engine_gradual_increase_load(self, **kwargs):
            call_order.append(("run_dual_engine_gradual_increase_load",))

    stub = _StubTest()
    monkeypatch.setattr(_perf_module, "skip_optional_stage", lambda _stage: False)

    _perf_module.PerformanceRegressionPredefinedStepsTest.test_dual_engine_mixed_gradual_increase_load(stub)

    # Schema dispatch must come before preload_data
    assert len(call_order) >= 2
    schema_idx = next(i for i, c in enumerate(call_order) if c[0] == "schema_dispatch")
    preload_idx = next(i for i, c in enumerate(call_order) if c[0] == "preload_data")
    assert schema_idx < preload_idx, f"Schema dispatch (pos {schema_idx}) must precede preload_data (pos {preload_idx})"

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

"""
Unit tests for the dynamic keyspace/table resolution logic in the gradual
performance testing framework (PerformanceRegressionPredefinedStepsTest).

Tests import the production code directly to ensure we're validating real behaviour.
"""

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

import performance_regression_gradual_grow_throughput as gradual_grow_module
from sdcm.utils.decorators import _find_hdr_tags


def _get_test_table_name(params, stress_cmds):
    """Call the production get_test_table_name with a minimal mock instance."""
    instance = SimpleNamespace(params=params)
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest.get_test_table_name(instance, stress_cmds)


# ---------------------------------------------------------------------------
# cassandra-stress / cql-stress / scylla-bench: read from YAML params
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cmd",
    [
        "cassandra-stress write cl=QUORUM n=1000",
        "cql-stress-cassandra-stress write cl=QUORUM n=1000",
        "scylla-bench -workload=sequential -mode=write",
    ],
)
def test_get_test_table_name_reads_from_yaml_params(cmd):
    """Non-latte tools must read keyspace and table from perf_stress_keyspace/perf_stress_table params."""
    keyspace, table = _get_test_table_name(
        {"perf_stress_keyspace": "mykeyspace", "perf_stress_table": "mytable"},
        [cmd],
    )
    assert keyspace == "mykeyspace"
    assert table == "mytable"


def test_get_test_table_name_raises_when_keyspace_missing():
    """Raise a clear ValueError when perf_stress_keyspace is not configured."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"perf_stress_keyspace": None, "perf_stress_table": "standard1"},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


def test_get_test_table_name_raises_when_table_missing():
    """Raise a clear ValueError when perf_stress_table is not configured."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"perf_stress_keyspace": "keyspace1", "perf_stress_table": None},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


# ---------------------------------------------------------------------------
# latte: keyspace and table from perf_stress_keyspace / perf_stress_table
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_perf_stress_params():
    """Latte keyspace/table resolved from perf_stress_keyspace/perf_stress_table."""
    keyspace, table = _get_test_table_name(
        {"perf_stress_keyspace": "my_ks", "perf_stress_table": "my_tbl"},
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "my_ks"
    assert table == "my_tbl"


def test_get_test_table_name_latte_perf_stress_params_take_priority():
    """perf_stress_keyspace/perf_stress_table take priority over latte_schema_parameters."""
    keyspace, table = _get_test_table_name(
        {
            "perf_stress_keyspace": "from_perf",
            "perf_stress_table": "from_perf_tbl",
            "latte_schema_parameters": {"keyspace": "from_schema", "table": "from_schema_tbl"},
        },
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "from_perf"
    assert table == "from_perf_tbl"


# ---------------------------------------------------------------------------
# latte: keyspace and table from latte_schema_parameters (fallback)
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_from_schema_params():
    """Latte keyspace/table resolved from latte_schema_parameters when perf_stress_* not set."""
    keyspace, table = _get_test_table_name(
        {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "my_latte_ks"
    assert table == "my_latte_tbl"


def test_get_test_table_name_latte_partial_perf_stress_with_schema_fallback():
    """perf_stress_keyspace set, table falls back to latte_schema_parameters."""
    keyspace, table = _get_test_table_name(
        {
            "perf_stress_keyspace": "from_perf",
            "perf_stress_table": None,
            "latte_schema_parameters": {"keyspace": "from_schema", "table": "from_schema_tbl"},
        },
        ["latte run /some/script.rn -f write"],
    )
    assert keyspace == "from_perf"
    assert table == "from_schema_tbl"


def test_get_test_table_name_latte_partial_schema_params_keyspace_only():
    """latte_schema_parameters has keyspace but no table — should raise ValueError."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks"}},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# latte: missing configuration raises ValueError
# ---------------------------------------------------------------------------


def test_get_test_table_name_latte_raises_when_keyspace_unresolvable():
    """Raise a clear ValueError when latte keyspace cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"latte_schema_parameters": {}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_name_latte_raises_when_table_unresolvable():
    """Raise a clear ValueError when latte table cannot be determined."""
    with pytest.raises(ValueError, match="perf_stress_table"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_ks"}},
            ["latte run /some/script.rn -f write"],
        )


def test_get_test_table_name_latte_raises_when_no_params_at_all():
    """Raise a clear ValueError when no configuration is provided for latte."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {},
            ["latte run /some/script.rn -f write"],
        )


# ---------------------------------------------------------------------------
# Non-latte tools ignore latte_schema_parameters
# ---------------------------------------------------------------------------


def test_get_test_table_name_non_latte_ignores_schema_params():
    """Non-latte tools should not fall back to latte_schema_parameters."""
    with pytest.raises(ValueError, match="perf_stress_keyspace"):
        _get_test_table_name(
            {"latte_schema_parameters": {"keyspace": "my_latte_ks", "table": "my_latte_tbl"}},
            ["cassandra-stress write cl=QUORUM n=1000"],
        )


# ---------------------------------------------------------------------------
# _aggregate_ops_rate: round-robin vs. additive command patterns
# ---------------------------------------------------------------------------


def _aggregate_ops_rate(results, num_loaders, num_commands):
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest._aggregate_ops_rate(
        results, num_loaders, num_commands
    )


def test_aggregate_ops_rate_round_robin_uses_average_times_loaders():
    """num_commands == num_loaders: one partition-slice command per loader, avg × loaders."""
    results = [{"op rate": "100"}, {"op rate": "200"}]
    assert _aggregate_ops_rate(results, num_loaders=2, num_commands=2) == 150 * 2


def test_aggregate_ops_rate_additive_commands_are_summed():
    """num_commands != num_loaders: distinct concurrent commands (write + read) are additive."""
    results = [{"op rate": "100"}, {"op rate": "50"}]
    assert _aggregate_ops_rate(results, num_loaders=1, num_commands=2) == 150 * 1


def test_aggregate_ops_rate_ignores_bad_values():
    """Non-numeric 'op rate' entries are treated as 0 rather than raising."""
    results = [{"op rate": "100"}, {"op rate": "not-a-number"}, {}]
    assert _aggregate_ops_rate(results, num_loaders=1, num_commands=3) == 100


def test_aggregate_ops_rate_empty_results():
    assert _aggregate_ops_rate([], num_loaders=2, num_commands=2) == 0.0


# ---------------------------------------------------------------------------
# check_latency_during_steps: an empty latency results file
# ---------------------------------------------------------------------------


def _check_latency_during_steps(tmp_path, content, step="unthrottled"):
    """Call the production check_latency_during_steps against a results file holding `content`."""
    results_file = tmp_path / "latency_results.json"
    results_file.write_text(content, encoding="utf-8")
    instance = SimpleNamespace(latency_results_file=str(results_file), log=logging.getLogger(__name__))
    result = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.check_latency_during_steps(instance, step)
    return result, results_file


@pytest.mark.parametrize(
    "content",
    (pytest.param("", id="empty"), pytest.param("  \n", id="whitespace-only")),
)
def test_check_latency_during_steps_tolerates_an_unfilled_results_file(tmp_path, content):
    """TestConfig.latency_results_file() creates the file empty, and the decorator leaves it that
    way when it fails to collect the results. That must report a step without latencies, not kill
    the test with a JSONDecodeError that hides the failure that actually happened."""
    result, results_file = _check_latency_during_steps(tmp_path, content)

    assert result == {"unthrottled": {"step": "unthrottled", "legend": "", "cycles": []}}
    # nothing was consumed, so the decorator can still fill the file in on the next step
    assert results_file.exists()


def test_check_latency_during_steps_still_processes_collected_results(tmp_path):
    """The happy path is untouched: the results are processed and the file is consumed."""
    collected = {"unthrottled": {"legend": "Gradual test step unthrottled op/s", "cycles": [{"duration": "0:10:00"}]}}

    with (
        patch.object(gradual_grow_module, "calculate_latency", side_effect=lambda results: results) as calculate,
        patch.object(gradual_grow_module, "analyze_hdr_percentiles", side_effect=lambda results: results),
    ):
        result, results_file = _check_latency_during_steps(tmp_path, json.dumps(collected))

    assert result["unthrottled"]["step"] == "unthrottled"
    assert result["unthrottled"]["cycles"] == [{"duration": "0:10:00"}]
    calculate.assert_called_once()
    assert not results_file.exists()


# ---------------------------------------------------------------------------
# run_step: the HDR tags handed to latency_calculator_decorator
# ---------------------------------------------------------------------------


def _run_step(queues_hdr_tags, stress_cmds=None, step_params=None, step_duration=None):
    """Call the production run_step with fake stress threads carrying the given hdr_tags."""
    queues = [SimpleNamespace(hdr_tags=list(tags)) for tags in queues_hdr_tags]
    started = iter(queues)
    instance = SimpleNamespace(
        log=logging.getLogger(__name__),
        run_stress_thread=lambda **_: next(started),
        get_stress_results=lambda queue, store_results: [{"op rate": "1000"}],
        _start_connection_check=lambda stress_cmds: None,
        _connection_check_result=lambda check: None,
    )
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest.run_step(
        instance,
        stress_cmds if stress_cmds is not None else [f"stress-cmd-{i}" for i in range(len(queues))],
        step_params or {},
        step_duration,
    )


@pytest.mark.parametrize(
    "queues_hdr_tags,expected",
    (
        pytest.param([["WRITE-st"]], ["WRITE-st"], id="cassandra-stress-unthrottled"),
        pytest.param([["WRITE-rt"]], ["WRITE-rt"], id="cassandra-stress-throttled"),
        pytest.param([["co-fixed"]], ["co-fixed"], id="scylla-bench"),
        pytest.param([["fn--write", "fn--read"]], ["fn--write", "fn--read"], id="latte"),
    ),
)
def test_run_step_reports_the_tags_of_whatever_stress_tool_ran(queues_hdr_tags, expected):
    """The tags are tool specific and, for cassandra-stress, step specific, so they can only come
    from the stress threads: each derives its own from the command it actually runs."""
    _, decorator_input = _run_step(queues_hdr_tags)
    assert decorator_input == {"hdr_tags": expected}


def test_run_step_merges_the_tags_of_every_stress_queue():
    """A step running a write and a read command must report both, not just the first queue's."""
    _, decorator_input = _run_step([["WRITE-st"], ["READ-st"]])
    assert decorator_input == {"hdr_tags": ["WRITE-st", "READ-st"]}


def test_run_step_deduplicates_repeated_tags():
    """Several commands of the same kind (split by -pop range) all report the same tag."""
    _, decorator_input = _run_step([["WRITE-st"], ["WRITE-st"]])
    assert decorator_input == {"hdr_tags": ["WRITE-st"]}


def test_run_step_skips_a_queue_that_carries_no_tags():
    """Not every stress tool sets 'hdr_tags', and one that does not must not break the step."""
    queues = [SimpleNamespace(), SimpleNamespace(hdr_tags=None), SimpleNamespace(hdr_tags=["WRITE-st"])]
    started = iter(queues)
    instance = SimpleNamespace(
        log=logging.getLogger(__name__),
        run_stress_thread=lambda **_: next(started),
        get_stress_results=lambda queue, store_results: [{"op rate": "1000"}],
        _start_connection_check=lambda stress_cmds: None,
        _connection_check_result=lambda check: None,
    )
    _, decorator_input = gradual_grow_module.PerformanceRegressionPredefinedStepsTest.run_step(
        instance, ["cmd-0", "cmd-1", "cmd-2"], {}, None
    )
    assert decorator_input == {"hdr_tags": ["WRITE-st"]}


def test_latency_decorator_finds_the_tags_run_step_returns():
    """The wiring that broke: what run_step returns must be what _find_hdr_tags picks up."""
    res = _run_step([["WRITE-st"], ["READ-st"]])
    kwargs = {"stress_cmds": ["stress-cmd-0", "stress-cmd-1"], "step_params": {}, "step_duration": None}
    assert _find_hdr_tags(kwargs, res, object()) == ["WRITE-st", "READ-st"]


# ---------------------------------------------------------------------------
# _collect_step_metrics: per-step Prometheus summary
# ---------------------------------------------------------------------------


def _collect_step_metrics(prometheus, start=1000, end=1900):
    instance = SimpleNamespace(
        prometheus_db=prometheus,
        loaders=SimpleNamespace(nodes=[SimpleNamespace(ip_address="10.0.1.1")]),
        log=logging.getLogger(__name__),
    )
    return gradual_grow_module.PerformanceRegressionPredefinedStepsTest._collect_step_metrics(instance, start, end)


def test_collect_step_metrics_stores_every_metric():
    prometheus = MagicMock()
    prometheus.query.return_value = [
        {"metric": {"instance": "10.0.0.1", "shard": "0"}, "values": [[1900, "42.5"]]},
        {"metric": {"instance": "10.0.0.2", "shard": "0"}, "values": [[1900, "NaN"]]},
    ]
    metrics = _collect_step_metrics(prometheus)

    assert metrics.keys() == gradual_grow_module.STEP_METRICS.keys()
    assert all(value == {"instance=10.0.0.1,shard=0": 42.5} for value in metrics.values())
    queries = [call.args[0] for call in prometheus.query.call_args_list]
    assert all("$" not in query for query in queries)
    assert all("[900s]" in query for query in queries)
    assert any('instance=~"10.0.1.1"' in query for query in queries)
    assert all(call.kwargs == {"start": 1900, "end": 1900} for call in prometheus.query.call_args_list)
    # PrometheusDBStats.query doesn't URL-encode, these would reach Prometheus altered
    assert not any(char in query for query in queries for char in "+&#%")


def test_collect_step_metrics_skips_failed_query():
    names = list(gradual_grow_module.STEP_METRICS)
    prometheus = MagicMock()
    prometheus.query.side_effect = [requests.HTTPError("400")] + [[{"metric": {}, "values": [[1900, "1"]]}]] * (
        len(names) - 1
    )
    metrics = _collect_step_metrics(prometheus)

    assert names[0] not in metrics
    assert all(metrics[name] == {"total": 1.0} for name in names[1:])


def test_collect_step_metrics_stops_on_unreachable_prometheus():
    prometheus = MagicMock()
    prometheus.query.side_effect = requests.ConnectionError("refused")

    assert _collect_step_metrics(prometheus) == {}
    assert prometheus.query.call_count == 1


def test_collect_step_metrics_without_monitor():
    assert _collect_step_metrics(None) == {}


# ---------------------------------------------------------------------------
# connection_check_verdict: every loader connection is alive and passing data
# ---------------------------------------------------------------------------

LOADER_IPS = {"10.0.1.1", "10.0.1.2"}


def _ss(byte_counts):
    """`ss -tinH state established` output: a socket line, then a tab-led info line."""
    return "".join(
        f"0      0      10.0.0.1:19042   {address}:{port} \n"
        f"\t cubic rto:204 bytes_sent:{count} bytes_acked:{count} bytes_received:{count} segs_out:10\n"
        for (address, port), count in byte_counts.items()
    )


def _verdict(clients, before, after, expected=2, shards=2):
    return gradual_grow_module.connection_check_verdict(expected, shards, LOADER_IPS, clients, _ss(before), _ss(after))


def test_connection_check_passes_full_shards_with_traffic():
    clients = [
        ("10.0.1.1", 40001, 0, "READY"),
        ("10.0.1.2", 40002, 0, "READY"),
        ("10.0.1.1", 40003, 1, "READY"),
        ("10.0.1.2", 40004, 1, "READY"),
        ("10.0.1.2", 40005, 1, "READY"),  # driver control connection on top of the pool, heartbeats only
        ("10.0.0.5", 50000, 0, "READY"),  # SCT's own session, not a loader
    ]
    before = {(address, port): 1000 for address, port, *_ in clients}
    after = {peer: 2000 for peer in before} | {("10.0.1.2", 40005): 1010, ("10.0.0.5", 50000): 1000}
    # IPv4-mapped peer as ss prints it when Scylla listens on ::
    before["[::ffff:10.0.1.2]", 40004] = before.pop(("10.0.1.2", 40004))
    after["[::ffff:10.0.1.2]", 40004] = after.pop(("10.0.1.2", 40004))

    verdict = _verdict(clients, before, after)

    assert verdict[0] == {"expected": 2, "ready": 2, "with_traffic": 2, "ok": True, "idle": []}
    assert verdict[1] == {"expected": 2, "ready": 3, "with_traffic": 2, "ok": True, "idle": ["10.0.1.2:40005"]}


def test_connection_check_fails_shard_below_expected_count():
    clients = [
        ("10.0.1.1", 40001, 0, "READY"),
        ("10.0.1.2", 40002, 0, "READY"),
        ("10.0.1.1", 40003, 1, "READY"),
        ("10.0.1.2", 40004, 1, "STARTUP"),
    ]
    before = {(address, port): 0 for address, port, *_ in clients}
    after = {peer: 5000 for peer in before}

    verdict = _verdict(clients, before, after, shards=3)

    assert verdict[0]["ok"]
    assert verdict[1] == {"expected": 2, "ready": 1, "with_traffic": 1, "ok": False, "idle": []}
    assert verdict[2] == {"expected": 2, "ready": 0, "with_traffic": 0, "ok": False, "idle": []}


def test_connection_check_fails_heartbeat_only_connection():
    clients = [("10.0.1.1", 40001, 0, "READY"), ("10.0.1.2", 40002, 0, "READY")]
    before = {("10.0.1.1", 40001): 1000, ("10.0.1.2", 40002): 1000}
    after = {("10.0.1.1", 40001): 9000, ("10.0.1.2", 40002): 1020}

    verdict = _verdict(clients, before, after, shards=1)

    assert verdict[0] == {"expected": 2, "ready": 2, "with_traffic": 1, "ok": False, "idle": ["10.0.1.2:40002"]}


def test_connection_check_is_off_without_connections_per_shard():
    check = gradual_grow_module.PerformanceRegressionPredefinedStepsTest._start_connection_check(
        SimpleNamespace(), ["cassandra-stress write cl=QUORUM duration=1m -rate threads=10"]
    )
    assert check is None


def test_check_connections_records_failed_node(monkeypatch):
    monkeypatch.setattr(gradual_grow_module.time, "sleep", lambda seconds: None)
    peer = ("10.0.1.1", 40001)
    samples = iter([_ss({peer: 0}), _ss({peer: 5000})])
    good = MagicMock(scylla_shards=1)
    good.name = "db-1"
    good.remoter.run.side_effect = lambda *args, **kwargs: SimpleNamespace(stdout=next(samples))
    broken = MagicMock(scylla_shards=1)
    broken.name = "db-2"
    broken.remoter.run.side_effect = ConnectionError("ssh down")
    db_cluster = MagicMock(nodes=[good, broken])
    session = db_cluster.cql_connection_patient_exclusive.return_value.__enter__.return_value
    session.execute.return_value = [(*peer, 0, "READY")]
    instance = SimpleNamespace(
        db_cluster=db_cluster, loaders=SimpleNamespace(nodes=[SimpleNamespace(ip_address="10.0.1.1")])
    )

    result = gradual_grow_module.PerformanceRegressionPredefinedStepsTest._check_connections(instance, expected=1)

    assert result["nodes"]["db-1"] == {0: {"expected": 1, "ready": 1, "with_traffic": 1, "ok": True, "idle": []}}
    assert "ssh down" in result["nodes"]["db-2"]["error"]
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# meets_p99 / search_max_rate: bisect the throttle rate under the P99 target
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "client_p99, server_p99, passed",
    [
        (12.0, 11.0, False),  # both over the target
        (12.0, 9.0, True),  # client over, server under
        (9.0, 12.0, True),  # client under, server over
        (12.0, None, False),  # server unknown, the client decides
    ],
)
def test_meets_p99_fails_only_when_both_are_over(client_p99, server_p99, passed):
    assert gradual_grow_module.meets_p99(client_p99, server_p99, target=10) is passed


def test_search_max_rate_brackets_the_p99_crossing():
    crossing = 87_300  # P99 is 10 ms here and grows with the rate
    probed = []

    def probe(rate):
        probed.append(rate)
        return gradual_grow_module.meets_p99(10 * (rate / crossing) ** 4, None, target=10)

    low, high = gradual_grow_module.search_max_rate(probe, low=50_000, high=150_000, tolerance=5_000)

    assert low <= crossing < high
    assert high - low <= 5_000
    assert probed[:2] == [150_000, 50_000]
    assert len(probed) <= 2 + 5  # bracket ends + ceil(log2(100k / 5k))


@pytest.mark.parametrize(
    "passing_rates, error",
    [
        ({50_000, 150_000}, "high end 150000 passes"),
        (set(), "low end 50000 fails"),
    ],
)
def test_search_max_rate_rejects_a_wrong_bracket(passing_rates, error):
    with pytest.raises(ValueError, match=rf"{error}.*\[50000, 150000\]"):
        gradual_grow_module.search_max_rate(lambda rate: rate in passing_rates, 50_000, 150_000, 5_000)

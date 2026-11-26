import re
import urllib.parse
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import performance_regression_gradual_grow_throughput as steps_test
from sdcm.db_stats import PrometheusDBStats
from sdcm.utils import perf_step_metrics
from sdcm.utils.perf_step_metrics import (
    CPU_QUERY,
    DISK_QUEUE_DEPTH_QUERY,
    DISK_READ_LATENCY_QUERY,
    DISK_READ_MBPS_QUERY,
    DISK_WRITE_IOPS_QUERY,
    PROMETHEUS_REQUEST_TIMEOUT_SEC,
    REACTOR_UTILIZATION_QUERY,
    STEP_DISK_METRICS,
    STEP_TAIL_SEC,
    StepResourceUsage,
    StepWindow,
    collect_step_metrics,
    instance_host,
    roles_by_address,
    series_average,
    step_disk_usage_table,
    step_resource_usage_table,
    step_window,
    summarize_by_role,
)


def node(ip, private_ip=None):
    return SimpleNamespace(name=f"node-{ip}", ip_address=ip, private_ip_address=private_ip or ip)


def series(instance, *values):
    return {"metric": {"instance": instance}, "values": [[1000 + i, str(value)] for i, value in enumerate(values)]}


class NodeWithoutExternalAddress:
    name = "loader-1"
    ip_address = "10.0.1.1"

    @property
    def external_address(self):
        raise RuntimeError("no public address")


class FakePrometheus:
    def __init__(self, results, failing=()):
        self.results = results
        self.failing = failing
        self.calls = []

    def query(self, query, start, end):
        query = urllib.parse.unquote(query)  # the query reaches the URL quoted, as PrometheusDBStats needs
        self.calls.append((query, start, end))
        template = re.sub(r"\[\d+s\]", "[${window}s]", query)
        if template in self.failing:
            raise ConnectionError("monitor is down")
        return self.results.get(template, [])


@pytest.mark.parametrize(
    "start, end, warmup_sec, expected",
    [
        pytest.param(0, 1800, 180, StepWindow(180, 1800 - STEP_TAIL_SEC, True), id="long-step-cut"),
        pytest.param(0, 2 * 180 + STEP_TAIL_SEC, 180, StepWindow(0, 420, False), id="exactly-too-short-whole"),
        pytest.param(0, 2 * 180 + STEP_TAIL_SEC + 1, 180, StepWindow(180, 361, True), id="just-long-enough-cut"),
        pytest.param(0, 300, 180, StepWindow(0, 300, False), id="short-step-whole"),
        pytest.param(0, 1800, 0, StepWindow(0, 1800, False), id="warmup-disabled"),
    ],
)
def test_step_window(start, end, warmup_sec, expected):
    assert step_window(start=start, end=end, warmup_sec=warmup_sec) == expected


def test_step_window_keeps_at_least_warmup_after_cut():
    window = step_window(start=0, end=2 * 180 + STEP_TAIL_SEC + 1, warmup_sec=180)
    assert window.duration > 180


@pytest.mark.parametrize(
    "instance, expected",
    [
        ("10.0.0.1:9100", "10.0.0.1"),
        ("10.0.0.1", "10.0.0.1"),
        ("[fd00::1]:9100", "fd00::1"),
        ("fd00::1", "fd00::1"),
    ],
)
def test_instance_host(instance, expected):
    assert instance_host(instance) == expected


def test_roles_by_address_uses_every_node_address():
    roles = roles_by_address(db_nodes=[node("10.0.0.1", "172.16.0.1")], loader_nodes=[node("10.0.1.1")])
    assert roles == {"10.0.0.1": "db", "172.16.0.1": "db", "10.0.1.1": "loader"}


def test_roles_by_address_skips_a_failing_address():
    assert roles_by_address(db_nodes=[], loader_nodes=[NodeWithoutExternalAddress()]) == {"10.0.1.1": "loader"}


def test_series_average_ignores_nan_samples():
    assert series_average([[1, "10"], [2, "NaN"], [3, "20"]]) == 15
    assert series_average([[1, "NaN"]]) is None


def test_summarize_by_role_gives_mean_and_hottest_node_average():
    roles = {"10.0.0.1": "db", "10.0.0.2": "db", "10.0.1.1": "loader"}
    data = [
        series("10.0.0.1:9100", 40, 60),  # average 50
        series("10.0.0.2:9100", 90, 90),  # average 90: the hottest node, no single spike above it
        series("10.0.1.1:9100", 100, 100),  # a loader, not counted for the DB nodes
        series("10.9.9.9:9100", 100, 100),  # the monitor, no role
    ]
    assert summarize_by_role(data, roles, "db") == (70, 90)
    assert summarize_by_role(data, roles, "loader") == (100, 100)
    assert summarize_by_role(data, roles, "monitor") is None


def test_collect_step_metrics_queries_each_query_once_over_the_window():
    roles = {"10.0.0.1": "db", "10.0.1.1": "loader", "10.0.1.2": "loader"}
    prometheus = FakePrometheus(
        {
            CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30), series("10.0.1.2:9100", 50)],
            REACTOR_UTILIZATION_QUERY: [series("10.0.0.1", 97.04)],
        }
    )
    window = StepWindow(start=100, end=200, trimmed=True)

    metrics, failures = collect_step_metrics(prometheus=prometheus, window=window, roles=roles)

    assert failures == {}
    assert metrics == {
        "DB CPU avg": 80,
        "DB CPU max": 80,
        "DB reactor utilization avg": 97.0,
        "DB reactor utilization max": 97.0,
        "Loader CPU avg": 40,
        "Loader CPU max": 50,
    }
    # Each query once, at the window's end, over the whole window
    assert sorted(prometheus.calls) == sorted(
        [
            (CPU_QUERY.replace("${window}", "100"), 200, 200),
            (REACTOR_UTILIZATION_QUERY.replace("${window}", "100"), 200, 200),
        ]
    )
    assert all("[100s]" in query for query, _, _ in prometheus.calls)


def test_collect_step_metrics_keeps_the_other_metrics_when_one_query_fails():
    roles = {"10.0.0.1": "db", "10.0.1.1": "loader"}
    prometheus = FakePrometheus(
        {CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30)]},
        failing=(REACTOR_UTILIZATION_QUERY,),
    )

    metrics, failures = collect_step_metrics(prometheus=prometheus, window=StepWindow(0, 100, False), roles=roles)

    assert metrics == {"DB CPU avg": 80, "DB CPU max": 80, "Loader CPU avg": 30, "Loader CPU max": 30}
    assert list(failures) == ["DB reactor utilization"]
    assert isinstance(failures["DB reactor utilization"], ConnectionError)


def test_disk_queries_use_only_the_scylla_data_device():
    for metric in STEP_DISK_METRICS:
        assert metric.role == "db"
        assert '{mountpoint="/var/lib/scylla"}' in metric.query
        assert metric.query.count("rate(") == metric.query.count("on (instance, device) group_left ()")


def test_collect_step_metrics_sends_the_queries_url_encoded():
    prometheus = MagicMock()
    prometheus.query.return_value = []

    collect_step_metrics(prometheus=prometheus, window=StepWindow(0, 600, True), roles={}, metrics=STEP_DISK_METRICS)

    for call in prometheus.query.call_args_list:
        query = call.args[0]
        # A raw '+' (in '+ 1' and '/dev/(.+)') is a space in a URL: Prometheus answered 400 Bad Request
        assert "+" not in query and " " not in query
        assert urllib.parse.unquote(query) in {metric.query.replace("${window}", "600") for metric in STEP_DISK_METRICS}


def test_collect_step_metrics_rounds_each_disk_metric_by_its_digits():
    roles = {"10.0.0.1": "db", "10.0.0.2": "db", "10.0.1.1": "loader"}
    prometheus = FakePrometheus(
        {
            DISK_READ_MBPS_QUERY: [series("10.0.0.1:9100", 900.04), series("10.0.0.2:9100", 950.06)],
            DISK_WRITE_IOPS_QUERY: [series("10.0.0.1:9100", 14321.6), series("10.0.0.2:9100", 14800.2)],
            DISK_QUEUE_DEPTH_QUERY: [series("10.0.0.1:9100", 2.914), series("10.0.0.2:9100", 3.006)],
            # A node without reads in the window: 0/0, NaN, left out of the mean
            DISK_READ_LATENCY_QUERY: [series("10.0.0.1:9100", 0.2034), series("10.0.0.2:9100", "NaN")],
        }
    )

    metrics, failures = collect_step_metrics(
        prometheus=prometheus, window=StepWindow(0, 600, True), roles=roles, metrics=STEP_DISK_METRICS
    )

    assert failures == {}
    assert metrics == {
        "Disk read avg": 925.0,
        "Disk read max": 950.1,
        "Disk write IOPS avg": 14561.0,
        "Disk write IOPS max": 14800.0,
        "Disk queue depth avg": 2.96,
        "Disk queue depth max": 3.01,
        "Disk read latency avg": 0.203,
        "Disk read latency max": 0.203,
    }
    # Only the disk queries, each over the whole window
    assert len(prometheus.calls) == len(STEP_DISK_METRICS)
    assert all("[600s]" in query and "${window}" not in query for query, _, _ in prometheus.calls)


def test_step_disk_usage_table_has_only_its_columns_and_the_steps_with_disk_data():
    table = step_disk_usage_table(
        workload="write",
        steps=[
            StepResourceUsage(step="150000", window=StepWindow(0, 1500, True), metrics={"DB CPU avg": 40.0}),
            StepResourceUsage(
                step="unthrottled",
                window=StepWindow(3600, 4200, False),
                metrics={"DB CPU avg": 97.0, "Disk write avg": 2550.3, "Disk write latency max": 0.131},
            ),
        ],
    )

    assert table.name == "write - Step disk usage"
    cells = {(result.column, result.row): result.value for result in table.results}
    assert cells == {
        ("Disk write avg", "unthrottled"): 2550.3,
        ("Disk write latency max", "unthrottled"): 0.131,
        ("window start", "unthrottled"): "01:00:00",
        ("window duration", "unthrottled"): 600,
    }
    units = {column.name: column.unit for column in table.columns}
    assert units["Disk write avg"] == "MB/s"
    assert units["Disk write IOPS max"] == "ops/s"
    assert units["Disk write latency avg"] == "ms"


def test_step_resource_usage_table_has_one_row_per_step():
    table = step_resource_usage_table(
        workload="mixed",
        steps=[
            StepResourceUsage(
                step="150000", window=StepWindow(start=0, end=1500, trimmed=True), metrics={"DB CPU avg": 40.0}
            ),
            StepResourceUsage(
                step="unthrottled",
                window=StepWindow(start=3600, end=4200, trimmed=False),
                metrics={"DB CPU avg": 80.0, "Loader CPU max": 50.0},
            ),
        ],
    )

    assert table.name == "mixed - Step resource usage"
    cells = {(result.column, result.row): result.value for result in table.results}
    assert cells == {
        ("DB CPU avg", "150000"): 40.0,
        ("window start", "150000"): "00:00:00",
        ("window duration", "150000"): 1500,
        ("DB CPU avg", "unthrottled"): 80.0,
        ("Loader CPU max", "unthrottled"): 50.0,
        ("window start", "unthrottled"): "01:00:00",
        ("window duration", "unthrottled"): 600,
    }


def fake_test(monitors):
    return SimpleNamespace(
        log=MagicMock(),
        params={"perf_steps_metrics_warmup_sec": 180},
        monitors=monitors,
        db_cluster=SimpleNamespace(nodes=[node("10.0.0.1")]),
        loaders=SimpleNamespace(nodes=[node("10.0.1.1")]),
        test_config=SimpleNamespace(argus_client=MagicMock()),
        _steps_resource_usage=[],
    )


def report(test, step="unthrottled"):
    steps_test.PerformanceRegressionPredefinedStepsTest.report_step_resource_usage(
        test, workload_type="mixed", step=step, start=0, end=1800
    )


def test_report_step_resource_usage_without_monitor_does_nothing(monkeypatch):
    prometheus = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.PrometheusDBStats", prometheus)

    report(fake_test(monitors=None))

    prometheus.assert_not_called()


def test_report_step_resource_usage_sends_the_step_row(monkeypatch):
    prometheus = FakePrometheus({CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30)]})
    monkeypatch.setattr(
        "performance_regression_gradual_grow_throughput.PrometheusDBStats", lambda host, **kwargs: prometheus
    )
    submit = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", submit)

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    window_end = 1800 - STEP_TAIL_SEC
    assert {call[1:] for call in prometheus.calls} == {(window_end, window_end)}
    assert all(f"[{window_end - 180}s]" in call[0] for call in prometheus.calls)
    table = submit.call_args.kwargs["result_table"]
    assert {(result.column, result.value) for result in table.results} >= {("DB CPU avg", 80), ("Loader CPU avg", 30)}


def test_report_step_resource_usage_sends_the_rows_of_all_steps_so_far(monkeypatch):
    prometheus = FakePrometheus({CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30)]})
    monkeypatch.setattr(
        "performance_regression_gradual_grow_throughput.PrometheusDBStats", lambda host, **kwargs: prometheus
    )
    submit = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", submit)
    test = fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")]))

    report(test, step="150000")
    report(test, step="unthrottled")

    # The last send has both rows: Argus rewrites them, and the table goes below the latest step's table
    sent_rows = [{result.row for result in call.kwargs["result_table"].results} for call in submit.call_args_list]
    assert sent_rows == [{"150000"}, {"150000", "unthrottled"}]


def test_report_step_resource_usage_sends_the_disk_table_after_the_resource_table(monkeypatch):
    prometheus = FakePrometheus(
        {
            CPU_QUERY: [series("10.0.0.1:9100", 97), series("10.0.1.1:9100", 30)],
            DISK_WRITE_IOPS_QUERY: [series("10.0.0.1:9100", 20000)],
        }
    )
    monkeypatch.setattr(
        "performance_regression_gradual_grow_throughput.PrometheusDBStats", lambda host, **kwargs: prometheus
    )
    submit = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", submit)

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    # The disk table is sent last, so Argus shows it at the bottom
    tables = [call.kwargs["result_table"] for call in submit.call_args_list]
    assert [table.name for table in tables] == ["mixed - Step resource usage", "mixed - Step disk usage"]
    assert ("Disk write IOPS avg", 20000) in {(result.column, result.value) for result in tables[1].results}
    assert not any(result.column.startswith("Disk") for result in tables[0].results)


def test_report_step_resource_usage_without_disk_data_sends_no_disk_table(monkeypatch):
    prometheus = FakePrometheus({CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30)]})
    monkeypatch.setattr(
        "performance_regression_gradual_grow_throughput.PrometheusDBStats", lambda host, **kwargs: prometheus
    )
    submit = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", submit)

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    assert [call.kwargs["result_table"].name for call in submit.call_args_list] == ["mixed - Step resource usage"]


def test_report_step_resource_usage_bounds_the_prometheus_requests(monkeypatch):
    prometheus = MagicMock(return_value=FakePrometheus({}))
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.PrometheusDBStats", prometheus)
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", MagicMock())

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    assert prometheus.call_args.kwargs["request_timeout"] == PROMETHEUS_REQUEST_TIMEOUT_SEC


def test_report_step_resource_usage_reports_a_failed_query_and_sends_the_others(monkeypatch):
    prometheus = FakePrometheus(
        {CPU_QUERY: [series("10.0.0.1:9100", 80), series("10.0.1.1:9100", 30)]},
        failing=(REACTOR_UTILIZATION_QUERY,),
    )
    monkeypatch.setattr(
        "performance_regression_gradual_grow_throughput.PrometheusDBStats", lambda host, **kwargs: prometheus
    )
    submit = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.submit_results_to_argus", submit)
    event = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.TestFrameworkEvent", event)

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    assert event.call_count == 1
    assert "DB reactor utilization" in event.call_args.kwargs["message"]
    assert event.call_args.kwargs["severity"] == steps_test.Severity.WARNING
    columns = {result.column for result in submit.call_args.kwargs["result_table"].results}
    assert {"DB CPU avg", "Loader CPU avg"} <= columns
    assert not any(column.startswith("DB reactor utilization") for column in columns)


def test_report_step_resource_usage_never_fails_the_test(monkeypatch):
    def broken_prometheus(host):
        raise ConnectionError("monitor is down")

    monkeypatch.setattr("performance_regression_gradual_grow_throughput.PrometheusDBStats", broken_prometheus)
    event = MagicMock()
    monkeypatch.setattr("performance_regression_gradual_grow_throughput.TestFrameworkEvent", event)

    report(fake_test(monitors=SimpleNamespace(nodes=[SimpleNamespace(external_address="10.9.9.9")])))

    assert event.call_args.kwargs["source_method"] == "report_step_resource_usage"
    event.return_value.publish_or_dump.assert_called_once()


@pytest.mark.parametrize(
    "table_class, metrics",
    [
        (perf_step_metrics.PerfStepResourceUsageResult, perf_step_metrics.STEP_METRICS),
        (perf_step_metrics.PerfStepDiskUsageResult, perf_step_metrics.STEP_DISK_METRICS),
    ],
)
def test_step_metrics_columns_match_the_table(table_class, metrics):
    columns = {column.name for column in table_class().columns}
    assert columns == {f"{metric.column} {stat}" for metric in metrics for stat in ("avg", "max")} | {
        "window start",
        "window duration",
    }


def test_run_step_records_the_load_time_without_the_decorator_work(monkeypatch):
    clock = iter([100.0, 1900.0])
    monkeypatch.setattr(steps_test.time, "time", lambda: next(clock))
    test = SimpleNamespace(
        log=MagicMock(),
        run_stress_thread=MagicMock(return_value=SimpleNamespace(hdr_tags=["WRITE-rt"])),
        get_stress_results=MagicMock(return_value=[{"op rate": "1000"}]),
    )

    steps_test.PerformanceRegressionPredefinedStepsTest.run_step(
        test, stress_cmds=["cassandra-stress write duration=$duration"], step_params={}, step_duration="30m"
    )

    assert (test._step_load_start, test._step_load_end) == (100.0, 1900.0)


def test_run_step_records_the_load_end_of_a_failed_load(monkeypatch):
    clock = iter([100.0, 700.0])
    monkeypatch.setattr(steps_test.time, "time", lambda: next(clock))
    test = SimpleNamespace(
        log=MagicMock(),
        run_stress_thread=MagicMock(return_value=SimpleNamespace(hdr_tags=["WRITE-rt"])),
        get_stress_results=MagicMock(side_effect=RuntimeError("stress failed")),
    )

    with pytest.raises(RuntimeError, match="stress failed"):
        steps_test.PerformanceRegressionPredefinedStepsTest.run_step(
            test, stress_cmds=["cassandra-stress write duration=$duration"], step_params={}, step_duration="30m"
        )

    # Set before the decorator's post-processing, not left for the caller to take after it
    assert (test._step_load_start, test._step_load_end) == (100.0, 700.0)


@pytest.mark.parametrize("request_timeout", [None, 30])
def test_prometheus_requests_use_the_request_timeout(monkeypatch, request_timeout):
    session = MagicMock()
    session.get.return_value.content = b'{"status": "success", "data": {}}'
    monkeypatch.setattr(PrometheusDBStats, "_create_session", staticmethod(lambda: session))
    monkeypatch.setattr(PrometheusDBStats, "get_configuration", lambda self: {})

    PrometheusDBStats(host="10.9.9.9", request_timeout=request_timeout).request(url="http://10.9.9.9:9090/api")

    assert session.get.call_args.kwargs["timeout"] == request_timeout

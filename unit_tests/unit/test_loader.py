import time

import prometheus_client

from sdcm.loader import CqlStressCassandraStressExporter

CQL_STRESS_LOG = """\
Keyspace: keyspace1
total ops ,    op/s,    mean,     med,     .95,     .99,    .999,     max,   time, errors
    30645,   30602,     0.3,     0.2,     0.7,     1.1,     1.7,     2.4,    1.0,      0
Operation failed, consistency: One, required: 1, alive: 0
    62965,   32336,     0.4,     0.3,     0.6,     0.7,     0.9,     1.3,    2.0,      0
"""


class RegistryMetrics:
    """Creates the exporter gauges in a private registry, so tests don't share the global one."""

    def __init__(self):
        self.registry = prometheus_client.CollectorRegistry()

    def create_gauge(self, name, desc, param_list):
        """Return a Gauge bound to this registry instead of the global one."""
        return prometheus_client.Gauge(name, desc, param_list, registry=self.registry)


def test_cql_stress_exporter_skips_error_line_between_stats_rows(tmp_path, monkeypatch):
    """An error line with commas after the stats header must not be parsed as a stats row (SCT-1511)."""
    # The gauge cache is class-level; a cached gauge would live in another test's registry.
    monkeypatch.setattr(CqlStressCassandraStressExporter, "METRICS_GAUGES", {})
    log_file = tmp_path / "cql-stress.log"
    log_file.write_text(CQL_STRESS_LOG)
    metrics = RegistryMetrics()
    exporter = CqlStressCassandraStressExporter(
        instance_name="loader-1",
        metrics=metrics,
        stress_operation="sct1511_write",
        stress_log_filename=str(log_file),
        loader_idx=1,
    )
    labels = {
        "cql_stress_cassandra_stress_sct1511_write": "0",
        "instance": "loader-1",
        "loader_idx": "1",
        "cpu_idx": "1",
        "keyspace": "keyspace1",
    }

    def metric(name):
        return metrics.registry.get_sample_value(
            "sct_cql_stress_cassandra_stress_sct1511_write_gauge", labels | {"type": name}
        )

    future = exporter.start()
    deadline = time.monotonic() + 10
    while metric("ops") != 32336 and not future.done() and time.monotonic() < deadline:
        time.sleep(0.1)
    exporter.stop()
    future.result(timeout=10)

    assert metric("ops") == 32336
    assert metric("lat_mean") == 0.4
    assert metric("lat_max") == 1.3
    assert metric("errors") == 0

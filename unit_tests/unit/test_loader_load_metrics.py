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

"""Unit tests for the per-step loader load reported next to the latencies (SCT-601)."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.argus_results import LOADER_LOAD_COLUMN_NAMES, workload_to_table
from sdcm.utils.latency import collect_loader_load


class FakePrometheus:
    """Answers the loader load queries with canned series, recording what was asked."""

    def __init__(self, values_by_metric=None, raises=False):
        self.values_by_metric = values_by_metric or {}
        self.raises = raises
        self.queries = []

    def query(self, query, start, end):  # noqa: ARG002
        self.queries.append(query)
        if self.raises:
            raise RuntimeError("prometheus is down")
        for metric, series in self.values_by_metric.items():
            if metric in query:
                return series
        return []


def _loader(ip_address, private_ip_address=None):
    node = MagicMock()
    node.ip_address = ip_address
    node.private_ip_address = private_ip_address or ip_address
    node.public_ip_address = None
    return node


def _series(*values):
    return [{"metric": {"instance": "10.0.0.1:9100"}, "values": [[0, str(value)] for value in values]}]


def _collect(prometheus, loaders):
    with patch("sdcm.utils.latency.PrometheusDBStats", return_value=prometheus):
        return collect_loader_load(MagicMock(), start=1, end=2, loader_nodes=loaders)


def test_collect_loader_load_reports_the_peak_of_the_window():
    prometheus = FakePrometheus(
        {
            'mode="idle"': _series(41.5, 89.64, 55.0),
            'mode="steal"': _series(0.0, 3.25),
            "node_pressure_cpu_waiting_seconds_total": _series(1.0, 17.9),
            "node_load1": _series(56.08, 42.0),
        }
    )

    result = _collect(prometheus, [_loader("10.0.0.1")])

    assert result == {
        "Loader CPU busy max": 89.64,
        "Loader CPU steal max": 3.25,
        "Loader CPU pressure max": 17.9,
        "Loader load1 max": 56.08,
    }


def test_collect_loader_load_omits_metrics_without_data():
    """A metric with no data must not be reported as 0 - that would read as an idle loader."""
    prometheus = FakePrometheus({"node_load1": _series(12.0)})

    result = _collect(prometheus, [_loader("10.0.0.1")])

    assert result == {"Loader load1 max": 12.0}


def test_collect_loader_load_ignores_nan_samples():
    prometheus = FakePrometheus({"node_load1": [{"metric": {}, "values": [[0, "NaN"], [1, "7.5"]]}]})

    assert _collect(prometheus, [_loader("10.0.0.1")]) == {"Loader load1 max": 7.5}


def test_collect_loader_load_queries_only_the_loader_instances():
    prometheus = FakePrometheus()

    _collect(prometheus, [_loader("10.0.0.1", "10.0.0.2")])

    assert prometheus.queries, "no query was sent"
    for query in prometheus.queries:
        assert "10\\.0\\.0\\.1:9100" in query or "10.0.0.1:9100" in query
        assert "10.0.0.2:9100" in query.replace("\\", "")
        # the node_exporter job holds the DB nodes as well, they must not be mixed in
        assert "10.0.0.9" not in query


def test_collect_loader_load_does_not_repeat_an_address_a_loader_has_twice():
    """A loader whose private and public address are the same must appear once in the filter."""
    prometheus = FakePrometheus()

    _collect(prometheus, [_loader("10.0.0.1", "10.0.0.1")])

    # the instances are regex escaped in the query
    assert prometheus.queries[0].replace("\\", "").count("10.0.0.1:9100") == 1


def test_collect_loader_load_without_loaders_does_not_query():
    prometheus = FakePrometheus()

    assert _collect(prometheus, []) == {}
    assert not prometheus.queries


def test_collect_loader_load_survives_a_prometheus_failure():
    """Diagnostics must never break the latency reporting of a step."""
    assert _collect(FakePrometheus(raises=True), [_loader("10.0.0.1")]) == {}


@pytest.mark.parametrize("workload", sorted(workload_to_table))
def test_loader_load_columns_are_declared_in_every_latency_table(workload):
    """An undeclared column is silently dropped, so the figures would never reach Argus."""
    declared = [column.name for column in workload_to_table[workload].Meta.Columns]

    assert set(LOADER_LOAD_COLUMN_NAMES) <= set(declared)

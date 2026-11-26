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

import logging
from unittest.mock import ANY, MagicMock

import pytest

from sdcm.argus_results import (
    MigratorBenchmarkResult,
    send_migrator_results_to_argus,
)

EXPECTED_BENCHMARK_COLUMNS = {
    "P99 write max",
    "P99 write avg",
    "Throughput max",
    "Throughput avg",
    "Data size",
    "Speed",
    "duration",
    "start time",
}


@pytest.fixture
def argus_client():
    """Mock Argus client whose submit_results calls are recorded."""
    return MagicMock()


@pytest.fixture
def sample_metrics():
    """Spark-migrator metrics dict with one value per benchmark column."""
    return {
        "P99 write max": 12.5,
        "P99 write avg": 8.3,
        "Throughput max": 50_000,
        "Throughput avg": 30_000,
        "Data size": 100.0,
        "Speed": 170.6,
        "duration": 600.0,
        "start time": "2026-05-15 10:00:00 UTC",
    }


def test_migrator_benchmark_result_schema():
    """MigratorBenchmarkResult declares the expected columns and table name."""
    table = MigratorBenchmarkResult()
    assert {col.name for col in table.columns} == EXPECTED_BENCHMARK_COLUMNS
    assert table.name == "spark-migrator - benchmark"
    assert table.description


def test_send_migrator_results_submits_benchmark_table(argus_client, sample_metrics):
    """send_migrator_results_to_argus submits one benchmark table carrying every metric value."""
    send_migrator_results_to_argus(argus_client, sample_metrics)

    # the table is a fresh object without __eq__; its type and cells are checked below
    argus_client.submit_results.assert_called_once_with(ANY)
    submitted_result = argus_client.submit_results.call_args.args[0]
    assert isinstance(submitted_result, MigratorBenchmarkResult)

    assert {cell.row for cell in submitted_result.results} == {"#1"}
    values = {cell.column: cell.value for cell in submitted_result.results}
    assert values["P99 write max"] == pytest.approx(12.5)
    assert values["P99 write avg"] == pytest.approx(8.3)
    assert values["Throughput max"] == 50_000
    assert values["Throughput avg"] == 30_000
    assert values["Data size"] == pytest.approx(100.0)
    assert values["Speed"] == pytest.approx(170.6)
    assert values["duration"] == pytest.approx(600.0)
    assert values["start time"] == "2026-05-15 10:00:00 UTC"


def test_send_migrator_results_skips_when_empty(argus_client):
    """An empty metrics dict submits nothing to Argus."""
    send_migrator_results_to_argus(argus_client, {})

    argus_client.submit_results.assert_not_called()


def test_send_migrator_results_without_client_returns_quietly(sample_metrics, caplog):
    """A missing Argus client logs a warning and does not raise."""
    with caplog.at_level(logging.WARNING, logger="sdcm.argus_results"):
        send_migrator_results_to_argus(None, sample_metrics)
    assert any("no client initialized" in rec.message for rec in caplog.records)

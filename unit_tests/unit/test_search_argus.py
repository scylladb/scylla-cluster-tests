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

"""Unit tests for sdcm/utils/search_argus.py: results records in, Argus tables out."""

import dataclasses

import pytest

from argus.client.generic_result import ResultType
from fts_test import FTS_WORKLOAD
from sdcm.utils.search_argus import SearchArgusReporter

WORKLOAD = dataclasses.replace(
    FTS_WORKLOAD, name="search_bench", description="Search", table_prefix="SB", record_label="document count"
)

BUILD = {
    "workload": "search_bench",
    "phase": "index_build",
    "dataset": "10M_20tok",
    "step": 2,
    "index": "idx",
    "record_count": 2000000,
    "build_time_s": 40.0,
    "indexing_throughput": 50000.0,
}

SEARCH = {
    "workload": "search_bench",
    "phase": "search",
    "dataset": "10M_20tok",
    "step": 1,
    "record_count": 1000000,
    "query_index": 1,
    "query_set": "term_common",
    "limit": 10,
    "stress_options": "--concurrency 8",
    "rate": 0,
    "duration": "5m",
    "qrels": False,
    "query_example": "zephyr obsidian",
    "expected_p99_read_ms": 20.0,
    "p50_ms": 1.0,
    "p90_ms": 2.0,
    "p95_ms": 3.0,
    "p99_ms": 4.0,
    "p99_9_ms": 5.0,
    "p99_99_ms": 6.0,
    "throughput_ops": 1234.5,
    "p99_within_expectation": True,
}

TRACKED_SEARCH = {**SEARCH, "track": "throughput", "label": "10M term_common"}
TRACKED_BUILD = {**BUILD, "track": "index build", "label": "10M_20tok"}


class _ArgusClient:
    def __init__(self):
        self.submitted = []

    def submit_results(self, table):
        self.submitted.append(table)


def _report(*records):
    client = _ArgusClient()
    reporter = SearchArgusReporter(WORKLOAD, client)
    for record in records:
        reporter.report(record)
    return client.submitted


def _cells(table):
    return {(cell.row, cell.column): cell.value for cell in table.results}


def _graphed(table):
    """Argus graphs every numeric column."""
    return [column.name for column in table.columns if column.type != ResultType.TEXT]


# ---------------------------------------------------------------------------
# The logs: every record, in one of two tables by kind, nothing graphed
# ---------------------------------------------------------------------------


def test_an_untracked_record_goes_only_to_its_log():
    tables = _report(SEARCH, BUILD)

    assert [table.name for table in tables] == ["SB searches", "SB index builds"]


def test_the_logs_graph_nothing():
    """They are the run's results to read, not a history: an exploratory entry costs no graph."""
    search, build = _report(SEARCH, BUILD)

    assert _graphed(search) == _graphed(build) == []


def test_each_log_has_only_its_own_columns():
    search, build = _report(SEARCH, BUILD)

    assert "build time" not in {column.name for column in search.columns}
    assert "P99" not in {column.name for column in build.columns}


def test_a_search_in_its_log():
    (table,) = _report(SEARCH)

    row = "#01 term_common"
    assert _cells(table) == {
        (row, "dataset"): "10M_20tok",
        (row, "step"): "1",
        (row, "document count"): "1,000,000",
        (row, "query set"): "term_common",
        (row, "limit"): "10",
        (row, "stress options"): "--concurrency 8",
        (row, "rate"): "unthrottled",
        (row, "P50"): "1",
        (row, "P90"): "2",
        (row, "P99"): "4",
        (row, "P99.9"): "5",
        (row, "throughput"): "1,234.5",
        (row, "P99 limit"): "20",
    }


def test_an_index_build_in_its_log():
    (table,) = _report(TRACKED_BUILD)[:1]

    row = "#01 index build"
    assert _cells(table) == {
        (row, "dataset"): "10M_20tok",
        (row, "step"): "2",
        (row, "document count"): "2,000,000",
        (row, "build time"): "40",
        (row, "indexing throughput"): "50,000",
        (row, "track"): "index build",
    }


def test_overview_rows_keep_the_run_order_and_a_repeated_configuration_apart():
    tables = _report(BUILD, SEARCH, {**SEARCH, "query_index": 2})

    assert [next(iter(table.results)).row.split(" ")[0] for table in tables] == ["#01", "#02", "#03"]


def test_an_unmeasured_value_is_left_out_rather_than_sent_as_null():
    (table,) = _report({**BUILD, "build_time_s": None, "indexing_throughput": None})

    assert {cell.column for cell in table.results} == {"dataset", "step", "document count"}


# ---------------------------------------------------------------------------
# Tracks: what the plan asks to graph
# ---------------------------------------------------------------------------


def test_a_tracked_search_is_also_a_row_of_its_track():
    overview, track = _report(TRACKED_SEARCH)

    assert overview.name == "SB searches"
    assert track.name == "SB throughput"
    assert _cells(track) == {
        ("10M term_common", "P50"): 1.0,
        ("10M term_common", "P90"): 2.0,
        ("10M term_common", "P99"): 4.0,
        ("10M term_common", "P99.9"): 5.0,
        ("10M term_common", "throughput"): 1234.5,
        ("10M term_common", "document count"): "1,000,000",
        ("10M term_common", "query set"): "term_common",
        ("10M term_common", "limit"): "10",
        ("10M term_common", "stress options"): "--concurrency 8",
        ("10M term_common", "rate"): "unthrottled",
        ("10M term_common", "query example"): "zephyr obsidian",
    }


def test_a_track_graphs_only_measurements():
    _, track = _report(TRACKED_SEARCH)

    assert _graphed(track) == ["P50", "P90", "P99", "P99.9", "throughput"]


def test_a_rate_limited_track_graphs_no_throughput():
    """Its throughput is the rate it was asked for."""
    _, track = _report({**TRACKED_SEARCH, "rate": 50})

    assert _graphed(track) == ["P50", "P90", "P99", "P99.9"]
    assert "throughput" not in {cell.column for cell in track.results}


def test_a_rate_limited_tracks_p99_is_checked_against_the_plans_expectation():
    _, track = _report({**TRACKED_SEARCH, "rate": 50})

    assert track.validation_rules["P99"].fixed_limit == 20.0


def test_an_unthrottled_track_checks_nothing():
    """At saturation P99 follows concurrency over throughput, so a P99 limit would check neither."""
    _, track = _report(TRACKED_SEARCH)

    assert track.validation_rules == {}
    assert "checked against" not in track.description


def test_a_tracked_index_build_is_a_row_of_its_track():
    _, track = _report(TRACKED_BUILD)

    assert track.name == "SB index build"
    assert _graphed(track) == ["build time", "indexing throughput"]
    assert _cells(track) == {
        ("10M_20tok", "build time"): 40.0,
        ("10M_20tok", "indexing throughput"): 50000.0,
        ("10M_20tok", "document count"): "2,000,000",
    }


def test_a_track_row_is_named_by_the_plan_alone():
    """A row's history across runs is one series only if its name does not depend on the run."""
    _, first = _report(TRACKED_SEARCH)
    _, second = _report({**TRACKED_SEARCH, "step": 3, "query_index": 4, "p99_ms": 9.0, "query_example": "other"})

    assert {cell.row for cell in first.results} == {cell.row for cell in second.results} == {"10M term_common"}


def test_a_workload_without_a_description_is_named_by_its_name():
    """Rather than a title starting with ': every measurement'."""
    unnamed = dataclasses.replace(WORKLOAD, description="")
    reporter = SearchArgusReporter(unnamed, _ArgusClient())

    tables = [
        reporter._log(SEARCH),
        reporter._log(BUILD),
        reporter._build_track(TRACKED_BUILD),
        reporter._search_track(TRACKED_SEARCH),
    ]

    assert all(table.description.startswith("search_bench") for table in tables)


def test_a_workload_without_a_table_prefix_names_its_tables_by_its_name():
    unprefixed = dataclasses.replace(WORKLOAD, table_prefix="")
    client = _ArgusClient()
    reporter = SearchArgusReporter(unprefixed, client)

    reporter.report(TRACKED_SEARCH)
    reporter.report(TRACKED_BUILD)

    assert [table.name for table in client.submitted] == [
        "search_bench searches",
        "search_bench throughput",
        "search_bench index builds",
        "search_bench index build",
    ]


def test_fts_tables_are_prefixed_fts():
    """The prefix is part of every table's name, and so of its history in Argus."""
    client = _ArgusClient()

    SearchArgusReporter(FTS_WORKLOAD, client).report({**TRACKED_SEARCH, "workload": FTS_WORKLOAD.name})

    assert [table.name for table in client.submitted] == ["FTS searches", "FTS throughput"]


def test_a_failed_log_row_still_sends_the_track_row():
    """The track row is the record's only graphed copy."""
    client = _ArgusClient()
    submit = client.submit_results

    def overview_fails(table):
        if table.name.endswith(" searches"):
            raise RuntimeError("argus down")
        submit(table)

    client.submit_results = overview_fails

    with pytest.raises(RuntimeError, match="argus down"):
        SearchArgusReporter(WORKLOAD, client).report(TRACKED_SEARCH)
    assert [table.name for table in client.submitted] == ["SB throughput"]


def test_an_unknown_phase_is_an_error():
    with pytest.raises(ValueError, match="Unknown results record phase 'warmup'"):
        _report({**BUILD, "phase": "warmup"})

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
from datetime import datetime, timezone

import pytest

from argus.client.generic_result import ResultType
from fts_test import FTS_WORKLOAD
from sdcm.utils.search_argus import SearchArgusReporter, sut_timestamp

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


class ArgusClient:
    """Fake Argus client that records the tables submitted to it."""

    def __init__(self):
        """Start with no submitted tables."""
        self.submitted = []

    def submit_results(self, table):
        """Record one submitted table."""
        self.submitted.append(table)


def report_records(*records, sut_timestamp=0):
    """Report the records with a fake client and return the tables it received, in order."""
    client = ArgusClient()
    reporter = SearchArgusReporter(WORKLOAD, client, sut_timestamp=sut_timestamp)
    for record in records:
        reporter.report(record)
    return client.submitted


def cells(table):
    """Map each (row, column) of a table to its value."""
    return {(cell.row, cell.column): cell.value for cell in table.results}


def graphed_columns(table):
    """Argus graphs every numeric column."""
    return [column.name for column in table.columns if column.type != ResultType.TEXT]


def day_timestamp(date: str) -> int:
    """Unix time of midnight UTC on a YYYYMMDD date."""
    return int(datetime.strptime(date, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# The logs: every record, in one of two tables by kind, nothing graphed
# ---------------------------------------------------------------------------


def test_an_untracked_record_goes_only_to_its_log():
    """An untracked record is logged to its kind's table only, with no track row."""
    tables = report_records(SEARCH, BUILD)

    assert [table.name for table in tables] == ["SB searches", "SB index builds"]


def test_the_logs_graph_nothing():
    """They are the run's results to read, not a history: an exploratory entry costs no graph."""
    search, build = report_records(SEARCH, BUILD)

    assert graphed_columns(search) == graphed_columns(build) == []


def test_each_log_has_only_its_own_columns():
    """The search log has no build columns and the build log no search columns."""
    search, build = report_records(SEARCH, BUILD)

    assert "build time" not in {column.name for column in search.columns}
    assert "P99" not in {column.name for column in build.columns}


def test_a_search_in_its_log():
    """A search's log row carries its configuration and latency percentiles, with the P99 limit."""
    (table,) = report_records(SEARCH)

    row = "#01 term_common"
    assert cells(table) == {
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
    """An index build's log row carries its size, build time and indexing throughput."""
    (table,) = report_records(TRACKED_BUILD)[:1]

    row = "#01 index build"
    assert cells(table) == {
        (row, "dataset"): "10M_20tok",
        (row, "step"): "2",
        (row, "document count"): "2,000,000",
        (row, "build time"): "40",
        (row, "indexing throughput"): "50,000",
        (row, "track"): "index build",
    }


def test_overview_rows_keep_the_run_order_and_a_repeated_configuration_apart():
    """Each log row is numbered in run order, so a repeated configuration gets its own row."""
    tables = report_records(BUILD, SEARCH, {**SEARCH, "query_index": 2})

    assert [next(iter(table.results)).row.split(" ")[0] for table in tables] == ["#01", "#02", "#03"]


def test_an_unmeasured_value_is_left_out_rather_than_sent_as_null():
    """A None measurement is omitted from the row, not sent as null."""
    (table,) = report_records({**BUILD, "build_time_s": None, "indexing_throughput": None})

    assert {cell.column for cell in table.results} == {"dataset", "step", "document count"}


# ---------------------------------------------------------------------------
# Tracks: what the plan asks to graph
# ---------------------------------------------------------------------------


def test_a_tracked_search_is_also_a_row_of_its_track():
    """A tracked search goes to its log and as a row of its track table."""
    overview, track = report_records(TRACKED_SEARCH)

    assert overview.name == "SB searches"
    assert track.name == "SB throughput"
    assert cells(track) == {
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
    """A track's graphed columns are its latency and throughput measurements only."""
    _, track = report_records(TRACKED_SEARCH)

    assert graphed_columns(track) == ["P50", "P90", "P99", "P99.9", "throughput"]


def test_a_rate_limited_track_graphs_no_throughput():
    """Its throughput is the rate it was asked for."""
    _, track = report_records({**TRACKED_SEARCH, "rate": 50})

    assert graphed_columns(track) == ["P50", "P90", "P99", "P99.9"]
    assert "throughput" not in {cell.column for cell in track.results}


def test_a_rate_limited_tracks_p99_is_checked_against_the_plans_expectation():
    """A rate-limited track validates P99 against the plan's expected read latency."""
    _, track = report_records({**TRACKED_SEARCH, "rate": 50})

    assert track.validation_rules["P99"].fixed_limit == 20.0


def test_an_unthrottled_track_checks_nothing():
    """At saturation P99 follows concurrency over throughput, so a P99 limit would check neither."""
    _, track = report_records(TRACKED_SEARCH)

    assert track.validation_rules == {}
    assert "checked against" not in track.description


def test_a_tracked_index_build_is_a_row_of_its_track():
    """A tracked index build goes to its log and as a row of its build track."""
    _, track = report_records(TRACKED_BUILD)

    assert track.name == "SB index build"
    assert graphed_columns(track) == ["build time", "indexing throughput"]
    assert cells(track) == {
        ("10M_20tok", "build time"): 40.0,
        ("10M_20tok", "indexing throughput"): 50000.0,
        ("10M_20tok", "document count"): "2,000,000",
    }


def test_a_track_row_is_named_by_the_plan_alone():
    """A row's history across runs is one series only if its name does not depend on the run."""
    _, first = report_records(TRACKED_SEARCH)
    _, second = report_records({**TRACKED_SEARCH, "step": 3, "query_index": 4, "p99_ms": 9.0, "query_example": "other"})

    assert {cell.row for cell in first.results} == {cell.row for cell in second.results} == {"10M term_common"}


def test_a_workload_without_a_description_is_named_by_its_name():
    """Rather than a title starting with ': every measurement'."""
    unnamed = dataclasses.replace(WORKLOAD, description="")
    client = ArgusClient()
    reporter = SearchArgusReporter(unnamed, client)

    for record in (SEARCH, BUILD, TRACKED_BUILD, TRACKED_SEARCH):
        reporter.report(record)

    assert client.submitted
    assert all(table.description.startswith("search_bench") for table in client.submitted)


def test_a_workload_without_a_table_prefix_names_its_tables_by_its_name():
    """Without a table prefix, table names start with the workload name."""
    unprefixed = dataclasses.replace(WORKLOAD, table_prefix="")
    client = ArgusClient()
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
    client = ArgusClient()

    SearchArgusReporter(FTS_WORKLOAD, client).report({**TRACKED_SEARCH, "workload": FTS_WORKLOAD.name})

    assert [table.name for table in client.submitted] == ["FTS searches", "FTS throughput"]


def test_a_failed_log_row_still_sends_the_track_row():
    """The track row is the record's only graphed copy."""
    client = ArgusClient()
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
    """A record with an unknown phase raises ValueError naming the phase."""
    with pytest.raises(ValueError, match="Unknown results record phase 'warmup'"):
        report_records({**BUILD, "phase": "warmup"})


def test_every_table_carries_the_runs_x_position():
    """Every table of a run carries the run's sut_timestamp."""
    tables = report_records(TRACKED_BUILD, TRACKED_SEARCH, sut_timestamp=1760000123)

    assert [table.sut_timestamp for table in tables] == [1760000123] * 4


# ---------------------------------------------------------------------------
# The x-axis
# ---------------------------------------------------------------------------


def test_the_later_build_sets_the_day():
    """The later of the two builds' dates sets the day of the x position."""
    vector_store_noon = day_timestamp("20261005") + 12 * 3600

    newer_scylla = sut_timestamp("20261007", "abc", vector_store_noon, "1.11.0")
    newer_vector_store = sut_timestamp("20261001", "abc", vector_store_noon, "1.11.0")

    assert day_timestamp("20261007") <= newer_scylla < day_timestamp("20261008")
    assert day_timestamp("20261005") <= newer_vector_store < day_timestamp("20261006")


def test_each_pair_of_builds_has_its_own_position_and_keeps_it():
    """An older ScyllaDB build than the vector-store commit must still move the point."""
    vector_store = day_timestamp("20261005")

    assert sut_timestamp("20261001", "abc", vector_store, "1.11.0") != sut_timestamp(
        "20261002", "def", vector_store, "1.11.0"
    )
    assert sut_timestamp("20261001", "abc", vector_store, "1.11.0") == sut_timestamp(
        "20261001", "abc", vector_store, "1.11.0"
    )
    # Two vector-store commits of one day, too.
    assert sut_timestamp("20261001", "abc", vector_store, "3f162c9") != sut_timestamp(
        "20261001", "abc", vector_store + 60, "4a2b1c0"
    )


def test_one_known_date_is_enough():
    """A single known date, ScyllaDB or vector-store, sets the x position."""
    assert day_timestamp("20261007") <= sut_timestamp("20261007", "abc", None, "") < day_timestamp("20261008")
    assert (
        day_timestamp("20261005")
        <= sut_timestamp(None, "", day_timestamp("20261005") + 5, "x")
        < day_timestamp("20261006")
    )


@pytest.mark.parametrize(
    "scylla_date",
    [
        pytest.param(None, id="no-date"),
        pytest.param("#NO_DATE", id="no-date-marker"),
    ],
)
def test_no_known_date_leaves_the_position_to_argus(scylla_date):
    """Without any known date the position is 0, left to Argus."""
    assert sut_timestamp(scylla_date, "", None, "") == 0

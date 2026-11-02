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

"""Argus tables of the search performance flow, built from the records of its results file.

Two kinds of table:

  - '<prefix> index builds' and '<prefix> searches': the run's log, every record in one of them,
    numbered in run order across both. All their columns are TEXT, so Argus shows them but graphs
    nothing: they are the run's overview, not a history.
  - '<prefix> <track>': the records whose plan entry names that track, one row per entry, under the
    label the plan gives it. Argus draws one graph per numeric column, with a series per row across
    runs, so these are where regressions show.

The prefix is the workload's 'table_prefix', or its name.

What is tracked, and under which names, is the plan's choice -- the entries it groups have values on
one scale, and labels it states do not move when the rest of the plan changes. See 'Results in
Argus' in docs/fts-search-test.md.
"""

from argus.client.generic_result import ColumnMetadata, GenericResultTable, ResultType, Status, ValidationRule
from sdcm.argus_results import submit_results_to_argus

LATENCY_COLUMNS = {"p50_ms": "P50", "p90_ms": "P90", "p99_ms": "P99", "p99_9_ms": "P99.9"}
THROUGHPUT_COLUMN = "throughput"
BUILD_TIME_COLUMN = "build time"
INDEXING_THROUGHPUT_COLUMN = "indexing throughput"


def rate_label(rate: int) -> str:
    return f"{rate}/s" if rate else "unthrottled"


def _label(name: str, unit: str = "") -> ColumnMetadata:
    return ColumnMetadata(name=name, unit=unit, type=ResultType.TEXT)


def _text(value) -> str | None:
    """A value as an overview cell: None stays absent, a float loses its noise digits."""
    if value is None or isinstance(value, str):
        return value
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    return f"{value:,}"


def _subject(workload) -> str:
    return workload.description or workload.name


def _table_name(workload, what: str) -> str:
    return f"{workload.table_prefix or workload.name} {what}"


def _log_description(workload, what: str) -> str:
    return (
        f"{_subject(workload)}: every {what} of the run, numbered in run order across this table and the "
        "other log. Text only, so nothing here is graphed or compared across runs -- the track tables are. "
        "The run's search_results.jsonl has the same records with all fields."
    )


def build_log_table(workload) -> GenericResultTable:
    return GenericResultTable(
        name=_table_name(workload, "index builds"),
        description=_log_description(workload, "index build"),
        columns=[
            _label("dataset"),
            _label("step"),
            _label(workload.record_label),
            _label(BUILD_TIME_COLUMN, "s"),
            _label(INDEXING_THROUGHPUT_COLUMN, f"{workload.item_noun}/s"),
            _label("track"),
        ],
    )


def search_log_table(workload) -> GenericResultTable:
    return GenericResultTable(
        name=_table_name(workload, "searches"),
        description=_log_description(workload, "query configuration"),
        columns=[
            _label("dataset"),
            _label("step"),
            _label(workload.record_label),
            _label("query set"),
            _label("limit"),
            _label("rate"),
            _label("stress options"),
            *(_label(name, "ms") for name in LATENCY_COLUMNS.values()),
            _label(THROUGHPUT_COLUMN, "op/s"),
            _label("P99 limit", "ms"),
            _label("track"),
        ],
    )


def build_track_table(workload, track: str) -> GenericResultTable:
    return GenericResultTable(
        name=_table_name(workload, track),
        description=(
            f"{_subject(workload)} index build time and indexing throughput of the plan steps in the "
            f"'{track}' track, one row per step under its 'build_label'. '{workload.record_label}' is how "
            "much the index covers."
        ),
        columns=[
            ColumnMetadata(name=BUILD_TIME_COLUMN, unit="s", type=ResultType.FLOAT, higher_is_better=False),
            ColumnMetadata(
                name=INDEXING_THROUGHPUT_COLUMN,
                unit=f"{workload.item_noun}/s",
                type=ResultType.FLOAT,
                higher_is_better=True,
            ),
            _label(workload.record_label),
        ],
    )


def search_track_table(workload, track: str, expected_p99_ms: float, rate_limited: bool) -> GenericResultTable:
    # NOTE: a rate-limited query runs at its target rate, so its throughput says nothing to graph. An
    #       unthrottled one runs at saturation, where P99 follows concurrency over throughput, so only
    #       a rate-limited track checks it.
    throughput = (
        []
        if rate_limited
        else [ColumnMetadata(name=THROUGHPUT_COLUMN, unit="op/s", type=ResultType.FLOAT, higher_is_better=True)]
    )
    limit = f"P99 is checked against {expected_p99_ms:g} ms." if rate_limited else ""
    rules = {LATENCY_COLUMNS["p99_ms"]: ValidationRule(fixed_limit=expected_p99_ms)} if rate_limited else {}
    return GenericResultTable(
        name=_table_name(workload, track),
        description=(
            f"{_subject(workload)} query latency{'' if rate_limited else ' and throughput'} of the plan "
            f"entries in the '{track}' track, one row per entry under its 'label'. limit: results per "
            "query; rate: target queries per second; stress options: the latte flags the plan adds, such as "
            "concurrency, queries in flight. "
            f"'{workload.record_label}' is how much was indexed when the queries ran. "
            f"{limit}"
        ),
        columns=[
            *(
                ColumnMetadata(name=name, unit="ms", type=ResultType.FLOAT, higher_is_better=False)
                for name in LATENCY_COLUMNS.values()
            ),
            *throughput,
            _label(workload.record_label),
            _label("query set"),
            _label("limit"),
            _label("rate"),
            _label("stress options"),
            _label("query example"),
        ],
        validation_rules=rules,
    )


class SearchArgusReporter:
    """Turns results records into Argus rows, one submission per table a record lands in.

    Argus merges the rows of every submission under a table's name into that run's table, so a
    record can be sent as soon as it is measured: a run that fails half way still reports what it
    got. Track rows need no deduplication here -- the plan is checked for repeated labels up front.
    """

    def __init__(self, workload, argus_client):
        self.workload = workload
        self.argus_client = argus_client
        self._records = 0

    def report(self, record: dict) -> None:
        if record["phase"] not in ("index_build", "search"):
            raise ValueError(f"Unknown results record phase {record['phase']!r}")
        self._records += 1
        tables = [self._log(record)]
        if record.get("track"):
            tables.append(self._build_track(record) if record["phase"] == "index_build" else self._search_track(record))
        # Each table on its own: a failed log row must not cost the track row, the graphed copy.
        failures = []
        for table in tables:
            try:
                submit_results_to_argus(self.argus_client, table)
            except Exception as exc:  # noqa: BLE001
                failures.append(exc)
        if failures:
            raise failures[0]

    @staticmethod
    def _add(table: GenericResultTable, row: str, cells: dict) -> None:
        for column, value in cells.items():
            # A measurement that was not taken is a null in the record, and an absent cell here.
            if value is not None:
                table.add_result(column=column, row=row, value=value, status=Status.UNSET)

    def _log(self, record: dict) -> GenericResultTable:
        search = record["phase"] == "search"
        table = (search_log_table if search else build_log_table)(self.workload)
        # Numbered across both logs, so rows keep the run's order and stay apart; the rest is in the columns.
        row = f"#{self._records:02d} {record['query_set'] if search else 'index build'}"
        cells = {
            "dataset": record["dataset"],
            "step": _text(record["step"]),
            self.workload.record_label: _text(record["record_count"]),
            "track": record.get("track"),
        }
        if search:
            cells |= {
                "query set": record["query_set"],
                "limit": _text(record["limit"]),
                "rate": rate_label(record["rate"]),
                "stress options": record["stress_options"],
                **{column: _text(record[field]) for field, column in LATENCY_COLUMNS.items()},
                THROUGHPUT_COLUMN: _text(record["throughput_ops"]),
                "P99 limit": _text(record["expected_p99_read_ms"]),
            }
        else:
            cells |= {
                BUILD_TIME_COLUMN: _text(record["build_time_s"]),
                INDEXING_THROUGHPUT_COLUMN: _text(record["indexing_throughput"]),
            }
        self._add(table, row, cells)
        return table

    def _build_track(self, record: dict) -> GenericResultTable:
        table = build_track_table(self.workload, record["track"])
        self._add(
            table,
            record["label"],
            {
                BUILD_TIME_COLUMN: record["build_time_s"],
                INDEXING_THROUGHPUT_COLUMN: record["indexing_throughput"],
                self.workload.record_label: _text(record["record_count"]),
            },
        )
        return table

    def _search_track(self, record: dict) -> GenericResultTable:
        rate_limited = record["rate"] > 0
        table = search_track_table(self.workload, record["track"], record["expected_p99_read_ms"], rate_limited)
        self._add(
            table,
            record["label"],
            {
                **{column: record[field] for field, column in LATENCY_COLUMNS.items()},
                **({} if rate_limited else {THROUGHPUT_COLUMN: record["throughput_ops"]}),
                self.workload.record_label: _text(record["record_count"]),
                "query set": record["query_set"],
                "limit": _text(record["limit"]),
                "rate": rate_label(record["rate"]),
                "stress options": record["stress_options"],
                "query example": record["query_example"],
            },
        )
        return table

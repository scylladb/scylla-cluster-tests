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

"""Unit tests for reading a vector-store index build time out of the node's log."""

import pytest

from sdcm.utils import vector_store_index
from sdcm.utils.vector_store_index import (
    index_build_columns,
    index_key,
    parse_full_scan_seconds,
    send_index_build_result,
    wait_for_index_build_seconds,
)

# Verbatim lines from real runs, so the parser is tested against both formats 'BaseNode.system_log'
# can resolve to. The docker one is the raw tracing line; the aws one carries a log-shipper prefix
# whose timestamp has no 'Z' -- which is what the regex uses to pick the tracing timestamp.
AWS_SCAN_LINES = (
    "2026-07-30T23:05:37.908 fts-search-vs-node-1  !INFO | vector-store[12767] "
    "2026-07-30T23:05:37.908018Z  INFO db:db-process:db_index{fts_bench.fts_idx_10m_20tok_0}: "
    "starting full scan on fts_bench.fts_idx_10m_20tok_0\n"
    "2026-07-30T23:06:43.914 fts-search-vs-node-1  !INFO | vector-store[12767] "
    "2026-07-30T23:06:43.914698Z  INFO db:db-process:db_index{fts_bench.fts_idx_10m_20tok_0}: "
    "finished full scan on fts_bench.fts_idx_10m_20tok_0\n"
)
DOCKER_SCAN_LINES = (
    "2026-07-30T21:28:00.727916Z  INFO db:db-process:db_index{fts_bench.fts_idx_local_tiny_0}: "
    "starting full scan on fts_bench.fts_idx_local_tiny_0\n"
    "2026-07-30T21:28:03.289113Z  INFO db:db-process:db_index{fts_bench.fts_idx_local_tiny_0}: "
    "finished full scan on fts_bench.fts_idx_local_tiny_0\n"
)


def _write_log(tmp_path, content):
    path = tmp_path / "system.log"
    path.write_text(content, encoding="utf-8")
    return str(path)


def test_parse_full_scan_seconds_aws_shipper_format(tmp_path):
    log = _write_log(tmp_path, AWS_SCAN_LINES)
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_10m_20tok_0") == pytest.approx(66.00668)


def test_parse_full_scan_seconds_docker_raw_format(tmp_path):
    log = _write_log(tmp_path, DOCKER_SCAN_LINES)
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_local_tiny_0") == pytest.approx(2.561197)


def test_parse_full_scan_seconds_matches_case_insensitively(tmp_path):
    """Scylla folds the index name, so the caller's un-folded name must still match."""
    log = _write_log(tmp_path, AWS_SCAN_LINES)
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_10M_20tok_0") == pytest.approx(66.00668)


def test_parse_full_scan_seconds_ignores_other_indexes(tmp_path):
    log = _write_log(tmp_path, DOCKER_SCAN_LINES + AWS_SCAN_LINES)
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_local_tiny_0") == pytest.approx(2.561197)
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_10m_20tok_0") == pytest.approx(66.00668)


def test_parse_full_scan_seconds_none_without_a_finish(tmp_path):
    log = _write_log(tmp_path, DOCKER_SCAN_LINES.splitlines(keepends=True)[0])
    assert parse_full_scan_seconds(log, "fts_bench.fts_idx_local_tiny_0") is None


def test_parse_full_scan_seconds_none_for_unknown_index(tmp_path):
    log = _write_log(tmp_path, DOCKER_SCAN_LINES)
    assert parse_full_scan_seconds(log, "fts_bench.nope") is None


def test_parse_full_scan_seconds_none_when_log_missing(tmp_path):
    assert parse_full_scan_seconds(str(tmp_path / "absent.log"), "fts_bench.idx") is None


def test_index_key_folds_case():
    assert index_key("fts_bench", "fts_idx_10M_20tok_0") == "fts_bench.fts_idx_10m_20tok_0"


def test_wait_for_index_build_seconds_returns_the_measurement(tmp_path):
    log = _write_log(tmp_path, DOCKER_SCAN_LINES)
    assert wait_for_index_build_seconds(log, "fts_bench", "fts_idx_local_tiny_0") == pytest.approx(2.561197)


def test_wait_for_index_build_seconds_retries_until_the_lines_are_shipped(tmp_path, monkeypatch):
    """The lines are written on the node and forwarded asynchronously, so an empty log means
    'not yet', not 'never'."""
    log = _write_log(tmp_path, "")
    monkeypatch.setattr(vector_store_index.time, "sleep", lambda _seconds: _write_log(tmp_path, DOCKER_SCAN_LINES))

    assert wait_for_index_build_seconds(log, "fts_bench", "fts_idx_local_tiny_0", timeout=10) == pytest.approx(2.561197)


def test_wait_for_index_build_seconds_gives_up_and_returns_none(tmp_path):
    """A missing measurement, not a failed build: the index is queryable either way."""
    log = _write_log(tmp_path, "")
    assert wait_for_index_build_seconds(log, "fts_bench", "idx", timeout=0) is None


def test_index_build_columns_name_the_count_in_the_workloads_own_words():
    columns = index_build_columns("document_count", "docs")
    assert [column.name for column in columns] == ["build_time", "document_count", "indexing_throughput"]
    assert [column.unit for column in columns] == ["s", "docs", "docs/s"]


class _RecordingTable:
    """Stands in for the workload's Argus table, keeping what was written to it."""

    def __init__(self):
        self.rows = {}

    def add_result(self, column, row, value, status):
        self.rows.setdefault(row, {})[column] = value


def _send(monkeypatch, build_time, count, count_column="document_count"):
    submitted = []
    monkeypatch.setattr(vector_store_index, "submit_results_to_argus", lambda client, table: submitted.append(table))
    table = _RecordingTable()
    send_index_build_result(
        argus_client=object(),
        result_table=table,
        count_column=count_column,
        build_time=build_time,
        count=count,
        row_key="local_tiny | 1,000 docs | build #1",
    )
    return table, submitted


def test_send_index_build_result_reports_the_throughput_the_two_others_imply(monkeypatch):
    """The throughput column is the only derived value in the row, and Argus keeps its history --
    a wrong one is indistinguishable from a real regression."""
    table, submitted = _send(monkeypatch, build_time=2.5, count=1000)
    assert table.rows == {
        "local_tiny | 1,000 docs | build #1": {
            "build_time": 2.5,
            "document_count": 1000,
            "indexing_throughput": 400.0,
        }
    }
    assert submitted == [table]


@pytest.mark.parametrize("build_time, count", ((0.0, 1000), (2.5, 0)))
def test_send_index_build_result_reports_zero_throughput_it_cannot_derive(monkeypatch, build_time, count):
    """Rather than dividing by zero, or reporting a throughput no documents were indexed at."""
    table, submitted = _send(monkeypatch, build_time=build_time, count=count)
    assert table.rows["local_tiny | 1,000 docs | build #1"]["indexing_throughput"] == 0.0
    assert submitted == [table]


def test_send_index_build_result_counts_in_the_workloads_own_column(monkeypatch):
    """The count column is named by the caller, and Argus keys the history by that name."""
    table, _ = _send(monkeypatch, build_time=2.0, count=10, count_column="vector_count")
    assert "vector_count" in table.rows["local_tiny | 1,000 docs | build #1"]

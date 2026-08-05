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
    index_key,
    parse_full_scan_seconds,
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

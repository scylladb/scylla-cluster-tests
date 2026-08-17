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
# Copyright (c) 2022 ScyllaDB

import pytest

from sdcm.stress_thread import (
    CassandraStressThread,
    apply_gemini_stress_duration,
    extract_gemini_seed,
    get_timeout_from_stress_cmd,
)
from sdcm.utils.common import time_period_str_to_seconds
from sdcm.utils.hdrhistogram import _HdrRangeHistogramBuilder


@pytest.mark.parametrize(
    "duration,seconds",
    (
        ("1h1m20s", 3680),
        ("1m20s", 80),
        ("1h20s", 3620),
        ("25m", 1500),
        ("10h", 36000),
        ("25s", 25),
    ),
)
def test_duration_str_to_seconds_function(duration, seconds):
    assert time_period_str_to_seconds(duration) == seconds


@pytest.mark.parametrize(
    "stress_cmd, timeout",
    (
        (
            "cassandra-stress counter_write cl=QUORUM duration=20m"
            " -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor=3)' no-warmup",
            1200 + 900,
        ),
        ("scylla-bench -workload=uniform -concurrency 64 -duration 1h -validate-data", 3600 + 900),
        ("scylla-bench -partition-count=20000 -duration=250s", 250 + 900),
        ("gemini -d --duration 10m --warmup 10s -c 5 -m write", 610 + 900),
        ("latte run --duration 10m --sampling 5s", 600 + 900),
        # Gemini commands from the issue - test case with 24h duration
        (
            "--duration 24h --warmup 10m --concurrency 200 --mode mixed --max-mutation-retries-backoff 10s",
            86400 + 600 + 900,
        ),
        # Gemini with equals sign format
        ("--duration=3h --warmup=30m --concurrency=50 --mode=mixed", 10800 + 1800 + 900),
        # Gemini command without warmup
        ("--duration 1h --concurrency 100 --mode write", 3600 + 900),
        # Critical case: YAML multiline format with newlines (the actual issue scenario)
        ("--duration 24h\n--warmup 10m\n--concurrency 200", 86400 + 600 + 900),
    ),
)
def test_get_timeout_from_stress_cmd(stress_cmd, timeout):
    assert get_timeout_from_stress_cmd(stress_cmd) == timeout


# --- _extract_user_profile_op_names tests ---


@pytest.mark.parametrize(
    "stress_cmd, expected",
    (
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1)' -rate threads=10",
            ["insert"],
            id="insert_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(read=2)' -rate threads=10",
            ["read"],
            id="read_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1,read=2)' -rate threads=10",
            ["insert", "read"],
            id="insert_and_read_mixed",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/mv_synchronous_updates.yaml"
            " ops'(select_base=3,select_mv=3,select_mv_2=3,url_column_update=1,row_delete=1)'"
            " cl=QUORUM duration=360m -mode cql3 native -rate threads=50",
            ["select_base", "select_mv", "select_mv_2", "url_column_update", "row_delete"],
            id="issue_13401_mv_synchronous_updates",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(select_base=3,select_mv=3)' -rate threads=10",
            ["select_base", "select_mv"],
            id="select_queries_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(url_column_update=1,row_delete=1)' -rate threads=10",
            ["url_column_update", "row_delete"],
            id="update_and_delete_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(lwt_update_one_column=1,lwt_update_two_columns=1)'"
            " -rate threads=10",
            ["lwt_update_one_column", "lwt_update_two_columns"],
            id="lwt_update_operations",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=2,read1=1,update_number=1,delete_row=1)'"
            " -rate threads=10",
            ["insert", "read1", "update_number", "delete_row"],
            id="cdc_profile_mixed_ops",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert_query=1)' -rate threads=10",
            ["insert_query"],
            id="insert_query_operation",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(scan_all=1)' -rate threads=10",
            ["scan_all"],
            id="scan_operation",
        ),
    ),
)
def test_extract_user_profile_op_names(stress_cmd, expected):
    assert CassandraStressThread._extract_user_profile_op_names(stress_cmd) == expected


def test_extract_user_profile_op_names_no_ops_clause_insert():
    """Fallback to legacy insert= matching when no ops() clause found."""
    stress_cmd = "cassandra-stress user profile=/tmp/test.yaml insert=1 -rate threads=10"
    assert CassandraStressThread._extract_user_profile_op_names(stress_cmd) == ["insert"]


def test_extract_user_profile_op_names_no_ops_clause_read():
    """Fallback to legacy read= matching when no ops() clause found."""
    stress_cmd = "cassandra-stress user profile=/tmp/test.yaml read=1 -rate threads=10"
    assert CassandraStressThread._extract_user_profile_op_names(stress_cmd) == ["read"]


# --- set_hdr_tags tests (using a lightweight stub to avoid full CassandraStressThread init) ---


def _make_hdr_tag_stub():
    """Create a minimal stub with the attributes set_hdr_tags needs."""
    stub = object.__new__(CassandraStressThread)
    stub.hdr_tags = []
    return stub


@pytest.mark.parametrize(
    "stress_cmd, expected_tags",
    (
        pytest.param(
            "cassandra-stress write cl=ONE duration=3m -mode cql3 native -rate threads=1000",
            ["WRITE-st"],
            id="standard_write_unthrottled",
        ),
        pytest.param(
            "cassandra-stress read cl=ONE duration=3m -mode cql3 native -rate threads=1000",
            ["READ-st"],
            id="standard_read_unthrottled",
        ),
        pytest.param(
            "cassandra-stress mixed cl=ONE duration=3m -mode cql3 native -rate threads=1000",
            ["WRITE-st", "READ-st"],
            id="standard_mixed_unthrottled",
        ),
        pytest.param(
            "cassandra-stress write cl=ONE duration=3m -mode cql3 native -rate 'fixed=100/s threads=10'",
            ["WRITE-rt"],
            id="standard_write_throttled",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1)' -mode cql3 native -rate threads=50",
            ["insert-st"],
            id="user_profile_insert",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(read=2)' -mode cql3 native -rate threads=50",
            ["read-st"],
            id="user_profile_read",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/mv_synchronous_updates.yaml"
            " ops'(select_base=3,select_mv=3,select_mv_2=3,url_column_update=1,row_delete=1)'"
            " cl=QUORUM duration=360m -mode cql3 native -rate threads=50",
            ["select_base-st", "select_mv-st", "select_mv_2-st", "url_column_update-st", "row_delete-st"],
            id="issue_13401_custom_ops_mixed",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(select_base=3,select_mv=1)'"
            " -mode cql3 native -rate threads=50",
            ["select_base-st", "select_mv-st"],
            id="user_profile_select_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(url_column_update=1,row_delete=1)'"
            " -mode cql3 native -rate threads=50",
            ["url_column_update-st", "row_delete-st"],
            id="user_profile_write_ops_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1)'"
            " -mode cql3 native -rate 'fixed=100/s threads=10'",
            ["insert-rt"],
            id="user_profile_insert_throttled",
        ),
    ),
)
def test_set_hdr_tags(stress_cmd, expected_tags):
    stub = _make_hdr_tag_stub()
    stub.set_hdr_tags(stress_cmd)
    assert stub.hdr_tags == expected_tags


def test_set_hdr_tags_user_profile_no_known_ops_raises():
    """set_hdr_tags should raise ValueError when no ops clause and no insert=/read= found."""
    stub = _make_hdr_tag_stub()
    with pytest.raises(ValueError, match="Cannot detect stress operation type"):
        stub.set_hdr_tags("cassandra-stress user profile=/tmp/test.yaml -mode cql3 native -rate threads=50")


# --- literal user-profile hdr_tags must still classify via the downstream
# _get_workload_type_by_hdr_tag() keyword matcher (the "existing approach" used by
# latte/scylla-bench tags), since set_hdr_tags() no longer pre-classifies them ---


@pytest.mark.parametrize(
    "hdr_tag, expected_workload",
    (
        pytest.param("insert-st", "WRITE", id="insert"),
        pytest.param("read-st", "READ", id="read"),
        pytest.param("stmt-select-rt", "READ", id="stmt_select"),
        pytest.param("stmt-update-if-cond-rt", "WRITE", id="stmt_update_if_cond"),
        pytest.param("stmt-insert-if-not-exists-st", "WRITE", id="stmt_insert_if_not_exists"),
        pytest.param("stmt-delete-if-exists-st", "WRITE", id="stmt_delete_if_exists"),
        pytest.param("lwt_update_one_column-st", "WRITE", id="lwt_update_one_column"),
        pytest.param("scan_all-st", "READ", id="scan_all"),
        pytest.param("url_column_update-st", "WRITE", id="url_column_update"),
    ),
)
def test_user_profile_hdr_tag_classified_by_workload_type(hdr_tag, expected_workload):
    builder = _HdrRangeHistogramBuilder(hdr_tags=[hdr_tag], stress_operation="user", start_time=0, end_time=0)
    assert builder._get_workload_type_by_hdr_tag(hdr_tag) == expected_workload


def test_user_profile_hdr_tag_unrecognized_op_name_raises():
    """Op names with no write/read keyword are no longer coerced into a 'mixed' guess -
    they now raise, since the tag must be classifiable on its own."""
    builder = _HdrRangeHistogramBuilder(hdr_tags=["simple1-st"], stress_operation="user", start_time=0, end_time=0)
    with pytest.raises(ValueError, match="Failed to detect the workload type"):
        builder._get_workload_type_by_hdr_tag("simple1-st")


@pytest.mark.parametrize(
    "original_cmd, stress_duration, expected_cmd",
    [
        # The core bug: YAML block scalar puts --duration at start with no leading space
        pytest.param(
            "--duration 3h\n--concurrency 50\n--mode mixed",
            60,
            "--duration 60m\n--concurrency 50\n--mode mixed",
            id="yaml-block-scalar-no-leading-space",
        ),
        # Space-delimited format (inline command)
        pytest.param(
            " --duration 3h --concurrency 50",
            60,
            " --duration 60m --concurrency 50",
            id="leading-space-before-duration",
        ),
        # warmup must be left completely untouched
        pytest.param(
            "--duration 8h\n--warmup 30m\n--concurrency 50",
            120,
            "--duration 120m\n--warmup 30m\n--concurrency 50",
            id="warmup-left-untouched",
        ),
        # --duration at end of string (no trailing whitespace)
        pytest.param(
            "--concurrency 50\n--duration 3h",
            45,
            "--concurrency 50\n--duration 45m",
            id="duration-at-end-of-command",
        ),
        # No --duration present → must be appended
        pytest.param(
            "--concurrency 50\n--mode mixed",
            90,
            "--concurrency 50\n--mode mixed --duration 90m",
            id="no-duration-injected",
        ),
        # Equals-form must also be rewritten, not silently kept
        pytest.param(
            "--duration=3h --concurrency=50 --mode=mixed",
            75,
            "--duration 75m --concurrency=50 --mode=mixed",
            id="equals-form-rewritten",
        ),
        pytest.param(
            "--duration=24h\n--warmup=10m\n--concurrency=200",
            45,
            "--duration 45m\n--warmup=10m\n--concurrency=200",
            id="equals-form-yaml-block-scalar",
        ),
    ],
)
def test_apply_gemini_stress_duration(original_cmd, stress_duration, expected_cmd):
    assert apply_gemini_stress_duration(original_cmd, stress_duration) == expected_cmd


@pytest.mark.parametrize(
    "gemini_cmd, expected_seed",
    [
        # The bug: gemini generates --seed=VALUE (equals), old regex required space-delimited
        pytest.param(
            "--duration 3h --seed=12345 --concurrency 50",
            12345,
            id="equals-sign-format",
        ),
        # Space-delimited format should also work
        pytest.param(
            "--duration 3h --seed 99999 --concurrency 50",
            99999,
            id="space-delimited-format",
        ),
        # No --seed present → should return -1
        pytest.param(
            "--duration 3h --concurrency 50",
            -1,
            id="no-seed-returns-minus-one",
        ),
    ],
)
def test_extract_gemini_seed(gemini_cmd, expected_seed):
    assert extract_gemini_seed(gemini_cmd) == expected_seed

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

from sdcm.stress_thread import CassandraStressThread, get_timeout_from_stress_cmd
from sdcm.utils.common import time_period_str_to_seconds


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


# --- _classify_user_profile_ops tests ---


@pytest.mark.parametrize(
    "stress_cmd, expected",
    (
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1)' -rate threads=10",
            (True, False),
            id="insert_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(read=2)' -rate threads=10",
            (False, True),
            id="read_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1,read=2)' -rate threads=10",
            (True, True),
            id="insert_and_read_mixed",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/mv_synchronous_updates.yaml"
            " ops'(select_base=3,select_mv=3,select_mv_2=3,url_column_update=1,row_delete=1)'"
            " cl=QUORUM duration=360m -mode cql3 native -rate threads=50",
            (True, True),
            id="issue_13401_mv_synchronous_updates",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(select_base=3,select_mv=3)' -rate threads=10",
            (False, True),
            id="select_queries_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(url_column_update=1,row_delete=1)' -rate threads=10",
            (True, False),
            id="update_and_delete_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(lwt_update_one_column=1,lwt_update_two_columns=1)'"
            " -rate threads=10",
            (True, False),
            id="lwt_update_operations",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=2,read1=1,update_number=1,delete_row=1)'"
            " -rate threads=10",
            (True, True),
            id="cdc_profile_mixed_ops",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert_query=1)' -rate threads=10",
            (True, False),
            id="insert_query_operation",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(scan_all=1)' -rate threads=10",
            (False, True),
            id="scan_operation",
        ),
    ),
)
def test_classify_user_profile_ops(stress_cmd, expected):
    assert CassandraStressThread._classify_user_profile_ops(stress_cmd) == expected


def test_classify_user_profile_ops_unknown_op_defaults_to_mixed():
    """Unknown operation names that don't match any keyword should be treated as mixed."""
    stress_cmd = "cassandra-stress user profile=/tmp/test.yaml ops'(custom_op=1)' -rate threads=10"
    has_write, has_read = CassandraStressThread._classify_user_profile_ops(stress_cmd)
    assert has_write is True
    assert has_read is True


def test_classify_user_profile_ops_no_ops_clause_insert():
    """Fallback to legacy insert= matching when no ops() clause found."""
    stress_cmd = "cassandra-stress user profile=/tmp/test.yaml insert=1 -rate threads=10"
    has_write, has_read = CassandraStressThread._classify_user_profile_ops(stress_cmd)
    assert has_write is True
    assert has_read is False


def test_classify_user_profile_ops_no_ops_clause_read():
    """Fallback to legacy read= matching when no ops() clause found."""
    stress_cmd = "cassandra-stress user profile=/tmp/test.yaml read=1 -rate threads=10"
    has_write, has_read = CassandraStressThread._classify_user_profile_ops(stress_cmd)
    assert has_write is False
    assert has_read is True


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
            ["WRITE-st"],
            id="user_profile_insert",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(read=2)' -mode cql3 native -rate threads=50",
            ["READ-st"],
            id="user_profile_read",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/mv_synchronous_updates.yaml"
            " ops'(select_base=3,select_mv=3,select_mv_2=3,url_column_update=1,row_delete=1)'"
            " cl=QUORUM duration=360m -mode cql3 native -rate threads=50",
            ["WRITE-st", "READ-st"],
            id="issue_13401_custom_ops_mixed",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(select_base=3,select_mv=1)'"
            " -mode cql3 native -rate threads=50",
            ["READ-st"],
            id="user_profile_select_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(url_column_update=1,row_delete=1)'"
            " -mode cql3 native -rate threads=50",
            ["WRITE-st"],
            id="user_profile_write_ops_only",
        ),
        pytest.param(
            "cassandra-stress user profile=/tmp/test.yaml ops'(insert=1)'"
            " -mode cql3 native -rate 'fixed=100/s threads=10'",
            ["WRITE-rt"],
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

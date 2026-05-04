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

"""Unit tests for BaseCassandraCluster.dump_schema."""

import re
from unittest.mock import MagicMock

import pytest
from invoke import Result

from sdcm.cluster_cassandra import BaseCassandraCluster
from unit_tests.lib.fake_remoter import FakeRemoter

_DESCRIBE_OUTPUT = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    " AND durable_writes = true;\n"
    "\n"
    "CREATE TABLE ks.t (id int PRIMARY KEY, data text) WITH bloom_filter_fp_chance = 0.01;\n"
)

# DESCRIBE output preceded by a cqlsh banner that contains semicolons — this must
# not confuse the stripper into cutting off before CREATE KEYSPACE.
_DESCRIBE_OUTPUT_WITH_BANNER = (
    "Connected to cluster; defaults set;\n"
    "\n"
    "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    " AND durable_writes = true;\n"
    "\n"
    "CREATE TABLE ks.t (id int PRIMARY KEY, data text) WITH bloom_filter_fp_chance = 0.01;\n"
)


def _make_cluster(result_map: dict) -> BaseCassandraCluster:
    """Minimal BaseCassandraCluster instance without triggering __init__."""
    node = MagicMock()
    node.ip_address = "1.2.3.4"
    node.remoter = FakeRemoter.__new__(FakeRemoter)
    node.remoter.result_map = result_map

    cluster = object.__new__(BaseCassandraCluster)
    cluster.nodes = [node]
    return cluster


def test_dump_schema_excludes_create_keyspace():
    """Test stripping CREATE KEYSPACE block when include_keyspace_stmt=False."""
    cluster = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_OUTPUT, stderr="", exited=0)})
    result = cluster.dump_schema("ks")
    assert "CREATE TABLE" in result
    assert "CREATE KEYSPACE" not in result


def test_dump_schema_include_keyspace_stmt():
    """Test that full output returned unchanged when include_keyspace_stmt=True."""
    cluster = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_OUTPUT, stderr="", exited=0)})
    assert cluster.dump_schema("ks", include_keyspace_stmt=True) == _DESCRIBE_OUTPUT


def test_dump_schema_command_failure_raises_runtime_error():
    """Test that RuntimeError is raised containing the stderr message."""
    stderr_msg = "Keyspace 'foo' not found"
    cluster = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout="", stderr=stderr_msg, exited=1)})
    with pytest.raises(RuntimeError, match=stderr_msg):
        cluster.dump_schema("foo")


def test_dump_schema_banner_with_semicolons_does_not_confuse_stripper():
    """regression: banner lines containing ';' before CREATE KEYSPACE must not be mistaken
    for the end of the CREATE KEYSPACE statement."""
    cluster = _make_cluster(
        {
            r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_OUTPUT_WITH_BANNER, stderr="", exited=0),
        }
    )
    result = cluster.dump_schema("ks", include_keyspace_stmt=False)
    assert "CREATE TABLE" in result
    assert "CREATE KEYSPACE" not in result
    assert "Connected to cluster" not in result


# Cassandra 4.1 DESCRIBE TABLE output with a full WITH block
_DESCRIBE_WITH_CASSANDRA_OPTIONS = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    " AND durable_writes = true;\n"
    "\n"
    "CREATE TABLE ks.t (\n"
    "    id int PRIMARY KEY,\n"
    "    data text\n"
    ") WITH additional_write_policy = '99p'\n"
    "    AND bloom_filter_fp_chance = 0.01\n"
    "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n"
    "    AND cdc = false\n"
    "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n"
    "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
    "    AND extensions = {}\n"
    "    AND gc_grace_seconds = 864000\n"
    "    AND memtable = 'default'\n"
    "    AND read_repair = 'BLOCKING';\n"
)

# output where first option of WITH is a Cassandra-only one
_DESCRIBE_WITH_LEADING_CASSANDRA_OPTION = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    " AND durable_writes = true;\n"
    "\n"
    "CREATE TABLE ks.t (\n"
    "    id int PRIMARY KEY,\n"
    "    data text\n"
    ") WITH additional_write_policy = '99p'\n"
    "    AND bloom_filter_fp_chance = 0.01\n"
    "    AND gc_grace_seconds = 864000;\n"
)

# output with no Cassandra-only options
_DESCRIBE_SCYLLA_SAFE = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}"
    " AND durable_writes = true;\n"
    "\n"
    "CREATE TABLE ks.t (\n"
    "    id int PRIMARY KEY,\n"
    "    data text\n"
    ") WITH bloom_filter_fp_chance = 0.01\n"
    "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}\n"
    "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'};\n"
)


def test_dump_schema_strips_cassandra_only_options():
    """Test that cassandra-only options are removed; Scylla-compatible options are preserved."""
    cluster = _make_cluster(
        {
            r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_WITH_CASSANDRA_OPTIONS, stderr="", exited=0),
        }
    )
    result = cluster.dump_schema("ks")
    for option in ("bloom_filter_fp_chance", "compaction", "compression", "gc_grace_seconds"):
        assert option in result
    for option in ("additional_write_policy", "read_repair", "memtable", "extensions", "cdc"):
        assert option not in result


def test_dump_schema_handles_leading_cassandra_only_option():
    """Test that when first option of WITH is Cassandra-only, the next clause is rewritten to WITH."""
    cluster = _make_cluster(
        {
            r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_WITH_LEADING_CASSANDRA_OPTION, stderr="", exited=0),
        }
    )
    result = cluster.dump_schema("ks")
    assert "additional_write_policy" not in result
    assert "bloom_filter_fp_chance" in result
    assert "gc_grace_seconds" in result

    # the surviving first option must be introduced by WITH, not a dangling AND
    first_line = next((ln for ln in result.splitlines() if "bloom_filter_fp_chance" in ln), None)
    assert first_line is not None, "bloom_filter_fp_chance must appear in result"
    assert re.search(r"WITH.*bloom_filter_fp_chance", first_line, re.IGNORECASE), (
        f"expected bloom_filter_fp_chance to be introduced by WITH, got: {first_line!r}"
    )
    assert not first_line.strip().lower().startswith("and "), (
        f"first surviving option must not be a bare AND clause, got: {first_line!r}"
    )


def test_dump_schema_idempotent_on_scylla_safe_schema():
    """Test that schema with no Cassandra-only options passes through unchanged."""
    cluster = _make_cluster(
        {
            r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=_DESCRIBE_SCYLLA_SAFE, stderr="", exited=0),
        }
    )
    result = cluster.dump_schema("ks")
    assert "CREATE KEYSPACE" not in result
    for option in ("CREATE TABLE", "bloom_filter_fp_chance", "compaction", "compression"):
        assert option in result


def test_dump_schema_strips_speculative_retry():
    """Cassandra emits `speculative_retry = '99p'` which Scylla mis-parses (sstring out of range)."""
    output = (
        "CREATE TABLE ks.t (id int PRIMARY KEY, data text) WITH bloom_filter_fp_chance = 0.01\n"
        "    AND speculative_retry = '99p'\n"
        "    AND gc_grace_seconds = 864000;\n"
    )
    cluster = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=output, stderr="", exited=0)})
    result = cluster.dump_schema("ks", include_keyspace_stmt=True)
    assert "speculative_retry" not in result
    assert "bloom_filter_fp_chance" in result
    assert "gc_grace_seconds" in result


def test_dump_schema_rewrites_compression_class_to_sstable_compression():
    """Cassandra's `compression = {'class': '...'}` must become `'sstable_compression'` for Scylla."""
    output = (
        "CREATE TABLE ks.t (id int PRIMARY KEY, data text) WITH bloom_filter_fp_chance = 0.01\n"
        "    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n"
        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'};\n"
    )
    cluster = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=output, stderr="", exited=0)})
    result = cluster.dump_schema("ks", include_keyspace_stmt=True)
    # compression dict: 'class' must be rewritten to 'sstable_compression'
    assert "'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'" in result
    # compaction dict: 'class' must be PRESERVED (Scylla expects it there)
    assert "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}" in result


def test_dump_schema_rewrites_compression_enabled_false_to_disabled():
    """Cassandra 4.x `compression = {'enabled': 'false'}` (no 'class' key) means
    compression is disabled. Scylla rejects that form with "Missing sub-option
    'sstable_compression'" - the equivalent on Scylla is `{'sstable_compression': ''}`.
    Latte's latte_cs_alike.rn schema emits exactly this disabled-compression form."""
    output = (
        'CREATE TABLE keyspace1.standard1 (key blob PRIMARY KEY, "C0" blob) WITH bloom_filter_fp_chance = 0.01\n'
        "    AND compression = {'enabled': 'false'}\n"
        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'};\n"
    )
    result = _make_cluster({r"cqlsh.*DESCRIBE KEYSPACE.*": Result(stdout=output, stderr="", exited=0)}).dump_schema(
        "keyspace1", include_keyspace_stmt=True
    )
    assert "compression = {'sstable_compression': ''}" in result
    assert "'enabled'" not in result
    assert "compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}" in result

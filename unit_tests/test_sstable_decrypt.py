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

"""Unit tests for sstable decryption helpers in sdcm.utils.sstable.sstable_utils."""

import re
from typing import Dict, Pattern
from unittest.mock import patch

import pytest
from invoke import Result

from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.sstable.sstable_utils import (
    decrypt_sstable_files_on_node,
    decrypt_sstables_on_node,
    strip_encryption_options_from_schema,
)

# ---------------------------------------------------------------------------
# Test data constants
# ---------------------------------------------------------------------------

SCHEMA_NO_ENCRYPTION = """\
CREATE TABLE test_ks.test_table (
    id int,
    name text,
    PRIMARY KEY (id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
"""

SCHEMA_WITH_ENCRYPTION = """\
CREATE TABLE test_ks.test_table (
    id int,
    name text,
    PRIMARY KEY (id)
) WITH bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}
    AND compaction = {'class': 'IncrementalCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND scylla_encryption_options = {'cipher_algorithm': 'AES/ECB/PKCS5Padding', 'key_provider': 'LocalFileSystemKeyProviderFactory', 'secret_key_file': '/etc/scylla/encrypt_conf/secret_key', 'secret_key_strength': '128'}
    AND tombstone_gc = {'mode': 'timeout', 'propagation_delay_in_seconds': '3600'};
"""


# ---------------------------------------------------------------------------
# Helpers / stubs
# ---------------------------------------------------------------------------


class FakeNode:
    """Minimal node stub for testing decrypt helpers without real SSH."""

    def __init__(self, remoter):
        self.remoter = remoter

    def add_install_prefix(self, path: str) -> str:
        return path


def ok(stdout: str = "") -> Result:
    return Result(stdout=stdout, exited=0)


def fail(stderr: str = "error") -> Result:
    return Result(stdout="", stderr=stderr, exited=1)


@pytest.fixture
def setup_node(fake_remoter):
    """Return a callable that, given a result_map, returns (node, mock_run).

    Usage::

        def test_something(setup_node):
            node, mock_run = setup_node({re.compile(r"cat .*"): ok("schema")})
            result = decrypt_sstables_on_node(node, "/snap", keyspace="ks", table="tbl")
            mock_run.assert_any_call("cat /snap/schema.cql", ...)
    """

    def factory(result_map: Dict[Pattern, Result]):
        fake_remoter.result_map = result_map
        node = FakeNode(RemoteCmdRunnerBase.create_remoter("test-node"))
        with patch.object(node.remoter, "run", wraps=node.remoter.run) as mock_run:
            return node, mock_run

    return factory


# ---------------------------------------------------------------------------
# strip_encryption_options_from_schema – pure function tests
# ---------------------------------------------------------------------------


def test_strip_encryption_options_removes_clause():
    """scylla_encryption_options AND clause is stripped from schema that contains it, but others clauses are preserved."""
    result = strip_encryption_options_from_schema(SCHEMA_WITH_ENCRYPTION)
    assert "scylla_encryption_options" not in result

    assert "tombstone_gc" in result
    assert "LZ4Compressor" in result
    assert "bloom_filter_fp_chance" in result


def test_strip_encryption_options_schema_without_encryption_unchanged():
    """Schema without scylla_encryption_options is returned byte-for-byte identical."""
    result = strip_encryption_options_from_schema(SCHEMA_NO_ENCRYPTION)
    assert result == SCHEMA_NO_ENCRYPTION


def test_strip_encryption_options_empty_string():
    """Empty input produces empty output."""
    assert strip_encryption_options_from_schema("") == ""


@pytest.mark.parametrize(
    "schema_text",
    [
        pytest.param(
            "CREATE TABLE t (id int PRIMARY KEY)\n    AND scylla_encryption_options = {'key_provider': 'Foo'};",
            id="minimal_single_property",
        ),
        pytest.param(
            "CREATE TABLE t (id int PRIMARY KEY)"
            "\n    AND scylla_encryption_options = {'a': 'b', 'c': 'd'}"
            "\n    AND other_prop = 'x';",
            id="followed_by_another_property",
        ),
        pytest.param(
            "CREATE TABLE t1 (id int PRIMARY KEY)"
            "\n    AND scylla_encryption_options = {'a': 'b'};\n"
            "CREATE TABLE t2 (id int PRIMARY KEY)"
            "\n    AND scylla_encryption_options = {'c': 'd'};",
            id="multiple_tables_both_encrypted",
        ),
    ],
)
def test_strip_encryption_options_various_formats(schema_text):
    """scylla_encryption_options is removed regardless of surrounding context."""
    result = strip_encryption_options_from_schema(schema_text)
    assert "scylla_encryption_options" not in result


def test_strip_encryption_options_case_insensitive():
    """Stripping works even when the property name is upper-cased."""
    schema = "CREATE TABLE t (id int PRIMARY KEY)\n    AND SCYLLA_ENCRYPTION_OPTIONS = {'a': 'b'};"
    assert "SCYLLA_ENCRYPTION_OPTIONS" not in strip_encryption_options_from_schema(schema)


# ---------------------------------------------------------------------------
# decrypt_sstables_on_node – behaviour tests using FakeRemoter
# ---------------------------------------------------------------------------


def test_decrypt_sstables_on_node_happy_path(setup_node):
    """decrypt_sstables_on_node returns the decrypted/ dir path on success."""
    snap = "/var/lib/scylla/data/ks/tbl-uuid/snapshots/snap"
    data_file = f"{snap}/me-big-Data.db"
    node, _ = setup_node(
        {
            re.compile(r"cat .*/schema\.cql"): ok(SCHEMA_WITH_ENCRYPTION),
            re.compile(r"ls .*/\*-Data\.db.*"): ok(data_file),
            re.compile(r"echo .* \| base64 -d > /tmp/sct_decrypt_schema.*"): ok(),
            re.compile(r"rm -rf .*/decrypted"): ok(),
            re.compile(r"mkdir -p .*/decrypted"): ok(),
            re.compile(r".*scylla sstable upgrade.*"): ok("Upgraded."),
            re.compile(r"ls .*/decrypted/"): ok("me-new-big-Data.db"),
            re.compile(r"rm -f /tmp/sct_decrypt_schema.*"): ok(),
        }
    )
    assert decrypt_sstables_on_node(node, snap, keyspace="ks", table="tbl") == f"{snap}/decrypted"


def test_decrypt_sstables_on_node_missing_schema_returns_none(setup_node):
    """decrypt_sstables_on_node returns None when schema.cql is missing."""
    node, mock_run = setup_node(
        {
            re.compile(r"cat .*/schema\.cql"): fail("No such file"),
        }
    )
    assert decrypt_sstables_on_node(node, "/snapshots/snap", keyspace="ks", table="tbl") is None
    assert not any("sstable upgrade" in c.args[0] for c in mock_run.call_args_list)


def test_decrypt_sstables_on_node_no_data_files_returns_none(setup_node):
    """decrypt_sstables_on_node returns None when there are no Data.db files."""
    node, mock_run = setup_node(
        {
            re.compile(r"cat .*/schema\.cql"): ok(SCHEMA_NO_ENCRYPTION),
            re.compile(r"ls .*/\*-Data\.db.*"): ok(""),
        }
    )
    assert decrypt_sstables_on_node(node, "/snapshots/snap", keyspace="ks", table="tbl") is None
    assert not any("sstable upgrade" in c.args[0] for c in mock_run.call_args_list)


def test_decrypt_sstables_on_node_upgrade_failure_returns_none(setup_node):
    """decrypt_sstables_on_node returns None when scylla sstable upgrade fails."""
    data_file = "/snapshots/snap/me-big-Data.db"
    node, _ = setup_node(
        {
            re.compile(r"cat .*/schema\.cql"): ok(SCHEMA_NO_ENCRYPTION),
            re.compile(r"ls .*/\*-Data\.db.*"): ok(data_file),
            re.compile(r"echo .* \| base64 -d > /tmp/sct_decrypt_schema.*"): ok(),
            re.compile(r"rm -rf .*/decrypted"): ok(),
            re.compile(r"mkdir -p .*/decrypted"): ok(),
            re.compile(r".*scylla sstable upgrade.*"): fail("upgrade failed"),
            re.compile(r"rm -f /tmp/sct_decrypt_schema.*"): ok(),
        }
    )
    assert decrypt_sstables_on_node(node, "/snapshots/snap", keyspace="ks", table="tbl") is None


def test_decrypt_sstables_on_node_empty_output_returns_none(setup_node):
    """decrypt_sstables_on_node returns None when upgrade exits 0 but produces no files."""
    data_file = "/snapshots/snap/me-big-Data.db"
    node, _ = setup_node(
        {
            re.compile(r"cat .*/schema\.cql"): ok(SCHEMA_NO_ENCRYPTION),
            re.compile(r"ls .*/\*-Data\.db.*"): ok(data_file),
            re.compile(r"echo .* \| base64 -d > /tmp/sct_decrypt_schema.*"): ok(),
            re.compile(r"rm -rf .*/decrypted"): ok(),
            re.compile(r"mkdir -p .*/decrypted"): ok(),
            re.compile(r".*scylla sstable upgrade.*"): ok("Done."),
            re.compile(r"ls .*/decrypted/"): ok(""),
            re.compile(r"rm -f /tmp/sct_decrypt_schema.*"): ok(),
        }
    )
    assert decrypt_sstables_on_node(node, "/snapshots/snap", keyspace="ks", table="tbl") is None


# ---------------------------------------------------------------------------
# decrypt_sstable_files_on_node – behaviour tests using FakeRemoter
# ---------------------------------------------------------------------------


def test_decrypt_sstable_files_on_node_happy_path(setup_node):
    """decrypt_sstable_files_on_node returns decrypted/ dir path on success."""
    data_file = "/var/lib/scylla/data/ks/tbl-uuid/me-big-Data.db"
    node, _ = setup_node(
        {
            re.compile(r"echo .* \| base64 -d > /tmp/sct_decrypt_schema.*"): ok(),
            re.compile(r"rm -rf .*/decrypted"): ok(),
            re.compile(r"mkdir -p .*/decrypted"): ok(),
            re.compile(r".*scylla sstable upgrade.*"): ok("Upgraded."),
            re.compile(r"ls .*/decrypted/"): ok("me-new-big-Data.db"),
            re.compile(r"rm -f /tmp/sct_decrypt_schema.*"): ok(),
        }
    )
    assert (
        decrypt_sstable_files_on_node(
            node=node, data_files=[data_file], schema_text=SCHEMA_WITH_ENCRYPTION, keyspace="ks", table="tbl"
        )
        == "/var/lib/scylla/data/ks/tbl-uuid/decrypted"
    )


def test_decrypt_sstable_files_on_node_empty_list_is_noop(setup_node):
    """decrypt_sstable_files_on_node returns None immediately when given an empty file list."""
    node, _ = setup_node({})
    assert (
        decrypt_sstable_files_on_node(node, data_files=[], schema_text=SCHEMA_NO_ENCRYPTION, keyspace="ks", table="tbl")
        is None
    )


def test_decrypt_sstable_files_on_node_upgrade_failure_returns_none(setup_node):
    """decrypt_sstable_files_on_node returns None when upgrade fails."""
    data_file = "/some/path/me-big-Data.db"
    node, _ = setup_node(
        {
            re.compile(r"echo .* \| base64 -d > /tmp/sct_decrypt_schema.*"): ok(),
            re.compile(r"rm -rf .*/decrypted"): ok(),
            re.compile(r"mkdir -p .*/decrypted"): ok(),
            re.compile(r".*scylla sstable upgrade.*"): fail("upgrade failed"),
            re.compile(r"rm -f /tmp/sct_decrypt_schema.*"): ok(),
        }
    )
    assert (
        decrypt_sstable_files_on_node(
            node=node, data_files=[data_file], schema_text=SCHEMA_NO_ENCRYPTION, keyspace="ks", table="tbl"
        )
        is None
    )

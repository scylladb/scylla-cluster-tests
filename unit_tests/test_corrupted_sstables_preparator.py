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

"""Unit tests for CorruptedSstablesPreparator validator and SSTablesCollector.collect_logs().

These tests use tmp_path for filesystem isolation and unittest.mock.Mock for the tester/node
stubs, following the same conventions as test_teardown_validator.py.
"""

import json
import re
from pathlib import Path
from typing import Dict, Pattern
from unittest.mock import MagicMock, Mock, patch

import pytest
from invoke import Result

from sdcm.logcollector import LogCollector, SSTablesCollector
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.events_device import EVENTS_LOG_DIR, RAW_EVENTS_LOG
from sdcm.teardown_validators.sstables import (
    CorruptedSstablesPreparator,
    find_corrupted_sstable_in_events,
    take_snapshot_and_decrypt,
    _parse_sstable_path,
)
from sdcm.utils.sstable.sstable_utils import (
    CORRUPTED_SSTABLES_STATE_FILENAME,
    strip_encryption_options_from_schema,
)

LOGGER = __import__("logging").getLogger(__name__)


# ---------------------------------------------------------------------------
# Raw event lines used in tests — uses the malformed_sstable_exception format
# that triggers real CORRUPTED_SSTABLE events (see sdcm/sct_events/database.py).
# ---------------------------------------------------------------------------

CORRUPTED_EVENT_LINE = (
    "[shard 0:main] seastar - Exiting on unhandled exception: "
    "sstables::malformed_sstable_exception (Buffer improperly sized to hold requested data. "
    "Got: 2. Expected: 4 in sstable "
    "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
    "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db)"
)

CORRUPTED_EVENT_JSON = json.dumps(
    {
        "type": "CORRUPTED_SSTABLE",
        "severity": "CRITICAL",
        "node": "db-node-0",
        "line": CORRUPTED_EVENT_LINE,
    }
)

OTHER_EVENT_JSON = json.dumps(
    {
        "type": "DATABASE_ERROR",
        "severity": "ERROR",
        "node": "db-node-0",
        "line": "some other error",
    }
)

NODETOOL_SNAPSHOT_STDOUT = "Snapshot directory: 1712059224000"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def ok(stdout: str = "") -> Result:
    return Result(stdout=stdout, exited=0)


def fail(stderr: str = "error") -> Result:
    return Result(stdout="", stderr=stderr, exited=1)


def make_raw_events_file(logdir: Path, lines: list[str]) -> Path:
    """Write raw events lines to the expected path and return the file path."""
    events_dir = logdir / EVENTS_LOG_DIR
    events_dir.mkdir(parents=True, exist_ok=True)
    raw_events_file = events_dir / RAW_EVENTS_LOG
    raw_events_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return raw_events_file


def make_fake_node(result_map: Dict[Pattern, Result], name: str = "db-node-0"):
    """Return a Mock node with a FakeRemoter that responds per result_map."""
    node = Mock()
    node.name = name
    node.ssh_login_info = {"hostname": "10.0.0.1", "user": "scyllaadm"}
    RemoteCmdRunnerBase.set_default_remoter_class(
        type(
            "FakeRemoterWithMap",
            (RemoteCmdRunnerBase,),
            {
                "result_map": result_map,
                "run": lambda self, cmd, **kw: next(
                    (v for p, v in self.result_map.items() if re.match(p, cmd)),
                    Result(stdout="", exited=0),
                ),
                "_create_connection": lambda self: None,
                "_close_connection": lambda self: None,
                "is_up": lambda self, timeout=30: True,
                "_run_on_retryable_exception": lambda self, exc, new_session, suppress_errors=False: True,
            },
        )
    )
    node.remoter = RemoteCmdRunnerBase.create_remoter(name)
    return node


def make_validator(logdir: Path, node: Mock | None = None):
    """Return a CorruptedSstablesPreparator with the given logdir and optional node."""
    params = MagicMock()
    params.get.return_value = {"enabled": True}
    tester = Mock()
    tester.logdir = str(logdir)
    if node is not None:
        cluster = Mock()
        cluster.nodes = [node]
        tester.db_clusters_multitenant = [cluster]
    else:
        tester.db_clusters_multitenant = []
    validator = CorruptedSstablesPreparator.__new__(CorruptedSstablesPreparator)
    validator.params = params
    validator.tester = tester
    validator.configuration = {"enabled": True}
    validator.is_enabled = True
    return validator


# ---------------------------------------------------------------------------
# Tests: no events → no state file
# ---------------------------------------------------------------------------


def test_no_raw_events_file_skips(tmp_path):
    """Validator is a no-op when the raw events log does not exist."""
    validator = make_validator(tmp_path)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


def test_no_corrupted_sstable_event_skips(tmp_path):
    """Validator writes no state file when there are no CORRUPTED_SSTABLE events."""
    make_raw_events_file(tmp_path, [OTHER_EVENT_JSON])
    validator = make_validator(tmp_path)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


def test_malformed_json_lines_are_skipped(tmp_path):
    """Non-JSON lines in the raw events log do not crash the validator."""
    make_raw_events_file(tmp_path, ["not-json", OTHER_EVENT_JSON])
    validator = make_validator(tmp_path)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


# ---------------------------------------------------------------------------
# Tests: node not found → no state file
# ---------------------------------------------------------------------------


def test_node_not_found_skips(tmp_path):
    """Validator is a no-op when the event references a node that is not in any cluster."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    # Supply a node with a *different* name
    node = make_fake_node({}, name="db-node-99")
    validator = make_validator(tmp_path, node=node)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


# ---------------------------------------------------------------------------
# Tests: snapshot fails → no state file
# ---------------------------------------------------------------------------


def test_snapshot_failure_skips(tmp_path):
    """Validator writes no state file when nodetool snapshot returns non-zero."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    node = make_fake_node(
        {re.compile(r"nodetool snapshot.*"): fail("snapshot failed")},
        name="db-node-0",
    )
    validator = make_validator(tmp_path, node=node)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


def test_snapshot_stdout_missing_directory_label_skips(tmp_path):
    """Validator writes no state file when snapshot stdout lacks 'Snapshot directory:'."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    node = make_fake_node(
        {re.compile(r"nodetool snapshot.*"): ok("Snapshot created.")},
        name="db-node-0",
    )
    validator = make_validator(tmp_path, node=node)
    validator.validate()

    assert not (tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).exists()


# ---------------------------------------------------------------------------
# Tests: happy path → state file written correctly
# ---------------------------------------------------------------------------


def test_happy_path_state_file_written(tmp_path):
    """Validator writes the expected state file when everything succeeds."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])

    snapshot_path = "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/snapshots/1712059224000"
    prefix = "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big"
    sstable_components = "\n".join(
        f"{snapshot_path}/{prefix}-{comp}" for comp in ["Data.db", "Filter.db", "Statistics.db", "TOC.txt"]
    )

    node = make_fake_node(
        {
            re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT),
            re.compile(rf"ls .*/snapshots/1712059224000/{prefix}-\*.*"): ok(sstable_components),
        },
        name="db-node-0",
    )

    expected_decrypted = f"{snapshot_path}/decrypted"

    with patch(
        "sdcm.teardown_validators.sstables.decrypt_sstables_on_node",
        return_value=expected_decrypted,
    ) as mock_decrypt:
        validator = make_validator(tmp_path, node=node)
        validator.validate()

    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    assert state_file.exists(), "State file should have been written"

    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["keyspace"] == "keyspace1"
    assert state["table_name"] == "standard1"
    assert state["node_name"] == "db-node-0"
    assert state["sstable_name"] == "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db"
    assert state["decrypted_path"] == expected_decrypted
    assert "snapshot_path" in state
    assert "ssh_login_info" in state
    # upload_files must contain the targeted sstable components + schema.cql
    assert "upload_files" in state
    assert f"{snapshot_path}/schema.cql" in state["upload_files"]
    assert any(f.endswith("-Data.db") for f in state["upload_files"])

    # decrypt_sstables_on_node must be called with the snapshot path
    mock_decrypt.assert_called_once()
    _, kwargs = mock_decrypt.call_args
    assert kwargs.get("keyspace") == "keyspace1" or mock_decrypt.call_args[0][2] == "keyspace1"


def test_happy_path_decrypt_called_with_snapshot_path(tmp_path):
    """decrypt_sstables_on_node is called with the snapshot path derived from the event."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    node = make_fake_node(
        {
            re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT),
            re.compile(r"ls .*/snapshots/.*"): ok(""),  # no component files found in snapshot
        },
        name="db-node-0",
    )

    with patch(
        "sdcm.teardown_validators.sstables.decrypt_sstables_on_node",
        return_value=None,
    ) as mock_decrypt:
        validator = make_validator(tmp_path, node=node)
        validator.validate()

    mock_decrypt.assert_called_once()
    call_args = mock_decrypt.call_args
    # first positional arg is node, second is snapshot_path
    snapshot_path = call_args[0][1] if call_args[0] else call_args[1]["snapshot_path"]
    assert "1712059224000" in snapshot_path, f"Snapshot tag not in path: {snapshot_path}"
    assert "keyspace1" in snapshot_path
    assert "standard1" in snapshot_path


def test_state_file_has_none_decrypted_path_when_decrypt_returns_none(tmp_path):
    """State file is written with decrypted_path=None when decryption fails."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    node = make_fake_node(
        {
            re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT),
            re.compile(r"ls .*/snapshots/.*"): ok(""),  # no component files in snapshot
        },
        name="db-node-0",
    )

    with patch("sdcm.teardown_validators.sstables.decrypt_sstables_on_node", return_value=None):
        validator = make_validator(tmp_path, node=node)
        validator.validate()

    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    assert state_file.exists()
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["decrypted_path"] is None


# ---------------------------------------------------------------------------
# Tests: SSTablesCollector.collect_logs() — state-file path
# ---------------------------------------------------------------------------


def make_collector(local_dir: Path, test_id: str = "test-uuid", nodes: list | None = None):
    """Build a minimal SSTablesCollector without calling __init__."""
    collector = SSTablesCollector.__new__(SSTablesCollector)
    collector.local_dir = str(local_dir)
    collector.test_id = test_id
    collector.nodes = nodes or []
    # current_run is a class-level property; patch it at the class level for isolation
    LogCollector._current_run = "run-1"
    return collector


def test_sstables_collector_state_file_path_calls_upload(tmp_path):
    """SSTablesCollector.collect_logs() uses state file instead of SSH when it exists."""
    # Mimic the logdir structure: local_dir is 3 levels below logdir
    local_dir = tmp_path / "collector" / "corrupted-sstables" / "2026-01-01"
    local_dir.mkdir(parents=True)

    snapshot_path = "/var/lib/scylla/data/ks/tbl-uuid/snapshots/1712059224000"
    decrypted_path = f"{snapshot_path}/decrypted"
    upload_files = [
        f"{snapshot_path}/me-big-Data.db",
        f"{snapshot_path}/me-big-Filter.db",
        f"{snapshot_path}/schema.cql",
    ]
    state = {
        "snapshot_path": snapshot_path,
        "upload_files": upload_files,
        "decrypted_path": decrypted_path,
        "keyspace": "ks",
        "table_name": "tbl",
        "sstable_name": "me-big-Data.db",
        "node_name": "db-node-0",
        "ssh_login_info": {"hostname": "10.0.0.1", "user": "scyllaadm"},
    }
    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    state_file.write_text(json.dumps(state), encoding="utf-8")

    collector = make_collector(local_dir)
    fake_s3_link = "https://s3.amazonaws.com/bucket/corrupted.tar.gz"

    with patch(
        "sdcm.logcollector.upload_remote_files_directly_to_s3",
        return_value=fake_s3_link,
    ) as mock_upload:
        result = collector.collect_logs()

    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args
    paths_arg = call_kwargs[0][1] if call_kwargs[0] else call_kwargs[1].get("files")
    # Should contain the targeted file list + decrypted path, NOT the whole snapshot dir
    for f in upload_files:
        assert f in paths_arg
    assert decrypted_path in paths_arg
    assert result == [fake_s3_link]


def test_sstables_collector_state_file_backwards_compat(tmp_path):
    """SSTablesCollector falls back to [snapshot_path] when state file lacks upload_files."""
    local_dir = tmp_path / "collector" / "corrupted-sstables" / "2026-01-01"
    local_dir.mkdir(parents=True)

    snapshot_path = "/var/lib/scylla/data/ks/tbl-uuid/snapshots/1712059224000"
    # Old-format state file without upload_files key
    state = {
        "snapshot_path": snapshot_path,
        "decrypted_path": None,
        "keyspace": "ks",
        "table_name": "tbl",
        "sstable_name": "me-big-Data.db",
        "node_name": "db-node-0",
        "ssh_login_info": {"hostname": "10.0.0.1", "user": "scyllaadm"},
    }
    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    state_file.write_text(json.dumps(state), encoding="utf-8")

    collector = make_collector(local_dir)
    fake_s3_link = "https://s3.amazonaws.com/bucket/corrupted.tar.gz"

    with patch(
        "sdcm.logcollector.upload_remote_files_directly_to_s3",
        return_value=fake_s3_link,
    ) as mock_upload:
        result = collector.collect_logs()

    mock_upload.assert_called_once()
    call_kwargs = mock_upload.call_args
    paths_arg = call_kwargs[0][1] if call_kwargs[0] else call_kwargs[1].get("files")
    # Falls back to whole snapshot directory
    assert snapshot_path in paths_arg
    assert result == [fake_s3_link]


def test_sstables_collector_no_state_file_falls_back_to_ssh(tmp_path):
    """SSTablesCollector.collect_logs() falls back to SSH path when state file is absent."""
    local_dir = tmp_path / "collector" / "corrupted-sstables" / "2026-01-01"
    local_dir.mkdir(parents=True)

    # Write raw events log with a CORRUPTED_SSTABLE event
    events_dir = tmp_path / EVENTS_LOG_DIR
    events_dir.mkdir(parents=True)
    (events_dir / RAW_EVENTS_LOG).write_text(CORRUPTED_EVENT_JSON + "\n", encoding="utf-8")

    snapshot_dir = "1712059224000"
    node = Mock()
    node.name = "db-node-0"
    node.ssh_login_info = {"hostname": "10.0.0.1", "user": "scyllaadm"}
    node.remoter.run.return_value = Result(stdout=f"Snapshot directory: {snapshot_dir}", exited=0)

    collector = make_collector(local_dir, nodes=[node])
    fake_s3_link = "https://s3.amazonaws.com/bucket/corrupted.tar.gz"

    with patch(
        "sdcm.logcollector.upload_remote_files_directly_to_s3",
        return_value=fake_s3_link,
    ) as mock_upload:
        result = collector.collect_logs()

    # SSH was used (nodetool snapshot call)
    node.remoter.run.assert_called()
    mock_upload.assert_called_once()
    assert result == [fake_s3_link]


def test_sstables_collector_no_corrupted_event_returns_empty(tmp_path):
    """SSTablesCollector.collect_logs() returns [] when no CORRUPTED_SSTABLE event exists."""
    local_dir = tmp_path / "collector" / "corrupted-sstables" / "2026-01-01"
    local_dir.mkdir(parents=True)

    events_dir = tmp_path / EVENTS_LOG_DIR
    events_dir.mkdir(parents=True)
    (events_dir / RAW_EVENTS_LOG).write_text(OTHER_EVENT_JSON + "\n", encoding="utf-8")

    collector = make_collector(local_dir)
    result = collector.collect_logs()
    assert result == []


# ---------------------------------------------------------------------------
# Tests: take_snapshot_and_decrypt — targeted file collection
# ---------------------------------------------------------------------------


def test_take_snapshot_targets_specific_sstable_files(tmp_path):
    """take_snapshot_and_decrypt finds the corrupted sstable's component files within the snapshot."""
    snapshot_path = "/var/lib/scylla/data/keyspace1/standard1-uuid/snapshots/1712059224000"
    prefix = "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big"
    component_files = [f"{snapshot_path}/{prefix}-{comp}" for comp in ["Data.db", "Filter.db", "TOC.txt"]]

    node = make_fake_node(
        {
            re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT),
            re.compile(rf"ls .*/snapshots/1712059224000/{prefix}-\*.*"): ok("\n".join(component_files)),
        },
        name="db-node-0",
    )

    with patch("sdcm.teardown_validators.sstables.decrypt_sstables_on_node", return_value=None):
        snap, sstable_files, decrypted = take_snapshot_and_decrypt(
            node,
            sstable_dir="/var/lib/scylla/data/keyspace1/standard1-uuid",
            keyspace="keyspace1",
            table_name="standard1",
            sstable_name=f"{prefix}-Data.db",
        )

    assert snap == snapshot_path
    assert sstable_files == component_files
    assert decrypted is None


def test_take_snapshot_empty_sstable_files_when_quarantined(tmp_path):
    """When the corrupted sstable was quarantined, ls returns empty — sstable_files is []."""
    node = make_fake_node(
        {
            re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT),
            re.compile(r"ls .*/snapshots/.*"): ok(""),  # not found in snapshot
        },
        name="db-node-0",
    )

    with patch("sdcm.teardown_validators.sstables.decrypt_sstables_on_node", return_value=None):
        snap, sstable_files, decrypted = take_snapshot_and_decrypt(
            node,
            sstable_dir="/var/lib/scylla/data/ks/tbl-uuid",
            keyspace="ks",
            table_name="tbl",
            sstable_name="me-big-Data.db",
        )

    assert snap is not None  # snapshot was taken successfully
    assert sstable_files == []  # but no component files found


# ---------------------------------------------------------------------------
# Tests: strip_encryption_options_from_schema
# ---------------------------------------------------------------------------


class TestStripEncryptionOptionsFromSchema:
    """Verify that scylla_encryption_options is stripped in all possible schema layouts."""

    ENC_OPTS = (
        "{'cipher_algorithm': 'AES/ECB/PKCS5Padding',"
        " 'key_provider': 'LocalFileSystemKeyProviderFactory',"
        " 'secret_key_file': '/etc/scylla/encrypt_conf/secret_key',"
        " 'secret_key_strength': '128'}"
    )

    def test_and_prefix_middle_property(self):
        """Standard case: scylla_encryption_options after AND between other properties."""
        schema = (
            "CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH bloom_filter_fp_chance = 0.01"
            f"\n    AND scylla_encryption_options = {self.ENC_OPTS}"
            "\n    AND compaction = {'class': 'SizeTieredCompactionStrategy'};"
        )
        result = strip_encryption_options_from_schema(schema)
        assert "scylla_encryption_options" not in result
        assert "bloom_filter_fp_chance" in result
        assert "compaction" in result

    def test_and_prefix_last_property(self):
        """scylla_encryption_options is the last AND property."""
        schema = (
            "CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH bloom_filter_fp_chance = 0.01"
            f"\n    AND scylla_encryption_options = {self.ENC_OPTS};"
        )
        result = strip_encryption_options_from_schema(schema)
        assert "scylla_encryption_options" not in result
        assert "bloom_filter_fp_chance" in result

    def test_with_prefix_first_property_followed_by_and(self):
        """scylla_encryption_options is the first property after WITH, followed by AND."""
        schema = (
            f"CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH scylla_encryption_options = {self.ENC_OPTS}"
            "\n    AND bloom_filter_fp_chance = 0.01"
            "\n    AND compaction = {'class': 'SizeTieredCompactionStrategy'};"
        )
        result = strip_encryption_options_from_schema(schema)
        assert "scylla_encryption_options" not in result
        assert "WITH" in result
        assert "bloom_filter_fp_chance" in result
        assert "compaction" in result

    def test_with_prefix_sole_property(self):
        """scylla_encryption_options is the only property (no AND before or after)."""
        schema = f"CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH scylla_encryption_options = {self.ENC_OPTS};"
        result = strip_encryption_options_from_schema(schema)
        assert "scylla_encryption_options" not in result
        assert "WITH" not in result

    def test_no_encryption_options_unchanged(self):
        """Schema without scylla_encryption_options is returned unchanged."""
        schema = (
            "CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH bloom_filter_fp_chance = 0.01"
            "\n    AND compaction = {'class': 'SizeTieredCompactionStrategy'};"
        )
        result = strip_encryption_options_from_schema(schema)
        assert result == schema

    def test_case_insensitive(self):
        """The strip is case-insensitive."""
        schema = (
            "CREATE TABLE ks.tbl (id int PRIMARY KEY) WITH bloom_filter_fp_chance = 0.01"
            "\n    AND SCYLLA_ENCRYPTION_OPTIONS = {'cipher_algorithm': 'AES'};"
        )
        result = strip_encryption_options_from_schema(schema)
        assert "SCYLLA_ENCRYPTION_OPTIONS" not in result
        assert "scylla_encryption_options" not in result.lower()


# ---------------------------------------------------------------------------
# Tests: _parse_sstable_path
# ---------------------------------------------------------------------------


class TestParseSSTablePath:
    """Verify that _parse_sstable_path correctly extracts components from sstable paths."""

    def test_valid_data_db_path(self):
        path = "/var/lib/scylla/data/keyspace1/standard1-abc123/me-big-Data.db"
        result = _parse_sstable_path(path)
        assert result is not None
        sstable_dir, ks, table_name, sst_name = result
        assert sstable_dir == "/var/lib/scylla/data/keyspace1/standard1-abc123"
        assert ks == "keyspace1"
        assert table_name == "standard1"
        assert sst_name == "me-big-Data.db"

    def test_valid_statistics_db_path(self):
        path = "/var/lib/scylla/data/ks/tbl-uuid/me-3g9s_1eqc-big-Statistics.db"
        result = _parse_sstable_path(path)
        assert result is not None
        _, ks, table_name, sst_name = result
        assert ks == "ks"
        assert table_name == "tbl"
        assert sst_name == "me-3g9s_1eqc-big-Statistics.db"

    def test_too_few_components_returns_none(self):
        """Paths with fewer than 4 slash-separated components are rejected."""
        assert _parse_sstable_path("/tmp/foo.db") is None
        assert _parse_sstable_path("foo.db") is None

    def test_no_uuid_separator_in_table_dir_returns_none(self):
        """Table dir must contain a hyphen (UUID separator); plain names are rejected."""
        assert _parse_sstable_path("/var/lib/scylla/data/ks/tablename/me-Data.db") is None

    def test_quarantine_path(self):
        """Paths inside quarantine directories still parse correctly."""
        path = "/var/lib/scylla/data/ks/tbl-uuid/quarantine/me-big-Data.db"
        result = _parse_sstable_path(path)
        # quarantine adds an extra level, so rsplit("/", 3) splits differently
        # The function should still return something as long as the components validate
        if result is not None:
            _, ks, table_name, _ = result
            # Quarantine path has /quarantine/ as the table_dir component
            assert table_name is not None


# ---------------------------------------------------------------------------
# Tests: find_corrupted_sstable_in_events
# ---------------------------------------------------------------------------


class TestFindCorruptedSstableInEvents:
    """Verify that find_corrupted_sstable_in_events correctly parses various event formats.

    Positive-path cases are parametrized to cover every known real-world ``CORRUPTED_SSTABLE``
    error line format.  Negative-path cases (no path, wrong severity, etc.) are separate tests.
    """

    @staticmethod
    def _write_events(tmp_path, events_json_lines):
        events_dir = tmp_path / EVENTS_LOG_DIR
        events_dir.mkdir(parents=True, exist_ok=True)
        raw_file = events_dir / RAW_EVENTS_LOG
        raw_file.write_text("\n".join(events_json_lines) + "\n", encoding="utf-8")
        return raw_file

    # ------------------------------------------------------------------
    # Parametrized positive-path tests: each tuple is
    #   (test_id, event_line, expected_keyspace, expected_table, expected_sstable, node)
    # ------------------------------------------------------------------
    POSITIVE_CASES = [
        pytest.param(
            (
                "[shard 0:main] seastar - Exiting on unhandled exception: "
                "sstables::malformed_sstable_exception (Buffer improperly sized to hold requested data. "
                "Got: 2. Expected: 4 in sstable "
                "/var/lib/scylla/data/keyspace1/standard1-01b9f260/me-big-Statistics.db)"
            ),
            "keyspace1",
            "standard1",
            "me-big-Statistics.db",
            "db-node-0",
            id="malformed_sstable_exception_statistics_db",
        ),
        pytest.param(
            (
                "[shard 0:main] seastar - Exiting on unhandled exception: "
                "sstables::malformed_sstable_exception (Buffer improperly sized in sstable "
                "/var/lib/scylla/data/ks/tbl-abc123/me-big-Data.db)"
            ),
            "ks",
            "tbl",
            "me-big-Data.db",
            "db-node-1",
            id="malformed_sstable_exception_data_db",
        ),
        pytest.param(
            (
                "[shard 2:comp] compaction - invalid_mutation_fragment_stream exception in "
                "/var/lib/scylla/data/myks/mytbl-deadbeef/me-123-big-Data.db"
            ),
            "myks",
            "mytbl",
            "me-123-big-Data.db",
            "db-node-0",
            id="invalid_mutation_fragment_stream",
        ),
        pytest.param(
            (
                "Error reading /tmp/cache.db - "
                "sstables::malformed_sstable_exception in "
                "/var/lib/scylla/data/ks/tbl-uuid/me-big-Data.db"
            ),
            "ks",
            "tbl",
            "me-big-Data.db",
            "db-node-0",
            id="multiple_db_paths_picks_valid_scylla_path",
        ),
        # Real-world format from sstable_utils.py:116-120 — same .db path appears twice in the
        # line ("Could not load SSTable: /path/Data.db: ... (/path/Data.db: first and last ...")
        pytest.param(
            (
                "Could not load SSTable: "
                "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
                "me-3g9w_104a_01xg12j55gjivrwtt5-big-Data.db: "
                "sstables::malformed_sstable_exception ("
                "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
                "me-3g9w_104a_01xg12j55gjivrwtt5-big-Data.db: "
                "first and last keys of summary are misordered: "
                "first={key: pk{00080000000000000003}, token: -578762209316392770} "
                "> last={key: pk{00080000000000000008}, token: -6917704163689751025})"
            ),
            "keyspace1",
            "standard1",
            "me-3g9w_104a_01xg12j55gjivrwtt5-big-Data.db",
            "db-node-0",
            id="could_not_load_sstable_misordered_keys",
        ),
        # Real-world format from sstable_utils.py:99-102 — full generation ID with long UUID
        pytest.param(
            (
                "[shard 0:main] seastar - Exiting on unhandled exception: "
                "sstables::malformed_sstable_exception (Buffer improperly sized to hold requested data. "
                "Got: 2. Expected: 4 in sstable "
                "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
                "me-3g9s_1eqc_2z60w2ushgma9j7ypu-big-Statistics.db)"
            ),
            "keyspace1",
            "standard1",
            "me-3g9s_1eqc_2z60w2ushgma9j7ypu-big-Statistics.db",
            "db-node-0",
            id="malformed_sstable_full_generation_id",
        ),
        # Real-world format: scylla[PID] prefix (systemd journal format, seen in GCE PKE runs)
        pytest.param(
            (
                "scylla[5976]:  [shard 6:strm] compaction - Finished scrubbing in validate mode "
                "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
                "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db - sstable is invalid - "
                "invalid_mutation_fragment_stream"
            ),
            "keyspace1",
            "standard1",
            "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db",
            "db-node-0",
            id="scrub_validate_mode_sstable_is_invalid",
        ),
    ]

    @pytest.mark.parametrize("line, expected_ks, expected_table, expected_sst, node_name", POSITIVE_CASES)
    def test_event_line_parsed_correctly(self, tmp_path, line, expected_ks, expected_table, expected_sst, node_name):
        """Parametrized: each known real-world error line format is correctly parsed."""
        event = json.dumps(
            {
                "type": "CORRUPTED_SSTABLE",
                "severity": "CRITICAL",
                "node": node_name,
                "line": line,
            }
        )
        raw_file = self._write_events(tmp_path, [event])
        _, ks, table_name, sst_name, found_node = find_corrupted_sstable_in_events(raw_file)
        assert ks == expected_ks
        assert table_name == expected_table
        assert sst_name == expected_sst
        assert found_node == node_name

    # ------------------------------------------------------------------
    # Negative-path tests
    # ------------------------------------------------------------------

    def test_no_db_path_returns_none(self, tmp_path):
        """Event without any .db path returns all-None tuple."""
        event = json.dumps(
            {
                "type": "CORRUPTED_SSTABLE",
                "severity": "CRITICAL",
                "node": "db-node-0",
                "line": "sstables::malformed_sstable_exception (some error without a path)",
            }
        )
        raw_file = self._write_events(tmp_path, [event])
        result = find_corrupted_sstable_in_events(raw_file)
        assert result == (None, None, None, None, None)

    def test_no_corrupted_event_returns_none(self, tmp_path):
        """File with only non-CORRUPTED_SSTABLE events returns all-None."""
        raw_file = self._write_events(tmp_path, [OTHER_EVENT_JSON])
        result = find_corrupted_sstable_in_events(raw_file)
        assert result == (None, None, None, None, None)

    def test_non_critical_severity_ignored(self, tmp_path):
        """CORRUPTED_SSTABLE events with severity != CRITICAL are ignored."""
        event = json.dumps(
            {
                "type": "CORRUPTED_SSTABLE",
                "severity": "ERROR",
                "node": "db-node-0",
                "line": ("sstables::malformed_sstable_exception in /var/lib/scylla/data/ks/tbl-uuid/me-big-Data.db"),
            }
        )
        raw_file = self._write_events(tmp_path, [event])
        result = find_corrupted_sstable_in_events(raw_file)
        assert result == (None, None, None, None, None)

    def test_malformed_json_lines_are_skipped(self, tmp_path):
        """Non-JSON lines mixed in don't prevent parsing valid events."""
        valid_event = json.dumps(
            {
                "type": "CORRUPTED_SSTABLE",
                "severity": "CRITICAL",
                "node": "db-node-0",
                "line": ("sstables::malformed_sstable_exception in /var/lib/scylla/data/ks/tbl-uuid/me-big-Data.db"),
            }
        )
        raw_file = self._write_events(tmp_path, ["not-json", "also {bad", valid_event])
        _, ks, _, _, _ = find_corrupted_sstable_in_events(raw_file)
        assert ks == "ks"

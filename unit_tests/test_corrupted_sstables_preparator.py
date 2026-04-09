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

from invoke import Result

from sdcm.logcollector import LogCollector, SSTablesCollector
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.events_device import EVENTS_LOG_DIR, RAW_EVENTS_LOG
from sdcm.teardown_validators.sstables import CorruptedSstablesPreparator
from sdcm.utils.sstable.sstable_utils import CORRUPTED_SSTABLES_STATE_FILENAME

LOGGER = __import__("logging").getLogger(__name__)


# ---------------------------------------------------------------------------
# Raw event lines used in tests
# ---------------------------------------------------------------------------

CORRUPTED_EVENT_LINE = (
    "INFO  2024-04-02 12:40:24,787 [shard 0:stre] sstable - Moving sstable "
    "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/"
    "me-3gn1_0bxv_16fs02rr7ufpbjejzy-big-Data.db to quarantine"
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
    node = make_fake_node(
        {re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT)},
        name="db-node-0",
    )

    expected_decrypted = (
        "/var/lib/scylla/data/keyspace1/standard1-01b9f260d55311ef8caf5992914eabbe/snapshots/1712059224000/decrypted"
    )

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

    # decrypt_sstables_on_node must be called with the snapshot path
    mock_decrypt.assert_called_once()
    _, kwargs = mock_decrypt.call_args
    assert kwargs.get("keyspace") == "keyspace1" or mock_decrypt.call_args[0][2] == "keyspace1"


def test_happy_path_decrypt_called_with_snapshot_path(tmp_path):
    """decrypt_sstables_on_node is called with the snapshot path derived from the event."""
    make_raw_events_file(tmp_path, [CORRUPTED_EVENT_JSON])
    node = make_fake_node(
        {re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT)},
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
        {re.compile(r"nodetool snapshot.*"): ok(NODETOOL_SNAPSHOT_STDOUT)},
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
    state = {
        "snapshot_path": snapshot_path,
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
    assert snapshot_path in paths_arg
    assert decrypted_path in paths_arg
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

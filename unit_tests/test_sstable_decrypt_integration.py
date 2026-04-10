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

"""Integration tests for the CorruptedSstablesPreparator → state file → SSTablesCollector pipeline.

Starts a real ScyllaDB Docker container with encryption-at-rest (EAR) enabled, creates encrypted
sstables, then exercises the complete new code path end-to-end using only the real constructors:

  1. CorruptedSstablesPreparator(params, tester).validate()
       reads raw events log → nodetool snapshot → decrypt_sstables_on_node → writes state file
  2. SSTablesCollector(nodes, test_id, storage_dir, params).collect_logs()
       reads state file → calls upload_remote_files_directly_to_s3 with snapshot + decrypted paths

Run with:
    pytest -m integration unit_tests/test_sstable_decrypt_integration.py -v

Requirements:
    - Docker available
    - scylladb/scylla:2026.1.0 image present (or pullable)
"""

import base64
import json
import logging
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from sdcm.logcollector import SSTablesCollector
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_events.events_device import EVENTS_LOG_DIR, RAW_EVENTS_LOG
from sdcm.teardown_validators.sstables import CorruptedSstablesPreparator, take_snapshot_and_decrypt
from sdcm.utils.sstable.sstable_utils import CORRUPTED_SSTABLES_STATE_FILENAME, decrypt_sstables_on_node

LOGGER = logging.getLogger(__name__)

SCYLLA_IMAGE = "scylladb/scylla:2026.1.0"
CONTAINER_NAME = "sct-ear-integ-test"

# AES-128 key for encryption (base64-encoded 16 bytes)
AES_KEY_B64 = base64.b64encode(os.urandom(16)).decode()

# scylla.yaml fragment that enables node-level EAR
EAR_YAML = """\
user_info_encryption:
  enabled: true
  key_provider: LocalFileSystemKeyProviderFactory
  secret_key_file: /etc/scylla/encrypt_conf/secret_key
system_info_encryption:
  enabled: true
  key_provider: LocalFileSystemKeyProviderFactory
  secret_key_file: /etc/scylla/encrypt_conf/secret_key
"""

# Secret key file content: <algorithm>:<key_strength>:<base64_key>
SECRET_KEY_CONTENT = f"AES/ECB/PKCS5Padding:128:{AES_KEY_B64}"

KEYSPACE = "test_ks_ear"
TABLE_NODE = "test_node_enc"


def docker_available() -> bool:
    return shutil.which("docker") is not None


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not docker_available(), reason="Docker not available"),
]


# ---------------------------------------------------------------------------
# SCTConfiguration subclass that enables CorruptedSstablesPreparator
# ---------------------------------------------------------------------------


class PreparatorSCTConfig(SCTConfiguration):
    """Minimal SCTConfiguration that enables the corrupted_sstables_preparator validator."""

    def _load_environment_variables(self):
        return {"teardown_validators": {"corrupted_sstables_preparator": {"enabled": True}}}


# ---------------------------------------------------------------------------
# Node stub: delegates remoter.run() / sudo() to docker exec
# ---------------------------------------------------------------------------


class FakeResult:
    """Minimal Result-like object returned by DockerExecNode.run()."""

    def __init__(self, stdout: str, stderr: str, exited: int):
        self.stdout = stdout
        self.stderr = stderr
        self.exited = exited
        self.ok = exited == 0
        self.failed = exited != 0

    def __bool__(self):
        return self.ok


class DockerExecNode:
    """Node stub that delegates remoter.run() / sudo() to docker exec.

    ``self.remoter = self`` so that code calling ``node.remoter.run(...)`` works
    without any extra wiring.  No mocking: every method calls the real container.
    """

    def __init__(self, container_name: str, node_name: str = CONTAINER_NAME):
        self._container = container_name
        self.name = node_name
        self.ssh_login_info = {"hostname": "127.0.0.1", "user": "root"}
        self.remoter = self

    def _container_ip(self) -> str:
        result = subprocess.run(
            ["docker", "exec", self._container, "/bin/sh", "-c", "hostname -I | awk '{print $1}'"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def run(self, cmd: str, ignore_status: bool = False, verbose: bool = True, **_kwargs) -> FakeResult:
        """Execute *cmd* inside the container; prepend ``-h <ip>`` for nodetool commands."""
        if cmd.startswith("nodetool "):
            ip = self._container_ip()
            cmd = f"nodetool -h {ip} {cmd[len('nodetool ') :]}"
        proc = subprocess.run(
            ["docker", "exec", self._container, "/bin/sh", "-c", cmd],
            capture_output=True,
            text=True,
            check=False,
        )
        if verbose:
            LOGGER.debug("docker exec %s: exit=%d", cmd[:120], proc.returncode)
        result = FakeResult(stdout=proc.stdout, stderr=proc.stderr, exited=proc.returncode)
        if not ignore_status and proc.returncode != 0:
            raise RuntimeError(f"Command failed (exit {proc.returncode}): {cmd}\nstderr: {proc.stderr}")
        return result

    def sudo(self, cmd: str, **kwargs) -> FakeResult:
        """Docker exec already runs as root — no sudo prefix needed."""
        return self.run(cmd, **kwargs)

    def add_install_prefix(self, path: str) -> str:
        return path


# ---------------------------------------------------------------------------
# Container helpers
# ---------------------------------------------------------------------------


def wait_for_scylla(container: str, timeout: int = 120) -> None:
    """Poll until CQL port is accepting connections inside the container."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ["docker", "exec", container, "cqlsh", "-e", "SELECT now() FROM system.local"],
            check=False,
            capture_output=True,
        )
        if result.returncode == 0:
            LOGGER.info("ScyllaDB is up in container %s", container)
            return
        LOGGER.debug("Waiting for ScyllaDB CQL in %s …", container)
        time.sleep(3)
    raise TimeoutError(f"ScyllaDB did not start within {timeout}s in container {container}")


def cql(container: str, stmt: str) -> str:
    result = subprocess.run(
        ["docker", "exec", container, "cqlsh", "-e", stmt],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"cqlsh failed: {result.stderr}\nstmt: {stmt}")
    return result.stdout


def nodetool(container: str, cmd: str) -> str:
    ip_result = subprocess.run(
        ["docker", "exec", container, "/bin/sh", "-c", "hostname -I | awk '{print $1}'"],
        capture_output=True,
        text=True,
        check=True,
    )
    container_ip = ip_result.stdout.strip()
    result = subprocess.run(
        ["docker", "exec", container, "nodetool", "-h", container_ip] + cmd.split(),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"nodetool {cmd} failed: {result.stderr}")
    return result.stdout


def has_encryption_in_metadata(container: str, path: str, keyspace: str, table: str) -> bool:
    """Return True if any sstable in *path* has scylla_encryption_options in its metadata."""
    cmd = (
        f"SCYLLA_CONF=/etc/scylla "
        f"/usr/bin/scylla sstable dump-scylla-metadata "
        f"--keyspace {keyspace} --table {table} --sstables "
        f"{path}/*-Data.db 2>/dev/null || true"
    )
    result = subprocess.run(
        ["docker", "exec", container, "/bin/sh", "-c", cmd],
        check=False,
        capture_output=True,
    )
    return b"scylla_encryption_options" in result.stdout


def find_live_data_file(container: str, keyspace: str, table: str) -> str:
    """Return path to the first live *-Data.db for the given table (not inside a snapshot)."""
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "find",
            f"/var/lib/scylla/data/{keyspace}",
            "-name",
            "*-Data.db",
            "-path",
            f"*/{table}-*",
            "!",
            "-path",
            "*/snapshots/*",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    paths = [p.strip() for p in result.stdout.splitlines() if p.strip()]
    if not paths:
        raise RuntimeError(
            f"No live Data.db files found for {keyspace}.{table} in {container}.\nstderr: {result.stderr}"
        )
    return paths[0]


def write_corrupted_sstable_event(logdir: Path, data_file_path: str, node_name: str) -> None:
    """Write a synthetic CORRUPTED_SSTABLE raw event to the events log under *logdir*."""
    events_dir = logdir / EVENTS_LOG_DIR
    events_dir.mkdir(parents=True, exist_ok=True)
    event = {
        "type": "CORRUPTED_SSTABLE",
        "severity": "CRITICAL",
        "node": node_name,
        "line": (
            f"INFO  2024-04-02 12:40:24,787 [shard 0:stre] sstable - Moving sstable {data_file_path} to quarantine"
        ),
    }
    (events_dir / RAW_EVENTS_LOG).write_text(json.dumps(event) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Module-scoped fixture: one container for all tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def scylla_container(tmp_path_factory, request):
    """Start a ScyllaDB container with node-level EAR; yield container name; stop on teardown."""
    encrypt_conf_dir = tmp_path_factory.mktemp("encrypt_conf")
    encrypt_conf_dir.chmod(0o777)
    (encrypt_conf_dir / "secret_key").write_text(SECRET_KEY_CONTENT)
    (encrypt_conf_dir / "secret_key").chmod(0o666)
    (encrypt_conf_dir / "ear.yaml").write_text(EAR_YAML)
    (encrypt_conf_dir / "ear.yaml").chmod(0o666)

    request.addfinalizer(
        lambda: subprocess.run(["docker", "rm", "-f", CONTAINER_NAME], capture_output=True, check=False)
    )

    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            CONTAINER_NAME,
            "-v",
            f"{encrypt_conf_dir}:/etc/scylla/encrypt_conf:z",
            "--cpus=1",
            SCYLLA_IMAGE,
            "--smp=1",
            "--options-file=/etc/scylla/encrypt_conf/ear.yaml",
            "--developer-mode=1",
        ],
        check=True,
        capture_output=True,
    )
    wait_for_scylla(CONTAINER_NAME)

    cql(
        CONTAINER_NAME,
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
    )
    cql(CONTAINER_NAME, f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_NODE} (id int PRIMARY KEY, name text);")
    cql(CONTAINER_NAME, f"INSERT INTO {KEYSPACE}.{TABLE_NODE} (id, name) VALUES (1, 'node_enc');")
    nodetool(CONTAINER_NAME, f"flush {KEYSPACE} {TABLE_NODE}")

    yield CONTAINER_NAME


@pytest.fixture(scope="module")
def scylla_node(scylla_container):
    return DockerExecNode(scylla_container)


# ---------------------------------------------------------------------------
# Smoke test: decrypt_sstables_on_node works end-to-end on a real snapshot
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
def test_decrypt_sstables_on_node_smoke(scylla_container, scylla_node):
    """decrypt_sstables_on_node produces readable (unencrypted) sstables from an EAR snapshot."""
    snap_stdout = nodetool(scylla_container, f"snapshot -t integ-smoke -cf {TABLE_NODE} -- {KEYSPACE}")
    snap_tag = snap_stdout.split("Snapshot directory: ")[1].strip()

    result = subprocess.run(
        [
            "docker",
            "exec",
            scylla_container,
            "find",
            f"/var/lib/scylla/data/{KEYSPACE}",
            "-type",
            "d",
            "-name",
            snap_tag,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    snap_path = result.stdout.strip().splitlines()[0].strip()

    assert has_encryption_in_metadata(scylla_container, snap_path, KEYSPACE, TABLE_NODE), (
        "Expected encrypted sstables before decryption"
    )

    decrypted_path = decrypt_sstables_on_node(scylla_node, snap_path, keyspace=KEYSPACE, table=TABLE_NODE)

    assert decrypted_path is not None, "decrypt_sstables_on_node returned None"
    assert not has_encryption_in_metadata(scylla_container, decrypted_path, KEYSPACE, TABLE_NODE), (
        "Expected no encryption metadata after decryption"
    )


# ---------------------------------------------------------------------------
# take_snapshot_and_decrypt: direct integration test for the extracted helper
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
def test_take_snapshot_and_decrypt(scylla_container, scylla_node):
    """take_snapshot_and_decrypt() returns (snapshot_path, decrypted_path) with real sstables."""
    data_file = find_live_data_file(scylla_container, KEYSPACE, TABLE_NODE)
    # Derive sstable_dir: /var/lib/scylla/data/<ks>/<table-dir>
    sstable_dir = "/".join(data_file.split("/")[:7])

    snapshot_path, decrypted_path = take_snapshot_and_decrypt(
        node=scylla_node,
        sstable_dir=sstable_dir,
        keyspace=KEYSPACE,
        table_name=TABLE_NODE,
    )

    assert snapshot_path is not None, "take_snapshot_and_decrypt returned None snapshot_path"
    assert decrypted_path is not None, "take_snapshot_and_decrypt returned None decrypted_path"

    assert has_encryption_in_metadata(scylla_container, snapshot_path, KEYSPACE, TABLE_NODE), (
        f"Snapshot at {snapshot_path} should still be encrypted"
    )
    assert not has_encryption_in_metadata(scylla_container, decrypted_path, KEYSPACE, TABLE_NODE), (
        f"Decrypted directory {decrypted_path} should have no encryption metadata"
    )


# ---------------------------------------------------------------------------
# CorruptedSstablesPreparator.validate(): state file written with correct fields
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
def test_preparator_writes_correct_state_file(scylla_container, scylla_node, tmp_path):
    """CorruptedSstablesPreparator(params, tester).validate() writes a correct state file.

    Uses the real __init__ constructor; tester.logdir is tmp_path; tester.db_clusters_multitenant
    contains the real DockerExecNode so nodetool snapshot and decryption run against the container.
    """
    data_file = find_live_data_file(scylla_container, KEYSPACE, TABLE_NODE)
    write_corrupted_sstable_event(tmp_path, data_file, scylla_node.name)

    cluster = Mock()
    cluster.nodes = [scylla_node]
    tester = Mock()
    tester.logdir = str(tmp_path)
    tester.db_clusters_multitenant = [cluster]

    params = PreparatorSCTConfig()
    validator = CorruptedSstablesPreparator(params, tester)

    assert validator.is_enabled, "Validator must be enabled for this test"
    validator.validate()

    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    assert state_file.exists(), "State file was not written by CorruptedSstablesPreparator.validate()"

    state = json.loads(state_file.read_text(encoding="utf-8"))
    LOGGER.info("State file contents: %s", state)

    assert state["keyspace"] == KEYSPACE
    assert state["table_name"] == TABLE_NODE
    assert state["node_name"] == scylla_node.name
    assert state["sstable_name"].endswith("-Data.db"), (
        f"sstable_name should be a Data.db filename, got: {state['sstable_name']}"
    )
    assert state["snapshot_path"], "snapshot_path must be non-empty"
    assert "ssh_login_info" in state


# ---------------------------------------------------------------------------
# CorruptedSstablesPreparator.validate(): snapshot is encrypted, decrypted is not
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
def test_preparator_decrypts_snapshot(scylla_container, scylla_node, tmp_path):
    """validate() produces a snapshot (still encrypted) and a decrypted sibling directory.

    Verifies encryption metadata is present in the snapshot and absent in the decrypted output.
    """
    data_file = find_live_data_file(scylla_container, KEYSPACE, TABLE_NODE)
    write_corrupted_sstable_event(tmp_path, data_file, scylla_node.name)

    cluster = Mock()
    cluster.nodes = [scylla_node]
    tester = Mock()
    tester.logdir = str(tmp_path)
    tester.db_clusters_multitenant = [cluster]

    CorruptedSstablesPreparator(PreparatorSCTConfig(), tester).validate()

    state = json.loads((tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME).read_text(encoding="utf-8"))

    assert state["decrypted_path"] is not None, (
        "decrypted_path is None — decrypt_sstables_on_node failed inside validate()"
    )
    assert has_encryption_in_metadata(scylla_container, state["snapshot_path"], KEYSPACE, TABLE_NODE), (
        f"Snapshot at {state['snapshot_path']} should still be encrypted"
    )
    assert not has_encryption_in_metadata(scylla_container, state["decrypted_path"], KEYSPACE, TABLE_NODE), (
        f"Decrypted directory {state['decrypted_path']} should have no encryption metadata"
    )


# ---------------------------------------------------------------------------
# Full pipeline: validate() → state file → collect_logs() passes correct paths to upload
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
def test_full_pipeline_collect_logs_uses_state_file(scylla_container, scylla_node, tmp_path):
    """End-to-end: validate() → state file → collect_logs() passes snapshot+decrypted to upload.

    Phase 1 — CorruptedSstablesPreparator(params, tester).validate() runs while the node is
    alive (simulates teardown before stop_resources).  The state file is written to tmp_path.

    Phase 2 — SSTablesCollector(nodes=[], test_id, storage_dir=tmp_path, params=None).collect_logs()
    runs with no live nodes (simulates post-stop_resources).  It reads the state file and must
    call upload_remote_files_directly_to_s3 with both snapshot_path and decrypted_path.
    """
    # Phase 1: validator runs while node is alive
    data_file = find_live_data_file(scylla_container, KEYSPACE, TABLE_NODE)
    write_corrupted_sstable_event(tmp_path, data_file, scylla_node.name)

    cluster = Mock()
    cluster.nodes = [scylla_node]
    tester = Mock()
    tester.logdir = str(tmp_path)
    tester.db_clusters_multitenant = [cluster]

    CorruptedSstablesPreparator(PreparatorSCTConfig(), tester).validate()

    state_file = tmp_path / CORRUPTED_SSTABLES_STATE_FILENAME
    assert state_file.exists(), "State file must exist before collect_logs() is called"
    state = json.loads(state_file.read_text(encoding="utf-8"))
    assert state["decrypted_path"] is not None, "decrypted_path is None in state — decryption failed during validate()"
    LOGGER.info("State after validate(): %s", state)

    # Phase 2: collector runs with no live node (node has been stopped).
    # In production: storage_dir = tester.logdir / "collected_logs"
    # local_dir = storage_dir/<run>/<type>-<id[:8]>  (3 levels under tester.logdir)
    # collect_logs() derives logdir as Path(local_dir).parent.parent.parent == tester.logdir
    storage_dir = tmp_path / "collected_logs"
    storage_dir.mkdir()
    test_id = str(uuid.uuid4())
    collector = SSTablesCollector(nodes=[], test_id=test_id, storage_dir=str(storage_dir), params=None)
    captured: dict = {}

    def fake_upload(ssh_login_info, paths, **_kwargs):
        captured["ssh_login_info"] = ssh_login_info
        captured["paths"] = list(paths)
        return "https://fake-s3/corrupted.tar.gz"

    with patch("sdcm.logcollector.upload_remote_files_directly_to_s3", side_effect=fake_upload):
        result = collector.collect_logs()

    assert result == ["https://fake-s3/corrupted.tar.gz"], f"Unexpected collect_logs result: {result}"
    assert "paths" in captured, "upload_remote_files_directly_to_s3 was never called"

    upload_paths = captured["paths"]
    LOGGER.info("Paths passed to upload: %s", upload_paths)

    assert state["snapshot_path"] in upload_paths, (
        f"snapshot_path {state['snapshot_path']} not in upload paths: {upload_paths}"
    )
    assert state["decrypted_path"] in upload_paths, (
        f"decrypted_path {state['decrypted_path']} not in upload paths: {upload_paths}"
    )

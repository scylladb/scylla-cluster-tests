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

"""Integration tests for sstable decryption helpers.

Starts a real ScyllaDB 2026.1.0 Docker container with encryption-at-rest (EAR) enabled,
creates encrypted sstables, then verifies that decrypt_sstables_on_node() produces
readable (unencrypted) output.

Run with:
    pytest -m integration unit_tests/test_sstable_decrypt_integration.py -v

Requirements:
    - Docker available
    - scylladb/scylla:2026.1.0 image present (or pullable)
"""

import base64
import logging
import os
import shutil
import subprocess
import time
from unittest.mock import patch

import pytest

from sdcm.teardown_validators.sstables import decrypt_corrupted_sstables
from sdcm.utils.sstable.s3_uploader import upload_sstables_to_s3
from sdcm.utils.sstable.sstable_utils import decrypt_sstables_on_node

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
TABLE_TABLE = "test_table_enc"


def docker_available() -> bool:
    return shutil.which("docker") is not None


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not docker_available(), reason="Docker not available"),
]


# ---------------------------------------------------------------------------
# Result stub and node stub
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
    """Minimal node stub that delegates remoter.run() to docker exec."""

    def __init__(self, container_name: str):
        self._container = container_name
        self.remoter = self

    def _container_ip(self) -> str:
        """Return the primary IP address of the container.

        Scylla binds its REST/JMX API to the container's listen_address rather than
        127.0.0.1, so ``nodetool`` commands must pass ``-h <ip>`` to reach it.
        """
        result = subprocess.run(
            ["docker", "exec", self._container, "/bin/sh", "-c", "hostname -I | awk '{print $1}'"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def run(self, cmd: str, ignore_status: bool = False, verbose: bool = True, **_kwargs):
        """Execute *cmd* inside the container and return an invoke-like Result.

        Commands that start with ``nodetool`` are automatically given a ``-h <container_ip>``
        flag because Scylla binds its API to the container IP, not 127.0.0.1.
        """
        if cmd.startswith("nodetool "):
            ip = self._container_ip()
            cmd = f"nodetool -h {ip} {cmd[len('nodetool ') :]}"
        full_cmd = ["docker", "exec", self._container, "/bin/sh", "-c", cmd]
        if verbose:
            LOGGER.debug("docker exec: %s", cmd)
        proc = subprocess.run(full_cmd, capture_output=True, text=True, check=False)
        result = FakeResult(stdout=proc.stdout, stderr=proc.stderr, exited=proc.returncode)
        if not ignore_status and proc.returncode != 0:
            raise RuntimeError(f"Command failed (exit {proc.returncode}): {cmd}\nstderr: {proc.stderr}")
        return result

    def sudo(self, cmd: str, **kwargs):
        """Run as root — docker exec is already root, so no sudo prefix needed."""
        return self.run(cmd, **kwargs)

    def add_install_prefix(self, path: str) -> str:
        return path


# ---------------------------------------------------------------------------
# Low-level container helpers (not fixtures — used only inside fixtures)
# ---------------------------------------------------------------------------


def wait_for_scylla(container: str, timeout: int = 120) -> None:
    """Poll until CQL port is accepting connections (via cqlsh inside the container)."""
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
    """Run a CQL statement via cqlsh inside the container."""
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
    """Run a nodetool command inside the container.

    Scylla's REST API is bound to the container's listen_address, not 127.0.0.1,
    so nodetool needs an explicit -h <container_ip> flag.
    """
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def scylla_container(tmp_path_factory, request):
    """
    Start a ScyllaDB container with EAR enabled; yield container name; stop on teardown.
    TODO: Unify with docker_scylla which needs refactor regardless
    """
    encrypt_conf_dir = tmp_path_factory.mktemp("encrypt_conf")

    # Write the secret key and EAR yaml.
    # The directory must be writable by the scylla user inside the container because the encryption
    # service writes a .tmp file alongside the key when loading it.
    encrypt_conf_dir.chmod(0o777)
    key_file = encrypt_conf_dir / "secret_key"
    ear_yaml_file = encrypt_conf_dir / "ear.yaml"
    key_file.write_text(SECRET_KEY_CONTENT)
    key_file.chmod(0o666)
    ear_yaml_file.write_text(EAR_YAML)
    ear_yaml_file.chmod(0o666)

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

    # Create keyspace, node-level-encrypted table and table-level-encrypted table
    cql(
        CONTAINER_NAME,
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};",
    )
    cql(CONTAINER_NAME, f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_NODE} (id int PRIMARY KEY, name text);")
    cql(CONTAINER_NAME, f"INSERT INTO {KEYSPACE}.{TABLE_NODE} (id, name) VALUES (1, 'node_enc');")
    nodetool(CONTAINER_NAME, f"flush {KEYSPACE} {TABLE_NODE}")

    ear_opts = (
        "{'cipher_algorithm': 'AES/ECB/PKCS5Padding',"
        " 'key_provider': 'LocalFileSystemKeyProviderFactory',"
        " 'secret_key_file': '/etc/scylla/encrypt_conf/secret_key',"
        " 'secret_key_strength': '128'}"
    )
    cql(
        CONTAINER_NAME,
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE_TABLE}"
        f" (id int PRIMARY KEY, name text)"
        f" WITH scylla_encryption_options = {ear_opts};",
    )
    cql(CONTAINER_NAME, f"INSERT INTO {KEYSPACE}.{TABLE_TABLE} (id, name) VALUES (1, 'table_enc');")
    nodetool(CONTAINER_NAME, f"flush {KEYSPACE} {TABLE_TABLE}")

    yield CONTAINER_NAME


@pytest.fixture(scope="module")
def scylla_node(scylla_container):
    """Return a DockerExecNode wrapping the running ScyllaDB container."""
    return DockerExecNode(scylla_container)


@pytest.fixture(scope="module")
def take_snapshot(scylla_container):
    """Return a callable that takes a snapshot and returns its absolute path inside the container."""

    def _take(keyspace: str, table: str, snap_name: str) -> str:
        nodetool(scylla_container, f"snapshot -t {snap_name} -cf {table} -- {keyspace}")
        result = subprocess.run(
            [
                "docker",
                "exec",
                scylla_container,
                "find",
                f"/var/lib/scylla/data/{keyspace}",
                "-type",
                "d",
                "-name",
                snap_name,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise RuntimeError(f"Could not locate snapshot '{snap_name}' for {keyspace}.{table}:\n{result.stderr}")
        paths = result.stdout.strip().splitlines()
        for path in paths:
            if f"/{table}-" in path or f"/{table}/" in path:
                return path.strip()
        return paths[0].strip()

    return _take


@pytest.fixture(scope="module")
def has_encryption_in_metadata(scylla_container):
    """Return a callable that checks whether any sstable in a snapshot dir is encrypted."""

    def _check(snap_path: str, keyspace: str, table: str) -> bool:
        cmd = (
            f"SCYLLA_CONF=/etc/scylla "
            f"/usr/bin/scylla sstable dump-scylla-metadata "
            f"--keyspace {keyspace} --table {table} --sstables "
            f"{snap_path}/*-Data.db 2>/dev/null || true"
        )
        # Use binary mode: the scylla_encryption_options value contains raw binary bytes
        # which cannot be decoded as UTF-8.
        result = subprocess.run(
            ["docker", "exec", scylla_container, "/bin/sh", "-c", cmd],
            check=False,
            capture_output=True,
        )
        return b"scylla_encryption_options" in result.stdout

    return _check


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
@pytest.mark.parametrize(
    "table,snap_name",
    [
        pytest.param(TABLE_NODE, "snap_node_enc", id="node_level_encryption"),
        pytest.param(TABLE_TABLE, "snap_table_enc", id="table_level_encryption"),
    ],
)
def test_decrypt_sstables_on_node(scylla_node, take_snapshot, has_encryption_in_metadata, table, snap_name):
    """Snapshot of an encrypted table can be decrypted by decrypt_sstables_on_node.

    Parametrized over node-level EAR (primary case) and table-level EAR.
    """
    snap_path = take_snapshot(KEYSPACE, table, snap_name)
    LOGGER.info("Snapshot path (%s): %s", snap_name, snap_path)

    assert has_encryption_in_metadata(snap_path, KEYSPACE, table), (
        f"Expected encrypted sstables to show scylla_encryption_options in metadata before decryption ({snap_name})"
    )

    decrypted_path = decrypt_sstables_on_node(scylla_node, snap_path, keyspace=KEYSPACE, table=table)

    assert decrypted_path is not None, f"decrypt_sstables_on_node returned None — decryption failed ({snap_name})"
    assert not has_encryption_in_metadata(decrypted_path, KEYSPACE, table), (
        f"Expected no scylla_encryption_options in sstable metadata after decryption ({snap_name})"
    )


# ---------------------------------------------------------------------------
# Helper: find live Data.db paths inside the container
# ---------------------------------------------------------------------------


def find_data_files(container: str, keyspace: str, table: str) -> list[str]:
    """Return absolute paths to *-Data.db files inside the container for the given table."""
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
            # Exclude any snapshot subdirectories created by previous tests so we always
            # get the live (non-snapshot) Data.db files.
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
            f"No live Data.db files found for {keyspace}.{table} in container {container}.\nstderr: {result.stderr}"
        )
    return paths


# ---------------------------------------------------------------------------
# Tests for decrypt_corrupted_sstables (teardown_validators path)
# ---------------------------------------------------------------------------


@pytest.mark.xdist_group("ear_scylla_container")
@pytest.mark.parametrize(
    "table",
    [
        pytest.param(TABLE_NODE, id="node_level_encryption"),
        pytest.param(TABLE_TABLE, id="table_level_encryption"),
    ],
)
def test_decrypt_corrupted_sstables(scylla_container, scylla_node, has_encryption_in_metadata, table):
    """decrypt_corrupted_sstables() takes live Data.db paths, snapshots the table for schema,
    and decrypts the files — exercising the full teardown_validators code path including the
    nodetool snapshot syntax and the -Data.db file filter.
    """
    data_files = find_data_files(scylla_container, KEYSPACE, table)
    LOGGER.info("Live Data.db files for %s.%s: %s", KEYSPACE, table, data_files)

    # Verify the live files are actually encrypted before we attempt decryption
    for f in data_files:
        parent = f.rsplit("/", 1)[0]
        assert has_encryption_in_metadata(parent, KEYSPACE, table), (
            f"Expected live sstables to be encrypted before decrypt_corrupted_sstables ({table})"
        )

    decrypted_dirs = decrypt_corrupted_sstables(scylla_node, data_files)

    assert decrypted_dirs, f"decrypt_corrupted_sstables returned an empty list — decryption failed for {table}"
    for decrypted_dir in decrypted_dirs:
        assert not has_encryption_in_metadata(decrypted_dir, KEYSPACE, table), (
            f"Expected no scylla_encryption_options in decrypted metadata for {table} in {decrypted_dir}"
        )


# ---------------------------------------------------------------------------
# Tests for upload_sstables_to_s3 decrypt path (s3_uploader path)
# ---------------------------------------------------------------------------


class DockerExecNodeWithMeta(DockerExecNode):
    """DockerExecNode extended with the attributes upload_sstables_to_s3 requires."""

    def __init__(self, container_name: str):
        super().__init__(container_name)
        self.name = container_name
        self.ssh_login_info = {}  # not used — upload is patched out


@pytest.fixture(scope="module")
def scylla_node_with_meta(scylla_container):
    """Return a DockerExecNodeWithMeta wrapping the running ScyllaDB container."""
    return DockerExecNodeWithMeta(scylla_container)


@pytest.mark.xdist_group("ear_scylla_container")
@pytest.mark.parametrize(
    "table",
    [
        pytest.param(TABLE_NODE, id="node_level_encryption"),
        pytest.param(TABLE_TABLE, id="table_level_encryption"),
    ],
)
def test_upload_sstables_to_s3_decrypt_path(scylla_container, scylla_node_with_meta, has_encryption_in_metadata, table):
    """upload_sstables_to_s3() calls decrypt_sstables_on_node for each snapshot and includes
    the decrypted directory in the upload paths.

    The S3 upload itself is patched out; we assert that the path list passed to it contains
    both the snapshot path and a decrypted sibling directory.
    """
    captured = {}

    def fake_upload(ssh_login_info, paths, **_kwargs):
        captured["paths"] = list(paths)
        return "https://fake-s3/link"

    with patch(
        "sdcm.utils.sstable.s3_uploader.upload_remote_files_directly_to_s3",
        side_effect=fake_upload,
    ):
        upload_sstables_to_s3(
            node=scylla_node_with_meta,
            keyspace=KEYSPACE,
            test_id="integ-test",
            tables=[table],
        )

    assert "paths" in captured, "upload_remote_files_directly_to_s3 was never called"
    upload_paths = captured["paths"]
    LOGGER.info("upload_paths for %s: %s", table, upload_paths)

    snapshot_paths = [p for p in upload_paths if "/snapshots/" in p and "decrypted" not in p]
    decrypted_paths = [p for p in upload_paths if "decrypted" in p]

    assert snapshot_paths, f"No snapshot path found in upload_paths for {table}: {upload_paths}"
    assert decrypted_paths, (
        f"No decrypted/ path found in upload_paths for {table}: {upload_paths} — "
        "decrypt_sstables_on_node may have failed or returned None"
    )

    # The decrypted output must not contain encrypted sstables
    for decrypted_dir in decrypted_paths:
        assert not has_encryption_in_metadata(decrypted_dir, KEYSPACE, table), (
            f"Expected no scylla_encryption_options in decrypted metadata for {table} in {decrypted_dir}"
        )

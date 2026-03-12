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

import json
from pathlib import Path
from time import sleep, time

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
import pytest

from sdcm.gemini_thread import GeminiStressThread
from sdcm.utils.docker_utils import running_in_docker
from unit_tests.conftest import configure_scylla_node
from unit_tests.dummy_remote import LocalLoaderSetDummy
from unit_tests.test_cluster import DummyDbCluster

pytestmark = [
    pytest.mark.usefixtures("events"),
    pytest.mark.integration,
    pytest.mark.xdist_group("docker_heavy"),
]

GEMINI_KEYSPACE = "ks1"
GEMINI_TABLE = "table1"
GEMINI_SCHEMA_PATH = Path(__file__).parent / "test_data" / "gemini_schemas" / "simple_two_partition_keys_schema.json"
GEMINI_STATEMENT_RATIOS_PATH = (
    Path(__file__).parent / "test_data" / "gemini_schemas" / "no_delete_statement_ratios.json"
)


@pytest.fixture(name="docker_scylla_oracle", scope="function")
def fixture_docker_scylla_oracle(params):
    """A standalone Scylla instance used as the oracle cluster in gemini tests.

    This is intentionally a fresh, independent container — not seeded from
    ``docker_scylla`` — so that gemini can treat it as a separate cluster.
    """
    scylla = configure_scylla_node({}, params)
    yield scylla
    scylla.kill()


@pytest.fixture(name="cql_session")
def fixture_cql_session(docker_scylla):
    """Yield an open CQL session connected to ``docker_scylla``, then shut it down."""
    if running_in_docker():
        address = f"{docker_scylla.internal_ip_address}:9042"
    else:
        address = docker_scylla.get_port("9042")
    node_ip, port = address.split(":")
    cluster = Cluster(
        [node_ip],
        port=int(port),
        auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"),
    )
    session = cluster.connect()
    try:
        yield session
    finally:
        session.shutdown()
        cluster.shutdown()


def load_statement_ratios():
    return json.dumps(json.loads(GEMINI_STATEMENT_RATIOS_PATH.read_text(encoding="utf-8")), separators=(",", ":"))


@pytest.fixture(name="gemini_thread")
def fixture_gemini_thread(request, params, docker_scylla, docker_scylla_oracle):
    """Build and teardown a GeminiStressThread for the standard oracle case.

    Test functions that need to vary mode, duration, or other options should
    call :func:`build_gemini_thread` directly instead.
    """
    thread = build_gemini_thread(params, docker_scylla, docker_scylla_oracle)
    request.addfinalizer(thread.kill)
    return thread


@pytest.fixture(name="gemini_thread_no_oracle")
def fixture_gemini_thread_no_oracle(request, params, docker_scylla):
    """Build and teardown a GeminiStressThread with no oracle cluster."""
    thread = build_gemini_thread(params, docker_scylla, oracle_node=None)
    request.addfinalizer(thread.kill)
    return thread


def build_gemini_thread(
    params,
    test_node,
    oracle_node=None,
    *,
    mode="mixed",
    duration="1m",
    extra_options=None,
    use_schema=False,
):
    """Construct a :class:`GeminiStressThread` for integration tests.

    Args:
        params: SCT params dict (from the ``params`` fixture).
        test_node: The primary Scylla Docker node to test against.
        oracle_node: Optional oracle Scylla Docker node; ``None`` disables oracle.
        mode: Gemini run mode (``"mixed"``, ``"write"``, etc.).
        duration: Gemini run duration string (e.g. ``"30s"``).
        extra_options: Additional CLI flags to append to the stress command.
        use_schema: When ``True``, sets ``gemini_schema_url`` to the bundled
            two-partition-key schema fixture file.

    Returns:
        GeminiStressThread: Configured but not yet started thread.
    """
    thread_params = params.copy()
    thread_params.update({"gemini_table_options": ["gc_grace_seconds=60"]})
    if use_schema:
        thread_params["gemini_schema_url"] = str(GEMINI_SCHEMA_PATH.resolve())

    loader_set = LocalLoaderSetDummy(params=thread_params)
    test_cluster = DummyDbCluster([test_node])
    oracle_cluster = DummyDbCluster([oracle_node]) if oracle_node else None
    options = [
        f"--duration={duration}",
        "--warmup=0",
        "--concurrency=5",
        f"--mode={mode}",
        "--cql-features=normal",
        "--max-mutation-retries=100",
        "--max-mutation-retries-backoff=1s",
        "--replication-strategy=\"{'class': 'SimpleStrategy', 'replication_factor': '1'}\"",
        "--oracle-replication-strategy=\"{'class': 'SimpleStrategy', 'replication_factor': '1'}\"",
        "--partition-count=100",
        "--max-errors-to-store=1",
        f"--statement-ratios='{load_statement_ratios()}'",
    ]
    if extra_options:
        options.extend(extra_options)
    return GeminiStressThread(
        loaders=loader_set,
        stress_cmd=" ".join(options),
        test_cluster=test_cluster,
        oracle_cluster=oracle_cluster,
        timeout=240,
        params=thread_params,
    )


def wait_for_table(session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE, timeout=60):
    """Poll until the given table exists in ``system_schema.tables``.

    Args:
        session: Open CQL session.
        keyspace: Target keyspace name.
        table: Target table name.
        timeout: Maximum seconds to wait before raising.

    Raises:
        AssertionError: If the table is not visible within *timeout* seconds.
    """
    deadline = time() + timeout
    while time() < deadline:
        row = session.execute(
            "SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s AND table_name=%s",
            (keyspace, table),
        ).one()
        if row:
            return
        session.cluster.refresh_schema_metadata()
        sleep(1)
    raise AssertionError(f"Gemini table {keyspace}.{table} was not created in time")


def get_table_metadata(session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE):
    """Return the Cassandra driver table metadata for *keyspace.table*.

    Args:
        session: Open CQL session.
        keyspace: Target keyspace name.
        table: Target table name.

    Returns:
        cassandra.metadata.TableMetadata: Live table metadata object.
    """
    session.cluster.refresh_schema_metadata()
    return session.cluster.metadata.keyspaces[keyspace].tables[table]


def assert_result_structure(result):
    """Assert that a gemini result dict has the expected structure and sane values.

    Args:
        result: Parsed gemini result dict (from ``get_gemini_results``).
    """
    assert result is not None, "Gemini result must not be None"

    for key in ("write_ops", "read_ops", "write_errors", "read_errors"):
        assert key in result, f"Result must contain {key}"

    assert result["write_ops"] >= 0, "write_ops must be non-negative"
    assert result["read_ops"] >= 0, "read_ops must be non-negative"
    assert result["write_errors"] == 0, f"Expected no write errors, got: {result['write_errors']}"
    assert result["read_errors"] == 0, f"Expected no read errors, got: {result['read_errors']}"

    errors = result.get("errors")
    assert not errors, f"Expected no validation errors, got: {errors}"


def assert_write_result(result):
    """Assert write-mode result: write_ops must be positive, read_ops can be zero.

    Args:
        result: Parsed gemini result dict.
    """
    assert_result_structure(result)
    assert result["write_ops"] > 0, "Write run produced zero write_ops — did gemini actually run?"


def assert_mixed_result(result):
    """Assert mixed-mode result: both write_ops and read_ops must be positive.

    Args:
        result: Parsed gemini result dict.
    """
    assert_result_structure(result)
    assert result["write_ops"] > 0, "Mixed run produced zero write_ops — did gemini actually run?"
    assert result["read_ops"] > 0, "Mixed run produced zero read_ops — did gemini actually run?"


def test_gemini_mixed_mode_with_oracle(request, docker_scylla, docker_scylla_oracle, params):
    """A full mixed-mode run against an oracle cluster must complete without errors.

    This is the most representative integration scenario: gemini writes data and
    validates it against the oracle cluster simultaneously. Both write_ops and
    read_ops must be positive, and the result must carry no errors.
    """
    thread = build_gemini_thread(params, docker_scylla, docker_scylla_oracle, mode="mixed", duration="30s")
    request.addfinalizer(thread.kill)
    thread.run()
    results = thread.get_gemini_results()
    stats = thread.verify_gemini_results(results)

    assert stats["status"] == "PASSED", f"Gemini mixed-mode run failed: {stats}"
    assert len(results) == 1, "Expected exactly one result from a single-loader run"
    assert_mixed_result(results[0])


def test_gemini_write_only_with_oracle(request, docker_scylla, docker_scylla_oracle, params):
    """A write-only run against an oracle cluster must succeed and produce non-zero write_ops.

    Write-only mode is used for CDC tests and write-only workloads where reads are not
    performed during the run. An oracle cluster is still present so that a subsequent
    read-mode run could validate consistency if needed.
    """
    thread = build_gemini_thread(params, docker_scylla, docker_scylla_oracle, mode="write", duration="30s")
    request.addfinalizer(thread.kill)
    thread.run()
    results = thread.get_gemini_results()
    stats = thread.verify_gemini_results(results)

    assert stats["status"] == "PASSED", f"Gemini write-only run failed: {stats}"
    assert len(results) == 1, "Expected exactly one result from a single-loader run"
    assert_write_result(results[0])


def test_gemini_with_schema(request, docker_scylla, docker_scylla_oracle, cql_session, params):
    """A mixed-mode run using an explicit schema file must succeed and produce non-zero ops.

    Using a schema file gives deterministic table structure (known partition key names,
    column types, etc.) which enables targeted CQL assertions after the run. The schema
    used here defines a two-partition-key table (pk1: int, pk2: varchar) with two
    regular columns, so we can verify that the keyspace and table were created as expected.
    """
    thread = build_gemini_thread(
        params, docker_scylla, docker_scylla_oracle, mode="mixed", duration="30s", use_schema=True
    )
    request.addfinalizer(thread.kill)

    # Verify the schema path is correctly injected into the gemini command
    gemini_cmd = thread.generate_gemini_command(schema_path="/tmp/schema.json")
    assert '--schema="/tmp/schema.json"' in gemini_cmd, "Schema path must appear in the generated command"
    assert "--partition-count=100" in gemini_cmd, "partition-count must be forwarded from stress_cmd"
    assert "--duration=30s" in gemini_cmd, "duration must appear in the generated command"
    assert "--warmup=0" in gemini_cmd, "warmup must appear in the generated command"
    assert f"--statement-ratios='{load_statement_ratios()}'" in gemini_cmd, (
        "statement-ratios must appear in the generated command"
    )

    thread.run()
    results = thread.get_gemini_results()
    stats = thread.verify_gemini_results(results)

    assert stats["status"] == "PASSED", f"Gemini schema run failed: {stats}"
    assert len(results) == 1, "Expected exactly one result from a single-loader run"
    assert_mixed_result(results[0])

    # Verify the schema was actually applied: the expected keyspace and table must exist
    wait_for_table(cql_session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE)
    metadata = get_table_metadata(cql_session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE)

    # The schema defines two partition keys: pk1 (int) and pk2 (varchar)
    pk_names = [col.name for col in metadata.partition_key]
    assert pk_names == ["pk1", "pk2"], f"Expected partition keys [pk1, pk2] from schema file, got {pk_names}"

    # The schema defines two regular columns: col1 (int) and col2 (varchar)
    regular_col_names = sorted(
        col_name
        for col_name, col in metadata.columns.items()
        if col_name not in {pk.name for pk in metadata.partition_key}
        and col_name not in {ck.name for ck in metadata.clustering_key}
    )
    assert regular_col_names == ["col1", "col2"], (
        f"Expected regular columns [col1, col2] from schema file, got {regular_col_names}"
    )


def test_gemini_without_oracle(request, docker_scylla, params):
    """A write-only run with no oracle cluster must succeed.

    The oracle-less code path (``oracle_cluster is None``) is used when no
    consistency verification is needed, e.g. pure write-load or smoke tests.
    No ``--oracle-cluster`` flag must appear in the generated command.
    """
    thread = build_gemini_thread(params, docker_scylla, oracle_node=None, mode="write", duration="30s")
    request.addfinalizer(thread.kill)

    # Confirm --oracle-cluster is absent from the generated command
    gemini_cmd = thread.generate_gemini_command()
    assert "--oracle-cluster" not in gemini_cmd, "oracle-cluster flag must not appear when oracle_cluster is None"

    thread.run()
    results = thread.get_gemini_results()
    stats = thread.verify_gemini_results(results)

    assert stats["status"] == "PASSED", f"Gemini no-oracle run failed: {stats}"
    assert len(results) == 1, "Expected exactly one result from a single-loader run"
    assert_write_result(results[0])

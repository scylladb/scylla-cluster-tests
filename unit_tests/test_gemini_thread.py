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
from contextlib import contextmanager
from pathlib import Path
from time import sleep, time

from cassandra.auth import PlainTextAuthProvider  # type: ignore[import-not-found]
from cassandra.cluster import Cluster  # type: ignore[import-not-found]
import pytest  # type: ignore[import-not-found]

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


class DBCluster(DummyDbCluster):
    def get_node_cql_ips(self, nodes=None):
        return [node.cql_address for node in (nodes or self.nodes)]


@pytest.fixture(name="docker_scylla_oracle", scope="function")
def fixture_docker_scylla_oracle(params):
    scylla = configure_scylla_node({}, params)
    yield scylla
    scylla.kill()


@contextmanager
def cql_session(node, username="cassandra", password="cassandra"):
    if running_in_docker():
        address = f"{node.internal_ip_address}:9042"
    else:
        address = node.get_port("9042")
    node_ip, port = address.split(":")
    cluster = Cluster(
        [node_ip],
        port=int(port),
        auth_provider=PlainTextAuthProvider(username=username, password=password),
    )
    session = cluster.connect()
    try:
        yield session
    finally:
        session.shutdown()
        cluster.shutdown()


def _load_statement_ratios():
    return json.dumps(json.loads(GEMINI_STATEMENT_RATIOS_PATH.read_text(encoding="utf-8")), separators=(",", ":"))


def _build_gemini_thread(params, test_node, oracle_node=None, *, mode="mixed", duration="1m", extra_options=None):
    thread_params = params.copy()
    thread_params.update(
        {
            "gemini_schema_url": str(GEMINI_SCHEMA_PATH.resolve()),
            "gemini_table_options": ["gc_grace_seconds=60"],
        }
    )
    loader_set = LocalLoaderSetDummy(params=thread_params)
    test_cluster = DBCluster([test_node])
    oracle_cluster = DBCluster([oracle_node]) if oracle_node else None
    options = [
        f"--duration={duration}",
        "--warmup=0",
        "--concurrency=5",
        f"--mode={mode}",
        "--cql-features=basic",
        "--max-mutation-retries=100",
        "--max-mutation-retries-backoff=1s",
        "--replication-strategy=\"{'class': 'SimpleStrategy', 'replication_factor': '1'}\"",
        "--oracle-replication-strategy=\"{'class': 'SimpleStrategy', 'replication_factor': '1'}\"",
        "--partition-count=100",
        "--max-errors-to-store=1",
        "--fail-fast=true",
        "--use-server-timestamps=false",
        f"--statement-ratios='{_load_statement_ratios()}'",
    ]
    if extra_options:
        options.extend(extra_options)
    return GeminiStressThread(
        loaders=loader_set,
        stress_cmd=" ".join(options),
        test_cluster=test_cluster,
        oracle_cluster=oracle_cluster,  # type: ignore[arg-type]
        timeout=240,
        params=thread_params,
    )


def _wait_for_table(session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE, timeout=60):
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


def _get_table_metadata(session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE):
    session.cluster.refresh_schema_metadata()
    return session.cluster.metadata.keyspaces[keyspace].tables[table]


def _primary_key_columns(metadata):
    return [*metadata.partition_key, *metadata.clustering_key]


def _primary_key_values(metadata, row):
    return [getattr(row, column.name) for column in _primary_key_columns(metadata)]


def _select_row_by_primary_key(session, metadata, values, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE):
    where_clause = " AND ".join(f"{column.name}=%s" for column in _primary_key_columns(metadata))
    query = f"SELECT * FROM {keyspace}.{table} WHERE {where_clause} LIMIT 1"
    return session.execute(query, values).one()


def _wait_for_replicated_row(test_session, oracle_session, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE, timeout=90):
    _wait_for_table(test_session, keyspace=keyspace, table=table, timeout=timeout)
    _wait_for_table(oracle_session, keyspace=keyspace, table=table, timeout=timeout)
    deadline = time() + timeout
    while time() < deadline:
        metadata = _get_table_metadata(test_session, keyspace=keyspace, table=table)
        partitions = list(test_session.execute(f"SELECT DISTINCT pk1, pk2 FROM {keyspace}.{table} LIMIT 10"))
        for partition in partitions:
            values = [partition.pk1, partition.pk2]
            row = _select_row_by_primary_key(test_session, metadata, values, keyspace=keyspace, table=table)
            if row and _select_row_by_primary_key(oracle_session, metadata, values, keyspace=keyspace, table=table):
                return metadata, row
        sleep(1)
    raise AssertionError("Gemini did not create a replicated row in time")


def _find_mutable_regular_column(metadata, row):
    primary_key_names = {column.name for column in _primary_key_columns(metadata)}
    for column in metadata.columns.values():
        if column.name in primary_key_names:
            continue
        value = getattr(row, column.name)
        if value is not None:
            return column, value
    raise AssertionError("Gemini row does not contain a mutable regular column")


def _update_row_discrepancy(session, metadata, row, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE):
    column, value = _find_mutable_regular_column(metadata, row)
    where_clause = " AND ".join(f"{pk_column.name}=%s" for pk_column in _primary_key_columns(metadata))
    query = f"UPDATE {keyspace}.{table} SET {column.name}=%s WHERE {where_clause}"
    if isinstance(value, int):
        next_value = value + 1
    else:
        next_value = f"{value}_changed"
    session.execute(query, [next_value, *_primary_key_values(metadata, row)])


def _delete_row_discrepancy(session, metadata, row, keyspace=GEMINI_KEYSPACE, table=GEMINI_TABLE):
    where_clause = " AND ".join(f"{pk_column.name}=%s" for pk_column in _primary_key_columns(metadata))
    query = f"DELETE FROM {keyspace}.{table} WHERE {where_clause}"
    session.execute(query, _primary_key_values(metadata, row))


def _assert_gemini_detects_discrepancy(gemini_thread):
    results = gemini_thread.get_gemini_results()
    stats = gemini_thread.verify_gemini_results(results)
    assert stats["status"] == "FAILED"
    assert stats["errors"]


def _run_and_cleanup_thread(request, gemini_thread):
    request.addfinalizer(gemini_thread.kill)
    gemini_thread.run()
    return gemini_thread.get_gemini_results()


@pytest.mark.parametrize(
    "discrepancy_action", [_update_row_discrepancy, _delete_row_discrepancy], ids=["update-row", "delete-row"]
)
def test_gemini_fails_on_manual_discrepancy(request, docker_scylla, docker_scylla_oracle, params, discrepancy_action):
    writer_thread = _build_gemini_thread(params, docker_scylla, docker_scylla_oracle, mode="write")
    writer_results = _run_and_cleanup_thread(request, writer_thread)
    writer_stats = writer_thread.verify_gemini_results(writer_results)
    assert writer_stats["status"] == "PASSED"

    with cql_session(docker_scylla) as test_session, cql_session(docker_scylla_oracle) as oracle_session:
        metadata, row = _wait_for_replicated_row(test_session, oracle_session)
        discrepancy_action(test_session, metadata, row)

    reader_thread = _build_gemini_thread(
        params,
        docker_scylla,
        docker_scylla_oracle,
        mode="read",
        extra_options=["--drop-schema=false"],
    )
    _run_and_cleanup_thread(request, reader_thread)
    _assert_gemini_detects_discrepancy(reader_thread)


def test_01_gemini_thread(request, docker_scylla, params):
    gemini_thread = _build_gemini_thread(params, docker_scylla, docker_scylla, mode="write")
    gemini_cmd = gemini_thread._generate_gemini_command(schema_path="/tmp/schema.json")

    assert '--schema="/tmp/schema.json"' in gemini_cmd
    assert f"--statement-ratios='{_load_statement_ratios()}'" in gemini_cmd
    assert "--partition-count=100" in gemini_cmd
    assert "--duration=1m" in gemini_cmd
    assert "--warmup=0" in gemini_cmd

    results = _run_and_cleanup_thread(request, gemini_thread)
    gemini_thread.verify_gemini_results(results)


def test_gemini_thread_without_cluster(request, docker_scylla, params):
    gemini_thread = _build_gemini_thread(params, docker_scylla, None, mode="write")
    results = _run_and_cleanup_thread(request, gemini_thread)
    gemini_thread.verify_gemini_results(results)

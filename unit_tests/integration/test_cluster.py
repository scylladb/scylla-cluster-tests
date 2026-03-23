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
# Copyright (c) 2020 ScyllaDB

"""
Integration tests for cluster functionality that require a running Docker-backed Scylla instance.
"""

import logging
import re

import pytest

from sdcm.nemesis.utils.indexes import get_column_names
from sdcm.utils.common import parse_nodetool_listsnapshots
from sdcm.utils.version_utils import ComparableScyllaVersion

# Import shared test infrastructure from the unit test module
from unit_tests.unit.test_cluster import DummyScyllaCluster

pytestmark = [
    pytest.mark.integration,
    pytest.mark.xdist_group("docker_heavy"),
]


@pytest.fixture
def prepared_keyspaces(docker_scylla, params):
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        ks = "testks"
        base_cf = "test_table"
        counter_cf = "counter_table"

        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {ks}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)

        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {ks}.{base_cf} (
                id uuid PRIMARY KEY,
                txt text,
                tags list<text>,
                kv map<text, text>,
                nums set<int>,
                d duration
            );
        """)

        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {ks}.{counter_cf} (
                id uuid PRIMARY KEY,
                cnt counter
            );
        """)

    return cluster, ks, base_cf, counter_cf


def test_get_any_ks_cf_list(docker_scylla, params, events):
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))"
        )
        session.execute(
            "CREATE MATERIALIZED VIEW mview.users_by_first_name AS SELECT * FROM mview.users WHERE first_name "
            "IS NOT NULL and username IS NOT NULL PRIMARY KEY (first_name, username)"
        )
        session.execute(
            "CREATE MATERIALIZED VIEW mview.users_by_last_name AS SELECT * FROM mview.users WHERE last_name "
            "IS NOT NULL and username IS NOT NULL PRIMARY KEY (last_name, username)"
        )
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')"
        )
        session.execute(
            "CREATE KEYSPACE \"123_keyspace\" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )
        session.execute(
            'CREATE TABLE "123_keyspace".users (username text, first_name text, last_name text, password text, email text, '
            "last_access timeuuid, PRIMARY KEY(username))"
        )
        session.execute(
            'CREATE TABLE "123_keyspace"."120users" (username text, first_name text, last_name text, password text, email text, '
            "last_access timeuuid, PRIMARY KEY(username))"
        )
        session.execute(
            'INSERT INTO "123_keyspace".users (username, first_name, last_name, password) VALUES '
            "('fruch', 'Israel', 'Fruchter', '1111')"
        )
        session.execute(
            'INSERT INTO "123_keyspace"."120users" (username, first_name, last_name, password) VALUES '
            "('fruch', 'Israel', 'Fruchter', '1111')"
        )

    docker_scylla.run_nodetool("flush")

    table_names = cluster.get_any_ks_cf_list(docker_scylla, filter_empty_tables=False)
    assert set(table_names) == {
        "system.runtime_info",
        "system_distributed.cdc_generation_timestamps",
        "system.config",
        "system.local",
        "system.token_ring",
        "system.clients",
        "system.commitlog_cleanups",
        "system.discovery",
        "system.group0_history",
        "system.raft",
        "system.raft_snapshot_config",
        "system.raft_snapshots",
        "system.raft_state",
        "system_schema.tables",
        "system_schema.columns",
        "system.compaction_history",
        "system.cdc_local",
        "system.versions",
        "system.view_build_status_v2",
        "system_distributed_everywhere.cdc_generation_descriptions_v2",
        "system_replicated_keys.encrypted_keys",
        "system.scylla_local",
        "system.cluster_status",
        "system.protocol_servers",
        "system_distributed.cdc_streams_descriptions_v2",
        "system_schema.keyspaces",
        "system.size_estimates",
        "system_schema.scylla_tables",
        "system.scylla_table_schema_history",
        "system_schema.views",
        "system_distributed.view_build_status",
        "system.built_views",
        "mview.users_by_first_name",
        "mview.users_by_last_name",
        "mview.users",
        'system."IndexInfo"',
        "system.batchlog",
        "system.compactions_in_progress",
        "system.hints",
        "system.large_cells",
        "system.large_partitions",
        "system.large_rows",
        "system.paxos",
        "system.peer_events",
        "system.peers",
        "system.range_xfers",
        "system.repair_history",
        "system.truncated",
        "system.views_builds_in_progress",
        "system_distributed.service_levels_v2",
        "system_schema.aggregates",
        "system_schema.dropped_columns",
        "system_schema.functions",
        "system_schema.indexes",
        "system_schema.scylla_aggregates",
        "system_schema.scylla_keyspaces",
        "system_schema.triggers",
        "system_schema.types",
        "system_schema.view_virtual_columns",
        "system.caches",
        '"123_keyspace"."120users"',
        '"123_keyspace".users',
    }

    table_names = cluster.get_non_system_ks_cf_list(docker_scylla)
    assert set(table_names) == {"mview.users", '"123_keyspace"."120users"', '"123_keyspace".users'}


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # All regular (non-PK) columns
        ({"is_primary_key": False}, {"txt", "tags", "kv", "nums", "d"}),
        # Filter out collections
        ({"is_primary_key": False, "filter_out_collections": True}, {"txt", "d"}),
        # Filter out unsupported types
        ({"is_primary_key": False, "filter_out_column_types": ["duration"]}, {"txt", "tags", "kv", "nums"}),
        # Filter out collections + unsupported types
        ({"is_primary_key": False, "filter_out_collections": True, "filter_out_column_types": ["duration"]}, {"txt"}),
        # Get PK column
        ({"is_primary_key": True}, {"id"}),
        # Filter out PK column by type
        ({"is_primary_key": True, "filter_out_column_types": ["uuid"]}, set()),
    ],
)
def test_general_table_column_filtering(prepared_keyspaces, kwargs, expected):
    cluster, ks, base_cf, _ = prepared_keyspaces
    with cluster.cql_connection_patient(cluster.nodes[0]) as session:
        result = set(get_column_names(session, ks, base_cf, **kwargs))
        assert result == expected


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # Regular counter column (non-PK)
        ({"is_primary_key": False}, {"cnt"}),
        # Filter out counter type (non-PK)
        ({"is_primary_key": False, "filter_out_column_types": ["counter"]}, set()),
        # Get PK column
        ({"is_primary_key": True}, {"id"}),
        # Filter out PK column by type
        ({"is_primary_key": True, "filter_out_column_types": ["uuid"]}, set()),
    ],
)
def test_counter_table_column_filtering(prepared_keyspaces, kwargs, expected):
    cluster, ks, _, counter_cf = prepared_keyspaces
    with cluster.cql_connection_patient(cluster.nodes[0]) as session:
        result = set(get_column_names(session, ks, counter_cf, **kwargs))
        assert result == expected


def test_filter_out_ks_with_rf_one(docker_scylla, params, events):
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}"
        )
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))"
        )
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')"
        )
        docker_scylla.run_nodetool("flush")

        table_names = cluster.get_non_system_ks_cf_list(docker_scylla, filter_func=cluster.is_ks_rf_one)
        assert table_names == []


def test_is_table_has_no_sstables(docker_scylla, params, events):
    """
    test is_table_has_no_sstables filter function, as it would be used in `disrupt_snapshot_operations` nemesis
    """
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}"
        )
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))"
        )
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')"
        )
        docker_scylla.run_nodetool("flush")

        def is_virtual_tables_get_snapshot():
            """
            scylla commit https://github.com/scylladb/scylladb/commit/24589cf00cf8f1fae0b19a2ac1bd7b637061301a
            has stopped creating snapshots for virtual tables.
            hence we need to filter them out when compare tables to snapshot content.
            """
            if docker_scylla.is_enterprise:
                return ComparableScyllaVersion(docker_scylla.scylla_version) >= "2024.3.0-dev"
            else:
                return ComparableScyllaVersion(docker_scylla.scylla_version) >= "6.3.0-dev"

        if is_virtual_tables_get_snapshot():
            ks_cf = cluster.get_any_ks_cf_list(
                docker_scylla, filter_func=cluster.is_table_has_no_sstables, filter_empty_tables=False
            )
        else:
            ks_cf = cluster.get_any_ks_cf_list(docker_scylla, filter_empty_tables=False)

        keyspace_table = []
        ks_cf = [k_c.replace('"', "") for k_c in ks_cf]
        keyspace_table.extend([k_c.split(".") for k_c in ks_cf])

        result = docker_scylla.run_nodetool("snapshot")
        snapshot_name = re.findall(r"(\d+)", result.stdout.split("snapshot name")[1])[0]

        result = docker_scylla.run_nodetool("listsnapshots")
        logging.debug(result)
        snapshots_content = parse_nodetool_listsnapshots(listsnapshots_output=result.stdout)
        snapshot_content = snapshots_content[snapshot_name]
        logging.debug(snapshot_content)

        snapshot_content_list = [[elem.keyspace_name, elem.table_name] for elem in snapshot_content]
        if sorted(keyspace_table) != sorted(snapshot_content_list):
            raise AssertionError(
                f"Snapshot content not as expected. \n"
                f"Expected content: {sorted(keyspace_table)} \n "
                f"Actual snapshot content: {sorted(snapshot_content_list)}"
            )


def test_exclusive_connection(docker_scylla, docker_scylla_2, params, events):
    """
    Test exclusive CQL connection creation for each node in the cluster.
    Ensures that the session connects to the correct node.
    Run 10 times to increase the chance of catching intermittent issues.
    """
    cluster = DummyScyllaCluster([docker_scylla, docker_scylla_2])
    cluster.params = params

    for i in range(10):
        for node in cluster.nodes:
            with cluster.cql_connection_patient_exclusive(node) as session:
                print(f"Iteration {i}, Node {node.cql_address}")
                local = session.execute("SELECT host_id, rpc_address FROM system.local").one()
                peers = session.execute("SELECT host_id, peer, rpc_address FROM system.peers").one()
                assert local.rpc_address == node.cql_address, (
                    f"Local rpc_address: {local.rpc_address}, expected: {node.cql_address}"
                )
                assert peers.rpc_address != node.cql_address, (
                    f"Peers rpc_address: {peers.rpc_address}, expected not: {node.cql_address}"
                )

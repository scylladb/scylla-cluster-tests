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

from unittest.mock import MagicMock

import pytest
from cassandra import InvalidRequest

from sdcm.utils.cluster_config import ConfigScope, get_cluster_config, set_cluster_config


@pytest.fixture()
def cql_session():
    """CQL session that records the statements sent to the cluster."""
    return MagicMock()


@pytest.fixture()
def node(cql_session):
    """Node whose cluster hands out the recording CQL session."""
    node = MagicMock()
    node.parent_cluster.cql_connection_patient.return_value.__enter__.return_value = cql_session
    return node


@pytest.mark.parametrize(
    "value,scope,statement",
    [
        pytest.param(True, (), "ALTER CLUSTER WITH auto_repair_enabled = true", id="cluster-bool"),
        pytest.param("3600", (), "ALTER CLUSTER WITH auto_repair_enabled = '3600'", id="cluster-string"),
        pytest.param(
            False, (ConfigScope.KEYSPACE, "ks"), "ALTER KEYSPACE ks WITH auto_repair_enabled = false", id="keyspace"
        ),
        pytest.param(
            False, (ConfigScope.TABLE, "ks.t"), "ALTER TABLE ks.t WITH auto_repair_enabled = false", id="table"
        ),
    ],
)
def test_set_cluster_config_runs_alter_at_scope(node, cql_session, value, scope, statement):
    """The option must be stored at the requested scope, with a non-boolean value sent as a quoted string."""
    set_cluster_config(node, "auto_repair_enabled", value, *scope)
    cql_session.execute.assert_called_once_with(statement)


def test_set_cluster_config_raises_when_rejected(node, cql_session):
    """A rejected option must fail the caller instead of letting the test run without the requested setting."""
    cql_session.execute.side_effect = InvalidRequest("Unknown cluster config option 'nonexistent_option'")
    with pytest.raises(InvalidRequest, match="nonexistent_option"):
        set_cluster_config(node, "nonexistent_option", True)


@pytest.mark.parametrize(
    "scope,query,query_args",
    [
        pytest.param(
            (), "SELECT configs[%s] FROM system_schema.scylla_clusters", ("auto_repair_enabled",), id="cluster"
        ),
        pytest.param(
            (ConfigScope.KEYSPACE, "ks"),
            "SELECT configs[%s] FROM system_schema.scylla_keyspaces WHERE keyspace_name = %s",
            ("auto_repair_enabled", "ks"),
            id="keyspace",
        ),
        pytest.param(
            (ConfigScope.TABLE, "ks.t"),
            "SELECT configs[%s] FROM system_schema.scylla_tables WHERE keyspace_name = %s AND table_name = %s",
            ("auto_repair_enabled", "ks", "t"),
            id="table",
        ),
    ],
)
def test_get_cluster_config_reads_the_scope(node, cql_session, scope, query, query_args):
    """The value must be read from the requested scope only, not from a broader one."""
    cql_session.execute.return_value.one.return_value = ("true",)
    assert get_cluster_config(node, "auto_repair_enabled", *scope) == "true"
    cql_session.execute.assert_called_once_with(query, query_args)

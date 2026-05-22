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

import logging
from unittest.mock import MagicMock

import pytest

from sdcm.cluster_docker import VectorStoreSetDocker

GRANT_STATEMENT = "GRANT VECTOR_SEARCH_INDEXING ON ALL KEYSPACES TO 'vs_user'"


@pytest.fixture(name="cql_session")
def fixture_cql_session():
    """CQL session that records the statements sent to the ScyllaDB cluster."""
    return MagicMock()


@pytest.fixture(name="vector_store_cluster")
def fixture_vector_store_cluster(cql_session):
    """Vector Store cluster without nodes whose ScyllaDB cluster hands out the recording CQL session."""
    cluster = VectorStoreSetDocker.__new__(VectorStoreSetDocker)
    cluster.nodes = []
    cluster.log = logging.getLogger(__name__)
    cluster.scylla_cluster = MagicMock()
    cluster.scylla_cluster.cql_connection_patient.return_value.__enter__.return_value = cql_session
    cluster.params = {
        "vector_store_scylla_username": "vs_user",
        "vector_store_scylla_password": "vs_password",
        "vector_store_service_level_shares": 500,
        "authenticator_user": "cassandra",
        "authenticator_password": "cassandra",
    }
    return cluster


@pytest.mark.parametrize(
    "authorizer, granted",
    [
        pytest.param("CassandraAuthorizer", True, id="cassandra-authorizer"),
        pytest.param("AllowAllAuthorizer", False, id="allow-all-authorizer"),
        pytest.param(None, False, id="no-authorizer"),
    ],
)
def test_setup_vector_store_scylla_auth_grants_indexing_only_with_cassandra_authorizer(
    vector_store_cluster, cql_session, authorizer, granted
):
    """The Vector Store role needs VECTOR_SEARCH_INDEXING to read indexed tables when CassandraAuthorizer checks permissions.

    AllowAllAuthorizer rejects GRANT statements, so the grant must be skipped there.
    """
    vector_store_cluster.params["authorizer"] = authorizer

    vector_store_cluster.configure_with_scylla_cluster(vector_store_cluster.scylla_cluster)

    statements = [call.args[0] for call in cql_session.execute.call_args_list]
    assert (GRANT_STATEMENT in statements) is granted
    assert "ATTACH SERVICE_LEVEL 'vector_store_sl' TO 'vs_user'" in statements

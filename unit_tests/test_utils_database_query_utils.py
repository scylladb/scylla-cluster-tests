import pytest

from sdcm.utils.database_query_utils import fetch_all_rows
from unit_tests.test_cluster import DummyScyllaCluster


@pytest.mark.integration
def test_fetch_all_rows(docker_scylla, params, events):

    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        for i in range(10):
            session.execute(
                "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
                f"('fruch', 'Israel', 'Fruchter', '{i}')")

        statement = 'select * from  mview.users;'
        full_res = fetch_all_rows(session=session, default_fetch_size=100, statement=statement)
        assert full_res

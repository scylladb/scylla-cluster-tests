import logging

import pytest

from unit_tests.test_cluster import DummyDbCluster


log = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.parametrize('encrypted', [
    pytest.param(True, marks=pytest.mark.docker_scylla_args(ssl=True), id='encrypted'),
    pytest.param(False, marks=pytest.mark.docker_scylla_args(ssl=False), id='clear')
])
def test_02_test_python_driver(docker_scylla, params, encrypted):

    params['client_encrypt'] = encrypted
    node = docker_scylla
    db_cluster = DummyDbCluster(nodes=[node], params=params)
    node.parent_cluster = db_cluster

    for func in [db_cluster.cql_connection_patient,
                 db_cluster.cql_connection_patient_exclusive]:

        with func(node) as session:
            for host in session.cluster.metadata.all_hosts():
                log.debug(host)
            res = session.execute("SELECT * FROM system.local")
            output = res.all()
            log.debug(output)
            assert len(output) == 1

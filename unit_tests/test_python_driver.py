import logging

import pytest

from unit_tests.test_cluster import DummyDbCluster, DummyNode, DummyRemote


log = logging.getLogger(__name__)


class Node(DummyNode):  # pylint: disable=abstract-method
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._host_id = ''

    @property
    def host_id(self):
        return self._host_id


@pytest.mark.skip("manual tests")
def test_01_test_python_driver_serverless_connectivity(params):

    node = Node(name='test_node',
                parent_cluster=None,
                ssh_login_info=dict(key_file='~/.ssh/scylla-test'))

    # local bundle file
    params['k8s_connection_bundle_file'] = '/home/fruch/Downloads/k8s_config.yaml'
    node._host_id = '0d56abe0-91f9-43ee-9b39-4536488b6089'  # pylint: disable=protected-access
    node.remoter = DummyRemote()
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

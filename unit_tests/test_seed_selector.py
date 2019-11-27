import unittest
import tempfile
import logging
import shutil
import os.path

from tenacity import RetryError

import sdcm.cluster
from unit_tests.dummy_remote import DummyRemote


class DummyNode(sdcm.cluster.BaseNode):  # pylint: disable=abstract-method

    @property
    def private_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')

    @property
    def public_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')

    @property
    def ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')


class DummyCluster(sdcm.cluster.BaseScyllaCluster):
    def __init__(self, *args, **kwargs):
        self.params = {}
        self.nodes = []
        super(DummyCluster, self).__init__(*args, **kwargs)

    def set_test_params(self, seeds_selector, seeds_num, db_type):
        self.params = {'seeds_selector': seeds_selector, 'seeds_num': seeds_num, 'db_type': db_type}


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestSeedSelector(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.cluster = None

    @classmethod
    def tearDownClass(cls):
        # stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def setup_cluster(self, nodes_number):
        self.cluster = DummyCluster()
        # Add 3 nodes
        for i in range(1, nodes_number+1):
            self.cluster.nodes.append(DummyNode(name='node%d' % i, parent_cluster=None,
                                                base_logdir=self.temp_dir,
                                                ssh_login_info=dict(key_file='~/.ssh/scylla-test')))
        for node in self.cluster.nodes:
            node.remoter = DummyRemote()

    def test_first_2_seeds(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='first', seeds_num=2, db_type='scylla')
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[1]])
        self.assertTrue(self.cluster.non_seed_nodes == [self.cluster.nodes[2]])
        self.assertTrue(self.cluster.seed_nodes_ips == [self.cluster.nodes[0].ip_address,
                                                        self.cluster.nodes[1].ip_address])

    def test_reflector_seed(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='reflector', seeds_num=1, db_type='scylla')
        sdcm.cluster.SCYLLA_YAML_PATH = os.path.join(os.path.dirname(__file__), 'test_data', 'scylla.yaml')
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[1]])
        self.assertTrue(self.cluster.non_seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[2]])
        self.assertTrue(self.cluster.seed_nodes_ips == [self.cluster.nodes[1].ip_address])

    def test_reuse_cluster_seed(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='first', seeds_num=2, db_type='scylla')
        sdcm.cluster.SCYLLA_YAML_PATH = os.path.join(os.path.dirname(__file__), 'test_data', 'scylla.yaml')
        sdcm.cluster.Setup.reuse_cluster(True)
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[1]])
        self.assertTrue(self.cluster.non_seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[2]])
        self.assertTrue(self.cluster.seed_nodes_ips == [self.cluster.nodes[1].ip_address])

    def test_random_2_seeds(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='random', seeds_num=2, db_type='scylla')
        self.cluster.set_seeds()
        self.assertTrue(len(self.cluster.seed_nodes) == 2)
        self.assertTrue(len(self.cluster.non_seed_nodes) == 1)
        self.assertTrue(len(self.cluster.seed_nodes_ips) == 2)

    def test_first_1_seed(self):
        self.setup_cluster(nodes_number=1)
        self.cluster.set_test_params(seeds_selector='first', seeds_num=1, db_type='scylla')
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[0]])
        self.assertTrue(self.cluster.non_seed_nodes == [])
        self.assertTrue(self.cluster.seed_nodes_ips == [self.cluster.nodes[0].ip_address])

    def test_reflector_node_not_exists(self):
        self.setup_cluster(nodes_number=1)
        self.cluster.set_test_params(seeds_selector='reflector', seeds_num=1, db_type='scylla')
        sdcm.cluster.SCYLLA_YAML_PATH = os.path.join(os.path.dirname(__file__), 'test_data', 'scylla.yaml')
        self.assertRaisesRegex(expected_exception=RetryError,
                               expected_regex='Waiting for seed is selected by reflector',
                               callable_obj=self.cluster.set_seeds,
                               wait_for_timeout=5)

import unittest
import tempfile
import logging
import shutil
import os.path

import sdcm.cluster
from sdcm import sct_config
from sdcm.test_config import TestConfig
from unit_tests.dummy_remote import DummyRemote


class DummyNode(sdcm.cluster.BaseNode):
    def init(self):
        super().init()
        self.remoter.stop()

    def do_default_installations(self):
        pass  # we don't need to install anything for this unittests

    def _get_private_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')

    def _get_public_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')

    @property
    def ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return '127.0.0.%s' % self.name.replace('node', '')

    @property
    def is_nonroot_install(self):
        return False


class DummyCluster(sdcm.cluster.BaseScyllaCluster):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.nodes = []
        self.node_type = "scylla-db"

    def set_test_params(self, seeds_selector, seeds_num, db_type):
        self.params.update({'seeds_selector': seeds_selector, 'seeds_num': seeds_num, 'db_type': db_type})


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
            self.cluster.nodes.append(DummyNode(name='node%d' % i, parent_cluster=self.cluster,
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
        self.assertTrue(self.cluster.seed_nodes_addresses == [self.cluster.nodes[0].ip_address,
                                                              self.cluster.nodes[1].ip_address])

    def test_reuse_cluster_seed(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='first', seeds_num=2, db_type='scylla')
        sdcm.cluster.SCYLLA_YAML_PATH = os.path.join(os.path.dirname(__file__), 'test_data', 'scylla.yaml')
        TestConfig().reuse_cluster(True)
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[1]])
        self.assertTrue(self.cluster.non_seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[2]])
        self.assertTrue(self.cluster.seed_nodes_addresses == [self.cluster.nodes[1].ip_address])

    def test_random_2_seeds(self):
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector='random', seeds_num=2, db_type='scylla')
        self.cluster.set_seeds()
        self.assertTrue(len(self.cluster.seed_nodes) == 2)
        self.assertTrue(len(self.cluster.non_seed_nodes) == 1)
        self.assertTrue(len(self.cluster.seed_nodes_addresses) == 2)

    def test_first_1_seed(self):
        self.setup_cluster(nodes_number=1)
        self.cluster.set_test_params(seeds_selector='first', seeds_num=1, db_type='scylla')
        self.cluster.set_seeds()
        self.assertTrue(self.cluster.seed_nodes == [self.cluster.nodes[0]])
        self.assertTrue(self.cluster.non_seed_nodes == [])
        self.assertTrue(self.cluster.seed_nodes_addresses == [self.cluster.nodes[0].ip_address])

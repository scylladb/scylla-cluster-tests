import logging

import pytest

import sdcm.cluster
from sdcm import sct_config
from sdcm.test_config import TestConfig
from unit_tests.lib.dummy_remote import DummyRemote


class DummyNode(sdcm.cluster.BaseNode):
    """BaseNode with no default installations and IPs derived from the node name (nodeN -> 127.0.0.N)."""

    def init(self):
        """Run the base node init, then stop the remoter."""
        super().init()
        self.remoter.stop()

    def do_default_installations(self):
        """No-op: unit tests install nothing on the node."""

    def _get_private_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    def _get_public_ip_address(self):
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    @property
    def ip_address(self):
        """Return 127.0.0.N for node name nodeN."""
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    @property
    def is_nonroot_install(self):
        """Report a root install."""
        return False


class DummyCluster(sdcm.cluster.BaseScyllaCluster):
    """BaseScyllaCluster with an empty node list and SCT params editable per test."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.nodes = []
        self.node_type = "scylla-db"

    def set_test_params(self, seeds_selector, seeds_num, db_type):
        """Set the seeds_selector, seeds_num and db_type params."""
        self.params.update({"seeds_selector": seeds_selector, "seeds_num": seeds_num, "db_type": db_type})


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestSeedSelector:
    """Seed node selection by BaseScyllaCluster.set_seeds with the first and random selectors and a reused cluster."""

    @pytest.fixture(autouse=True, scope="class")
    def inject_temp_dir(self, tmp_path_factory):
        """Create a per-class temp dir for node logs and reset the shared cluster."""
        TestSeedSelector.temp_dir = str(tmp_path_factory.mktemp("seed_selector"))
        TestSeedSelector.cluster = None

    @pytest.fixture(autouse=True)
    def inject_test_data_dir(self, test_data_dir):
        """Store the test data dir on the test instance."""
        self.test_data_dir = test_data_dir

    def setup_cluster(self, nodes_number):
        """Build a DummyCluster of `nodes_number` DummyNodes with DummyRemote remoters."""
        self.cluster = DummyCluster()
        # Add 3 nodes
        for i in range(1, nodes_number + 1):
            self.cluster.nodes.append(
                DummyNode(
                    name="node%d" % i,
                    parent_cluster=self.cluster,
                    base_logdir=self.temp_dir,
                    ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519"),
                )
            )
        for node in self.cluster.nodes:
            node.remoter = DummyRemote()

    def test_first_2_seeds(self):
        """The "first" selector picks the first 2 nodes as seeds and the rest as non-seeds."""
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector="first", seeds_num=2, db_type="scylla")
        self.cluster.set_seeds()
        assert self.cluster.seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[1]]
        assert self.cluster.non_seed_nodes == [self.cluster.nodes[2]]
        assert self.cluster.seed_nodes_addresses == [self.cluster.nodes[0].ip_address, self.cluster.nodes[1].ip_address]

    def test_reuse_cluster_seed(self):
        """With a reused cluster the seeds are read from the existing scylla.yaml instead of the selector."""
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector="first", seeds_num=2, db_type="scylla")
        original_scylla_yaml_path = sdcm.cluster.SCYLLA_YAML_PATH
        try:
            sdcm.cluster.SCYLLA_YAML_PATH = str(self.test_data_dir / "scylla.yaml")
            TestConfig().reuse_cluster(True)
            self.cluster.set_seeds()
            assert self.cluster.seed_nodes == [self.cluster.nodes[1]]
            assert self.cluster.non_seed_nodes == [self.cluster.nodes[0], self.cluster.nodes[2]]
            assert self.cluster.seed_nodes_addresses == [self.cluster.nodes[1].ip_address]
        finally:
            sdcm.cluster.SCYLLA_YAML_PATH = original_scylla_yaml_path
            TestConfig().reuse_cluster(False)

    def test_random_2_seeds(self):
        """The "random" selector picks the requested number of seeds out of the nodes."""
        self.setup_cluster(nodes_number=3)
        self.cluster.set_test_params(seeds_selector="random", seeds_num=2, db_type="scylla")
        self.cluster.set_seeds()
        assert len(self.cluster.seed_nodes) == 2
        assert len(self.cluster.non_seed_nodes) == 1
        assert len(self.cluster.seed_nodes_addresses) == 2

    def test_first_1_seed(self):
        """The "first" selector on a 1-node cluster makes that node the only seed."""
        self.setup_cluster(nodes_number=1)
        self.cluster.set_test_params(seeds_selector="first", seeds_num=1, db_type="scylla")
        self.cluster.set_seeds()
        assert self.cluster.seed_nodes == [self.cluster.nodes[0]]
        assert self.cluster.non_seed_nodes == []
        assert self.cluster.seed_nodes_addresses == [self.cluster.nodes[0].ip_address]

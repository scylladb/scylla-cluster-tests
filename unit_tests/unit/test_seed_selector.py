import logging

import pytest

import sdcm.cluster
from sdcm import sct_config
from sdcm.test_config import TestConfig
from unit_tests.lib.dummy_remote import DummyRemote


class DummyNode(sdcm.cluster.BaseNode):
    """BaseNode with no SSH remoter and fixed 127.0.0.<n> addresses taken from the node name."""

    def init(self):
        """Stop the remoter right after the base init."""
        super().init()
        self.remoter.stop()

    def do_default_installations(self):
        """Skip default installations, not needed for these unit tests."""

    def _get_private_ip_address(self):
        """Return 127.0.0.<n> for node name node<n>."""
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    def _get_public_ip_address(self):
        """Return 127.0.0.<n> for node name node<n>."""
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    @property
    def ip_address(self):
        """Return 127.0.0.<n> for node name node<n>."""
        # Expected node name like : node1, node2, node3 ...
        return "127.0.0.%s" % self.name.replace("node", "")

    @property
    def is_nonroot_install(self):
        """Nodes in these tests always run as root."""
        return False


class DummyCluster(sdcm.cluster.BaseScyllaCluster):
    """BaseScyllaCluster with an empty node list and a settable params object."""

    def __init__(self, *args, **kwargs):
        """Build the cluster with a default SCT config in test_region."""
        super().__init__(*args, **kwargs)
        self.params = sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.nodes = []
        self.node_type = "scylla-db"

    def set_test_params(self, seeds_selector, seeds_num, db_type):
        """Set the seed selection parameters read by set_seeds."""
        self.params.update({"seeds_selector": seeds_selector, "seeds_num": seeds_num, "db_type": db_type})


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


@pytest.fixture(scope="module")
def temp_dir(tmp_path_factory):
    """Module-wide base log directory for the dummy nodes."""
    return str(tmp_path_factory.mktemp("seed_selector"))


@pytest.fixture()
def make_cluster(temp_dir):
    """Factory returning a DummyCluster with nodes_number DummyNodes, each with a DummyRemote."""

    def apply(nodes_number):
        cluster = DummyCluster()
        for i in range(1, nodes_number + 1):
            cluster.nodes.append(
                DummyNode(
                    name="node%d" % i,
                    parent_cluster=cluster,
                    base_logdir=temp_dir,
                    ssh_login_info=dict(key_file="~/.ssh/scylla_test_id_ed25519"),
                )
            )
        for node in cluster.nodes:
            node.remoter = DummyRemote()
        return cluster

    return apply


@pytest.fixture()
def reuse_cluster():
    """Turn TestConfig reuse_cluster on for the test and off afterwards."""
    TestConfig().reuse_cluster(True)
    yield
    TestConfig().reuse_cluster(False)


def test_first_2_seeds(make_cluster):
    """seeds_selector=first with seeds_num=2 makes the first two nodes seeds."""
    cluster = make_cluster(nodes_number=3)
    cluster.set_test_params(seeds_selector="first", seeds_num=2, db_type="scylla")
    cluster.set_seeds()
    assert cluster.seed_nodes == [cluster.nodes[0], cluster.nodes[1]]
    assert cluster.non_seed_nodes == [cluster.nodes[2]]
    assert cluster.seed_nodes_addresses == [cluster.nodes[0].ip_address, cluster.nodes[1].ip_address]


def test_reuse_cluster_seed(make_cluster, test_data_dir, monkeypatch, reuse_cluster):
    """With reuse_cluster the seeds come from the scylla.yaml of the existing cluster."""
    cluster = make_cluster(nodes_number=3)
    cluster.set_test_params(seeds_selector="first", seeds_num=2, db_type="scylla")
    monkeypatch.setattr(sdcm.cluster, "SCYLLA_YAML_PATH", str(test_data_dir / "scylla.yaml"))
    cluster.set_seeds()
    assert cluster.seed_nodes == [cluster.nodes[1]]
    assert cluster.non_seed_nodes == [cluster.nodes[0], cluster.nodes[2]]
    assert cluster.seed_nodes_addresses == [cluster.nodes[1].ip_address]


def test_random_2_seeds(make_cluster):
    """seeds_selector=random with seeds_num=2 picks two seeds and leaves one non-seed."""
    cluster = make_cluster(nodes_number=3)
    cluster.set_test_params(seeds_selector="random", seeds_num=2, db_type="scylla")
    cluster.set_seeds()
    assert len(cluster.seed_nodes) == 2
    assert len(cluster.non_seed_nodes) == 1
    assert len(cluster.seed_nodes_addresses) == 2


def test_first_1_seed(make_cluster):
    """seeds_selector=first with seeds_num=1 makes the single node a seed."""
    cluster = make_cluster(nodes_number=1)
    cluster.set_test_params(seeds_selector="first", seeds_num=1, db_type="scylla")
    cluster.set_seeds()
    assert cluster.seed_nodes == [cluster.nodes[0]]
    assert cluster.non_seed_nodes == []
    assert cluster.seed_nodes_addresses == [cluster.nodes[0].ip_address]

"""Mock classes"""

from dataclasses import dataclass, field

from sdcm.cluster import BaseScyllaCluster
from sdcm.utils.nemesis_utils.node_allocator import NemesisNodeAllocator
from unit_tests.dummy_remote import LocalLoaderSetDummy


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)


@dataclass
class Node:
    running_nemesis = None
    public_ip_address: str = '127.0.0.1'
    name: str = 'Node1'

    @property
    def scylla_shards(self):
        return 8

    def log_message(self, *args, **kwargs):
        pass


@dataclass
class Cluster:
    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)

    def check_cluster_health(self):
        pass

    @property
    def data_nodes(self):
        return self.nodes

    @property
    def zero_nodes(self):
        return self.nodes

    def log_message(self, *args, **kwargs):
        pass


@dataclass
class FakeTester:
    params: dict = field(default_factory=lambda: PARAMS)
    loaders: LocalLoaderSetDummy = field(default_factory=LocalLoaderSetDummy)
    db_cluster: Cluster | BaseScyllaCluster = field(default_factory=lambda: Cluster(nodes=[Node(), Node()]))
    monitors: list = field(default_factory=list)
    nemesis_allocator: NemesisNodeAllocator = None

    @property
    def all_db_nodes(self):
        return self.db_cluster.nodes if self.db_cluster else []

    def __post_init__(self):
        self.db_cluster.params = self.params
        self.nemesis_allocator = NemesisNodeAllocator(self)

    def create_stats(self):
        pass

    def update(self, *args, **kwargs):
        pass

    def get_scylla_versions(self):
        pass

    def get_test_details(self):
        pass

    def id(self):
        return 0

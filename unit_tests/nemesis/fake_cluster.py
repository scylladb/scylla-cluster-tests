"""Mock classes"""

from dataclasses import dataclass, field
from unittest.mock import MagicMock

from sdcm.cluster import BaseScyllaCluster
from sdcm.utils.nemesis_utils.node_allocator import NemesisNodeAllocator
from unit_tests.dummy_remote import LocalLoaderSetDummy


PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)


class FakeTestConfig:
    """Mock TestConfig for argus client calls"""

    def argus_client(self):
        mock_client = MagicMock()
        mock_client.submit_nemesis = MagicMock()
        mock_client.finalize_nemesis = MagicMock()
        return mock_client


@dataclass
class Node:
    running_nemesis = None
    public_ip_address: str = "127.0.0.1"
    name: str = "Node1"
    _test_config: FakeTestConfig = field(default_factory=FakeTestConfig)

    @property
    def test_config(self):
        return self._test_config

    @property
    def scylla_shards(self):
        return 8

    def log_message(self, *args, **kwargs):
        pass


@dataclass
class Cluster:
    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)
    _test_config: FakeTestConfig = field(default_factory=FakeTestConfig)

    @property
    def test_config(self):
        return self._test_config

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
    events_processes_registry: object = None

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

    def get_event_summary(self):
        """Return empty event summary for tests"""
        return {}

    def id(self):
        return 0

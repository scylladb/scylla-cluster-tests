"""Mock classes"""

from contextlib import contextmanager
from dataclasses import dataclass, field
from unittest.mock import MagicMock

from sdcm.cluster import BaseScyllaCluster
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.lib.dummy_remote import LocalLoaderSetDummy

PARAMS = dict(nemesis_interval=1, nemesis_filter_seeds=False)


@dataclass
class Node:
    """Fake db node with a name-based hash and a fixed shard count."""

    running_nemesis = None
    public_ip_address: str = "127.0.0.1"
    name: str = "Node1"
    _is_zero_token_node: bool = False

    def __hash__(self):
        """Hash by name so the node can be used in sets."""
        return hash(self.name)

    @property
    def scylla_shards(self):
        """Fixed shard count reported for every fake node."""
        return 8

    def log_message(self, *args, **kwargs):
        """Discard log messages."""


@dataclass
class Cluster:
    """Fake db cluster exposing its nodes as data and zero-token nodes."""

    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)

    def check_cluster_health(self):
        """No-op health check."""

    @property
    def data_nodes(self):
        """All nodes of the fake cluster."""
        return self.nodes

    @property
    def zero_nodes(self):
        """All nodes of the fake cluster, as zero-token nodes."""
        return self.nodes

    def log_message(self, *args, **kwargs):
        """Discard log messages."""


@dataclass
class FakeTester:
    """Fake SCT tester wiring a fake db cluster to a real NemesisNodeAllocator."""

    params: dict = field(default_factory=lambda: PARAMS)
    loaders: LocalLoaderSetDummy = field(default_factory=LocalLoaderSetDummy)
    db_cluster: Cluster | BaseScyllaCluster = field(default_factory=lambda: Cluster(nodes=[Node(), Node()]))
    monitors: list = field(default_factory=list)
    nemesis_allocator: NemesisNodeAllocator = None

    @property
    def all_db_nodes(self):
        """Nodes of db_cluster, or an empty list when there is no cluster."""
        return self.db_cluster.nodes if self.db_cluster else []

    def __post_init__(self):
        """Share params with the db cluster and build the nemesis allocator and a MagicMock test_config."""
        self.db_cluster.params = self.params
        self.nemesis_allocator = NemesisNodeAllocator(self)
        self.test_config = MagicMock()

    def create_stats(self):
        """No-op stats creation."""

    def update(self, *args, **kwargs):
        """No-op stats update."""

    def get_scylla_versions(self):
        """No-op version lookup."""

    def get_test_details(self):
        """No-op test details lookup."""

    def id(self):
        """Fixed test id of 0."""
        return 0

    @contextmanager
    def run_nemesis_hooks(self):
        """Context manager that runs no nemesis hooks."""
        yield

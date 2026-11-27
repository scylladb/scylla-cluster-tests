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
    """Minimal DB node stand-in with a name, public IP and zero-token flag, hashed by name."""

    running_nemesis = None
    public_ip_address: str = "127.0.0.1"
    name: str = "Node1"
    _is_zero_token_node: bool = False

    def __hash__(self):
        return hash(self.name)

    @property
    def scylla_shards(self):
        """Return a fixed shard count of 8."""
        return 8

    def log_message(self, *args, **kwargs):
        """Stub: accept the log message and return immediately."""


@dataclass
class Cluster:
    """Minimal DB cluster stand-in whose data and zero-token nodes are all its nodes."""

    nodes: list
    params: dict = field(default_factory=lambda: PARAMS)

    def check_cluster_health(self):
        """Stub: return immediately without raising, so the health check passes."""

    @property
    def data_nodes(self):
        """Return all nodes as data nodes."""
        return self.nodes

    @property
    def zero_nodes(self):
        """Return all nodes as zero-token nodes."""
        return self.nodes

    def log_message(self, *args, **kwargs):
        """Stub: accept the log message and return immediately."""


@dataclass
class FakeTester:
    """Minimal tester stand-in with params, dummy loaders, a two-node cluster and a nemesis node allocator."""

    params: dict = field(default_factory=lambda: PARAMS)
    loaders: LocalLoaderSetDummy = field(default_factory=LocalLoaderSetDummy)
    db_cluster: Cluster | BaseScyllaCluster = field(default_factory=lambda: Cluster(nodes=[Node(), Node()]))
    monitors: list = field(default_factory=list)
    nemesis_allocator: NemesisNodeAllocator = None

    @property
    def all_db_nodes(self):
        """Return the DB cluster's nodes, or an empty list without a cluster."""
        return self.db_cluster.nodes if self.db_cluster else []

    def __post_init__(self):
        self.db_cluster.params = self.params
        self.nemesis_allocator = NemesisNodeAllocator(self)
        self.test_config = MagicMock()

    def create_stats(self):
        """Stub: accept the call and return immediately; the tests need no stats."""

    def update(self, *args, **kwargs):
        """Stub: accept the stats update and return immediately."""

    def get_scylla_versions(self):
        """Return None; the tests need no Scylla versions."""

    def get_test_details(self):
        """Return None; the tests need no test details."""

    def id(self):
        """Return a fixed test ID of 0."""
        return 0

    @contextmanager
    def run_nemesis_hooks(self):
        """Yield without running any nemesis hooks."""
        yield

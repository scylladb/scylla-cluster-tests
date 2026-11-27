from collections import namedtuple
from unittest.mock import MagicMock

import pytest

from sdcm.exceptions import DatacenterNotResolvedError
from sdcm.utils import replication_strategy_utils as replication_strategy_utils_module
from sdcm.utils.replication_strategy_utils import (
    DataCenterTopologyRfControl,
    LocalReplicationStrategy,
    NetworkTopologyReplicationStrategy,
    ReplicationStrategy,
    SimpleReplicationStrategy,
    temporary_replication_strategy_setter,
)


class TestReplicationStrategies:
    """Construction, string rendering and CQL parsing of the replication strategy classes."""

    def test_can_create_simple_replication_strategy(self):
        """SimpleReplicationStrategy renders as a CQL replication map with its replication_factor."""
        strategy = SimpleReplicationStrategy(replication_factor=3)
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 3}"

    def test_can_create_network_topology_replication_strategy(self):
        """NetworkTopologyReplicationStrategy renders a CQL replication map with one RF per datacenter."""
        strategy = NetworkTopologyReplicationStrategy(dc1=3, dc2=8)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 8}"

    def test_can_create_network_topology_replication_strategy_with_default_rf(self):
        """A positional default RF renders as replication_factor ahead of the per-DC RFs."""
        strategy = NetworkTopologyReplicationStrategy(2, dc1=3, dc2=8)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2, 'dc1': 3, 'dc2': 8}"

    def test_can_create_network_topology_replication_strategy_only_with_default_rf(self):
        """A positional default RF alone renders as a map holding only replication_factor."""
        strategy = NetworkTopologyReplicationStrategy(3)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"

    def test_can_create_simple_replication_strategy_from_string(self):
        """from_string parses a SimpleStrategy clause regardless of keyword case and whitespace."""
        strategy = ReplicationStrategy.from_string(
            "REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 4}"
        )
        assert isinstance(strategy, SimpleReplicationStrategy)
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 4}"

        # test regex match is case insensitive and white spaces insensitive
        strategy = ReplicationStrategy.from_string("replication = {'class': 'SimpleStrategy', 'replication_factor': 4}")
        assert isinstance(strategy, SimpleReplicationStrategy)
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 4}"

    def test_can_create_network_topology_replication_strategy_from_string(self):
        """from_string parses a NetworkTopologyStrategy clause and keeps the datacenter names as written."""
        strategy = ReplicationStrategy.from_string(
            "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 2, 'DC2': 8}"
        )
        assert isinstance(strategy, NetworkTopologyReplicationStrategy)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 8}"

    def test_can_create_network_topology_replication_strategy_from_string_with_replication_factor(self):
        """from_string parses a NetworkTopologyStrategy clause that carries only a default replication_factor."""
        strategy = ReplicationStrategy.from_string(
            "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}"
        )
        assert isinstance(strategy, NetworkTopologyReplicationStrategy)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"

    def test_get_replication_startegy_from_string_with_few_curly_braces(self):
        """from_string reads the replication map from a full DESCRIBE line with tablets and class FQCN."""
        strategy = ReplicationStrategy.from_string(
            "replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}"
        )
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 1}"

    def test_cannot_create_network_topology_replication_strategy_without_replication_factor(self):
        """NetworkTopologyReplicationStrategy requires a default RF or at least one per-DC RF."""
        with pytest.raises(ValueError):
            NetworkTopologyReplicationStrategy()

    def test_can_create_local_replication_strategy(self):
        """LocalReplicationStrategy renders as a CQL map holding only the LocalStrategy class."""
        strategy = LocalReplicationStrategy()
        assert str(strategy) == "{'class': 'LocalStrategy'}"


class Cluster:
    """Cluster stub whose CQL session prints executed statements and raises on 'some error'."""

    class Session:
        @staticmethod
        def execute(cql, timeout=None):
            """Print the CQL statement, raising AttributeError when it contains 'some error'."""
            if "some error" in cql:
                raise AttributeError("found some error")
            print(cql)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    @staticmethod
    def cql_connection_patient(node, connect_timeout=None):
        """Return a new printing Session stub."""
        return Cluster.Session()

    @staticmethod
    def wait_for_schema_agreement(timeout=None):
        """Mock method that does nothing."""


class Node:
    """Node stub that prints cqlsh commands and describes every keyspace with SimpleStrategy RF 4."""

    def __init__(self):
        self.parent_cluster = Cluster()

    def run_cqlsh(self, cql):
        """Print the CQL command and return a DESCRIBE result showing SimpleStrategy RF 4."""
        if "some error" in cql:
            raise AttributeError("found some error")
        print(cql)
        ret = namedtuple("Result", "stdout")
        ret.stdout = f"\n dd replication = {SimpleReplicationStrategy(4)}"
        return ret


class TestReplicationStrategySetter:
    """Applying and rolling back keyspace replication through temporary_replication_strategy_setter."""

    def test_temporary_replication_strategy_setter_rolls_back_on_exit(self, capsys):
        """On normal exit, the setter restores every changed keyspace to its original replication strategy."""
        with temporary_replication_strategy_setter(node=Node()) as replication_setter:
            replication_setter(ks=SimpleReplicationStrategy(3), ks2=NetworkTopologyReplicationStrategy(dc1=3, dc2=8))
            replication_setter(ks=NetworkTopologyReplicationStrategy(dc1=8, dc2=9))
        out = iter(capsys.readouterr().out.splitlines())
        assert next(out) == "describe ks"
        assert next(out) == f"ALTER KEYSPACE ks WITH replication = {SimpleReplicationStrategy(3)}"
        assert next(out) == "describe ks2"
        assert next(out) == f"ALTER KEYSPACE ks2 WITH replication = {NetworkTopologyReplicationStrategy(dc1=3, dc2=8)}"
        assert next(out) == f"ALTER KEYSPACE ks WITH replication = {NetworkTopologyReplicationStrategy(dc1=8, dc2=9)}"
        # rollback validation
        assert next(out) == f"ALTER KEYSPACE ks WITH replication = {SimpleReplicationStrategy(4)}"
        assert next(out) == f"ALTER KEYSPACE ks2 WITH replication = {SimpleReplicationStrategy(4)}"
        with pytest.raises(StopIteration):
            # shouldn't do anything else
            next(out)

    def test_temporary_replication_strategy_setter_rolls_back_on_failure(self, capsys):
        """When an ALTER fails, the setter restores the keyspaces it already touched and re-raises the error."""
        with pytest.raises(AttributeError), temporary_replication_strategy_setter(node=Node()) as replication_setter:
            replication_setter(
                keyspace=SimpleReplicationStrategy(3),
                keyspace_x="some error",
                keyspace2=NetworkTopologyReplicationStrategy(dc1=3, dc2=8),
            )
        out = iter(capsys.readouterr().out.splitlines())
        assert next(out) == "describe keyspace"
        assert next(out) == f"ALTER KEYSPACE keyspace WITH replication = {SimpleReplicationStrategy(3)}"
        assert next(out) == "describe keyspace_x"
        # rollback validation
        assert next(out) == f"ALTER KEYSPACE keyspace WITH replication = {SimpleReplicationStrategy(4)}"
        assert next(out) == f"ALTER KEYSPACE keyspace_x WITH replication = {SimpleReplicationStrategy(4)}"
        with pytest.raises(StopIteration):
            # shouldn't do anything else
            next(out)


class DcNode:
    """Node stub whose `.datacenter` returns successive values, simulating a driver
    metadata refresh that resolves the datacenter only after a few reads.
    """

    def __init__(self, datacenter_values, name="node-1", dc_idx=0):
        self.name = name
        self.dc_idx = dc_idx
        self._datacenter_values = iter(datacenter_values)
        self._last_datacenter = None
        self.parent_cluster = MagicMock(data_nodes=[self])

    @property
    def datacenter(self):
        """Return the next datacenter value from the sequence, repeating the last one once it runs out."""
        self._last_datacenter = next(self._datacenter_values, self._last_datacenter)
        return self._last_datacenter


@pytest.fixture
def _fast_datacenter_retries(monkeypatch):
    monkeypatch.setattr(replication_strategy_utils_module, "DATACENTER_RESOLVE_RETRY_STEP", 0.01)
    monkeypatch.setattr(replication_strategy_utils_module, "DATACENTER_RESOLVE_RETRY_TIMEOUT", 0.05)


def test_resolve_datacenter_retries_until_it_becomes_available(_fast_datacenter_retries):
    """DataCenterTopologyRfControl retries the datacenter lookup until the node reports one."""
    node = DcNode(datacenter_values=[None, None, "dc1"])
    rf_control = DataCenterTopologyRfControl(target_node=node)
    assert rf_control.datacenter == "dc1"


def test_resolve_datacenter_gives_up_after_timeout_without_raising(_fast_datacenter_retries):
    """DataCenterTopologyRfControl leaves datacenter as None when the lookup keeps failing past the timeout."""
    node = DcNode(datacenter_values=[None] * 100)
    rf_control = DataCenterTopologyRfControl(target_node=node)
    assert rf_control.datacenter is None


class RaisingDcNode:
    """Node stub whose `.datacenter` raises, simulating a real (non-transient) failure."""

    name = "node-1"
    dc_idx = 0

    def __init__(self):
        self.parent_cluster = MagicMock(data_nodes=[self])

    @property
    def datacenter(self):
        """Raise KeyError to simulate a failed datacenter lookup."""
        raise KeyError("eu-west")


def test_resolve_datacenter_propagates_real_errors_immediately(_fast_datacenter_retries):
    """A lookup error other than an unresolved datacenter propagates from the constructor at once."""
    with pytest.raises(KeyError):
        DataCenterTopologyRfControl(target_node=RaisingDcNode())


def test_get_keyspaces_to_decrease_rf_raises_clear_error_when_datacenter_unresolved(_fast_datacenter_retries):
    """Computing keyspaces to decrease RF raises DatacenterNotResolvedError before querying the session."""
    node = DcNode(datacenter_values=[None] * 100)
    rf_control = DataCenterTopologyRfControl(target_node=node)
    session = MagicMock()

    with pytest.raises(DatacenterNotResolvedError):
        rf_control._get_keyspaces_to_decrease_rf(session=session)

    session.execute.assert_not_called()


RACK_LIST_DESCRIBE = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
    "'DC1': ['RACK1', 'RACK2'], 'DC2': '3'} AND durable_writes = true AND tablets = {'enabled': true};"
)


def test_network_topology_strategy_with_rack_lists():
    """Rack-list replication parses, counts racks as the DC's RF and round-trips through from_string."""
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    expected = "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 3}"
    assert str(strategy) == expected
    assert strategy.replication_factors == [2, 3]
    assert str(ReplicationStrategy.from_string(f"replication = {strategy}")) == expected


def test_network_topology_strategy_drop_dc_with_rack_lists():
    """A strategy built from parsed rack-list factors with one DC dropped to 0 keeps the other DC's rack list."""
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    rollback = NetworkTopologyReplicationStrategy(**{**strategy.replication_factors_per_dc, "DC2": 0})
    assert str(rollback) == "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 0}"
    assert rollback.replication_factors == [2, 0]

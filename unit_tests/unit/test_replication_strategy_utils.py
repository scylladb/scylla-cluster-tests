"""Tests for sdcm.utils.replication_strategy_utils."""

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

RACK_LIST_DESCRIBE = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
    "'DC1': ['RACK1', 'RACK2'], 'DC2': '3'} AND durable_writes = true AND tablets = {'enabled': true};"
)


class Cluster:
    """Stub cluster exposing one session and a cql_connection_patient factory."""

    class Session:
        """Stub CQL session that prints the CQL it gets."""

        @staticmethod
        def execute(cql, timeout=None):
            """Print the CQL, raising AttributeError when it contains 'some error'."""
            if "some error" in cql:
                raise AttributeError("found some error")
            print(cql)

        def __enter__(self):
            """Return the session as a context manager."""
            return self

        def __exit__(self, *args):
            """Close nothing; the stub holds no connection."""

    @staticmethod
    def cql_connection_patient(node, connect_timeout=None):
        """Return a stub session for any node."""
        return Cluster.Session()

    @staticmethod
    def wait_for_schema_agreement(timeout=None):
        """Mock method that does nothing."""


class Node:
    """Stub node holding a stub cluster and a cqlsh runner."""

    def __init__(self):
        """Create the node with a stub parent cluster."""
        self.parent_cluster = Cluster()

    def run_cqlsh(self, cql):
        """Print the CQL and return a result whose stdout shows a SimpleStrategy(4) map, raising on 'some error'."""
        if "some error" in cql:
            raise AttributeError("found some error")
        print(cql)
        ret = namedtuple("Result", "stdout")
        ret.stdout = f"\n dd replication = {SimpleReplicationStrategy(4)}"
        return ret


class DcNode:
    """Node stub whose `.datacenter` returns successive values, simulating a driver
    metadata refresh that resolves the datacenter only after a few reads.
    """

    def __init__(self, datacenter_values, name="node-1", dc_idx=0):
        """Create the stub with a scripted list of datacenter values."""
        self.name = name
        self.dc_idx = dc_idx
        self._datacenter_values = iter(datacenter_values)
        self._last_datacenter = None
        self.parent_cluster = MagicMock(data_nodes=[self])

    @property
    def datacenter(self):
        """Next scripted datacenter value, repeating the last one once the script runs out."""
        self._last_datacenter = next(self._datacenter_values, self._last_datacenter)
        return self._last_datacenter


class RaisingDcNode:
    """Node stub whose `.datacenter` raises, simulating a real (non-transient) failure."""

    name = "node-1"
    dc_idx = 0

    def __init__(self):
        """Create the stub with itself as the only data node."""
        self.parent_cluster = MagicMock(data_nodes=[self])

    @property
    def datacenter(self):
        """Always raise KeyError, as a real lookup failure would."""
        raise KeyError("eu-west")


@pytest.fixture
def fast_datacenter_retries(monkeypatch):
    """Shrink the datacenter resolve retry step and timeout so retry tests run fast."""
    monkeypatch.setattr(replication_strategy_utils_module, "DATACENTER_RESOLVE_RETRY_STEP", 0.01)
    monkeypatch.setattr(replication_strategy_utils_module, "DATACENTER_RESOLVE_RETRY_TIMEOUT", 0.05)


def test_can_create_simple_replication_strategy():
    """SimpleReplicationStrategy renders as a CQL replication map."""
    strategy = SimpleReplicationStrategy(replication_factor=3)
    assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 3}"


def test_can_create_network_topology_replication_strategy():
    """NetworkTopologyReplicationStrategy renders the per-DC replication factors."""
    strategy = NetworkTopologyReplicationStrategy(dc1=3, dc2=8)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 8}"


def test_can_create_network_topology_replication_strategy_with_default_rf():
    """NetworkTopologyReplicationStrategy keeps a default replication_factor next to the per-DC factors."""
    strategy = NetworkTopologyReplicationStrategy(2, dc1=3, dc2=8)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2, 'dc1': 3, 'dc2': 8}"


def test_can_create_network_topology_replication_strategy_only_with_default_rf():
    """NetworkTopologyReplicationStrategy accepts a default replication_factor alone."""
    strategy = NetworkTopologyReplicationStrategy(3)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"


def test_can_create_simple_replication_strategy_from_string():
    """ReplicationStrategy.from_string parses a SimpleStrategy map, ignoring case and whitespace."""
    strategy = ReplicationStrategy.from_string("REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 4}")
    assert isinstance(strategy, SimpleReplicationStrategy)
    assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 4}"

    # test regex match is case insensitive and white spaces insensitive
    strategy = ReplicationStrategy.from_string("replication = {'class': 'SimpleStrategy', 'replication_factor': 4}")
    assert isinstance(strategy, SimpleReplicationStrategy)
    assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 4}"


def test_can_create_network_topology_replication_strategy_from_string():
    """ReplicationStrategy.from_string parses a NetworkTopologyStrategy map with per-DC factors."""
    strategy = ReplicationStrategy.from_string(
        "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 2, 'DC2': 8}"
    )
    assert isinstance(strategy, NetworkTopologyReplicationStrategy)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 8}"


def test_can_create_network_topology_replication_strategy_from_string_with_replication_factor():
    """ReplicationStrategy.from_string parses a NetworkTopologyStrategy map with a default replication_factor."""
    strategy = ReplicationStrategy.from_string(
        "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}"
    )
    assert isinstance(strategy, NetworkTopologyReplicationStrategy)
    assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"


def test_get_replication_startegy_from_string_with_few_curly_braces():
    """ReplicationStrategy.from_string extracts the replication map from a line with other curly-brace options."""
    strategy = ReplicationStrategy.from_string(
        "replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
        "AND durable_writes = true AND tablets = {'enabled': false}"
    )
    assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 1}"


def test_cannot_create_network_topology_replication_strategy_without_replication_factor():
    """NetworkTopologyReplicationStrategy without any factors raises ValueError."""
    with pytest.raises(ValueError):
        NetworkTopologyReplicationStrategy()


def test_can_create_local_replication_strategy():
    """LocalReplicationStrategy renders as LocalStrategy."""
    strategy = LocalReplicationStrategy()
    assert str(strategy) == "{'class': 'LocalStrategy'}"


def test_temporary_replication_strategy_setter_rolls_back_on_exit(capsys):
    """temporary_replication_strategy_setter restores each keyspace's original replication on normal exit."""
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


def test_temporary_replication_strategy_setter_rolls_back_on_failure(capsys):
    """temporary_replication_strategy_setter restores the changed keyspaces when a later change raises."""
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


def test_resolve_datacenter_retries_until_it_becomes_available(fast_datacenter_retries):
    """DataCenterTopologyRfControl keeps retrying the datacenter lookup until it resolves."""
    node = DcNode(datacenter_values=[None, None, "dc1"])
    rf_control = DataCenterTopologyRfControl(target_node=node)
    assert rf_control.datacenter == "dc1"


def test_resolve_datacenter_gives_up_after_timeout_without_raising(fast_datacenter_retries):
    """DataCenterTopologyRfControl stops retrying after the timeout and leaves the datacenter unset."""
    node = DcNode(datacenter_values=[None] * 100)
    rf_control = DataCenterTopologyRfControl(target_node=node)
    assert rf_control.datacenter is None


def test_resolve_datacenter_propagates_real_errors_immediately(fast_datacenter_retries):
    """DataCenterTopologyRfControl re-raises a non-transient lookup error without retrying."""
    with pytest.raises(KeyError):
        DataCenterTopologyRfControl(target_node=RaisingDcNode())


def test_get_keyspaces_to_decrease_rf_raises_clear_error_when_datacenter_unresolved(fast_datacenter_retries):
    """get_keyspaces_to_decrease_rf raises DatacenterNotResolvedError and runs no CQL when the datacenter is unknown."""
    node = DcNode(datacenter_values=[None] * 100)
    rf_control = DataCenterTopologyRfControl(target_node=node)
    session = MagicMock()

    with pytest.raises(DatacenterNotResolvedError):
        rf_control._get_keyspaces_to_decrease_rf(session=session)

    session.execute.assert_not_called()


def test_network_topology_strategy_with_rack_lists():
    """from_string parses rack-list replication in a DESCRIBE line and round-trips through str."""
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    expected = "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 3}"
    assert str(strategy) == expected
    assert strategy.replication_factors == [2, 3]
    assert str(ReplicationStrategy.from_string(f"replication = {strategy}")) == expected


def test_network_topology_strategy_drop_dc_with_rack_lists():
    """Dropping a DC from a rack-list strategy yields a rollback map with that DC at replication factor 0."""
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    rollback = NetworkTopologyReplicationStrategy(**{**strategy.replication_factors_per_dc, "DC2": 0})
    assert str(rollback) == "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 0}"
    assert rollback.replication_factors == [2, 0]

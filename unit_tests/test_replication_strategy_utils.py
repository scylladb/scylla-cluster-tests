from collections import namedtuple
import pytest

from sdcm.utils.replication_strategy_utils import (
    temporary_replication_strategy_setter,
    SimpleReplicationStrategy,
    NetworkTopologyReplicationStrategy,
    ReplicationStrategy,
    LocalReplicationStrategy,
)


class TestReplicationStrategies:
    def test_can_create_simple_replication_strategy(self):
        strategy = SimpleReplicationStrategy(replication_factor=3)
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 3}"

    def test_can_create_network_topology_replication_strategy(self):
        strategy = NetworkTopologyReplicationStrategy(dc1=3, dc2=8)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 8}"

    def test_can_create_network_topology_replication_strategy_with_default_rf(self):
        strategy = NetworkTopologyReplicationStrategy(2, dc1=3, dc2=8)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2, 'dc1': 3, 'dc2': 8}"

    def test_can_create_network_topology_replication_strategy_only_with_default_rf(self):
        strategy = NetworkTopologyReplicationStrategy(3)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"

    def test_can_create_simple_replication_strategy_from_string(self):
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
        strategy = ReplicationStrategy.from_string(
            "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 2, 'DC2': 8}"
        )
        assert isinstance(strategy, NetworkTopologyReplicationStrategy)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'DC1': 2, 'DC2': 8}"

    def test_can_create_network_topology_replication_strategy_from_string_with_replication_factor(self):
        strategy = ReplicationStrategy.from_string(
            "REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 2}"
        )
        assert isinstance(strategy, NetworkTopologyReplicationStrategy)
        assert str(strategy) == "{'class': 'NetworkTopologyStrategy', 'replication_factor': 2}"

    def test_get_replication_startegy_from_string_with_few_curly_braces(self):
        strategy = ReplicationStrategy.from_string(
            "replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}"
        )
        assert str(strategy) == "{'class': 'SimpleStrategy', 'replication_factor': 1}"

    def test_cannot_create_network_topology_replication_strategy_without_replication_factor(self):
        with pytest.raises(ValueError):
            NetworkTopologyReplicationStrategy()

    def test_can_create_local_replication_strategy(self):
        strategy = LocalReplicationStrategy()
        assert str(strategy) == "{'class': 'LocalStrategy'}"


class Cluster:
    class Session:
        @staticmethod
        def execute(cql, timeout=None):
            if "some error" in cql:
                raise AttributeError("found some error")
            print(cql)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    @staticmethod
    def cql_connection_patient(node, connect_timeout=None):
        return Cluster.Session()

    @staticmethod
    def wait_for_schema_agreement(timeout=None):
        """Mock method that does nothing."""


class Node:
    def __init__(self):
        self.parent_cluster = Cluster()

    def run_cqlsh(self, cql):
        if "some error" in cql:
            raise AttributeError("found some error")
        print(cql)
        ret = namedtuple("Result", "stdout")
        ret.stdout = f"\n dd replication = {SimpleReplicationStrategy(4)}"
        return ret


class TestReplicationStrategySetter:
    def test_temporary_replication_strategy_setter_rolls_back_on_exit(self, capsys):
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


RACK_LIST_DESCRIBE = (
    "CREATE KEYSPACE ks WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', "
    "'DC1': ['RACK1', 'RACK2'], 'DC2': '3'} AND durable_writes = true AND tablets = {'enabled': true};"
)


def test_network_topology_strategy_with_rack_lists():
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    expected = "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 3}"
    assert str(strategy) == expected
    assert strategy.replication_factors == [2, 3]
    assert str(ReplicationStrategy.from_string(f"replication = {strategy}")) == expected


def test_network_topology_strategy_drop_dc_with_rack_lists():
    strategy = ReplicationStrategy.from_string(RACK_LIST_DESCRIBE)
    rollback = NetworkTopologyReplicationStrategy(**{**strategy.replication_factors_per_dc, "DC2": 0})
    assert str(rollback) == "{'class': 'NetworkTopologyStrategy', 'DC1': ['RACK1', 'RACK2'], 'DC2': 0}"
    assert rollback.replication_factors == [2, 0]

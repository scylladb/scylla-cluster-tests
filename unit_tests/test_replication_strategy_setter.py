from collections import namedtuple
import pytest

from sdcm.utils.replication_strategy_utils import temporary_replication_strategy_setter, KeyspaceReplicationStrategy


class Node():  # pylint: disable=too-few-public-methods

    def __init__(self):
        pass

    def run_cqlsh(self, cql):  # pylint: disable=no-self-use
        print(cql)
        ret = namedtuple("Result", 'stdout')
        ret.stdout = "\n dd replication = {simple: 3}"
        return ret


class TestReplicationStrategySetter:

    def test_temporary_replication_strategy_setter_rolls_back_on_exit(self, capsys):  # pylint: disable=no-self-use
        with temporary_replication_strategy_setter(node=Node()) as replication_setter:
            replication_setter([KeyspaceReplicationStrategy('keyspace', "my repl strategy"),
                                KeyspaceReplicationStrategy('keyspace2', "my repl strategy 2")])
        out = iter(capsys.readouterr().out.splitlines())
        assert next(out) == "describe keyspace"
        assert next(out) == "ALTER KEYSPACE keyspace WITH replication = my repl strategy"
        assert next(out) == "describe keyspace2"
        assert next(out) == "ALTER KEYSPACE keyspace2 WITH replication = my repl strategy 2"
        # rollback validation
        assert next(out) == "ALTER KEYSPACE keyspace WITH replication = {simple: 3}"
        assert next(out) == "ALTER KEYSPACE keyspace2 WITH replication = {simple: 3}"
        with pytest.raises(StopIteration):
            # shouldn't do anything else
            next(out)

    def test_temporary_replication_strategy_setter_rolls_back_on_failure(self, capsys):  # pylint: disable=no-self-use
        with pytest.raises(AttributeError), temporary_replication_strategy_setter(node=Node()) as replication_setter:
            replication_setter([KeyspaceReplicationStrategy('keyspace', "my repl strategy"), "error injection",
                                KeyspaceReplicationStrategy('keyspace2', "my repl strategy 2")])
        out = iter(capsys.readouterr().out.splitlines())
        assert next(out) == "describe keyspace"
        assert next(out) == "ALTER KEYSPACE keyspace WITH replication = my repl strategy"
        # rollback validation
        assert next(out) == "ALTER KEYSPACE keyspace WITH replication = {simple: 3}"
        with pytest.raises(StopIteration):
            # shouldn't do anything else
            next(out)

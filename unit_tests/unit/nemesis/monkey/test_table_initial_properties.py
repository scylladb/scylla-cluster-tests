"""Tests for sdcm.nemesis.monkey.modify_table.TableInitialProperties.

The registry snapshots table properties as they were before any nemesis ran and
compares them against the ScyllaDB defaults it reads from a canary table, so the
ModifyTable monkeys can leave explicitly configured tables alone (SCT-100).

``is_explicitly_configured()`` is a pure function of the ``initial`` and
``defaults`` dicts, so those tests populate the dicts directly.  Only the
``refresh()`` tests need a session, faked by ``SchemaSession`` below — the one
place in the test suite that mimics CQL.
"""

from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.monkey.modify_table import TableInitialProperties


class SchemaSession:
    """Just enough of a CQL session for ``TableInitialProperties.refresh()``.

    ``tables`` maps "ks.table" to the properties of its ``system_schema.tables``
    row; ``defaults`` is the row the canary table reports.  Everything that is
    not a SELECT (the canary DDL) is recorded in ``ddl``.
    """

    def __init__(self):
        self.defaults: dict = {}
        self.tables: dict[str, dict] = {}
        self.ddl: list[str] = []

    def execute(self, statement: str):
        if not statement.startswith("SELECT"):
            self.ddl.append(statement)
            return None
        if TableInitialProperties.CANARY_KEYSPACE in statement:
            result = MagicMock()
            result.one.return_value = self._row(dict(self.defaults))
            return result
        return [
            self._row({"keyspace_name": ks_cf.split(".")[0], "table_name": ks_cf.split(".")[1], **properties})
            for ks_cf, properties in self.tables.items()
        ]

    @staticmethod
    def _row(properties: dict) -> MagicMock:
        row = MagicMock()
        row._asdict.return_value = properties
        return row


@pytest.fixture()
def session():
    return SchemaSession()


@pytest.fixture()
def cluster(session):
    cluster = MagicMock()
    cluster.racks_count = 3
    cluster.cql_connection_patient.return_value.__enter__.return_value = session
    return cluster


# ---------------------------------------------------------------------------
# refresh() — the only part that talks to the database
# ---------------------------------------------------------------------------


def test_defaults_are_read_from_a_canary_table(cluster, session):
    """The defaults come from a table created with no options, which is then dropped."""
    session.defaults = {"gc_grace_seconds": 864000}
    registry = TableInitialProperties()

    registry.refresh(cluster, node=None)

    assert registry.defaults["gc_grace_seconds"] == 864000
    assert any(cmd.startswith("CREATE TABLE") for cmd in session.ddl)
    assert any(cmd.startswith("DROP KEYSPACE") for cmd in session.ddl)


def test_canary_keyspace_replication_matches_rack_count(cluster, session):
    """The canary keyspace uses RF equal to the per-DC rack count, so its creation
    also succeeds on clusters that enforce ``rf_rack_valid_keyspaces``."""
    registry = TableInitialProperties()

    registry.refresh(cluster, node=None)

    create_keyspace = next(cmd for cmd in session.ddl if cmd.startswith("CREATE KEYSPACE"))
    assert "'replication_factor': 3" in create_keyspace


def test_initial_values_are_captured_only_once(cluster, session):
    """Values captured on first sight survive later refreshes (nemesis ALTERs must not leak in)."""
    session.tables["ks1.tbl1"] = {"comment": "original"}
    registry = TableInitialProperties()
    registry.refresh(cluster, node=None)

    # a nemesis ALTER changes the live schema, and a new table shows up
    session.tables["ks1.tbl1"] = {"comment": "changed-by-nemesis", "gc_grace_seconds": 100}
    session.tables["ks1.tbl2"] = {}
    registry.refresh(cluster, node=None)

    assert registry.initial["ks1.tbl1"]["comment"] == "original"
    assert "ks1.tbl2" in registry.initial
    # the defaults are fetched (and the canary created) exactly once
    assert sum(cmd.startswith("CREATE KEYSPACE") for cmd in session.ddl) == 1


# ---------------------------------------------------------------------------
# is_explicitly_configured() — pure logic over the captured state
# ---------------------------------------------------------------------------


def test_non_default_value_counts_as_explicitly_configured():
    registry = TableInitialProperties()
    registry.defaults = {"comment": "", "gc_grace_seconds": 864000}
    registry.initial = {"ks1.tbl1": {"comment": "set by the test", "gc_grace_seconds": 864000}}

    assert registry.is_explicitly_configured("ks1.tbl1", ["comment"]) is True
    assert registry.is_explicitly_configured("ks1.tbl1", ["gc_grace_seconds"]) is False
    assert registry.is_explicitly_configured("ks1.tbl1", ["gc_grace_seconds", "comment"]) is True


def test_defaults_are_never_assumed():
    """Whatever the canary reports is the default, so no ScyllaDB value is hardcoded.

    Here the cluster's default compaction is ICS, which is not the upstream default:
    a table matching it is untouched, and only a table differing from it counts as
    explicitly configured.
    """
    registry = TableInitialProperties()
    registry.defaults = {"compaction": {"class": "IncrementalCompactionStrategy"}}
    registry.initial = {
        "ks1.same_as_default": {"compaction": {"class": "IncrementalCompactionStrategy"}},
        "ks1.differs": {"compaction": {"class": "LeveledCompactionStrategy"}},
    }

    assert registry.is_explicitly_configured("ks1.same_as_default", ["compaction"]) is False
    assert registry.is_explicitly_configured("ks1.differs", ["compaction"]) is True


def test_unseen_table_counts_as_explicitly_configured():
    """A table whose pre-nemesis values were never captured is conservatively excluded."""
    registry = TableInitialProperties()
    registry.defaults = {}

    assert registry.is_explicitly_configured("ks9.unknown", ["comment"]) is True

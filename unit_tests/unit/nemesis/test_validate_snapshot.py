"""Tests for NemesisRunner._validate_snapshot secondary-index name handling.

Covers https://scylladb.atlassian.net/browse/SCT-426: since Scylla 2026.2.0
(https://github.com/scylladb/scylladb/commit/39baa1870e2cd84304ec453a12955fdb0a0abc61),
`nodetool listsnapshots` reports a secondary index's backing view under its
logical index name instead of the view's physical "<index_name>_index" name.
"""

from collections import namedtuple

import pytest

from sdcm.nemesis import NemesisRunner
from unit_tests.unit.nemesis import TestRunner

SnapshotDetails = namedtuple("SnapshotDetails", ["keyspace_name", "table_name"])
IndexRow = namedtuple("IndexRow", ["keyspace_name", "index_name"])


def _make_runner(scylla_version, ks_cf, index_rows=()):
    runner = TestRunner()
    runner.target_node.scylla_version = scylla_version
    runner.cluster.get_any_ks_cf_list.return_value = ks_cf
    session = runner.cluster.cql_connection_patient.return_value.__enter__.return_value

    def execute(query):
        # keep TestRunner's contract of recording every CQL statement in `executed`
        runner.executed.append(query)
        return list(index_rows)

    session.execute.side_effect = execute
    return runner


def test_validate_snapshot_pre_2026_2_expects_index_suffix():
    """Before the Scylla change, the backing view keeps its "_index" suffix."""
    runner = _make_runner(
        scylla_version="2026.1.5",
        ks_cf=["sec_index.users", "sec_index.users_address_ind_index"],
    )
    snapshot_content = [
        SnapshotDetails("sec_index", "users"),
        SnapshotDetails("sec_index", "users_address_ind_index"),
    ]
    NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot", snapshot_content=snapshot_content)
    assert not runner.executed, "no CQL query expected below the 2026.2.0 version gate"


def test_validate_snapshot_2026_2_plus_normalizes_index_backing_view_name():
    """From 2026.2.0 on, listsnapshots reports the logical index name instead."""
    runner = _make_runner(
        scylla_version="2026.2.3",
        ks_cf=["sec_index.users", "sec_index.users_address_ind_index"],
        index_rows=[IndexRow("sec_index", "users_address_ind")],
    )
    snapshot_content = [
        SnapshotDetails("sec_index", "users"),
        SnapshotDetails("sec_index", "users_address_ind"),
    ]
    NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot", snapshot_content=snapshot_content)
    assert any("system_schema.indexes" in query for query in runner.executed)


def test_validate_snapshot_2026_2_plus_real_mismatch_still_raises():
    """Normalization must not mask a genuinely missing table in the snapshot."""
    runner = _make_runner(
        scylla_version="2026.2.3",
        ks_cf=["sec_index.users", "sec_index.users_address_ind_index"],
        index_rows=[IndexRow("sec_index", "users_address_ind")],
    )
    snapshot_content = [SnapshotDetails("sec_index", "users")]
    with pytest.raises(AssertionError, match="Snapshot content not as expected"):
        NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot", snapshot_content=snapshot_content)


def test_validate_snapshot_2026_2_plus_unrelated_index_named_view_untouched():
    """A plain view whose name happens to end in "_index" must not be renamed.

    An actual secondary index exists alongside it, so the test proves the
    normalization matches on exact (keyspace, backing-view name) pairs rather
    than renaming anything that ends with "_index".
    """
    runner = _make_runner(
        scylla_version="2026.2.3",
        ks_cf=["sec_index.users", "sec_index.users_by_email_index", "sec_index.users_address_ind_index"],
        index_rows=[IndexRow("sec_index", "users_address_ind")],
    )
    snapshot_content = [
        SnapshotDetails("sec_index", "users"),
        SnapshotDetails("sec_index", "users_by_email_index"),
        SnapshotDetails("sec_index", "users_address_ind"),
    ]
    NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot", snapshot_content=snapshot_content)


def test_validate_snapshot_2026_2_plus_kc_snapshot_normalizes_index_backing_view_name():
    """Normalization also applies when the snapshot was taken with "-kc <keyspace>"."""
    runner = _make_runner(
        scylla_version="2026.2.3",
        ks_cf=["sec_index.users", "sec_index.users_address_ind_index", "other_ks.tbl"],
        index_rows=[IndexRow("sec_index", "users_address_ind")],
    )
    snapshot_content = [
        SnapshotDetails("sec_index", "users"),
        SnapshotDetails("sec_index", "users_address_ind"),
    ]
    NemesisRunner._validate_snapshot(runner, nodetool_cmd="snapshot -kc sec_index", snapshot_content=snapshot_content)

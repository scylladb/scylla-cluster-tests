"""Unit tests for the system.truncated record decisions used by the rolling-upgrade truncate check."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from upgrade_test import (
    TruncationCheck,
    UpgradeTest,
    changed_records,
    expected_record_hosts,
    missing_fresh_records,
)

UpgradeTest.__test__ = False

T1 = datetime(2026, 10, 11, 21, 59, 30)
T2 = T1 + timedelta(seconds=5)


def test_expected_hosts_are_replicas_present_before_and_after_the_truncate():
    assert expected_record_hosts({"a", "b", "c"}, {"a", "b", "d"}, {"a", "b", "c", "d", "e", "f"}) == {"a", "b"}


def test_vnode_table_expects_a_record_on_every_host():
    assert expected_record_hosts(set(), set(), {"a", "b", "c"}) == {"a", "b", "c"}


def test_missing_fresh_records_reports_absent_and_stale_rows():
    baseline = {"node-1": T1, "node-2": None, "node-3": T1, "node-4": None}
    found = {"node-1": T2, "node-2": None, "node-3": T1, "node-4": T1}
    assert missing_fresh_records(baseline, found) == {
        "node-2": "no record",
        "node-3": f"record {T1} is not newer than {T1}",
    }


def test_changed_records_reports_gone_and_rewritten_rows():
    recorded = {"node-1": T2, "node-2": T2, "node-3": T2}
    found = {"node-1": T2, "node-2": None, "node-3": T1}
    assert changed_records(recorded, found) == {
        "node-2": f"record {T2} is gone",
        "node-3": f"record {T2} became {T1}",
    }


def test_host_ids_and_records_are_read_from_each_node_over_its_own_connection():
    host_id, table_id = uuid4(), uuid4()
    tester = MagicMock()
    session = tester.db_cluster.cql_connection_patient_exclusive.return_value.__enter__.return_value
    host_row = MagicMock()
    host_row.one.return_value = MagicMock(host_id=host_id)
    # two shards hold a record of truncate_table0 and the oldest one counts; nothing holds the record of truncate_table1
    shard_rows = [MagicMock(table_uuid=table_id, truncated_at=T2), MagicMock(table_uuid=table_id, truncated_at=T1)]
    session.execute.side_effect = lambda query, *args: host_row if "system.local" in query else shard_rows
    node = MagicMock()
    node.name = "node-1"
    nodes_by_host_id, records = UpgradeTest._read_host_ids_and_records(
        tester, [node], {"truncate_table0": table_id, "truncate_table1": uuid4()}
    )
    assert nodes_by_host_id == {str(host_id): node}
    assert records == {"truncate_table0": {"node-1": T1}, "truncate_table1": {"node-1": None}}


def _raise(msg):
    raise AssertionError(msg)


def test_truncate_check_fails_when_every_replica_moved_during_the_truncate():
    tester = MagicMock()
    tester.fail.side_effect = _raise
    tester._tablet_replica_hosts.return_value = {"h2"}
    tester._read_host_ids_and_records.return_value = ({}, {"truncate_table0": {}})
    check = TruncationCheck(
        table_ids={"truncate_table0": uuid4()},
        nodes_by_host_id={"h1": MagicMock(), "h2": MagicMock()},
        replicas_before={"truncate_table0": {"h1"}},
        baseline={"truncate_table0": {}},
    )
    with pytest.raises(AssertionError, match="no replica stayed in place"):
        UpgradeTest._verify_truncation_recorded(tester, MagicMock(), check)

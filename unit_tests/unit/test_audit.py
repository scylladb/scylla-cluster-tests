from datetime import datetime
from pathlib import Path


from sdcm.audit import get_audit_log_rows
from sdcm.cluster import BaseNode


class DummyAuditNode(BaseNode):
    def __init__(self, system_log: Path, **kwargs):
        super().__init__(**kwargs)
        self._system_log_path = system_log

    @property
    def system_log(self):
        return str(self._system_log_path)


def test_get_audit_log_rows_can_be_filtered_by_time(test_data_dir):
    node = DummyAuditNode(
        system_log=test_data_dir / "test_audit.log",
        name="dummy-node",
        parent_cluster=None,
    )
    # no date filter provided
    rows = get_audit_log_rows(node, from_datetime=None)
    assert len(list(rows)) == 69

    # filter by date
    start_time = datetime(2025, 5, 17, 5, 49, 37, 280)  # 2025-05-17T05:49:37.280
    rows = get_audit_log_rows(node, from_datetime=start_time)
    rows = list(rows)
    assert len(rows) == 4
    assert not [row for row in rows if row.event_time < start_time.replace(microsecond=0)]


def test_get_audit_log_rows_can_be_filtered_by_time_comma_separated(test_data_dir):
    node = DummyAuditNode(
        system_log=test_data_dir / "test_audit_comma_sep.log",
        name="dummy-node",
        parent_cluster=None,
    )
    # no date filter provided
    rows = get_audit_log_rows(node, from_datetime=None)
    assert len(list(rows)) == 211

    # filter by date
    start_time = datetime(2025, 7, 19, 16, 1, 31, 790)  # 2025-07-19T16:01:31.790
    rows = get_audit_log_rows(node, from_datetime=start_time)
    rows = list(rows)
    assert len(rows) == 209
    assert not [row for row in rows if row.event_time < start_time.replace(microsecond=0)]


def test_get_audit_log_rows_can_be_filtered_by_category(test_data_dir):
    node = DummyAuditNode(
        system_log=test_data_dir / "test_audit.log",
        name="dummy-node",
        parent_cluster=None,
    )
    # no date filter provided
    rows = get_audit_log_rows(node, from_datetime=None, category="DML")
    rows = list(rows)
    assert rows
    assert not [row for row in rows if row.category != "DML"]

    # filter by date and category
    start_time = datetime(2023, 7, 24, 11, 39, 1, 123)  # 2023-07-24T11:39:01.123
    rows = get_audit_log_rows(node, from_datetime=start_time, category="QUERY")
    rows = list(rows)
    assert rows
    assert not [row for row in rows if row.category != "DML" and row.event_time < start_time.replace(microsecond=0)]


def test_get_audit_log_rows_can_be_filtered_by_operation(test_data_dir):
    node = DummyAuditNode(
        system_log=test_data_dir / "test_audit.log",
        name="dummy-node",
        parent_cluster=None,
    )
    # no date filter provided
    rows = get_audit_log_rows(node, from_datetime=None, operation='USE "audit_keyspace"')
    rows = list(rows)
    assert rows
    assert not [row for row in rows if row.operation != 'USE "audit_keyspace"']

    # filter by date, category and operation
    start_time = datetime(2023, 7, 24, 11, 38, 59, 123)  # 2023-07-24T11:38:59.123
    rows = get_audit_log_rows(node, from_datetime=start_time, category="DML", operation='USE "audit_keyspace"')
    rows = list(rows)
    assert rows
    assert not [
        row
        for row in rows
        if row.category != "DML"
        and row.operation != 'USE "audit_keyspace"'
        and row.event_time < start_time.replace(microsecond=0)
    ]

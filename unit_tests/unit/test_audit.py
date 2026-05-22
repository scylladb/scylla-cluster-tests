from contextlib import contextmanager
from datetime import datetime

import pytest

from sdcm.audit import Audit, AuditConfiguration, get_audit_log_rows
from sdcm.provision.scylla_yaml import ScyllaYaml
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode


class AuditConfigNode:
    name = "dummy-node"

    def __init__(self):
        self.scylla_yaml = ScyllaYaml(audit="none")

    @contextmanager
    def remote_scylla_yaml(self):
        yield self.scylla_yaml


class AuditCluster(DummyDbCluster):
    def __init__(self, node):
        super().__init__([node])
        self.restarted = False

    def restart_scylla(self, nodes=None, random_order=False):
        self.restarted = True


@pytest.fixture
def audit_rules():
    return [
        {
            "sinks": ["syslog"],
            "categories": ["DML"],
            "qualified_table_names": ["audit_keyspace.*"],
            "roles": ["*"],
        }
    ]


@pytest.fixture
def audit_node():
    return AuditConfigNode()


@pytest.fixture
def audit_cluster(audit_node):
    return AuditCluster(audit_node)


@pytest.fixture
def audit_with_rules(audit_cluster, audit_rules):
    audit_cluster.nodes[0].scylla_yaml.update(
        {
            "audit": "syslog",
            "audit_categories": "",
            "audit_tables": "",
            "audit_keyspaces": "",
            "audit_rules": audit_rules,
        }
    )
    return Audit(audit_cluster)


def get_node_scylla_yaml(cluster):
    return cluster.nodes[0].scylla_yaml.model_dump(exclude_defaults=True, exclude_unset=True, exclude_none=True)


def get_audit_log_node(test_data_dir, log_name):
    node = DummyNode(
        name="dummy-node",
        parent_cluster=None,
    )
    node.system_log = str(test_data_dir / log_name)
    return node


def test_configure_audit_rules_keeps_sink_enabled(audit_cluster, audit_rules, events):
    audit = Audit(audit_cluster)

    audit.configure(AuditConfiguration(store="syslog", categories=[], tables=[], keyspaces=[], rules=audit_rules))

    assert get_node_scylla_yaml(audit_cluster) == {
        "audit": "syslog",
        "audit_categories": "",
        "audit_tables": "",
        "audit_keyspaces": "",
        "audit_rules": audit_rules,
    }
    assert audit_cluster.restarted
    assert audit.is_enabled()


def test_empty_audit_rules_do_not_disable_legacy_sink(audit_cluster):
    audit_cluster.nodes[0].scylla_yaml.update({"audit": "syslog", "audit_rules": []})
    audit = Audit(audit_cluster)

    assert audit.is_enabled()


def test_configure_legacy_audit_clears_existing_rules(audit_cluster, audit_with_rules, events):
    audit_with_rules.configure(AuditConfiguration(store="syslog", categories=["DML"], tables=[], keyspaces=["ks"]))

    assert get_node_scylla_yaml(audit_cluster) == {
        "audit": "syslog",
        "audit_categories": "DML",
        "audit_tables": "",
        "audit_keyspaces": "ks",
        "audit_rules": [],
    }


def test_disable_audit_rules_clears_rules(audit_cluster, audit_with_rules, events):
    audit_with_rules.disable()

    assert get_node_scylla_yaml(audit_cluster) == {
        "audit": "none",
        "audit_categories": "",
        "audit_tables": "",
        "audit_keyspaces": "",
        "audit_rules": [],
    }
    assert audit_cluster.restarted
    assert not audit_with_rules.is_enabled()


def test_get_audit_log_rows_can_be_filtered_by_time(test_data_dir):
    node = get_audit_log_node(test_data_dir, "test_audit.log")
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
    node = get_audit_log_node(test_data_dir, "test_audit_comma_sep.log")
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
    node = get_audit_log_node(test_data_dir, "test_audit.log")
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
    node = get_audit_log_node(test_data_dir, "test_audit.log")
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

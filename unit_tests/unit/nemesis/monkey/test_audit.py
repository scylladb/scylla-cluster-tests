"""Tests for sdcm.nemesis.monkey.audit module."""

import copy
import shlex
from datetime import date, datetime
from unittest.mock import ANY, MagicMock, call, patch

import pytest

from sdcm.audit import AuditConfiguration, AuditLogRow
from sdcm.exceptions import AuditLogTestFailure, UnsupportedNemesis
from sdcm.nemesis.monkey.audit import ToggleAuditNemesisSyslog, ToggleAuditRulesNemesisSyslog

MODULE = "sdcm.nemesis.monkey.audit"

pytestmark = pytest.mark.usefixtures("events")


@pytest.fixture()
def scylla_version():
    """Scylla version of the target node; recent enough for both nemesis."""
    return "2026.2.0"


@pytest.fixture()
def scylla_yaml_audit():
    """``audit`` value in the target node's scylla.yaml; None means audit is not configured."""
    return None


@pytest.fixture()
def runner(base_runner, scylla_version, scylla_yaml_audit):
    """``base_runner`` on an enterprise, non-k8s cluster, with a CQL session that records calls only."""
    base_runner.target_node.is_enterprise = True
    base_runner.target_node.scylla_version = scylla_version
    base_runner.target_node.remote_scylla_yaml.return_value.__enter__.return_value.audit = scylla_yaml_audit
    base_runner._is_it_on_kubernetes = MagicMock(return_value=False)
    base_runner._nemesis_stress_failure_handler = MagicMock()
    base_runner.tester.reliable_replication_factor = 3
    base_runner.cluster.cql_session.execute.side_effect = None
    return base_runner


@pytest.fixture()
def audit_log():
    """Audit log rows ``Audit.get_audit_log`` returns per category: 1000 writes, 1000 reads, and the DDL statement."""
    return {
        "DML": audit_log_rows(1000, "DML", "INSERT INTO audit_keyspace.standard1"),
        "QUERY": audit_log_rows(1000, "QUERY", "SELECT * FROM audit_keyspace.standard1"),
        "DDL": audit_log_rows(1, "DDL", "CREATE TABLE IF NOT EXISTS audit_keyspace.audit_cf"),
    }


@pytest.fixture()
def configured_audits():
    """Copies of the configurations passed to ``Audit.configure``, in call order."""
    return []


@pytest.fixture(autouse=True)
def audit(audit_log, configured_audits):
    """Autospecced ``Audit``: disabled at first, serving ``audit_log`` and recording ``configure`` calls."""
    with patch(f"{MODULE}.Audit", autospec=True) as audit_cls:
        audit = audit_cls.return_value
        audit.is_enabled.return_value = False
        audit.get_audit_log.side_effect = lambda category, **kwargs: audit_log[category]
        audit.configure.side_effect = lambda config: configured_audits.append(copy.deepcopy(config))
        yield audit


@pytest.fixture(
    params=[
        pytest.param(ToggleAuditNemesisSyslog, id="categories"),
        pytest.param(ToggleAuditRulesNemesisSyslog, id="audit_rules"),
    ]
)
def monkey_class(request):
    """Each audit nemesis: one configures audit categories and keyspaces, the other audit_rules."""
    return request.param


@pytest.fixture()
def expected_configs(monkey_class):
    """Configurations ``monkey_class`` applies: all categories on audit_keyspace, then all but DML and QUERY."""
    all_categories = ["DCL", "DDL", "AUTH", "ADMIN", "DML", "QUERY"]
    reduced_categories = ["DCL", "DDL", "AUTH", "ADMIN"]
    return {
        ToggleAuditNemesisSyslog: [
            AuditConfiguration(store="syslog", categories=categories, tables=[], keyspaces=["audit_keyspace"])
            for categories in (all_categories, reduced_categories)
        ],
        ToggleAuditRulesNemesisSyslog: [
            AuditConfiguration(
                store="syslog",
                categories=[],
                tables=[],
                keyspaces=[],
                rules=[
                    {
                        "sinks": ["syslog"],
                        "categories": categories,
                        "qualified_table_names": ["audit_keyspace.*"],
                        "roles": ["*"],
                    }
                ],
            )
            for categories in (all_categories, reduced_categories)
        ],
    }[monkey_class]


def audit_log_rows(count, category, operation):
    """``count`` syslog audit rows of one category and operation."""
    row = AuditLogRow(
        date=date(2026, 11, 12),
        node="10.0.0.1",
        event_time=datetime(2026, 11, 12, 10, 0),
        category=category,
        consistency="ONE",
        error=False,
        keyspace_name="audit_keyspace",
        operation=operation,
        source="10.0.0.2",
        table_name="standard1",
        username="anonymous",
    )
    return [row] * count


@pytest.mark.parametrize(
    "setup,match",
    [
        pytest.param(lambda r: setattr(r.target_node, "is_enterprise", False), "Scylla Enterprise", id="oss"),
        pytest.param(lambda r: r._is_it_on_kubernetes.configure_mock(return_value=True), "Kubernetes", id="k8s"),
        pytest.param(lambda r: setattr(r.target_node, "scylla_version", "2024.2.0"), "2025.2", id="old-version"),
    ],
)
def test_unsupported_setups_skip_before_configuring_audit(runner, audit, monkey_class, setup, match):
    """Both nemesis need enterprise Scylla 2025.2+ off Kubernetes, and skip before touching the audit config."""
    setup(runner)
    with pytest.raises(UnsupportedNemesis, match=match):
        monkey_class(runner).disrupt()
    audit.configure.assert_not_called()


@pytest.mark.parametrize(
    "scylla_version,scylla_yaml_audit,match",
    [
        pytest.param("2026.1.0", None, "audit_rules are supported by Scylla 2026.2.0-dev", id="before-2026.2"),
        pytest.param("2026.2.0", "table", "requires audit to be initially disabled", id="audit-configured"),
    ],
)
def test_audit_rules_unsupported_setups_skip_before_configuring_audit(runner, audit, match):
    """The audit_rules nemesis also needs Scylla 2026.2+ and no audit configured in scylla.yaml."""
    with pytest.raises(UnsupportedNemesis, match=match):
        ToggleAuditRulesNemesisSyslog(runner).disrupt()
    audit.configure.assert_not_called()


def test_enabled_audit_is_disabled_and_skipped(runner, audit):
    """Audit already enabled: the nemesis disables it and skips the run."""
    audit.is_enabled.return_value = True

    with pytest.raises(UnsupportedNemesis, match="Audit was enabled -> disabling it"):
        ToggleAuditNemesisSyslog(runner).disrupt()

    audit.disable.assert_called_once_with()
    audit.configure.assert_not_called()


def test_full_flow_audits_stress_then_ddl(runner, monkey_class, configured_audits, expected_configs):
    """Full audit covers a c-s write and read; reduced audit covers CREATE, ALTER and DROP TABLE."""
    monkey_class(runner).disrupt()

    assert configured_audits == expected_configs
    schema = "'replication(strategy=NetworkTopologyStrategy,replication_factor=3) keyspace=audit_keyspace'"
    load = "-mode cql3 native -rate 'threads=1 throttle=1000/s' -pop seq=1..1000 -col 'n=FIXED(1) size=FIXED(128)'"
    stress_calls = runner.tester.run_stress_thread.call_args_list
    assert [shlex.split(stress_call.kwargs["stress_cmd"]) for stress_call in stress_calls] == [
        shlex.split(f"cassandra-stress {mode} no-warmup cl=ONE n=1000 -schema {schema} {load} -log interval=5")
        for mode in ("write", "read")
    ]
    assert stress_calls == [call(stress_cmd=ANY, round_robin=True, stop_test_on_failure=False)] * 2
    assert (
        runner.tester.verify_stress_thread.call_args_list
        == [call(runner.tester.run_stress_thread.return_value, error_handler=runner._nemesis_stress_failure_handler)]
        * 2
    )
    assert runner.cluster.cql_session.execute.call_args_list == [
        call("CREATE TABLE IF NOT EXISTS audit_keyspace.audit_cf (id int PRIMARY KEY, value timestamp)", timeout=300),
        call("ALTER TABLE audit_keyspace.audit_cf WITH read_repair_chance = 0.0", timeout=300),
        call("DROP TABLE audit_keyspace.audit_cf", timeout=300),
    ]


def test_missing_audit_rows_reported_together(runner, monkey_class, audit_log, configured_audits, expected_configs):
    """All missing DML, QUERY and DDL rows are reported in one error, raised after audit is reduced.

    The DML log also holds USE statements, which are not counted (scylladb/scylla-enterprise#3169).
    """
    audit_log.update(
        DML=audit_log_rows(999, "DML", "INSERT INTO audit_keyspace.standard1")
        + audit_log_rows(5, "DML", "USE audit_keyspace"),
        QUERY=[],
        DDL=[],
    )

    with pytest.raises(AuditLogTestFailure) as exc_info:
        monkey_class(runner).disrupt()

    assert str(exc_info.value).splitlines() == [
        "Audit log for DML contains 999 rows while should contain 1000 rows",
        "Audit log for QUERY contains 0 rows while should contain 1000 rows",
        "Audit log is empty while should contain executed DDL (create table) operation",
        "Audit log is empty while should contain executed DDL (alter table) operation",
        "Audit log is empty while should contain executed DDL (drop table) operation",
    ]
    assert configured_audits == expected_configs


def test_stress_failure_reduces_audit_and_reraises(runner, monkey_class, configured_audits, expected_configs):
    """A failure under full audit still reduces the categories, then re-raises before any DDL runs."""
    runner.tester.verify_stress_thread.side_effect = RuntimeError("stress failed")

    with pytest.raises(RuntimeError, match="stress failed"):
        monkey_class(runner).disrupt()

    assert configured_audits == expected_configs
    runner.cluster.cql_session.execute.assert_not_called()

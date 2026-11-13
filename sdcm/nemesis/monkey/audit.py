"""
Module containing the audit log nemesis classes.

Both nemesis enable full syslog auditing on a dedicated keyspace, verify the DML/QUERY
audit log entries, then narrow the audited categories and verify DDL entries are logged.
Audit is left enabled with the reduced categories.
"""

import abc
from datetime import datetime, timedelta

from sdcm.audit import Audit, AuditConfiguration, AuditStore
from sdcm.exceptions import AuditLogTestFailure, UnsupportedNemesis
from sdcm.nemesis import NemesisBaseClass
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.version_utils import ComparableScyllaVersion

REDUCED_AUDIT_CATEGORIES = ["DCL", "DDL", "AUTH", "ADMIN"]


class ToggleAuditBaseMonkey(NemesisBaseClass, abc.ABC):
    """Shared logic for the syslog audit nemesis (legacy categories/keyspaces config and audit_rules)."""

    disruptive = True
    schema_changes = True
    config_changes = True
    free_tier_set = True

    def reduce_audit_categories(self, audit: Audit, audit_config: AuditConfiguration, use_audit_rules: bool):
        """Reconfigure audit with DML and QUERY dropped from the audited categories."""
        if use_audit_rules:
            audit_config.rules[0]["categories"] = REDUCED_AUDIT_CATEGORIES
        else:
            audit_config.categories = REDUCED_AUDIT_CATEGORIES
        with self.runner.action_log_scope(f"Reconfiguring audit with {REDUCED_AUDIT_CATEGORIES} categories"):
            audit.configure(audit_config)

    def toggle_audit(self, store: AuditStore, use_audit_rules: bool = False):  # noqa: PLR0914
        """
        Enable audit log with all categories and user keyspaces (if audit already enabled, disable it and finish the Nemesis),
        verify audit log content,
        reduce categories by excluding DML and QUERY,
        verify DDL are logged in audit log correctly. Leaves audit log enabled this way.
        """
        target_node = self.runner.target_node
        if not target_node.is_enterprise:
            raise UnsupportedNemesis("Auditing feature is only supported by Scylla Enterprise")

        if store == "syslog" and self.runner._is_it_on_kubernetes():
            # generally syslog is not supported on K8S because of different log line format
            raise UnsupportedNemesis("syslog store is not supported on Kubernetes scylladb/scylla-operator#1299")
        if ComparableScyllaVersion(target_node.scylla_version) < ComparableScyllaVersion("2025.2.0-dev"):
            raise UnsupportedNemesis(
                "Audit feature log format was changed in Scylla 2025.2 and later. Use old sct-branch for Scylla < 2025.2"
            )
        if use_audit_rules and (
            ComparableScyllaVersion(target_node.scylla_version) < ComparableScyllaVersion("2026.2.0-dev")
        ):
            raise UnsupportedNemesis("audit_rules are supported by Scylla 2026.2.0-dev and later")

        audit = Audit(self.runner.cluster)

        if audit.is_enabled():
            self.runner.actions_log.info("Disabling audit as it was already enabled")
            audit.disable()
            raise UnsupportedNemesis("Audit was enabled -> disabling it")

        audit_keyspace = "audit_keyspace"
        keyspaces_for_audit = [audit_keyspace]
        audit_categories = ["DCL", "DDL", "AUTH", "ADMIN", "DML", "QUERY"]
        audit_rules = None
        if use_audit_rules:
            audit_rules = [
                {
                    "sinks": [store],
                    "categories": audit_categories,
                    "qualified_table_names": [f"{audit_keyspace}.*"],
                    "roles": ["*"],
                }
            ]
            audit_categories = []
            keyspaces_for_audit = []
        InfoEvent(f"Enabling full audit for keyspaces: {[audit_keyspace]}").publish()
        audit_config = AuditConfiguration(
            store=store,
            categories=audit_categories,
            keyspaces=keyspaces_for_audit,
            tables=[],
            rules=audit_rules,
        )
        errors = []
        try:
            with self.runner.action_log_scope(
                f"Enable {store} audit on categories: {audit_config.categories or audit_rules} "
                f"in {[audit_keyspace]} keyspace"
            ):
                audit.configure(audit_config)
            audit_start = datetime.now() - timedelta(seconds=5)
            InfoEvent(message="Writing/Reading data from audited keyspace").publish()
            rf = self.runner.tester.reliable_replication_factor
            write_cmd = (
                f"cassandra-stress write no-warmup cl=ONE n=1000 -schema"
                f" 'replication(strategy=NetworkTopologyStrategy,replication_factor={rf})"
                f" keyspace={audit_keyspace}' -mode cql3 native -rate 'threads=1 throttle=1000/s'"
                f" -pop seq=1..1000 -col 'n=FIXED(1) size=FIXED(128)' -log interval=5"
            )
            write_thread = self.runner.tester.run_stress_thread(
                stress_cmd=write_cmd, round_robin=True, stop_test_on_failure=False
            )
            self.runner.tester.verify_stress_thread(
                write_thread, error_handler=self.runner._nemesis_stress_failure_handler
            )
            read_cmd = (
                f"cassandra-stress read no-warmup cl=ONE n=1000 "
                f" -schema 'replication(strategy=NetworkTopologyStrategy,replication_factor={rf})"
                f" keyspace={audit_keyspace}' -mode cql3 native -rate 'threads=1 throttle=1000/s'"
                f" -pop seq=1..1000 -col 'n=FIXED(1) size=FIXED(128)' -log interval=5"
            )
            read_thread = self.runner.tester.run_stress_thread(
                stress_cmd=read_cmd, round_robin=True, stop_test_on_failure=False
            )
            self.runner.tester.verify_stress_thread(
                read_thread, error_handler=self.runner._nemesis_stress_failure_handler
            )
            InfoEvent(message="Verifying Audit table contents").publish()
            self.runner.actions_log.info("Verifying DML Audit log contents")
            rows = audit.get_audit_log(from_datetime=audit_start, category="DML", limit_rows=1500)
            # filter out USE keyspace rows due to https://github.com/scylladb/scylla-enterprise/issues/3169
            rows = [row for row in rows if not row.operation.startswith("USE")]
            if len(rows) != 1000:
                errors.append(f"Audit log for DML contains {len(rows)} rows while should contain 1000 rows")
                for row in rows:
                    self.runner.log.error("DML audit log row: %s", row)
            self.runner.actions_log.info("Verifying QUERY Audit log contents")
            rows = audit.get_audit_log(from_datetime=audit_start, category="QUERY", limit_rows=1500)
            if len(rows) != 1000:
                errors.append(f"Audit log for QUERY contains {len(rows)} rows while should contain 1000 rows")
                for row in rows:
                    self.runner.log.error("QUERY audit log row: %s", row)
        except Exception as ex:
            self.runner.log.error("Exception while testing full audit: %s", ex)
            self.reduce_audit_categories(audit, audit_config, use_audit_rules)
            raise

        InfoEvent("Reducing audit categories and setting back audited keyspaces").publish()
        self.reduce_audit_categories(audit, audit_config, use_audit_rules)

        table_name = f"{audit_keyspace}.audit_cf"
        ddl_queries = {
            "create table": f"CREATE TABLE IF NOT EXISTS {table_name} (id int PRIMARY KEY, value timestamp)",
            "alter table": f"ALTER TABLE {table_name} WITH read_repair_chance = 0.0",
            "drop table": f"DROP TABLE {table_name}",
        }
        with self.runner.cluster.cql_connection_patient(node=target_node) as session:
            for operation, query in ddl_queries.items():
                audit_start = datetime.now() - timedelta(seconds=5)
                session.execute(query, timeout=300)
                self.runner.cluster.wait_for_schema_agreement()
                audit_rows = audit.get_audit_log(
                    from_datetime=audit_start, category="DDL", operation=query, limit_rows=10
                )
                if not audit_rows:
                    errors.append(f"Audit log is empty while should contain executed DDL ({operation}) operation")

        if errors:
            raise AuditLogTestFailure("\n".join(errors))


class ToggleAuditNemesisSyslog(ToggleAuditBaseMonkey):
    """Enable audit logging to syslog for all categories, verify it, narrow the categories and verify DDL logs."""

    def disrupt(self):
        """Toggle syslog auditing."""
        self.toggle_audit(store="syslog")


class ToggleAuditRulesNemesisSyslog(ToggleAuditBaseMonkey):
    """Enable audit logging to syslog via audit_rules, verify it, narrow the categories and verify DDL logs."""

    additional_configs = ["configurations/toggle-audit-rules-nemesis.yaml"]

    def disrupt(self):
        """Toggle syslog audit rules."""
        with self.runner.target_node.remote_scylla_yaml() as scylla_yaml:
            configured_audit = scylla_yaml.audit or "none"
        if configured_audit != "none":
            raise UnsupportedNemesis("ToggleAuditRulesNemesisSyslog requires audit to be initially disabled")
        self.toggle_audit(store="syslog", use_audit_rules=True)

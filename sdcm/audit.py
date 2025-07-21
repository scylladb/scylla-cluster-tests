# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2023 ScyllaDB
import logging
import re
from dataclasses import dataclass
from datetime import datetime, date
from typing import Literal, Optional, List

from cassandra.util import uuid_from_time, datetime_from_uuid1

from sdcm.sct_events import Severity
from sdcm.sct_events.group_common_events import decorate_with_context, ignore_ycsb_connection_refused
from sdcm.sct_events.system import InfoEvent

LOGGER = logging.getLogger(__name__)
AuditStore = Literal["none", "table", "syslog"]
AuditCategory = Literal["AUTH", "DML", "DDL", "DCL", "QUERY", "ADMIN"]


@dataclass
class AuditConfiguration:
    """https://enterprise.docs.scylladb.com/stable/operating-scylla/security/auditing.html"""

    store: AuditStore  # if store is none, then audit is disabled
    categories: List[AuditCategory]
    tables: List[str]
    keyspaces: List[str]

    @classmethod
    def from_scylla_yaml(cls, scylla_yaml):
        store = scylla_yaml.audit if scylla_yaml.audit else "none"
        categories = scylla_yaml.audit_categories.split(",") if scylla_yaml.audit_categories else []
        tables = scylla_yaml.audit_tables.split(",") if scylla_yaml.audit_tables else []
        keyspaces = scylla_yaml.audit_keyspaces.split(",") if scylla_yaml.audit_keyspaces else []
        return cls(store=store, categories=categories, tables=tables, keyspaces=keyspaces)


@dataclass
class AuditLogRow:
    date: date
    node: str
    event_time: datetime
    category: AuditCategory
    consistency: str
    error: bool
    keyspace_name: str
    operation: str
    source: str
    table_name: str
    username: str


AUDIT_LOG_REGEX = re.compile(
    r"""
    node="(?P<node>.*?)"[,\s]*                # Node identifier
    category="(?P<category>.*?)"[,\s]*        # Audit category (e.g., AUTH, DML)
    cl="(?P<consistency>.*?)"[,\s]*           # Consistency level
    error="(?P<error>.*?)"[,\s]*              # Error flag (true/false)
    keyspace="(?P<keyspace_name>.*?)"[,\s]*   # Keyspace name
    query="(?P<operation>.*?),?"[,\s]*        # Query or operation performed
    client_ip="(?P<source>.*?)"[,\s]*         # Source IP address
    table="(?P<table_name>.*?)"[,\s]*         # Table name
    username="(?P<username>.*?)"[,\s\n]*      # Username of the client
    """,
    re.VERBOSE
)


class AuditLogReader:

    def __init__(self, cluster):
        self._cluster = cluster

    def read(self, from_datetime: Optional[datetime] = None,
             category: Optional[AuditCategory] = None,
             operation: Optional[str] = None,
             limit_rows: int = 1000
             ) -> List[AuditLogRow]:
        raise NotImplementedError()


class TableAuditLogReader(AuditLogReader):

    def read(self, from_datetime: Optional[datetime] = None,
             category: Optional[AuditCategory] = None,
             operation: Optional[str] = None,
             limit_rows: int = 1000
             ) -> List[AuditLogRow]:
        """Return audit log rows based on the given filters."""
        where_list = []
        if from_datetime:
            where_list.append(f"event_time > {uuid_from_time(from_datetime.timestamp())}")
        if category:
            where_list.append(f"category = '{category}'")
        if operation:
            where_list.append(f"operation = '{operation}'")

        limit = f" limit {limit_rows}" if limit_rows else ""
        where = " where " + " and ".join(where_list) + f"{limit} ALLOW FILTERING" if where_list else ""
        LOGGER.debug("Audit query filter: %s", where)
        with self._cluster.cql_connection_patient(node=self._cluster.nodes[0]) as session:
            rows = list(session.execute(f"select * from audit.audit_log{where} using timeout 5m"))
        rows = [AuditLogRow(date=row.date.date(),
                            node=row.node,
                            event_time=datetime_from_uuid1(row.event_time),
                            category=row.category,
                            consistency=row.consistency,
                            error=row.error,
                            keyspace_name=row.keyspace_name,
                            operation=row.operation,
                            source=row.source,
                            table_name=row.table_name,
                            username=row.username) for row in rows
                ]
        LOGGER.debug("Found %s audit log rows", len(rows))
        return rows


def get_audit_log_rows(node,
                       from_datetime: Optional[datetime] = None,
                       category: Optional[AuditCategory] = None,
                       operation: Optional[str] = None,
                       limit_rows: int = 1000
                       ) -> List[AuditLogRow]:
    with node.open_system_log(on_datetime=from_datetime) as log_file:
        found_rows = 0
        for line in log_file:
            if '!NOTICE' in line[:120] and 'scylla-audit' in line[:120]:
                while line[-2] != '"':
                    # read multiline audit log (must end with ")
                    line += log_file.readline()  # noqa: PLW2901
                if match := AUDIT_LOG_REGEX.search(line):
                    found_audit_log_fields = match.groupdict()
                    if category and found_audit_log_fields.get('category', "") != category:
                        continue
                    found_audit_log_fields["operation"] = re.sub(
                        r'\\(.)', r'\1', found_audit_log_fields.get('operation', ""))
                    if operation and found_audit_log_fields["operation"] != operation:
                        continue
                    event_time = datetime.fromisoformat(line.split(' ')[0]).replace(tzinfo=None)
                    event_date = event_time.date()
                    found_audit_log_fields['error'] = found_audit_log_fields.get('error', 'false') == 'true'

                    yield AuditLogRow(
                        date=event_date,
                        event_time=event_time,
                        **found_audit_log_fields)
                    found_rows += 1
                    if found_rows >= limit_rows:
                        break
                else:
                    LOGGER.error("Failed to parse audit log line: %s", line)
                    continue


class SyslogAuditLogReader(AuditLogReader):

    def read(self, from_datetime: Optional[datetime] = None, category: Optional[AuditCategory] = None,
             operation: Optional[str] = None,
             limit_rows: int = 1000) -> List:
        """Return audit log rows from syslog based on the given filters."""
        rows = []
        for node in self._cluster.nodes:
            rows += list(get_audit_log_rows(node,
                                            from_datetime=from_datetime,
                                            category=category,
                                            operation=operation,
                                            limit_rows=limit_rows))
        LOGGER.debug("Found %s audit log rows", len(rows))
        return rows


class Audit:
    """Manage audit state and query audit log on Scylla cluster."""

    def __init__(self, cluster: "BaseCluster"):  # noqa: F821
        self._cluster = cluster
        self._configuration = self._get_audit_configuration()

    def _get_audit_configuration(self) -> AuditConfiguration:
        """Return audit configuration from Scylla cluster (and check that it is consistent across the nodes)."""
        prev_audit_config = None
        for node in self._cluster.nodes:
            with node.remote_scylla_yaml() as scylla_yaml:
                audit_config = AuditConfiguration.from_scylla_yaml(scylla_yaml)
                if prev_audit_config and prev_audit_config != audit_config:
                    InfoEvent(message=f"audit configuration is inconsistent between nodes: {prev_audit_config} != {audit_config}",
                              severity=Severity.ERROR).publish()
        return audit_config

    @decorate_with_context(ignore_ycsb_connection_refused)
    def configure(self, audit_configuration: AuditConfiguration):
        """Configure audit on all nodes in the cluster and restart them."""
        LOGGER.debug("Configuring audit on all nodes: %s", audit_configuration)
        for node in self._cluster.nodes:
            LOGGER.debug("Configuring audit on node %s", node.name)
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.update({
                    'audit': audit_configuration.store,
                    'audit_categories': ",".join(audit_configuration.categories),
                    'audit_tables': ",".join(audit_configuration.tables),
                    'audit_keyspaces': ",".join(audit_configuration.keyspaces)
                })
        self._cluster.restart_scylla()
        LOGGER.debug("Audit configuration completed")
        self._configuration = audit_configuration

    def get_audit_log(self, from_datetime: Optional[datetime] = None, category: Optional[AuditCategory] = None,
                      operation: Optional[str] = None,
                      limit_rows: int = 1000) -> List:
        """Return audit log rows based on the given filters."""
        reader: AuditLogReader
        if not self.is_enabled():
            LOGGER.warning("Audit is not enabled skipping query")
            return []
        if self._configuration.store == 'table':
            reader = TableAuditLogReader(self._cluster)
        else:
            reader = SyslogAuditLogReader(self._cluster)
        return reader.read(from_datetime=from_datetime, category=category, operation=operation, limit_rows=limit_rows)

    def is_enabled(self):
        return self._configuration.store != "none"

    def disable(self):
        self.configure(AuditConfiguration(store="none", categories=[], tables=[], keyspaces=[]))
        LOGGER.info("Audit has been disabled on all nodes")

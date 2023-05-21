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
from dataclasses import dataclass
from typing import Literal, Optional, List

from cassandra.util import uuid_from_time  # pylint: disable=no-name-in-module

from sdcm.sct_events import Severity
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


class Audit:
    """Manage audit state and query audit log on Scylla cluster."""

    def __init__(self, cluster: "BaseCluster"):
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

    def configure(self, audit_configuration: AuditConfiguration):
        """Configure audit on all nodes in the cluster and restart them."""
        LOGGER.debug("Configuring audit on all nodes: %s", audit_configuration)
        for node in self._cluster.nodes:
            LOGGER.debug("Configuring audit on node %s", node.name)
            with node.remote_scylla_yaml() as scylla_yaml:
                scylla_yaml.audit = audit_configuration.store
                scylla_yaml.audit_categories = ",".join(audit_configuration.categories)
                scylla_yaml.audit_tables = ",".join(audit_configuration.tables)
                scylla_yaml.audit_keyspaces = ",".join(audit_configuration.keyspaces)
        self._cluster.restart_scylla()
        LOGGER.debug("Audit configuration completed")
        self._configuration = audit_configuration

    def get_audit_log(self, from_timestamp: Optional[float] = None, category: Optional[AuditCategory] = None,
                      operation: Optional[str] = None,
                      limit_rows: int = None) -> List:
        """Return audit log rows based on the given filters."""
        where_list = []
        if from_timestamp:
            where_list.append(f"event_time > {uuid_from_time(from_timestamp)}")
        if category:
            where_list.append(f"category = '{category}'")
        if operation:
            where_list.append(f"operation = '{operation}'")

        limit = f" limit {limit_rows}" if limit_rows else ""
        where = " where " + " and ".join(where_list) + f"{limit} ALLOW FILTERING" if where_list else ""
        LOGGER.debug("Audit query filter: %s", where)
        with self._cluster.cql_connection_patient(node=self._cluster.nodes[0]) as session:
            rows = list(session.execute(f"select * from audit.audit_log{where} using timeout 5m"))
        LOGGER.debug("Found %s audit log rows", len(rows))

        return rows

    def is_enabled(self):
        return self._configuration.store != "none"

    def disable(self):
        self.configure(AuditConfiguration(store="none", categories=[], tables=[], keyspaces=[]))
        LOGGER.info("Audit has been disabled on all nodes")

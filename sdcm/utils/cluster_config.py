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
# Copyright (c) 2026 ScyllaDB

"""CQL cluster configuration: options stored at CLUSTER, KEYSPACE or TABLE scope.

A value stored at a narrower scope wins: TABLE over KEYSPACE over CLUSTER. Scylla also has
DATACENTER, RACK and NODE scopes, but no option supports them yet.

https://docs.scylladb.com/manual/master/cql/cluster-config.html
"""

import logging
from enum import StrEnum

LOGGER = logging.getLogger(__name__)


class ConfigScope(StrEnum):
    """Scope that stores an option. The value is the keyword of the ALTER statement."""

    CLUSTER = "CLUSTER"
    KEYSPACE = "KEYSPACE"
    TABLE = "TABLE"


# Where system_schema stores the options of each scope
SCOPE_STORAGE = {
    ConfigScope.CLUSTER: "scylla_clusters",
    ConfigScope.KEYSPACE: "scylla_keyspaces WHERE keyspace_name = %s",
    ConfigScope.TABLE: "scylla_tables WHERE keyspace_name = %s AND table_name = %s",
}


def get_cluster_config(
    node, option: str, scope: ConfigScope = ConfigScope.CLUSTER, name: str | None = None
) -> str | None:
    """Return the value stored for an option at one scope, or None when nothing is stored there.

    `name` is the keyspace for the KEYSPACE scope, `keyspace.table` for the TABLE scope, and not
    given for the CLUSTER scope. This is the stored value, not the effective one: a table that
    stores nothing returns None even when its keyspace or the cluster stores a value.

    Reads the system_schema tables that store the options, until Scylla has a statement for it (SCYLLADB-5994).

    Raises InvalidRequest on Scylla releases without CQL cluster configuration.
    """
    scope_key = name.split(".") if name else ()
    with node.parent_cluster.cql_connection_patient(node, verbose=False) as session:
        row = session.execute(
            f"SELECT configs[%s] FROM system_schema.{SCOPE_STORAGE[scope]}", (option, *scope_key)
        ).one()
    return row[0] if row else None


def set_cluster_config(
    node, option: str, value: bool | str, scope: ConfigScope = ConfigScope.CLUSTER, name: str | None = None
) -> None:
    """Store an option value at one scope, running the statement on `node`. Raises when Scylla rejects it.

    `scope` and `name` are as in get_cluster_config. A boolean is sent as true or false. Any other value
    is sent as a quoted string, which Scylla parses as the option's type, so '3600' sets an integer option.
    """
    target = f"{scope} {name}" if name else scope
    literal = str(value).lower() if isinstance(value, bool) else f"'{value}'"
    statement = f"ALTER {target} WITH {option} = {literal}"
    with node.parent_cluster.cql_connection_patient(node, verbose=False) as session:
        session.execute(statement)
    LOGGER.info("Ran %s on node %s", statement, node.name)

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

"""CQL cluster configuration at CLUSTER scope (ALTER CLUSTER).

https://docs.scylladb.com/manual/master/cql/cluster-config.html
"""

import logging
from typing import Optional

LOGGER = logging.getLogger(__name__)

# Partition key of the single row of system_schema.scylla_clusters, fixed by Scylla
# (CLUSTER_CONFIG_SINGLETON_KEY) rather than the cluster name.
CLUSTER_SCOPE_KEY = "cluster"


def alter_cluster_statement(option: str, value: bool | int | str) -> str:
    """Return the ALTER CLUSTER statement that stores an option value.

    A string is sent quoted, and Scylla parses it as the option's type, so 'true' or '3600' work
    as well as true or 3600.
    """
    if isinstance(value, bool):
        literal = str(value).lower()
    elif isinstance(value, int):
        literal = str(value)
    else:
        literal = "'{}'".format(value.replace("'", "''"))
    return f"ALTER CLUSTER WITH {option} = {literal}"


def get_cluster_config(db_cluster, option: str) -> Optional[str]:
    """Return the value stored for a CQL cluster configuration option at CLUSTER scope, or None
    when nothing is stored there. Raises InvalidRequest on Scylla releases without CQL cluster
    configuration, where system_schema.scylla_clusters does not exist.
    """
    query = "SELECT configs[%s] FROM system_schema.scylla_clusters WHERE cluster_name = %s"
    with db_cluster.cql_connection_patient(db_cluster.data_nodes[0], verbose=False) as session:
        row = session.execute(query, (option, CLUSTER_SCOPE_KEY)).one()
    return row[0] if row else None


def set_cluster_config(db_cluster, option: str, value: bool | int | str) -> bool:
    """Store a CQL cluster configuration option at CLUSTER scope, returning False on failure."""
    statement = alter_cluster_statement(option, value)
    try:
        with db_cluster.cql_connection_patient(db_cluster.data_nodes[0], verbose=False) as session:
            session.execute(statement)
    except Exception as exc:  # noqa: BLE001
        LOGGER.error("Failed to run %s: %s", statement, exc)
        return False
    LOGGER.info("Ran %s", statement)
    return True


def apply_cluster_config(db_cluster, cluster_config: dict) -> None:
    """Store each option of the `cluster_config` SCT parameter at CLUSTER scope.

    Unlike set_cluster_config, a failure raises: a test that asks for a setting must not run without it.
    """
    with db_cluster.cql_connection_patient(db_cluster.data_nodes[0]) as session:
        for option, value in cluster_config.items():
            statement = alter_cluster_statement(option, value)
            session.execute(statement)
            LOGGER.info("Ran %s", statement)

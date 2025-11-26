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
import random
import time

from cassandra.query import SimpleStatement

from sdcm.cluster import BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsFilter
from sdcm.sct_events.system import InfoEvent
from sdcm.exceptions import UnsupportedNemesis
from sdcm.utils.cql_utils import cql_quote_if_needed, cql_unquote_if_needed

LOGGER = logging.getLogger(__name__)


def is_cf_a_view(node: BaseNode, ks, cf) -> bool:
    """
    Check if a CF is a materialized-view or not (a normal table)
    """
    with node.parent_cluster.cql_connection_patient(node) as session:
        try:
            result = session.execute(f"SELECT view_name FROM system_schema.views"
                                     f" WHERE keyspace_name = '{ks}'"
                                     f" AND view_name = '{cf}'")
            return result and bool(len(result.one()))
        except Exception as exc:  # noqa: BLE001
            LOGGER.debug('Got no result from system_schema.views for %s.%s table. Error: %s', ks, cf, exc)
            return False


def get_column_names(session, ks, cf, is_primary_key: bool = False, filter_out_collections: bool = False,
                     filter_out_static_columns: bool = False, filter_out_column_types: list[str] | None = None) -> list:
    column_types = "'regular'" if filter_out_static_columns else "'static', 'regular'"
    filter_kind = f" kind in ({column_types})" if not is_primary_key else "kind in ('partition_key', 'clustering')"
    res = session.execute(f"SELECT column_name, type FROM system_schema.columns"
                          f" WHERE keyspace_name = '{ks}'"
                          f" AND table_name = '{cf}'"
                          f" AND {filter_kind}"
                          f" ALLOW FILTERING")
    res_list = list(res)

    filter_out = [*filter_out_column_types] if filter_out_column_types else []
    if filter_out_collections:
        filter_out += ['list', 'set', 'map']

    column_names = [row.column_name for row in res_list if not str(row.type).startswith(tuple(filter_out))]
    return column_names


def get_random_column_name(session, ks, cf, filter_out_collections: bool = False,
                           filter_out_static_columns: bool = False,
                           filter_out_column_types: list[str] | None = None) -> str | None:
    if column_names := get_column_names(session=session, ks=ks, cf=cf, filter_out_collections=filter_out_collections,
                                        filter_out_static_columns=filter_out_static_columns,
                                        filter_out_column_types=filter_out_column_types):
        return random.choice(column_names)
    return None


def create_index(session, ks, cf, column) -> str:
    InfoEvent(message=f"Starting creating index: {ks}.{cf}({column})").publish()
    index_name = f"{cf}_{column}_nemesis".lower()
    session.execute(f'CREATE INDEX {index_name} ON {ks}.{cf}("{column}")', timeout=600)
    return index_name


def wait_for_index_to_be_built(node: BaseNode, ks, index_name, timeout=300) -> None:
    wait_for_view_to_be_built(node=node, ks=ks, view_name=f'{index_name}_index', timeout=timeout)


def wait_for_view_to_be_built(node: BaseNode, ks, view_name, timeout=300) -> None:
    LOGGER.info(f"waiting {timeout} seconds for view/index {view_name} to be built")
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = node.run_nodetool(f"viewbuildstatus {ks}.{view_name}",
                                   ignore_status=True, verbose=True, publish_event=True)
        LOGGER.debug("View Status: %s", result.stdout)
        if f"{ks}.{view_name}_index has finished building" in result.stdout:
            InfoEvent(message=f"Index {ks}.{view_name} was built").publish()
            return
        if f"{ks}.{view_name} has finished building" in result.stdout:
            InfoEvent(message=f"View/index {ks}.{view_name} was built").publish()
            return
        time.sleep(30)
    raise TimeoutError(f"Timeout error ({timeout} seconds) while creating view/index {view_name}.\n"
                       f"stdout\n: {result.stdout}\n"
                       f"stderr\n: {result.stderr}")


def verify_query_by_index_works(session, ks, cf, column) -> None:
    # get some value from table to use it in query by index
    result = session.execute(SimpleStatement(f'SELECT "{column}" FROM {ks}.{cf} limit 1', fetch_size=1))
    value = list(result)[0][0]
    if value is None:
        # scylla does not support 'is null' in where clause: https://github.com/scylladb/scylladb/issues/8517
        InfoEvent(message=f"No value for column {column} in {ks}.{cf}, skipping querying created index.",
                  severity=Severity.NORMAL).publish()
        return
    try:
        query = SimpleStatement(f'SELECT * FROM {ks}.{cf} WHERE "{column}" = %s LIMIT 100', fetch_size=100)
        LOGGER.debug("Verifying query by index works: %s", query)
        result = session.execute(query, parameters=(value,))
    except Exception as exc:  # noqa: BLE001
        InfoEvent(message=f"Index {ks}.{cf}({column}) does not work in query: {query}. Reason: {exc}",
                  severity=Severity.ERROR).publish()
    if len(list(result)) == 0:
        InfoEvent(message=f"Index {ks}.{cf}({column}) does not work. No rows returned for query {query}",
                  severity=Severity.ERROR).publish()


def drop_index(session, ks, index_name) -> None:
    InfoEvent(message=f"Starting dropping index: {ks}.{index_name}").publish()
    session.execute(SimpleStatement(f'DROP INDEX {ks}.{index_name}'), timeout=300)


def drop_materialized_view(session, ks, view_name, timeout=300) -> None:
    LOGGER.info('start dropping MV: %s.%s', ks, view_name)
    with EventsFilter(
            event_class=DatabaseLogEvent.DATABASE_ERROR,
            regex=".*Error applying view update.*",
            extra_time_to_expiration=180):
        session.execute(SimpleStatement(f'DROP MATERIALIZED VIEW {ks}.{view_name}'), timeout=timeout)


def create_materialized_view(session, ks_name, base_table_name, mv_name, mv_partition_key, mv_clustering_key,  # noqa: PLR0913
                             mv_columns='*', speculative_retry=None, read_repair=None, compression=None,
                             gc_grace=None, compact_storage=False):

    # Fix quotes for column names, only use quotes where needed
    if mv_columns != '*':
        mv_columns = [
            cql_quote_if_needed(cql_unquote_if_needed(col))
            for col in (mv_columns if isinstance(mv_columns, list) else list(mv_columns))
        ]
    mv_partition_key = [
        cql_quote_if_needed(cql_unquote_if_needed(pk))
        for pk in (mv_partition_key if isinstance(mv_partition_key, list) else list(mv_partition_key))
    ]
    mv_clustering_key = [
        cql_quote_if_needed(cql_unquote_if_needed(cl))
        for cl in (mv_clustering_key if isinstance(mv_clustering_key, list) else list(mv_clustering_key))
    ]

    where_clause = ' and '.join([f'{kc} is not null' for kc in mv_partition_key + mv_clustering_key])
    select_clause = ', '.join(mv_columns)
    pk_clause = ', '.join(mv_partition_key)
    cl_clause = ', '.join(mv_clustering_key)

    query = f"CREATE MATERIALIZED VIEW {ks_name}.{mv_name} AS SELECT {select_clause} FROM {ks_name}.{base_table_name} " \
        f"WHERE {where_clause} PRIMARY KEY ({pk_clause}, {cl_clause}) WITH comment='test MV'"
    if compression is not None:
        query += f" AND compression = {{ 'sstable_compression': '{compression}Compressor' }}"
    if read_repair is not None:
        query += f" AND read_repair_chance={read_repair}"
    if gc_grace is not None:
        query += f" AND gc_grace_seconds={gc_grace}"
    if speculative_retry is not None:
        query += f" AND speculative_retry='{speculative_retry}'"
    if compact_storage:
        query += ' AND COMPACT STORAGE'

    LOGGER.debug(f'MV create statement: {query}')
    session.execute(query, timeout=600)


def create_materialized_view_for_random_column(session, keyspace_name, base_table_name, view_name):
    """
    Creates a materialized view for a randomly selected column in the specified base table.

    This function selects a random column from the base table that is not a collection type,
    static column, or of an unsupported type (e.g., 'duration', 'counter') and uses it to
    create a materialized view. The primary key columns of the base table are also included
    in the materialized view.

    Args:
        session (cassandra.cluster.Session): The Cassandra session to execute queries.
        keyspace_name (str): The name of the keyspace containing the base table.
        base_table_name (str): The name of the base table for which the materialized view is created.
        view_name (str): The name of the materialized view to be created.

    Raises:
        UnsupportedNemesis: If no suitable column is found for creating the materialized view.

    Notes:
        - The function filters out unsupported column types ('duration', 'counter') and
          collection types when selecting the column for the materialized view.
        - An `InfoEvent` is published to log the creation of the materialized view.
        - A temporary `EventsFilter` is used to suppress specific database log events
          related to errors during the view update process.
    """
    unsupported_primary_key_columns = ['duration', 'counter']
    primary_key_columns = get_column_names(
        session=session, ks=keyspace_name, cf=base_table_name, is_primary_key=True,
        filter_out_column_types=unsupported_primary_key_columns)
    # selecting a supported column for creating a materialized-view (not a collection type).
    column = get_random_column_name(session=session, ks=keyspace_name,
                                    cf=base_table_name, filter_out_collections=True,
                                    filter_out_static_columns=True,
                                    filter_out_column_types=unsupported_primary_key_columns)
    if not column:
        raise UnsupportedNemesis(
            'A supported column for creating MV is not found. nemesis can\'t run')
    InfoEvent(message=f'Create a materialized-view for table {keyspace_name}.{base_table_name}').publish()
    with EventsFilter(event_class=DatabaseLogEvent,
                      regex='.*Error applying view update.*',
                      extra_time_to_expiration=180):
        create_materialized_view(session, keyspace_name, base_table_name, view_name, [column],
                                 primary_key_columns,
                                 mv_columns=[column] + primary_key_columns)


class ViewFinishedBuildingException(Exception):
    """Exception raised when a materialized view has finished building."""


def wait_materialized_view_building_tasks_started(session, ks_name, view_name, timeout=600):
    """
    Waits for materialized view building tasks to start within a specified timeout period.

    This function queries the system schema to check if the specified materialized view exists
    and then monitors the `system.view_building_tasks` table to detect when the building tasks
    for the view have started.

    Args:
        session (cassandra.cluster.Session): The Cassandra session to execute queries.
        ks_name (str): The name of the keyspace containing the materialized view.
        view_name (str): The name of the materialized view to monitor.
        timeout (int, optional): The maximum time (in seconds) to wait for the building tasks
            to start. Defaults to 600 seconds.

    Raises:
        AssertionError: If the specified materialized view is not found in the system schema.
        TimeoutError: If the building tasks do not start within the specified timeout period.
    """
    LOGGER.info(f"waiting {timeout} seconds for view {ks_name}.{view_name} to start building")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            row = session.execute(
                f"select id from system_schema.views where keyspace_name = '{ks_name}' and view_name = '{view_name}'").one()
            assert row.id, f"View {ks_name}.{view_name} was not found"
            result = list(session.execute(
                f"select * from system.view_building_tasks where key = 'view_building' and view_id = {row.id} ALLOW FILTERING;"))
            if len(result) > 0:
                return
            if session.execute(f"SELECT * FROM system.built_views WHERE keyspace_name='{ks_name}' AND view_name='{view_name}'"):
                raise ViewFinishedBuildingException(f"View {ks_name}.{view_name} has already finished building.")
        except ViewFinishedBuildingException:
            raise
        except:
            continue
        time.sleep(15)
    raise TimeoutError(
        f"Timeout error ({timeout} seconds) while waiting for view {ks_name}.{view_name} to start building")

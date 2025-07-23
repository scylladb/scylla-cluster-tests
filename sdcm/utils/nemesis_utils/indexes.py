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
from cassandra.cluster import Session

from sdcm.cluster import BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import EventsFilter
from sdcm.sct_events.system import InfoEvent
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


def create_materialized_view(session: Session, ks_name: str, base_table_name: str, mv_name: str,  # noqa: PLR0913
                             mv_partition_key: str | list[str], mv_clustering_key: str | list[str],
                                mv_columns: str | list[str] = '*', speculative_retry: str = None, read_repair: str = None,
                                compression: str | None = None, gc_grace: str | None = None,
                                compact_storage: bool = False, timeout: int | float = 600):

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
    session.execute(query, timeout=timeout)


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

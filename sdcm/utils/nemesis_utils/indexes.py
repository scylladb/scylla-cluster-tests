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

from cassandra.query import SimpleStatement  # pylint: disable=no-name-in-module

from sdcm.cluster import BaseNode
from sdcm.sct_events import Severity
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.sct_events.system import InfoEvent

LOGGER = logging.getLogger(__name__)


def get_column_names(session, ks, cf, is_partition_key: bool = False) -> list:
    filter_kind = " kind in ('static', 'regular')" if not is_partition_key else "kind = 'partition_key'"
    res = session.execute(f"SELECT column_name FROM system_schema.columns"
                          f" WHERE keyspace_name = '{ks}'"
                          f" AND table_name = '{cf}'"
                          f" AND {filter_kind}"
                          f" ALLOW FILTERING")
    return [row.column_name for row in list(res)]


def get_random_column_name(session, ks, cf) -> str:
    return random.choice(get_column_names(session=session, ks=ks, cf=cf))


def get_partition_key_name(session, ks, cf) -> str:
    return get_column_names(session=session, ks=ks, cf=cf, is_partition_key=True)[0]


def create_index(session, ks, cf, column) -> str:
    InfoEvent(message=f"Starting creating index: {ks}.{cf}({column})").publish()
    index_name = f"{cf}_{column}_nemesis".lower()
    session.execute(f'CREATE INDEX {index_name} ON {ks}.{cf}("{column}")')
    return index_name


def wait_for_index_to_be_built(node: BaseNode, ks, index_name, timeout=300) -> None:
    wait_for_view_to_be_built(node=node, ks=ks, view_name=f'{index_name}_index', timeout=timeout)


def wait_for_view_to_be_built(node: BaseNode, ks, view_name, timeout=300) -> None:
    LOGGER.info('waiting for view/index %s to be built', view_name)
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = node.run_nodetool(f"viewbuildstatus {ks}.{view_name}",
                                   ignore_status=True, verbose=False, publish_event=False)
        if f"{ks}.{view_name}_index has finished building" in result.stdout:
            InfoEvent(message=f"Index {ks}.{view_name} was built").publish()
        if f"{ks}.{view_name} has finished building" in result.stdout:
            InfoEvent(message=f"View/index {ks}.{view_name} was built").publish()
            return
        time.sleep(30)
    raise TimeoutError(f"Timeout error while creating view/index {view_name}. "
                       f"stdout\n: {result.stdout}\n"
                       f"stderr\n: {result.stderr}")


def verify_query_by_index_works(session, ks, cf, column) -> None:
    # get some value from table to use it in query by index
    result = session.execute(SimpleStatement(f'SELECT "{column}" FROM {ks}.{cf} limit 1', fetch_size=1))
    value = list(result)[0][0]
    match type(value).__name__:
        case "str" | "datetime":
            value = f"'{value}'"
        case "bytes":
            value = "0x" + value.hex()
    try:
        query = SimpleStatement(f'SELECT * FROM {ks}.{cf} WHERE "{column}" = {value} LIMIT 100', fetch_size=100)
        LOGGER.debug("Verifying query by index works: %s", query)
        result = session.execute(query)
    except Exception as exc:  # pylint: disable=broad-except
        InfoEvent(message=f"Index {ks}.{cf}({column}) does not work in query: {query}. Reason: {exc}",
                  severity=Severity.ERROR).publish()
    if len(list(result)) == 0:
        InfoEvent(message=f"Index {ks}.{cf}({column}) does not work. No rows returned for query {query}",
                  severity=Severity.ERROR).publish()


def drop_index(session, ks, index_name) -> None:
    InfoEvent(message=f"Starting dropping index: {ks}.{index_name}").publish()
    with DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Error applying view update"):
        session.execute(f'DROP INDEX {ks}.{index_name}')
        time.sleep(30)  # errors can happen only within several seconds after index drop #12977


def drop_materialized_view(session, ks, view_name) -> None:
    LOGGER.info('start dropping MV: %s.%s', ks, view_name)
    with DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Error applying view update"):
        session.execute(f'DROP MATERIALIZED VIEW {ks}.{view_name}')
        time.sleep(30)  # errors can happen only within several seconds after index drop #12977

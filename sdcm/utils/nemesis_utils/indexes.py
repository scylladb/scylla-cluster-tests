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


def get_random_column_name(session, ks, cf) -> str:
    res = session.execute(f"SELECT column_name FROM system_schema.columns"
                          f" WHERE keyspace_name = '{ks}'"
                          f" AND table_name = '{cf}'"
                          f" AND kind in ('static', 'regular')"
                          f" ALLOW FILTERING")
    column = random.choice(list(res)).column_name
    return column


def create_index(session, ks, cf, column) -> str:
    InfoEvent(message=f"Starting creating index: {ks}.{cf}({column})").publish()
    index_name = f"{cf}_{column}_nemesis".lower()
    session.execute(f'CREATE INDEX {index_name} ON {ks}.{cf}("{column}")')
    return index_name


def wait_for_index_to_be_built(node: BaseNode, ks, index_name, timeout=300) -> None:
    LOGGER.info('waiting for index %s to be built', index_name)
    start_time = time.time()
    while time.time() - start_time < timeout:
        result = node.run_nodetool(f"viewbuildstatus {ks}.{index_name}_index",
                                   ignore_status=True, verbose=False, publish_event=False)
        if f"{ks}.{index_name}_index has finished building" in result.stdout:
            InfoEvent(message=f"Index {ks}.{index_name} was built").publish()
            return
        time.sleep(30)
    raise TimeoutError(f"Timeout error while creating index {index_name}. "
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

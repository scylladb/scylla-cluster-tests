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
import datetime
import logging
from typing import NamedTuple
from uuid import UUID

LOGGER = logging.getLogger(__name__)


class NotFoundMatchedRowsInAuditLog(Exception):
    pass


class TableColumns(NamedTuple):
    date: datetime.datetime = None
    node: str = None
    event_time: UUID = None
    category: str = None
    consistency: str = None
    error: bool = None
    keyspace_name: str = None
    operation: str = None
    source: str = None
    table_name: str = None
    username: str = None


# pylint: disable=too-few-public-methods
class AuditSystemSchema:
    DEFAULT_FETCH_SIZE = 1000

    def __init__(self, tester):
        self.tester = tester

    # TODO: add retry and timeout
    def audit_log_filter(self, session, start_from_event_time: UUID, filter_columns: TableColumns, limit_rows: int = None):
        """
        Get rows from audit.audit_log and filter it by column values included in filter_columns dict
        :param tester: tester object
        :param session: session object to connect
        :param start_from_event_time: select all rows with `event_time` > start_from_event_time
        :param filter_columns: dictionary. Filter `audit.audit_log` data by values in this parameter.
               `audit.audit_log` columns:
               - date: datetime
               - node: IP, str
               - event_time: timeuuid
               - category: str
               - consistency: str
               - error: bool
               - keyspace_name: str
               - operation: str
               - source: str
               - table_name: str
               - username: str
               PRIMARY KEY ((date, node), event_time)
        :param limit_rows: if it needs to fetch all rows, remain this parameter = None
        :return:
        """
        where_list = []
        if start_from_event_time:
            where_list.append(f"event_time > {start_from_event_time}")

        filter_columns_asdict = filter_columns._asdict()
        for filter_column, filter_value in filter_columns_asdict.items():
            if filter_value:
                where_list.append(f"{filter_column} = '{filter_value}'")

        limit = f" limit {limit_rows}" if limit_rows else ""
        where = " where " + " and ".join(where_list) + f"{limit} ALLOW FILTERING" if where_list else ""
        LOGGER.debug("Filter data with: %s", where)

        if not (rows := self.tester.fetch_all_rows(session=session, default_fetch_size=self.DEFAULT_FETCH_SIZE,
                                                   statement=f"select * from audit.audit_log{where}",
                                                   return_row_as_dict=True)):
            raise NotFoundMatchedRowsInAuditLog(f"Not found rows matched for filter '{where}' in the audit.audit_log")

        LOGGER.debug("Found rows are matched to filter \"%s\": %s", where, len(rows))

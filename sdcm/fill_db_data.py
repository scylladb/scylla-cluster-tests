__author__ = 'Roy Dahan'

# !/usr/bin/env python

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
# Copyright (c) 2016 ScyllaDB

# pylint: disable=too-many-lines,eval-used, line-too-long
import contextlib
import logging
import random
import time
import re

from collections import OrderedDict
from uuid import UUID

from cassandra import InvalidRequest
from cassandra.util import sortedset, SortedSet  # pylint: disable=no-name-in-module
from cassandra import ConsistencyLevel
from cassandra.protocol import ProtocolException  # pylint: disable=no-name-in-module
from pkg_resources import parse_version

from sdcm.tester import ClusterTester
from sdcm.utils.decorators import retrying
from sdcm.utils.cdc.options import CDC_LOGTABLE_SUFFIX


LOGGER = logging.getLogger(__name__)


class FillDatabaseData(ClusterTester):
    """
    Fill scylla with many types of records, tables and data types (taken from dtest) originally by Andrei.

    """
    NON_FROZEN_SUPPORT_OS_MIN_VERSION = '4.1'  # open source version with non-frozen user_types support
    NON_FROZEN_SUPPORT_ENTERPRISE_MIN_VERSION = '2020'  # enterprise version with non-frozen user_types support
    NULL_VALUES_SUPPORT_OS_MIN_VERSION = "4.4.rc0"
    NULL_VALUES_SUPPORT_ENTERPRISE_MIN_VERSION = "2021.1.3"
    NEW_SORTING_ORDER_WITH_SECONDARY_INDEXES_OS_MIN_VERSION = "4.4.rc0"
    NEW_SORTING_ORDER_WITH_SECONDARY_INDEXES_ENTERPRISE_MIN_VERSION = "2021.2.dev"
    CDC_SUPPORT_MIN_VERSION = "4.3"
    CDC_SUPPORT_MIN_ENTERPRISE_VERSION = "2021.1.dev"

    base_ks = "keyspace_fill_db_data"
    # List of dictionaries for all items tables and their data
    all_verification_items = [
        {
            'name': "range_tombstones_test: Test deletion by 'composite prefix' (range tombstones)",
            'create_tables': ["""CREATE TABLE range_tombstones_test (
                            k int,
                            c1 int,
                            c2 int,
                            v1 int,
                            v2 int,
                            PRIMARY KEY (k, c1, c2)
                        )"""],
            'truncates': [],
            'inserts': ["INSERT INTO range_tombstones_test (k, c1, c2, v1, v2) VALUES (%d, %d, %d, %d, %d)" % (
                i, j, k, (i * 4) + (j * 2) + k, (i * 4) + (j * 2) + k) for i in range(0, 5) for j in range(0, 2)
                for k
                in range(0, 2)],
            'queries': ["SELECT v1, v2 FROM range_tombstones_test where k = %d" % 0,
                        "SELECT v1, v2 FROM range_tombstones_test where k = %d" % 1,
                        "SELECT v1, v2 FROM range_tombstones_test where k = %d" % 2,
                        "SELECT v1, v2 FROM range_tombstones_test where k = %d" % 3,
                        "SELECT v1, v2 FROM range_tombstones_test where k = %d" % 4,
                        "DELETE FROM range_tombstones_test WHERE k = %d AND c1 = 0" % 0,
                        "DELETE FROM range_tombstones_test WHERE k = %d AND c1 = 0" % 1,
                        "DELETE FROM range_tombstones_test WHERE k = %d AND c1 = 0" % 2,
                        "DELETE FROM range_tombstones_test WHERE k = %d AND c1 = 0" % 3,
                        "DELETE FROM range_tombstones_test WHERE k = %d AND c1 = 0" % 4,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 0,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 1,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 2,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 3,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 4,
                        "#REMOTER_SUDO nodetool flush",
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 0,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 1,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 2,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 3,
                        "SELECT v1, v2 FROM range_tombstones_test WHERE k = %d" % 4],
            'results': [[[x, x] for x in range(0 * 4, (0 + 1) * 4)],
                        [[x, x] for x in range(1 * 4, (1 + 1) * 4)],
                        [[x, x] for x in range(2 * 4, (2 + 1) * 4)],
                        [[x, x] for x in range(3 * 4, (3 + 1) * 4)],
                        [[x, x] for x in range(4 * 4, (4 + 1) * 4)],
                        [],
                        [],
                        [],
                        [],
                        [],
                        [[x, x] for x in range(0 * 4 + 2, (0 + 1) * 4)],
                        [[x, x] for x in range(1 * 4 + 2, (1 + 1) * 4)],
                        [[x, x] for x in range(2 * 4 + 2, (2 + 1) * 4)],
                        [[x, x] for x in range(3 * 4 + 2, (3 + 1) * 4)],
                        [[x, x] for x in range(4 * 4 + 2, (4 + 1) * 4)],
                        None,
                        [[x, x] for x in range(0 * 4 + 2, (0 + 1) * 4)],
                        [[x, x] for x in range(1 * 4 + 2, (1 + 1) * 4)],
                        [[x, x] for x in range(2 * 4 + 2, (2 + 1) * 4)],
                        [[x, x] for x in range(3 * 4 + 2, (3 + 1) * 4)],
                        [[x, x] for x in range(4 * 4 + 2, (4 + 1) * 4)]],
            'min_version': '',
            'max_version': '',
            'skip': ''},
        {
            'name': "range_tombstones_compaction_test: Test deletion by 'composite prefix' "
                    "(range tombstones) with compaction",
            'create_tables': ["""CREATE TABLE range_tombstones_compaction_test (
                            k int,
                            c1 int,
                            c2 int,
                            v1 text,
                            PRIMARY KEY (k, c1, c2)
                        )"""],
            'truncates': [],
            'inserts': ["INSERT INTO range_tombstones_compaction_test (k, c1, c2, v1) VALUES (0, %d, %d, '%s')" % (
                c1, c2, '%i%i' % (c1, c2)) for c1 in range(0, 4) for c2 in range(0, 2)],
            'queries': ["#REMOTER_SUDO nodetool flush",
                        "DELETE FROM range_tombstones_compaction_test WHERE k = 0 AND c1 = 1",
                        "#REMOTER_SUDO nodetool flush",
                        "#REMOTER_SUDO nodetool compact",
                        "SELECT v1 FROM range_tombstones_compaction_test WHERE k = 0"],
            'results': [None,
                        [],
                        None,
                        None,
                        [['%i%i' % (c1, c2)] for c1 in range(0, 4) for c2 in range(0, 2) if c1 != 1]],
            'min_version': '',
            'max_version': '',
            'skip': ''},

    ]

    @staticmethod
    def _get_table_name_from_query(query):
        regexp = re.compile(r"create table (?P<table_name>[a-z0-9_]+)\s?", re.MULTILINE | re.IGNORECASE)
        match = regexp.search(query)
        if match:
            return match.groupdict()["table_name"]
        return None

    @staticmethod
    def cql_create_simple_tables(session, rows):
        """ Create tables for truncate test """
        create_query = "CREATE TABLE IF NOT EXISTS truncate_table%d (my_id int PRIMARY KEY, col1 int, value int) " \
            "with cdc = {'enabled': true, 'ttl': 0}"
        for i in range(rows):
            session.execute(create_query % i)
            # Added sleep after each created table
            time.sleep(15)

    @staticmethod
    def cql_insert_data_to_simple_tables(session, rows):  # pylint: disable=invalid-name
        def insert_query():
            return f'INSERT INTO truncate_table{i} (my_id, col1, value) VALUES ( {k}, {k}, {k})'
        for i in range(rows):  # pylint: disable=unused-variable
            for k in range(100):  # pylint: disable=unused-variable
                session.execute(insert_query())

    @staticmethod
    def cql_truncate_simple_tables(session, rows):
        truncate_query = 'TRUNCATE TABLE truncate_table%d'
        for i in range(rows):
            session.execute(truncate_query % i)

    def fill_db_data_for_truncate_test(self, insert_rows):
        # Prepare connection and keyspace
        with self.db_cluster.cql_connection_patient(self.db_cluster.nodes[0]) as session:
            # override driver consistency level
            session.default_consistency_level = ConsistencyLevel.QUORUM

            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS truncate_ks
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} AND durable_writes = true;
                """)
            session.set_keyspace("truncate_ks")

            # Create all tables according the above list
            self.cql_create_simple_tables(session, rows=insert_rows)

            # Insert data to the tables
            self.cql_insert_data_to_simple_tables(session, rows=insert_rows)

    def _enable_cdc(self, item, create_table):
        cdc_properties = "cdc = {'enabled': true, 'preimage': true, 'postimage': true, 'ttl': 36000}"

        if item.get("no_cdc"):
            self.log.warning("Skip adding cdc enabling properties due %s", item['no_cdc'])
            return create_table

        if "CREATE TABLE" in create_table.upper() and "COUNTER" not in create_table.upper():
            create_table = create_table.replace(";", "")
            table_name = self._get_table_name_from_query(create_table)
            if " WITH " in create_table.upper():
                create_table = f"{create_table} AND {cdc_properties}"
            else:
                create_table = f"{create_table} WITH {cdc_properties}"
            # set cdc table name
            if not item.get("cdc_tables"):
                item["cdc_tables"] = {}
            item["cdc_tables"][f"{table_name}{CDC_LOGTABLE_SUFFIX}"] = None

        return create_table

    def cql_create_tables(self, session):
        truncates = []
        self.log.info('Start table creation')
        # Run through the list of items and create all tables
        for test_num, item in enumerate(self.all_verification_items):
            test_name = item.get('name', 'Test #' + str(test_num))
            # Check if current cluster version supports non-frozed UDT
            if 'skip_condition' in item and 'non_frozen_udt' in item['skip_condition'] \
                    and not eval(item['skip_condition']):
                item['skip'] = 'skip'
                self.all_verification_items[test_num]['skip'] = 'skip'
                self.log.debug("Version doesn't support the item, skip it: %s.", item['create_tables'])

            # TODO: fix following condition to make "skip_condition" really skip stuff
            # when it is True, not False as it is now.
            # As of now it behaves as "run_condition".
            if not item['skip'] and ('skip_condition' not in item or eval(str(item['skip_condition']))):
                # NOTE: skip condition may change during upgrade, nail down it
                # to be able to run proper queries on target scylla version
                self.all_verification_items[test_num]['skip_condition'] = True
                with self._execute_and_log(f'Created tables for test "{test_name}" in {{}} seconds'):
                    for create_table in item['create_tables']:
                        if self.version_cdc_support():
                            create_table = self._enable_cdc(item, create_table)
                        # wait a while before creating index, there is a delay of create table for
                        # waiting the schema agreement
                        if 'CREATE INDEX' in create_table.upper():
                            time.sleep(15)
                        self.log.debug("create table: %s", create_table)
                        session.execute(create_table)
                        # sleep for 15 seconds to wait creating cdc tables
                        self.db_cluster.wait_for_schema_agreement()
                        if 'CREATE TYPE' in create_table.upper():
                            time.sleep(15)
                    for truncate in item['truncates']:
                        truncates.append(truncate)
            else:
                self.all_verification_items[test_num]['skip_condition'] = False
        # Sleep a while after creating test tables to avoid schema disagreement.
        # Refs: https://github.com/scylladb/scylla/issues/5235
        time.sleep(30)
        for truncate in truncates:
            session.execute(truncate)

    @property
    def parsed_scylla_version(self):
        return parse_version(self.db_cluster.nodes[0].scylla_version)

    @property
    def is_enterprise(self) -> bool:
        return self.db_cluster.nodes[0].is_enterprise

    def version_null_values_support(self):
        if self.is_enterprise:
            version_with_support = self.NULL_VALUES_SUPPORT_ENTERPRISE_MIN_VERSION
        else:
            version_with_support = self.NULL_VALUES_SUPPORT_OS_MIN_VERSION
        return self.parsed_scylla_version >= parse_version(version_with_support)

    def version_new_sorting_order_with_secondary_indexes(self):
        if self.is_enterprise:
            version_with_support = self.NEW_SORTING_ORDER_WITH_SECONDARY_INDEXES_ENTERPRISE_MIN_VERSION
        else:
            version_with_support = self.NEW_SORTING_ORDER_WITH_SECONDARY_INDEXES_OS_MIN_VERSION
        return self.parsed_scylla_version >= parse_version(version_with_support)

    def version_non_frozen_udt_support(self):
        """
        Check if current version supports non-frozen user type
        Issue: https://github.com/scylladb/scylla/pull/4934
        """
        if self.is_enterprise:
            version_with_support = self.NON_FROZEN_SUPPORT_ENTERPRISE_MIN_VERSION
        else:
            version_with_support = self.NON_FROZEN_SUPPORT_OS_MIN_VERSION
        return self.parsed_scylla_version >= parse_version(version_with_support)

    def version_cdc_support(self):
        if self.is_enterprise:
            version_with_support = self.CDC_SUPPORT_MIN_ENTERPRISE_VERSION
        else:
            version_with_support = self.CDC_SUPPORT_MIN_VERSION
        return self.parsed_scylla_version >= parse_version(version_with_support)

    @retrying(n=3, sleep_time=20, allowed_exceptions=ProtocolException)
    def truncate_table(self, session, truncate):  # pylint: disable=no-self-use
        LOGGER.debug(truncate)
        session.execute(truncate)

    @contextlib.contextmanager
    def _execute_and_log(self, message):
        start_time = time.time()
        yield
        self.log.info(message.format(int(time.time() - start_time)))

    def truncate_tables(self, session):
        # Run through the list of items and create all tables
        self.log.info('Start table truncation')
        for test_num, item in enumerate(self.all_verification_items):
            test_name = item.get('name', 'Test #' + str(test_num))
            if not item['skip'] and ('skip_condition' not in item or eval(str(item['skip_condition']))):
                for truncate in item['truncates']:
                    with self._execute_and_log(f'Truncated table for test "{test_name}" in {{}} seconds'):
                        self.truncate_table(session, truncate)

    def cql_insert_data_to_tables(self, session, default_fetch_size):
        self.log.info('Start to populate data into tables')
        # pylint: disable=too-many-nested-blocks
        for test_num, item in enumerate(self.all_verification_items):
            test_name = item.get('name', 'Test #' + str(test_num))
            # TODO: fix following condition to make "skip_condition" really skip stuff
            # when it is True, not False as it is now.
            # As of now it behaves as "run_condition".
            if not item['skip'] and ('skip_condition' not in item or eval(str(item['skip_condition']))):
                if 'disable_paging' in item and item['disable_paging']:
                    session.default_fetch_size = 0
                else:
                    session.default_fetch_size = default_fetch_size
                for insert in item['inserts']:
                    with self._execute_and_log(f'Populated data for test "{test_name}" in {{}} seconds'):
                        try:
                            if insert.startswith("#REMOTER_SUDO"):
                                for node in self.db_cluster.nodes:
                                    node.remoter.sudo(insert.replace('#REMOTER_SUDO', ''))
                            else:
                                session.execute(insert)
                        except Exception as ex:
                            LOGGER.exception("failed to insert: %s", insert)
                            raise ex
                    # Add delay on client side for inserts of list to avoid list order issue
                    # Referencing https://github.com/scylladb/scylla-enterprise/issues/1177#issuecomment-568762357
                    if 'list<' in item['create_tables'][0]:
                        time.sleep(1)
                if item.get("cdc_tables"):
                    with self._execute_and_log(f'Read CDC logs for test "{test_name}" in {{}} seconds'):
                        for cdc_table in item["cdc_tables"]:
                            item["cdc_tables"][cdc_table] = self.get_cdc_log_rows(session, cdc_table)

    def _run_db_queries(self, item, session):
        for i in range(len(item['queries'])):
            try:
                if item['queries'][i].startswith("#SORTED"):
                    res = session.execute(item['queries'][i].replace('#SORTED', ''))
                    self.assertEqual(sorted([list(row) for row in res]), item['results'][i])
                elif item['queries'][i].startswith("#REMOTER_SUDO"):
                    for node in self.db_cluster.nodes:
                        node.remoter.sudo(item['queries'][i].replace('#REMOTER_SUDO', ''))
                elif item['queries'][i].startswith("#LENGTH"):
                    res = session.execute(item['queries'][i].replace('#LENGTH', ''))
                    self.assertEqual(len([list(row) for row in res]), item['results'][i])
                elif item['queries'][i].startswith("#STR"):
                    res = session.execute(item['queries'][i].replace('#STR', ''))
                    self.assertEqual(str([list(row) for row in res]), item['results'][i])
                else:
                    res = session.execute(item['queries'][i])
                    self.assertEqual([list(row) for row in res], item['results'][i])
            except Exception as ex:
                LOGGER.exception(item['queries'][i])
                raise ex

    @staticmethod
    def _run_invalid_queries(item, session):
        for i in range(len(item['invalid_queries'])):
            try:
                session.execute(item['invalid_queries'][i])
                # self.fail("query '%s' is not valid" % item['invalid_queries'][i])
                LOGGER.error("query '%s' is valid", item['invalid_queries'][i])
            except InvalidRequest as ex:
                LOGGER.debug("Found error '%s' as expected", ex)

    def _read_cdc_tables(self, item, session):
        for cdc_table in item["cdc_tables"]:
            actual_result = self.get_cdc_log_rows(session, cdc_table)
            # try..except mainly added to avoid test termination
            # because  Row(f=inf) in different select query is not equal
            try:
                assert all(row in actual_result for row in item["cdc_tables"][cdc_table]), \
                    f"cdc tables content are differes\n Initial:{item['cdc_tables'][cdc_table]}\n" \
                    f"New_result: {actual_result}"
            except AssertionError as err:
                LOGGER.error("content was differ %s", err)

    def run_db_queries(self, session, default_fetch_size):
        self.log.info('Start to running queries')
        # pylint: disable=too-many-branches,too-many-nested-blocks
        for test_num, item in enumerate(self.all_verification_items):
            test_name = item.get('name', 'Test #' + str(test_num))
            # Some queries contains statement of switch keyspace, reset keyspace at the beginning
            session.set_keyspace(self.base_ks)
            # TODO: fix following condition to make "skip_condition" really skip stuff
            # when it is True, not False as it is now.
            # As of now it behaves as "run_condition".
            if not item['skip'] and ('skip_condition' not in item or eval(str(item['skip_condition']))):
                if 'disable_paging' in item and item['disable_paging']:
                    session.default_fetch_size = 0
                else:
                    session.default_fetch_size = default_fetch_size
                with self._execute_and_log(f'Ran queries for test "{test_name}" in {{}} seconds'):
                    self._run_db_queries(item, session)

                if 'invalid_queries' in item:
                    with self._execute_and_log(f'Ran invalid queries for test "{test_name}" in {{}} seconds'):
                        self._run_invalid_queries(item, session)

                if item.get("cdc_tables"):
                    with self._execute_and_log(f'Read CDC tables for test "{test_name}" in {{}} seconds'):
                        self._read_cdc_tables(item, session)
                    # udpate cdc log tables after queries,
                    # which could change base table content
                    with self._execute_and_log(f'Update CDC tables for test "{test_name}" in {{}} seconds'):
                        for cdc_table in item["cdc_tables"]:
                            item["cdc_tables"][cdc_table] = self.get_cdc_log_rows(session, cdc_table)
                            LOGGER.debug(item["cdc_tables"][cdc_table])

    def get_cdc_log_rows(self, session, cdc_log_table):
        return list(session.execute(f"select * from {self.base_ks}.{cdc_log_table}"))

    def fill_db_data(self):
        """
        Run a set of different cql queries against various types/tables before
        and after upgrade of every node to check the consistency of data
        """
        node = self.db_cluster.nodes[0]

        with self.db_cluster.cql_connection_patient(node) as session:
            # pylint: disable=no-member
            # override driver consistency level
            session.default_consistency_level = ConsistencyLevel.QUORUM
            # clean original test data by truncate
            try:
                session.set_keyspace(self.base_ks)
                self.truncate_tables(session)
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.debug("Found error in truncate tables: '%s'", ex)

            # Insert data to the tables according to the "inserts" and flush to disk in several cases (nodetool flush)
            self.cql_insert_data_to_tables(session, session.default_fetch_size)

    def prepare_keyspaces_and_tables(self):
        """
        Prepare keyspaces and tables
        """
        # Prepare connection and keyspace
        node = self.db_cluster.nodes[0]

        with self.db_cluster.cql_connection_patient(node) as session:
            # override driver consistency level
            session.default_consistency_level = ConsistencyLevel.QUORUM

            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {self.base_ks}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '3'}} AND durable_writes = true;
                """)
            session.set_keyspace(self.base_ks)

            # Create all tables according the above list
            self.cql_create_tables(session)

    def verify_db_data(self):
        # Prepare connection
        node = self.db_cluster.nodes[0]
        with self.db_cluster.cql_connection_patient(node, keyspace=self.base_ks) as session:
            # override driver consistency level
            session.default_consistency_level = ConsistencyLevel.QUORUM
            self.run_db_queries(session, session.default_fetch_size)

    def paged_query(self, keyspace='keyspace_complex'):
        # Prepare connection
        def create_table():
            session.execute('CREATE TABLE IF NOT EXISTS paged_query_test (k int PRIMARY KEY, v1 int, v2 int)')

        def fill_table():
            for i in range(1000):
                random_int = random.randint(1, 1000000)
                session.execute(f'INSERT INTO paged_query_test (k, v1, v2) VALUES ({i}, {random_int}, {random_int+i})')
        node = self.db_cluster.nodes[-1]
        with self.db_cluster.cql_connection_patient(node, keyspace=keyspace) as session:
            create_table()
            self.db_cluster.wait_for_schema_agreement()  # WORKAROUND TO SCHEMA PROPAGATION TAKING TOO LONG
            fill_table()
            statement = f'select * from {keyspace}.paged_query_test;'
            self.log.info('running now session.execute')
            full_query_res = self.rows_to_list(session.execute(statement))
            if not full_query_res:
                assert f'Query "{statement}" returned no entries'
            self.log.info('running now fetch_all_rows')
            full_res = self.rows_to_list(
                self.fetch_all_rows(session=session, default_fetch_size=100, statement=statement))
            if not full_res:
                assert f'Paged query "{statement}" returned no value'
            self.log.info('will now compare results from session.execute and fetch_all_rows')
            self.assertEqual(sorted(full_query_res), sorted(full_res), "Results should be identical")

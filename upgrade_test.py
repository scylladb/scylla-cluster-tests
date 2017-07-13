#!/usr/bin/env python

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


from avocado import main
import random
from uuid import UUID

from sdcm.tester import ClusterTester

from sdcm.nemesis import UpgradeNemesis, UpgradeNemesisOneNode
from sdcm.nemesis import RollbackNemesis


class UpgradeTest(ClusterTester):
    """
    Test a Scylla cluster upgrade.

    :avocado: enable
    """

    default_params = {'timeout': 650000}

    def test_upgrade_cql_queries(self):
        """
        Run a set of different cql queries against various types/tables before
        and after upgrade of every node to check the consistency of data
        """
        all_verification_items = [
            # order_by_with_in_test: Check that order-by works with IN
            {
                'create_tables': ["""
              CREATE TABLE order_by_with_in_test(
                  my_id varchar,
                  col1 int,
                  value varchar,
                  PRIMARY KEY (my_id, col1)
              )"""],
                'truncates': ['TRUNCATE order_by_with_in_test'],
                'inserts': ["INSERT INTO order_by_with_in_test(my_id, col1, value) VALUES ( 'key1', 1, 'a')",
                            "INSERT INTO order_by_with_in_test(my_id, col1, value) VALUES ( 'key2', 3, 'c')",
                            "INSERT INTO order_by_with_in_test(my_id, col1, value) VALUES ( 'key3', 2, 'b')",
                            "INSERT INTO order_by_with_in_test(my_id, col1, value) VALUES ( 'key4', 4, 'd')"],
                'queries': [
                    "SELECT col1 FROM order_by_with_in_test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1",
                    "SELECT col1, my_id FROM order_by_with_in_test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1",
                    "SELECT my_id, col1 FROM order_by_with_in_test WHERE my_id in('key1', 'key2', 'key3') ORDER BY col1"],
                'results': [[[1], [2], [3]],
                            [[1, 'key1'], [2, 'key3'], [3, 'key2']],
                            [['key1', 1], ['key3', 2], ['key2', 3]]],
                'min_version': '2.0',
                'max_version': '',
                'skip': '',
                'disable_paging': True},
            # static_cf_test: Test static CF syntax
            {
                'create_tables': ["""CREATE TABLE static_cf_test (
                userid uuid PRIMARY KEY,
                firstname text,
                lastname text,
                age int
            );"""],
                'truncates': ['TRUNCATE static_cf_test'],
                'inserts': [
                    "INSERT INTO static_cf_test (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "UPDATE static_cf_test SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479"],
                'queries': [
                    "SELECT firstname, lastname FROM static_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM static_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM static_cf_test"],
                'results': [[['Frodo', 'Baggins']],
                            [[UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']],
                            [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
                             [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']]],
                'min_version': '1.0',
                'max_version': '',
                'skip': ''},
            # static_cf_test_batch: Test static CF syntax with batch
            {
                'create_tables': ["""CREATE TABLE static_cf_test_batch (
            userid uuid PRIMARY KEY,
            firstname text,
            lastname text,
            age int
        );"""],
                'truncates': ['TRUNCATE static_cf_test_batch'],
                'inserts': [
                    "INSERT INTO static_cf_test_batch (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "UPDATE static_cf_test_batch SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                    """BEGIN BATCH
                        INSERT INTO static_cf_test_batch (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                        UPDATE static_cf_test_batch SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                        DELETE firstname, lastname FROM static_cf_test_batch WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                        DELETE firstname, lastname FROM static_cf_test_batch WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                       APPLY BATCH"""
                ],
                'queries': [
                    "SELECT * FROM static_cf_test_batch"],
                'results': [[[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None],
                             [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None]]],
                'min_version': '1.0',
                'max_version': '',
                'skip': ''},
            # noncomposite_static_cf_test: Test non-composite static CF syntax
            {
                'create_tables': ["""CREATE TABLE noncomposite_static_cf_test (
                userid uuid PRIMARY KEY,
                firstname ascii,
                lastname ascii,
                age int
            ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE noncomposite_static_cf_test'],
                'inserts': [
                    "INSERT INTO noncomposite_static_cf_test (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "UPDATE noncomposite_static_cf_test SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479"],
                'queries': [
                    "SELECT firstname, lastname FROM noncomposite_static_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM noncomposite_static_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM noncomposite_static_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                    "SELECT * FROM noncomposite_static_cf_test"],
                'results': [[['Frodo', 'Baggins']],
                            [[UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']],
                            [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee']],
                            [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 33, 'Samwise', 'Gamgee'],
                             [UUID('550e8400-e29b-41d4-a716-446655440000'), 32, 'Frodo', 'Baggins']]],
                'min_version': '1.0',
                'max_version': '',
                'skip': ''},
            # noncomposite_static_cf_test_batch: Test non-composite static CF syntax with batch
            {
                'create_tables': ["""CREATE TABLE noncomposite_static_cf_test_batch (
                userid uuid PRIMARY KEY,
                firstname ascii,
                lastname ascii,
                age int
            ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE noncomposite_static_cf_test_batch'],
                'inserts': [
                    "INSERT INTO noncomposite_static_cf_test_batch (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "UPDATE noncomposite_static_cf_test_batch SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                    """BEGIN BATCH
                        INSERT INTO noncomposite_static_cf_test_batch (userid, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 36)
                        UPDATE noncomposite_static_cf_test_batch SET age = 37 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                        DELETE firstname, lastname FROM noncomposite_static_cf_test_batch WHERE userid = 550e8400-e29b-41d4-a716-446655440000
                        DELETE firstname, lastname FROM noncomposite_static_cf_test_batch WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479
                        APPLY BATCH"""],
                'queries': [
                    "SELECT * FROM noncomposite_static_cf_test_batch"],
                'results': [[[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 37, None, None],
                             [UUID('550e8400-e29b-41d4-a716-446655440000'), 36, None, None]]],
                'min_version': '1.0',
                'max_version': '',
                'skip': ''},
            # dynamic_cf_test: Test non-composite dynamic CF syntax
            {
                'create_tables': ["""CREATE TABLE dynamic_cf_test (
                userid uuid,
                url text,
                time bigint,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE dynamic_cf_test'],
                'inserts': [
                    "INSERT INTO dynamic_cf_test (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo.bar', 42)",
                    "INSERT INTO dynamic_cf_test (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://foo-2.bar', 24)",
                    "INSERT INTO dynamic_cf_test (userid, url, time) VALUES (550e8400-e29b-41d4-a716-446655440000, 'http://bar.bar', 128)",
                    "UPDATE dynamic_cf_test SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 and url = 'http://bar.foo'",
                    "UPDATE dynamic_cf_test SET time = 12 WHERE userid IN (f47ac10b-58cc-4372-a567-0e02b2c3d479, 550e8400-e29b-41d4-a716-446655440000) and url = 'http://foo-3'"],
                'queries': [
                    "SELECT url, time FROM dynamic_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM dynamic_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                    "SELECT time FROM dynamic_cf_test"],
                'results': [
                    [['http://bar.bar', 128], ['http://foo-2.bar', 24], ['http://foo-3', 12], ['http://foo.bar', 42]],
                    [[UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://bar.foo', 24],
                     [UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'), 'http://foo-3', 12]],
                    [[24], [12], [128], [24], [12], [42]]],
                'invalid_queries': [
                    # Error from server: code=2200 [Invalid query] message="Missing PRIMARY KEY part url"
                    "INSERT INTO dynamic_cf_test (userid, url, time) VALUES (810e8500-e29b-41d4-a716-446655440000, '', 42)"],
                'min_version': '1.0',
                'max_version': '',
                'skip': ''},
            # dense_cf_test: Test composite 'dense' CF syntax
            {
                'create_tables': ["""CREATE TABLE dense_cf_test (
                      userid uuid,
                      ip text,
                      port int,
                      time bigint,
                      PRIMARY KEY (userid, ip, port)
                        ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE dense_cf_test'],
                'inserts': [
                    "INSERT INTO dense_cf_test (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.1', 80, 42)",
                    "INSERT INTO dense_cf_test (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 80, 24)",
                    "INSERT INTO dense_cf_test (userid, ip, port, time) VALUES (550e8400-e29b-41d4-a716-446655440000, '192.168.0.2', 90, 42)",
                    "UPDATE dense_cf_test SET time = 24 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.2' AND port = 80",
                    "INSERT INTO dense_cf_test (userid, ip, time) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, '192.168.0.3', 42)",
                    "UPDATE dense_cf_test SET time = 42 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'"],
                'queries': [
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip >= '192.168.0.2'",
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip = '192.168.0.2'",
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 and ip > '192.168.0.2'",
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'",
                    "SELECT ip, port, time FROM dense_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.4'",
                    "DELETE time FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND ip = '192.168.0.2' AND port = 80",
                    "SELECT * FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "DELETE FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "SELECT * FROM dense_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000",
                    "DELETE FROM dense_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'",
                    "SELECT * FROM dense_cf_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND ip = '192.168.0.3'"
                ],
                'results': [[['192.168.0.1', 80, 42], ['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]],
                            [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]],
                            [['192.168.0.2', 80, 24], ['192.168.0.2', 90, 42]],
                            [],
                            [['192.168.0.3', None, 42]],
                            [['192.168.0.4', None, 42]],
                            [],
                            [[UUID('550e8400-e29b-41d4-a716-446655440000'), '192.168.0.1', 80, 42],
                             [UUID('550e8400-e29b-41d4-a716-446655440000'), '192.168.0.2', 90, 42]],
                            [],
                            [],
                            [],
                            [],
                            ],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # sparse_cf_test: Test composite 'sparse' CF syntax
            {
                'create_tables': ["""CREATE TABLE sparse_cf_test (
                userid uuid,
                posted_month int,
                posted_day int,
                body ascii,
                posted_by ascii,
                PRIMARY KEY (userid, posted_month, posted_day)
            );"""],
                'truncates': ['TRUNCATE sparse_cf_test'],
                'inserts': [
                    "INSERT INTO sparse_cf_test (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 12, 'Something else', 'Frodo Baggins')",
                    "INSERT INTO sparse_cf_test (userid, posted_month, posted_day, body, posted_by) VALUES (550e8400-e29b-41d4-a716-446655440000, 1, 24, 'Something something', 'Frodo Baggins')",
                    "UPDATE sparse_cf_test SET body = 'Yo Froddo', posted_by = 'Samwise Gamgee' WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND posted_month = 1 AND posted_day = 3",
                    "UPDATE sparse_cf_test SET body = 'Yet one more message' WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 and posted_day = 30"],
                'queries': [
                    "SELECT body, posted_by FROM sparse_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day = 24",
                    "SELECT posted_day, body, posted_by FROM sparse_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1 AND posted_day > 12",
                    "SELECT posted_day, body, posted_by FROM sparse_cf_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND posted_month = 1"],
                'results': [
                    [['Something something', 'Frodo Baggins']],
                    [[24, 'Something something', 'Frodo Baggins'],
                     [30, 'Yet one more message', None]],
                    [[12, 'Something else', 'Frodo Baggins'],
                     [24, 'Something something', 'Frodo Baggins'],
                     [30, 'Yet one more message', None]]
                ],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # simple_tuple_query_test: CASSANDRA-8613
            {
                'create_tables': [
                    "create table simple_tuple_query_test (a int, b int, c int, d int , e int, PRIMARY KEY (a, b, c, d, e))"],
                'truncates': ['TRUNCATE simple_tuple_query_test'],
                'inserts': [
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 2, 0, 0, 0);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 1, 0, 0, 0);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 0, 0, 0, 0);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 0, 2, 2, 2);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 0, 3, 3, 3);",
                    "INSERT INTO simple_tuple_query_test (a, b, c, d, e) VALUES (0, 0, 1, 1, 1);"
                ],
                'queries': [
                    "SELECT * FROM simple_tuple_query_test WHERE b=0 AND (c, d, e) > (1, 1, 1) ALLOW FILTERING;"],
                'results': [
                    [[0, 0, 2, 2, 2], [0, 0, 3, 3, 3]]
                ],
                'min_version': '',
                'max_version': '',
                'skip': 'Clustering columns may not be skipped in multi-column relations. They should appear in the PRIMARY KEY order. Got (c, d, e) > (1, 1, 1)'},
            # counters_test: Validate counter support
            {
                'create_tables': [
                    """CREATE TABLE counters_test (
                userid int,
                url text,
                total counter,
                PRIMARY KEY (userid, url)
            ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE counters_test'],
                'inserts': [
                    "UPDATE counters_test SET total = 0 WHERE userid = 1 AND url = 'http://foo.com'",
                    "UPDATE counters_test SET total = total + 1 WHERE userid = 1 AND url = 'http://foo.com'",
                ],
                'queries': [
                    "SELECT total FROM counters_test WHERE userid = 1 AND url = 'http://foo.com'",
                    "UPDATE counters_test SET total = total - 4 WHERE userid = 1 AND url = 'http://foo.com'",
                    "SELECT total FROM counters_test WHERE userid = 1 AND url = 'http://foo.com'",
                    "UPDATE counters_test SET total = total+1 WHERE userid = 1 AND url = 'http://foo.com'",
                    "SELECT total FROM counters_test WHERE userid = 1 AND url = 'http://foo.com'",
                    "UPDATE counters_test SET total = total -2 WHERE userid = 1 AND url = 'http://foo.com'",
                    "SELECT total FROM counters_test WHERE userid = 1 AND url = 'http://foo.com'"],
                'results': [
                    [[1]],
                    None,
                    [[-3]],
                    None,
                    [[-2]],
                    None,
                    [[-4]]
                ],
                'min_version': '1.7',
                'max_version': '',
                'skip': 'Counter support is not enabled'},
            # indexed_with_eq_test: Check that you can query for an indexed column even with a key EQ clause
            {
                'create_tables': ["""
                CREATE TABLE indexed_with_eq_test (
                  userid uuid PRIMARY KEY,
                  firstname text,
                  lastname text,
                  age int);"""],
                'truncates': ['CREATE INDEX byAge ON indexed_with_eq_test(age);'],
                'inserts': [
                    "INSERT INTO indexed_with_eq_test (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "UPDATE indexed_with_eq_test SET firstname = 'Samwise', lastname = 'Gamgee', age = 33 WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479",
                ],
                'queries': [
                    "SELECT firstname FROM indexed_with_eq_test WHERE userid = 550e8400-e29b-41d4-a716-446655440000 AND age = 33",
                    "SELECT firstname FROM indexed_with_eq_test WHERE userid = f47ac10b-58cc-4372-a567-0e02b2c3d479 AND age = 33"],
                'results': [
                    [],
                    [['Samwise']]
                ],
                'min_version': '2.0',
                'max_version': '',
                'skip': 'Index support is not enabled'},
            # select_key_in_test: Query for KEY IN (...)
            {
                'create_tables': ["""CREATE TABLE select_key_in_test (
                  userid uuid PRIMARY KEY,
                  firstname text,
                  lastname text,
                  age int);"""],
                'truncates': ['TRUNCATE select_key_in_test'],
                'inserts': [
                    "INSERT INTO select_key_in_test (userid, firstname, lastname, age) VALUES (550e8400-e29b-41d4-a716-446655440000, 'Frodo', 'Baggins', 32)",
                    "INSERT INTO select_key_in_test (userid, firstname, lastname, age) VALUES (f47ac10b-58cc-4372-a567-0e02b2c3d479, 'Samwise', 'Gamgee', 33)",
                ],
                'queries': [
                    "SELECT firstname, lastname FROM select_key_in_test WHERE userid IN (550e8400-e29b-41d4-a716-446655440000, f47ac10b-58cc-4372-a567-0e02b2c3d479)"],
                'results': [
                    [['Frodo', 'Baggins'], ['Samwise', 'Gamgee']]
                ],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # exclusive_slice_test: Test SELECT respects inclusive and exclusive bounds
            {
                'create_tables': ["""CREATE TABLE exclusive_slice_test (
                  k int,
                  c int,
                  v int,
                  PRIMARY KEY (k, c)
                    ) WITH COMPACT STORAGE;"""],
                'truncates': ['TRUNCATE exclusive_slice_test'],
                'inserts': [
                    """BEGIN BATCH
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 0, 0)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 1, 1)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 2, 2)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 3, 3)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 4, 4)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 5, 5)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 6, 6)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 7, 7)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 8, 8)
                        INSERT INTO exclusive_slice_test (k, c, v) VALUES (0, 9, 9)
                       APPLY BATCH"""
                ],
                'queries': [
                    "SELECT v FROM exclusive_slice_test WHERE k = 0",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c >= 2 AND c <= 6",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c > 2 AND c <= 6",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c >= 2 AND c < 6",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c > 2 AND c < 6",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c > 2 AND c <= 6 LIMIT 2",
                    "SELECT v FROM exclusive_slice_test WHERE k = 0 AND c >= 2 AND c < 6 ORDER BY c DESC LIMIT 2"
                ],
                'results': [
                    [[x] for x in range(10)],
                    [[x] for x in range(2, 7)],
                    [[x] for x in range(3, 7)],
                    [[x] for x in range(2, 6)],
                    [[x] for x in range(3, 6)],
                    [[3], [4]],
                    [[5], [4]]
                ],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # in_clause_wide_rows_test: Check IN support for 'wide rows' in SELECT statement
            {
                'create_tables': ["""CREATE TABLE in_clause_wide_rows_test1 (
                                        k int,
                                        c int,
                                        v int,
                                        PRIMARY KEY (k, c)) WITH COMPACT STORAGE;""",
                                  """CREATE TABLE in_clause_wide_rows_test2 (
                                        k int,
                                        c1 int,
                                        c2 int,
                                        v int,
                                        PRIMARY KEY (k, c1, c2)) WITH COMPACT STORAGE;
                                          """
                                  ],
                'truncates': ['TRUNCATE in_clause_wide_rows_test1', 'TRUNCATE in_clause_wide_rows_test2'],
                'inserts': [
                    """BEGIN BATCH
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 0, 0)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 1, 1)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 2, 2)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 3, 3)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 4, 4)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 5, 5)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 6, 6)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 7, 7)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 8, 8)
                        INSERT INTO in_clause_wide_rows_test1 (k, c, v) VALUES (0, 9, 9)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 0, 0)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 1, 1)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 2, 2)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 3, 3)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 4, 4)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 5, 5)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 6, 6)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 7, 7)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 8, 8)
                        INSERT INTO in_clause_wide_rows_test2 (k, c1, c2, v) VALUES (0, 0, 9, 9)
                    APPLY BATCH"""
                ],
                'queries': [
                    "SELECT v FROM in_clause_wide_rows_test1 WHERE k = 0 AND c IN (5, 2, 8)",
                    "SELECT v FROM in_clause_wide_rows_test2 WHERE k = 0 AND c1 IN (5, 2, 8) AND c2 = 3"
                    ""
                ],
                'results': [
                    [[2], [5], [8]],
                    []],
                'min_version': '',
                'max_version': '',
                'skip': 'Clustering column "c2" cannot be restricted by an IN relatio'},
            # order_by_test: Check ORDER BY support in SELECT statement
            {
                'create_tables': ["""CREATE TABLE order_by_test1 (
                k int,
                c int,
                v int,
                PRIMARY KEY (k, c)
            ) WITH COMPACT STORAGE;""",
                                  """CREATE TABLE order_by_test2 (
                k int,
                c1 int,
                c2 int,
                v int,
                PRIMARY KEY (k, c1, c2)
            );"""
                                  ],
                'truncates': ['TRUNCATE order_by_test1', 'TRUNCATE order_by_test2'],
                'inserts': [
                    """BEGIN BATCH
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 0, 0)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 1, 1)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 2, 2)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 3, 3)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 4, 4)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 5, 5)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 6, 6)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 7, 7)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 8, 8)
                        INSERT INTO order_by_test1 (k, c, v) VALUES (0, 9, 9)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 0, 0, 0)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 0, 1, 1)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 1, 0, 2)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 1, 1, 3)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 2, 0, 4)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 2, 1, 5)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 3, 0, 6)
                        INSERT INTO order_by_test2 (k, c1, c2, v) VALUES (0, 3, 1, 7)
                    APPLY BATCH"""
                ],
                'queries': [
                    "SELECT v FROM order_by_test1 WHERE k = 0 ORDER BY c DESC",
                    "SELECT v FROM order_by_test2 WHERE k = 0 ORDER BY c1 DESC",
                    "SELECT v FROM order_by_test2 WHERE k = 0 ORDER BY c1"
                ],
                'results': [
                    [[x] for x in reversed(range(10))],
                    [[x] for x in reversed(range(8))],
                    [[x] for x in range(8)]],
                'invalid_queries': [
                    "SELECT v FROM order_by_test2 WHERE k = 0 ORDER BY c DESC",
                    "SELECT v FROM order_by_test2 WHERE k = 0 ORDER BY c2 DESC",
                    "SELECT v FROM order_by_test2 WHERE k = 0 ORDER BY k DESC"],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # more_order_by_test: More ORDER BY checks CASSANDRA-4160
            {
                'create_tables': ["""CREATE COLUMNFAMILY more_order_by_test1 (
                row text,
                number int,
                string text,
                PRIMARY KEY (row, number)
            ) WITH COMPACT STORAGE""",
                                  """CREATE COLUMNFAMILY more_order_by_test2 (
                row text,
                number int,
                number2 int,
                string text,
                PRIMARY KEY (row, number, number2)
            ) WITH COMPACT STORAGE"""],
                'truncates': ['TRUNCATE more_order_by_test1'],
                'inserts': [
                    "INSERT INTO more_order_by_test1 (row, number, string) VALUES ('row', 1, 'one');",
                    "INSERT INTO more_order_by_test1 (row, number, string) VALUES ('row', 2, 'two');",
                    "INSERT INTO more_order_by_test1 (row, number, string) VALUES ('row', 3, 'three');",
                    "INSERT INTO more_order_by_test1 (row, number, string) VALUES ('row', 4, 'four');",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 1, 0, 'a')",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 2, 0, 'a')",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 2, 1, 'a')",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 3, 0, 'a')",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 3, 1, 'a')",
                    "INSERT INTO more_order_by_test2 (row, number, number2, string) VALUES ('a', 4, 0, 'a')",

                ],
                'queries': [
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number < 3 ORDER BY number ASC",
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number >= 3 ORDER BY number ASC",
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number < 3 ORDER BY number DESC",
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number >= 3 ORDER BY number DESC",
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number > 3 ORDER BY number DESC",
                    "SELECT number FROM more_order_by_test1 WHERE row='row' AND number <= 3 ORDER BY number DESC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number < 3 ORDER BY number ASC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number >= 3 ORDER BY number ASC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number < 3 ORDER BY number DESC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number >= 3 ORDER BY number DESC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number > 3 ORDER BY number DESC",
                    "SELECT number, number2 FROM more_order_by_test2 WHERE row='a' AND number <= 3 ORDER BY number DESC"
                ],
                'results': [
                    [[1], [2]],
                    [[3], [4]],
                    [[2], [1]],
                    [[4], [3]],
                    [[4]],
                    [[3], [2], [1]],
                    [[1, 0], [2, 0], [2, 1]],
                    [[3, 0], [3, 1], [4, 0]],
                    [[2, 1], [2, 0], [1, 0]],
                    [[4, 0], [3, 1], [3, 0]],
                    [[4, 0]],
                    [[3, 1], [3, 0], [2, 1], [2, 0], [1, 0]]],
                'min_version': '',
                'max_version': '',
                'skip': ''},
            # order_by_validation_test:  Check we don't allow order by on row key CASSANDRA-4246
            {
                'create_tables': ["""CREATE TABLE order_by_validation_test (
                k1 int,
                k2 int,
                v int,
                PRIMARY KEY (k1, k2)
            )"""],
                'truncates': ['TRUNCATE order_by_validation_test'],
                'inserts': [
                    "INSERT INTO order_by_validation_test (k1, k2, v) VALUES (0, 0, 0)",
                    "INSERT INTO order_by_validation_test (k1, k2, v) VALUES (1, 1, 1)",
                    "INSERT INTO order_by_validation_test (k1, k2, v) VALUES (2, 2, 2)"
                ],
                'queries': [],
                'results': [],
                'invalid_queries': [
                    "SELECT * FROM order_by_validation_test ORDER BY k2"],
                'min_version': '',
                'max_version': '',
                'skip': ''},

        ]

        node = self.db_cluster.nodes[0]
        session = self.cql_connection_patient(node)
        default_fetch_size = session.default_fetch_size
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS keyspace1
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true;
        """)
        session.execute("USE keyspace1;")
        for a in all_verification_items:
            if not a['skip']:
                for create_table in a['create_tables']:
                    session.execute(create_table)
                for truncate in a['truncates']:
                    session.execute(truncate)

        for a in all_verification_items:
            if not a['skip']:
                if 'disable_paging' in a and a['disable_paging']:
                    session.default_fetch_size = 0
                else:
                    session.default_fetch_size = default_fetch_size
                for insert in a['inserts']:
                    session.execute(insert)
                for i in range(len(a['queries'])):
                    res = session.execute(a['queries'][i])
                    self.assertEqual([list(row) for row in res], a['results'][i])
                if 'invalid_queries' in a:
                    for i in range(len(a['invalid_queries'])):
                        try:
                            session.execute(a['invalid_queries'][i])
                        except Exception as e:
                            print e

        duration = 10 * len(self.db_cluster.nodes)

        stress_cmd = self._cs_add_node_flag(self.params.get('stress_cmd'))
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd,
                                              duration=duration)

        l = len(self.db_cluster.nodes)
        # prepare an array containing the indexes
        indexes = [x for x in range(l)]
        # shuffle it so we will upgrade the nodes in a
        # random order
        random.shuffle(indexes)

        # upgrade all the nodes in random order
        for i in indexes:
            self.db_cluster.node_to_upgrade = self.db_cluster.nodes[i]
            self.db_cluster.add_nemesis(nemesis=UpgradeNemesisOneNode,
                                        loaders=self.loaders,
                                        monitoring_set=self.monitors)
            self.db_cluster.start_nemesis(interval=10)
            # 10 minutes to upgrade 1 node
            self.db_cluster.stop_nemesis(timeout=10 * 60)
            session = self.cql_connection_patient(self.db_cluster.node_to_upgrade)
            session.execute("USE keyspace1;")

            for a in all_verification_items:
                if not a['skip']:
                    if 'disable_paging' in a and a['disable_paging']:
                        session.default_fetch_size = 0
                    else:
                        session.default_fetch_size = default_fetch_size
                    for insert in a['inserts']:
                        session.execute(insert)
                    for i in range(len(a['queries'])):
                        res = session.execute(a['queries'][i])
                        self.assertEqual([list(row) for row in res], a['results'][i])
                    if 'invalid_queries' in a:
                        for i in range(len(a['invalid_queries'])):
                            try:
                                session.execute(a['invalid_queries'][i])
                            except Exception as e:
                                print e

        self.verify_stress_thread(stress_queue)

        def test_20_minutes(self):
            """
            Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
            If upgrade_node_packages defined we specify duration 10 * len(nodes) minutes.
            """
            self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                        loaders=self.loaders,
                                        monitoring_set=self.monitors)
            self.db_cluster.start_nemesis(interval=10)
            duration = 20
            if self.params.get('upgrade_node_packages'):
                duration = 30 * len(self.db_cluster.nodes)
            self.run_stress(stress_cmd=self.params.get('stress_cmd'), duration=duration)

        def test_20_minutes_rollback(self):
            """
            Run cassandra-stress on a cluster for 20 minutes, together with node upgrades.
            """
            self.db_cluster.add_nemesis(nemesis=UpgradeNemesis,
                                        loaders=self.loaders,
                                        monitoring_set=self.monitors)
            self.db_cluster.start_nemesis(interval=10)
            self.db_cluster.stop_nemesis(timeout=None)

            self.db_cluster.clean_nemesis()

            self.db_cluster.add_nemesis(nemesis=RollbackNemesis,
                                        loaders=self.loaders,
                                        monitoring_set=self.monitors)
            self.db_cluster.start_nemesis(interval=10)
            self.run_stress(stress_cmd=self.params.get('stress_cmd'),
                            duration=self.params.get('cassandra_stress_duration', 20))


if __name__ == '__main__':
    main()

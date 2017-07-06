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
import time
from uuid import UUID, uuid4

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
                'skip': ''}
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

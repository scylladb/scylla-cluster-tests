from __future__ import absolute_import
import unittest

from sdcm.utils.common import get_non_system_ks_cf_list


class DummyNode():  # pylint: disable=too-few-public-methods
    def run_cqlsh(self, cmd, timeout, verbose, target_db_node, split, connect_timeout):  # pylint: disable=too-many-arguments,unused-argument,no-self-use
        result = None
        if 'system_schema.views' in cmd:
            result = ['Disabled Query paging.', '',
                      'keyspace_name | view_name',
                      '---------------+---------------------',
                      'mview | users_by_first_name',
                      'mview |  users_by_last_name',
                      '', '(2 rows)']
        elif 'system_schema.columns' in cmd:
            result = ['Disabled Query paging.', '',
                      'keyspace_name      | table_name                      | type',
                      '--------------------+---------------------------------+-------------------------',
                      'system_auth |                    role_members |                    text',
                      'system_auth |                    role_members |                    text',
                      'system_auth |                           roles |                 boolean',
                      'system_auth |                           roles |                 boolean',
                      'system_auth |                           roles |               set<text>',
                      'system_auth |                           roles |                    text',
                      'system_auth |                           roles |                    text',
                      'system_schema |                      aggregates |                    text',
                      'system_schema |                      aggregates |      frozen<list<text>>',
                      'system_schema |                      aggregates |                    text',
                      'system_schema |                      aggregates |                    text',
                      'system_schema |                      aggregates |                    text',
                      'system_schema |                      aggregates |                    text',
                      'system_schema |                      aggregates |                    text',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                        counter1 |                    text',
                      'keyspace1 |                        counter1 |                    blob',
                      'keyspace1 |                        counter1 |                 counter',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    text',
                      'keyspace1 |                       standard1 |                    blob',
                      'keyspace1 |                       standard1 |                    blob',
                      'mview |                           users |                    text',
                      'mview |                           users |                    text',
                      'mview |                           users |                timeuuid',
                      'mview |                           users |                    text',
                      'mview |                           users |                    text',
                      'mview |                           users |                    text',
                      'mview |             users_by_first_name |                    text',
                      'mview |             users_by_first_name |                    text',
                      'mview |             users_by_first_name |                timeuuid',
                      'mview |             users_by_first_name |                    text',
                      'mview |             users_by_first_name |                    text',
                      'mview |             users_by_first_name |                    text',
                      'mview |              users_by_last_name |                    text',
                      'mview |              users_by_last_name |                    text',
                      'mview |              users_by_last_name |                timeuuid',
                      'mview |              users_by_last_name |                    text',
                      'mview |              users_by_last_name |                    text',
                      'mview |              users_by_last_name |                    text',
                      '', '(293 rows)']
        return result


class DummyNodeEmpty():  # pylint: disable=too-few-public-methods

    def run_cqlsh(self, cmd, timeout, verbose, target_db_node, split, connect_timeout):  # pylint: disable=too-many-arguments,unused-argument,no-self-use
        return ['Disabled Query paging.', '', '', '(2 rows)']


class DummyNodeNone():  # pylint: disable=too-few-public-methods

    def run_cqlsh(self, cmd, timeout, verbose, target_db_node, split, connect_timeout):  # pylint: disable=too-many-arguments,unused-argument,no-self-use
        return None


class TestNonSystemEntityList(unittest.TestCase):

    def test_default_params(self):
        dummy_node = DummyNode()
        expected_result = set(['mview.users', 'mview.users_by_first_name', 'keyspace1.standard1',
                               'mview.users_by_last_name', 'keyspace1.counter1'])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node)), expected_result)

    def test_filter_out_counter(self):
        dummy_node = DummyNode()
        expected_result = set(['mview.users', 'mview.users_by_first_name', 'keyspace1.standard1',
                               'mview.users_by_last_name'])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node,
                                                       filter_out_table_with_counter=True)), expected_result)

    def test_filter_out_mv(self):
        dummy_node = DummyNode()
        expected_result = set(['mview.users', 'keyspace1.standard1', 'keyspace1.counter1'])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node,
                                                       filter_out_mv=True)), expected_result)

    def test_filter_table_only_no_counter(self):  # pylint: disable=invalid-name
        dummy_node = DummyNode()
        expected_result = set(['mview.users', 'keyspace1.standard1'])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node,
                                                       filter_out_table_with_counter=True,
                                                       filter_out_mv=True)), expected_result)

    def test_empty_result(self):
        dummy_node = DummyNodeEmpty()
        expected_result = set([])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node,
                                                       filter_out_table_with_counter=True,
                                                       filter_out_mv=True)), expected_result)

    def test_none_result(self):
        dummy_node = DummyNodeNone()
        expected_result = set([])
        self.assertEqual(set(get_non_system_ks_cf_list(loader_node=dummy_node, db_node=dummy_node,
                                                       filter_out_table_with_counter=True,
                                                       filter_out_mv=True)), expected_result)

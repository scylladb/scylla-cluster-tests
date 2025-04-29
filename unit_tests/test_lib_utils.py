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
# Copyright (c) 2020 ScyllaDB

import logging
import unittest

from test_lib.utils import get_data_by_path, MagicList


logging.basicConfig(level=logging.DEBUG)


class TestClass:
    __test__ = False  # Mark this class to be not collected by pytest.

    def __init__(self, **kwargs):
        for arg_name, arg_value in kwargs.items():
            setattr(self, arg_name, arg_value)

    def __str__(self):
        body = ','.join([f'{attr_name}={repr(attr_value)}' for attr_name, attr_value in self.__dict__.items()])
        return f'<{body}>'

    __repr__ = __str__


class TestLibUtilsTest(unittest.TestCase):
    def test_magic_list(self):
        tmp = MagicList([
            TestClass(val1=1, val2=2),
            TestClass(val1=3, val2=5),
            TestClass(val1=10, val2=0)])

        self.assertEqual(repr(tmp.sort_by('val1')), "[<val1=1,val2=2>, <val1=3,val2=5>, <val1=10,val2=0>]")
        self.assertEqual(repr(tmp.sort_by('val2')), "[<val1=10,val2=0>, <val1=1,val2=2>, <val1=3,val2=5>]")

        self.assertEqual(
            repr(sorted(tmp.group_by('val1').items(), key=lambda item: item[0])),
            "[(1, [<val1=1,val2=2>]), (3, [<val1=3,val2=5>]), (10, [<val1=10,val2=0>])]")
        self.assertEqual(
            repr(sorted(tmp.group_by('val2').items(), key=lambda item: item[0])),
            "[(0, [<val1=10,val2=0>]), (2, [<val1=1,val2=2>]), (5, [<val1=3,val2=5>])]")

    def test_get_data_by_path(self):
        data = {
            'l1_key': {
                'l2_key': 10
            },
            'l1_instance': TestClass(l2_key=20)
        }
        self.assertEqual(
            data.get('l1_key'),
            get_data_by_path(data, data_path='l1_key')
        )
        self.assertEqual(
            10,
            get_data_by_path(data, data_path='l1_key.l2_key')
        )
        self.assertEqual(
            20,
            get_data_by_path(data, data_path='l1_instance.l2_key')
        )
        self.assertEqual(
            data.get('l1_instance'),
            get_data_by_path(data, data_path='l1_instance')
        )
        try:
            get_data_by_path(data, data_path='wrong_ley')
            self.fail("Should have failed")
        except ValueError:
            pass
        self.assertEqual(
            None,
            get_data_by_path(data, data_path='l1_key_wrong', default=None)
        )
        self.assertEqual(
            None,
            get_data_by_path(data, data_path='l1_key.l2_key_wrong', default=None)
        )
        self.assertEqual(
            None,
            get_data_by_path(data, data_path='l1_instance.l2_key_wrong', default=None)
        )
        self.assertEqual(
            None,
            get_data_by_path(data, data_path='l1_instance.l2_key.l3_key', default=None)
        )
        self.assertEqual(
            '<NONE>',
            get_data_by_path(data, data_path='l1_key_wrong', default='<NONE>')
        )
        self.assertEqual(
            '<NONE>',
            get_data_by_path(data, data_path='l1_key.l2_key_wrong', default='<NONE>')
        )
        self.assertEqual(
            '<NONE>',
            get_data_by_path(data, data_path='l1_instance.l2_key_wrong', default='<NONE>')
        )
        self.assertEqual(
            '<NONE>',
            get_data_by_path(data, data_path='l1_instance.l2_key.l3_key', default='<NONE>')
        )
        self.assertEqual(
            '',
            get_data_by_path(data, data_path='l1_key_wrong', default='')
        )
        self.assertEqual(
            '',
            get_data_by_path(data, data_path='l1_key.l2_key_wrong', default='')
        )
        self.assertEqual(
            '',
            get_data_by_path(data, data_path='l1_instance.l2_key_wrong', default='')
        )
        self.assertEqual(
            '',
            get_data_by_path(data, data_path='l1_instance.l2_key.l3_key', default='')
        )
        self.assertEqual(
            0,
            get_data_by_path(data, data_path='l1_key_wrong', default=0)
        )
        self.assertEqual(
            0,
            get_data_by_path(data, data_path='l1_key.l2_key_wrong', default=0)
        )
        self.assertEqual(
            0,
            get_data_by_path(data, data_path='l1_instance.l2_key_wrong', default=0)
        )
        self.assertEqual(
            0,
            get_data_by_path(data, data_path='l1_instance.l2_key.l3_key', default=0)
        )

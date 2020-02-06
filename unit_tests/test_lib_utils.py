from __future__ import absolute_import

import logging
import unittest


from test_lib.utils import get_data_by_path, MagicList

logging.basicConfig(level=logging.DEBUG)


class TestClass:  # pylint: disable=too-few-public-methods
    def __init__(self, **kwargs):
        for arg_name, arg_value in kwargs.items():
            setattr(self, arg_name, arg_value)

    def __str__(self):
        body = ','.join([f'{attr_name}={repr(attr_value)}' for attr_name, attr_value in self.__dict__.items()])
        return f'<{body}>'

    __repr__ = __str__


class TestLibUtilsTest(unittest.TestCase):
    def test_magic_list(self):  # pylint: disable=no-self-use
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

    def test_get_data_by_path(self):  # pylint: disable=no-self-use
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

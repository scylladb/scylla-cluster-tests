from __future__ import absolute_import
import logging
import unittest

from sdcm.wait import wait_for

logging.basicConfig(level=logging.DEBUG)


class TestSdcmWait(unittest.TestCase):
    def test_01_simple(self):
        calls = []

        def callback(arg1, arg2):
            calls.append((arg1, arg2))
            raise Exception("error")

        wait_for(callback, timeout=1, step=0.5, arg1=1, arg2=3, throw_exc=False)
        self.assertEqual(len(calls), 3)

    def test_02_throw_exc(self):
        calls = []

        def callback(arg1, arg2):
            calls.append((arg1, arg2))
            raise Exception("error")

        self.assertRaisesRegex(Exception, r'error', wait_for, callback,
                               throw_exc=True, timeout=2, step=0.5, arg1=1, arg2=3)
        self.assertEqual(len(calls), 5)

    def test_03_false_return(self):
        calls = []

        def callback(arg1, arg2):
            calls.append((arg1, arg2))
            return False

        wait_for(callback, timeout=1, step=0.5, arg1=1, arg2=3, throw_exc=False)
        self.assertEqual(len(calls), 3)

    def test_03_false_return_rerise(self):
        calls = []

        def callback(arg1, arg2):
            calls.append((arg1, arg2))
            return False

        self.assertRaisesRegex(Exception, "callback: timeout - 2 seconds - expired", wait_for,
                               callback, timeout=2, throw_exc=True, step=0.5, arg1=1, arg2=3)
        self.assertEqual(len(calls), 5)

    def test_03_return_value(self):
        calls = []

        def callback(arg1, arg2):
            calls.append((arg1, arg2))
            return 'what ever'

        self.assertEqual(wait_for(callback, timeout=2, step=0.5, arg1=1, arg2=3, throw_exc=False), 'what ever')
        self.assertEqual(len(calls), 1)

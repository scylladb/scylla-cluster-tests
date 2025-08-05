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


from __future__ import absolute_import
import logging
import threading
import time
import unittest

from concurrent.futures import ThreadPoolExecutor

import pytest

from sdcm.cluster import BaseNode
from sdcm.wait import wait_for, wait_for_log_lines, WaitForTimeoutError, ExitByEventError

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


from parameterized import parameterized


class TestSdcmWaitWithEventStop(unittest.TestCase):

    def setUp(self):
        self.calls = []
        self.callback_return_true_after = 0
        self.ev = threading.Event()

    def tearDown(self):
        self.calls = []
        self.callback_return_true_after = 0
        self.ev.set()

    def callback(self, arg1, arg2):
        self.calls.append((arg1, arg2))
        if len(self.calls) == self.callback_return_true_after:
            return "what ever"
        return False

    def set_stop_in_timeout(self, ev: threading.Event, set_after: int):
        while not ev.is_set():
            if len(self.calls) == set_after:
                ev.set()

    @parameterized.expand([(True, ), (False, )])
    def test_04_stop_by_event(self, throw_exc):
        self.callback_return_true_after = 3
        th = threading.Thread(target=self.set_stop_in_timeout, kwargs={"ev": self.ev, "set_after": 1})
        th.start()
        if throw_exc:
            self.assertRaisesRegex(ExitByEventError, "callback: stopped by Event", wait_for,
                                   self.callback, timeout=3, throw_exc=throw_exc, stop_event=self.ev, step=0.5, arg1=1, arg2=3)
        else:
            res = wait_for(self.callback, timeout=3, step=.5, throw_exc=throw_exc, stop_event=self.ev, arg1=1, arg2=3)
            self.assertFalse(res)

        self.assertTrue(len(self.calls) < 6, f"{len(self.calls)}")

    def test_04_stop_by_event_in_main_thread(self):
        self.callback_return_true_after = 3
        th = ThreadPoolExecutor(max_workers=1).submit(wait_for, func=self.callback, timeout=self.callback_return_true_after,
                                                      step=.5, throw_exc=False, stop_event=self.ev, arg1=1, arg2=3)

        self.set_stop_in_timeout(self.ev, set_after=1)
        res = th.result()
        exc = th.exception()
        self.assertFalse(exc, f"{exc}")
        self.assertFalse(res, f"{res}")
        self.assertTrue(len(self.calls) < 5)

    def test_04_return_result_before_stop_event_and_wait_timeout(self):
        self.callback_return_true_after = 2
        th = threading.Thread(target=self.set_stop_in_timeout, kwargs={"ev": self.ev, "set_after": 4})
        th.start()
        res = wait_for(self.callback, timeout=3, step=.5, throw_exc=False, stop_event=self.ev, arg1=1, arg2=3)
        self.assertEqual(res, "what ever")
        self.assertEqual(len(self.calls), 2)

    def test_04_raise_by_timeout_before_set_event(self):
        self.callback_return_true_after = 8

        th = threading.Thread(target=self.set_stop_in_timeout, kwargs={"ev": self.ev, "set_after": 7})
        th.start()
        self.assertRaisesRegex(WaitForTimeoutError, "callback: timeout - 3 seconds - expired", wait_for,
                               self.callback, timeout=3, throw_exc=True, stop_event=self.ev, step=0.5, arg1=1, arg2=3)
        self.assertEqual(len(self.calls), 7)

    @parameterized.expand([(True, ), (False, )])
    def test_04_raise_exception_in_func_before_set_event(self, throw_exc):

        def callback(arg1, arg2):
            self.calls.append((arg1, arg2))
            if len(self.calls) == 3:
                raise Exception("Raise before event")

            if len(self.calls) == 10:
                return "what ever"
            return False
        th = threading.Thread(target=self.set_stop_in_timeout, kwargs={"ev": self.ev, "set_after": 5})
        th.start()
        if throw_exc == True:
            self.assertRaisesRegex(ExitByEventError, "callback: stopped by Event", wait_for,
                                   callback, timeout=4, throw_exc=throw_exc, stop_event=self.ev, step=0.5, arg1=1, arg2=3)
        else:
            res = wait_for(callback, timeout=4, throw_exc=throw_exc, stop_event=self.ev, step=.5, arg1=1, arg2=3)
            self.assertFalse(res)
        self.assertEqual(len(self.calls), 6)

    def test_04_set_event_timeout_at_same_time(self):
        """ if event was set at same time as timeout exceed
        and throw_exc is true wait_for will raise Exception with
        message wait_for stopped by event"""
        self.callback_return_true_after = 8
        th = threading.Thread(target=self.set_stop_in_timeout, kwargs={"ev": self.ev, "set_after": 4})
        th.start()
        self.assertRaisesRegex(ExitByEventError, "callback: stopped by Event", wait_for,
                               self.callback, timeout=4, throw_exc=True, stop_event=self.ev, step=0.5, arg1=1, arg2=3)
        self.assertEqual(len(self.calls), 5)


class DummyNode(BaseNode):
    name = "node_1"
    system_log = ""

    def __init__(self, log_path):
        self.system_log = log_path


def write_to_file(filename, lines):
    for line in lines:
        with open(filename, 'a') as f:
            f.write(f"{line}\n")
        time.sleep(0.01)


class TestWaitForLogLines:

    def test_can_wait_for_log_start_line_and_end_line(self, tmp_path):
        file_path = tmp_path/"wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = [" [shard  0] repair - rebuild_with_repair: started with keyspaces=drop_table_during_repair",
                 "some line in between",
                 "[shard  0] repair - rebuild_with_repair: finished with keyspaces=drop_table_during_repair"]
        write_thread = threading.Thread(target=write_to_file, args=(file_path, lines))
        write_thread.daemon = True
        file_path.touch()
        with wait_for_log_lines(node=node, start_line_patterns=["rebuild.*started with keyspaces=", "Rebuild starts"],
                                end_line_patterns=["rebuild.*finished with keyspaces=", "Rebuild succeeded"],
                                start_timeout=3, end_timeout=5):
            write_thread.start()

    def test_wait_for_log_timeout_when_no_start_line(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["end", "cde", "end"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(TimeoutError, match="Timeout occurred while waiting for start log line"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.4, end_timeout=1.2):
            t.start()

    def test_wait_for_log_timeout_when_no_end_line(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["start", "cde", "start"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(TimeoutError, match="Timeout occurred while waiting for end log line"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.5, end_timeout=0.7):
            t.start()

    def test_wait_for_log_reraises_exception(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["start", "cde", "end"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(ValueError, match="dummy error"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.5, end_timeout=0.7):
            t.start()
            raise ValueError('dummy error')

    def test_wait_for_log_reraises_exception_and_timeout_error(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        file_path.touch()
        with pytest.raises(TimeoutError, match="Timeout occurred while waiting for start log line") as exc_info, \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.4, end_timeout=0.7):
            raise ValueError('dummy error')
        assert "ValueError" in str(exc_info.getrepr())

    def test_wait_for_log_reraises_timeout_error_with_error_context(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["start", "cde", "start"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        expected_match = r"Timeout occurred while waiting for end log line \['end'\] on node: node_1. Context: Wait end line"
        with pytest.raises(TimeoutError, match=expected_match), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.4, end_timeout=0.7, error_msg_ctx="Wait end line"):
            t.start()

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
# pylint: disable=all

from __future__ import absolute_import
import logging
import threading
import time
import unittest

import pytest

from sdcm.cluster import BaseNode
from sdcm.wait import wait_for, wait_for_log_lines

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
        with wait_for_log_lines(node=node, start_line_patterns=["rebuild.*started with keyspaces="],
                                end_line_patterns=["rebuild.*finished with keyspaces="],
                                start_timeout=3, end_timeout=5):
            write_thread.start()

    def test_wait_for_log_timeout_when_no_start_line(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["end", "cde", "end"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(TimeoutError, match="timeout occurred while waiting for start log line"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.3, end_timeout=1):
            t.start()

    def test_wait_for_log_timeout_when_no_end_line(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["start", "cde", "start"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(TimeoutError, match="timeout occurred while waiting for end log line"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.1, end_timeout=0.3):
            t.start()

    def test_wait_for_log_reraises_exception(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        lines = ["start", "cde", "end"]  # no start line provided
        t = threading.Thread(target=write_to_file, args=(file_path, lines))
        t.daemon = True
        file_path.touch()
        with pytest.raises(ValueError, match="dummy error"), \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.1, end_timeout=0.3):
            t.start()
            raise ValueError('dummy error')

    def test_wait_for_log_reraises_exception_and_timeout_error(self, tmp_path):
        file_path = tmp_path / "wait_for_log_lines_1.log"
        node = DummyNode(log_path=file_path)
        file_path.touch()
        with pytest.raises(TimeoutError, match="timeout occurred while waiting for start log line") as exc_info, \
                wait_for_log_lines(node=node, start_line_patterns=["start"], end_line_patterns=["end"], start_timeout=0.1, end_timeout=0.3):
            raise ValueError('dummy error')
        assert "ValueError" in str(exc_info.getrepr())

from __future__ import print_function

from __future__ import absolute_import
import unittest
import tempfile
import logging
import shutil
import os
import json
import queue


from sdcm.cluster import BaseNode, Setup
from sdcm.sct_events import start_events_device, stop_events_device
from sdcm.sct_events import EVENTS_PROCESSES

from unit_tests.dummy_remote import DummyRemote


class DummyNode(BaseNode):  # pylint: disable=abstract-method
    _database_log = None

    @property
    def private_ip_address(self):
        return '127.0.0.1'

    @property
    def public_ip_address(self):
        return '127.0.0.1'

    def start_task_threads(self):
        # disable all background threads
        pass

    def set_hostname(self):
        pass

    @property
    def database_log(self):
        return self._database_log

    @database_log.setter
    def database_log(self, x):
        self._database_log = x

    def copy_scylla_debug_info(self, node, scylla_debug_file):
        return "scylla_debug_info_file"

    def get_scylla_debuginfo_file(self):
        return "scylla_debug_info_file"


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestDecodeBactraces(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir, timeout=5)

        cls.node = DummyNode(name='test_node', parent_cluster=None,
                             base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.remoter = DummyRemote()

        cls.monitor_node = DummyNode(name='test_monitor_node', parent_cluster=None,
                                     base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.monitor_node.remoter = DummyRemote()

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database.log')

    def test_01_reactor_stall_is_not_decoded_if_disabled(self):
        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = False

        self.monitor_node.start_decode_on_monitor_node_thread()
        _ = self.node.search_database_log()
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')

        events = []
        for line in events_file.readlines():
            events.append(json.loads(line))

        for event in events:
            if event.get('raw_backtrace'):
                self.assertIsNone(event['backtrace'])

    def test_02_reactor_stalls_is_decoded_if_enabled(self):
        Setup.BACKTRACE_DECODING = True

        Setup.DECODING_QUEUE = queue.Queue()

        self.monitor_node.start_decode_on_monitor_node_thread()
        _ = self.node.search_database_log()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')

        events = []
        for line in events_file.readlines():
            events.append(json.loads(line))

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_03_decode_interlace_reactor_stall(self):  # pylint: disable=invalid-name

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database_interlace_stall.log')

        _ = self.node.search_database_log()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')
        events = []
        for line in events_file.readlines():
            events.append(json.loads(line))

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_04_decode_backtraces_core(self):

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database_core.log')

        _ = self.node.search_database_log()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')
        events = []
        for line in events_file.readlines():
            events.append(json.loads(line))

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

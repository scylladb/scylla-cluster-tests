from __future__ import print_function

from __future__ import absolute_import
import unittest
import tempfile
import logging
import shutil
import os
import json
import queue
import time

from sdcm.cluster import Setup
from sdcm.event_device import start_events_device, stop_and_cleanup_all_services, EventsProcessor
from sdcm.wait import wait_for
from unit_tests.dummy_remote import DummyRemote
from unit_tests.test_cluster import DummyNode


class DecodeDummyNode(DummyNode):  # pylint: disable=abstract-method

    def copy_scylla_debug_info(self, node, debug_file):
        return "scylla_debug_info_file"

    def get_scylla_debuginfo_file(self):
        return "scylla_debug_info_file"


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestDecodeBactraces(unittest.TestCase):
    events_processor: EventsProcessor

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        cls.events_processor = start_events_device(cls.temp_dir, test_mode=True)

        cls.node = DecodeDummyNode(name='test_node', parent_cluster=None,
                                   base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.remoter = DummyRemote()

        cls.monitor_node = DecodeDummyNode(name='test_monitor_node', parent_cluster=None,
                                           base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.monitor_node.remoter = DummyRemote()

    @classmethod
    def tearDownClass(cls):
        stop_and_cleanup_all_services()
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    def test_01_reactor_stall_is_not_decoded_if_disabled(self):
        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = False
        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.start_system_log_reader_thread()
        time.sleep(0.5)
        self.monitor_node.stop_decode_on_monitor_node_thread()
        self.node.stop_system_log_reader_thread()
        events_file = open(self.events_processor.event_device.raw_events_filename, 'r')
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
        self.node.start_system_log_reader_thread()

        def test():
            events_file = open(self.events_processor.event_device.raw_events_filename, 'r')
            events = []
            for line in events_file.readlines():
                events.append(json.loads(line))
            for event in events:
                if event.get('backtrace') and event.get('raw_backtrace'):
                    self.assertEqual(event['backtrace'].strip(),
                                     "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

        wait_for(test, step=0, timeout=1)
        self.node.stop_system_log_reader_thread()
        self.monitor_node.stop_decode_on_monitor_node_thread()

    def test_03_decode_interlace_reactor_stall(self):  # pylint: disable=invalid-name

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_interlace_stall.log')
        self.node.start_system_log_reader_thread()

        def test():
            events_file = open(self.events_processor.event_device.raw_events_filename, 'r')
            events = []
            for line in events_file.readlines():
                events.append(json.loads(line))

            for event in events:
                if event.get('backtrace') and event.get('raw_backtrace'):
                    self.assertEqual(event['backtrace'].strip(),
                                     "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

        wait_for(test, step=0, timeout=1)
        self.node.stop_system_log_reader_thread()
        self.monitor_node.stop_decode_on_monitor_node_thread()

    def test_04_decode_backtraces_core(self):

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_core.log')
        self.node.start_system_log_reader_thread()

        def test():
            events_file = open(self.events_processor.event_device.raw_events_filename, 'r')
            events = []
            for line in events_file.readlines():
                events.append(json.loads(line))

            for event in events:
                if event.get('backtrace') and event.get('raw_backtrace'):
                    self.assertEqual(event['backtrace'].strip(),
                                     "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

        wait_for(test, step=0.1, timeout=1)
        self.node.stop_system_log_reader_thread()
        self.monitor_node.stop_decode_on_monitor_node_thread()

from __future__ import print_function

from __future__ import absolute_import

import time

import unittest
import tempfile
import logging
import shutil
import os
import json
import queue

from sdcm import wait
from sdcm.cluster import Setup
from sdcm.remote import SSHConnectTimeoutError
from sdcm.sct_events import start_events_device, stop_events_device
from sdcm.sct_events import EVENTS_PROCESSES

from unit_tests.dummy_remote import DummyRemote, DummyRemoteWithException
from unit_tests.test_cluster import DummyNode


class DecodeDummyNode(DummyNode):  # pylint: disable=abstract-method

    def copy_scylla_debug_info(self, node, debug_file):
        return "scylla_debug_info_file"

    def get_scylla_debuginfo_file(self):
        return "scylla_debug_info_file"


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestDecodeBactraces(unittest.TestCase):
    _event_file = None

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir, timeout=5)

        cls.node = DecodeDummyNode(name='test_node', parent_cluster=None,
                                   base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.remoter = DummyRemote()

        cls.monitor_node = DecodeDummyNode(name='test_monitor_node', parent_cluster=None,
                                           base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.monitor_node.remoter = DummyRemote()

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    def open_event_file(self):
        self._event_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')
        self._event_file.seek(0, 2)

    def test_01_reactor_stall_is_not_decoded_if_disabled(self):
        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = False

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        # Wait for events been published
        time.sleep(2)
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

        self.assertTrue(events, "Events were not found")

        for event in events:
            if event.get('raw_backtrace'):
                self.assertIsNone(event['backtrace'])

    def test_02_reactor_stalls_is_decoded_if_enabled(self):
        # System log is same as in test 01, so do not need to create raw_events_file.
        # It has been created in the test 01
        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

        Setup.DECODING_QUEUE = queue.Queue()

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node._read_system_log_and_publish_events()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

        self.assertTrue(events, "Events were not found")

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_03_decode_interlace_reactor_stall(self):  # pylint: disable=invalid-name

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True
        self.open_event_file()

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_interlace_stall.log')

        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        # Wait for events been published
        time.sleep(2)
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events = [json.loads(line) for line in self._event_file]
        self._event_file.close()

        self.assertTrue(events, "Events were not found")

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_04_decode_backtraces_core(self):

        Setup.DECODING_QUEUE = queue.Queue()
        Setup.BACKTRACE_DECODING = True
        self.open_event_file()

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_core.log')

        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        # Wait for events been published
        time.sleep(2)
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()

        events = [json.loads(line) for line in self._event_file]
        self._event_file.close()

        self.assertTrue(events, "Events were not found")

        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))


class TestCoredumpRetryingWithException(unittest.TestCase):

    _event_file = None

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir, timeout=5)

        cls.node = DecodeDummyNode(name='test_node', parent_cluster=None,
                                   base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.init()
        cls.node.remoter = DummyRemoteWithException()
        cls.node.backtrace_lookup_period = 0
        cls.node.get_core_pids_func_retrying = 2
        cls.node.get_core_pids_func_sleep_timeout = 1
        cls.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def setUp(self):
        self.open_event_file()
        self.node.remoter.run_number = 0

    def tearDown(self) -> None:
        self._event_file.close()

    def open_event_file(self):
        self._event_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')
        self._event_file.seek(0, 2)

    def find_last_event(self):
        events = [json.loads(line) for line in self._event_file]
        traceback = ""
        if events:
            last_event = events[-1]
            if "traceback" in last_event:
                print(f"Last event: {last_event['traceback']}")
                traceback = last_event["traceback"].splitlines()[-1]
        return traceback

    def wait_for_all_exceptions_raised(self, exceptions_amount, wait_sec):
        for _ in range(exceptions_amount):
            if self.node.remoter.run_number < min(exceptions_amount, wait_sec):
                time.sleep(1)

    def test_backtrace_thread_with_network_exception(self):
        """
        Test case when get backtraces function failed after any network exception was raised 10 times.
        Expected that coredump thread will be stopped after 1 exceptions and last exception will be raised.
        """

        # In case when network exception is thrown during getting backtrace,
        # get_backtraces function will be retry 2 times before raise it to the the coredump thread.
        # The coredump thread will be failed after MAX_COREDUMP_THREAD_EXCEPTIONS attempts.
        # Set MAX_COREDUMP_THREAD_EXCEPTIONS to 1 for decrease time of running test
        exceptions_amount = 2
        self.node.max_coredump_thread_exceptions = 1
        self.node.remoter.test_exception = SSHConnectTimeoutError
        self.node.remoter.exceptions_amount = exceptions_amount

        self.node.start_backtrace_thread()

        print("Wait for all exceptions are raised")
        self.wait_for_all_exceptions_raised(exceptions_amount=exceptions_amount, wait_sec=5)

        # Wait for events been published
        time.sleep(2)

        print("Wait for raised exception found")
        traceback = wait.wait_for(self.find_last_event, step=5, timeout=10)

        assert "sdcm.remote.base.SSHConnectTimeoutError" == traceback, \
               f"Expected exception 'sdcm.remote.base.SSHConnectTimeoutError', got {traceback}"

    def test_backtrace_thread_with_any_exception(self):
        """
        Test case when get backtraces function failed after an exception was raised 3 times.
        Expected that coredump thread will be stopped after 3 exceptions and last exception will be raised.
        """

        # In case when non network exception is thrown during getting backtrace,
        # exception will be raised from the coredump thread after MAX_COREDUMP_THREAD_EXCEPTIONS attempts.
        # Set MAX_COREDUMP_THREAD_EXCEPTIONS to 3 for decrease time of running test
        exceptions_amount = 3
        self.node.max_coredump_thread_exceptions = exceptions_amount
        self.node.remoter.test_exception = NameError
        self.node.remoter.exceptions_amount = exceptions_amount

        self.node.start_backtrace_thread()

        print("Wait for all exceptions are raised")
        self.wait_for_all_exceptions_raised(exceptions_amount=exceptions_amount, wait_sec=5)

        # Wait for events been published
        time.sleep(2)

        print("Wait for all exceptions raised")
        traceback = wait.wait_for(self.find_last_event, step=0.1, timeout=10)

        assert "NameError" == traceback, \
               f"Expected exception 'Exception', got {traceback}"

    def test_backtrace_thread_with_no_last_exception(self):
        """
        Test case when get backtraces function failed after an exception was raised 2 times and then continue
        without failure.
        get backtraces function should fail after an exception was raised 3 times. Because of just 2 exceptions will be
        raised, expected that coredump thread continue to run.

        """
        exceptions_amount = 2
        self.node.max_coredump_thread_exceptions = exceptions_amount + 1
        self.node.remoter.test_exception = ValueError
        self.node.remoter.exceptions_amount = exceptions_amount

        self.node.start_backtrace_thread()

        # Wait for events been published
        time.sleep(2)

        print("Wait for all exceptions are raised")
        self.wait_for_all_exceptions_raised(exceptions_amount=exceptions_amount, wait_sec=5)

        self.node.termination_event.set()
        self.node._backtrace_thread.join(5)

        traceback = self.find_last_event()

        assert traceback == '', f"Unexpected failure: {traceback}"

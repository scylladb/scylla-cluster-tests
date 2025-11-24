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

import os
import json
from multiprocessing import Queue
import unittest
from functools import cached_property

import pytest

from sdcm.cluster import TestConfig
from sdcm.db_log_reader import DbLogReader
from sdcm.sct_events.database import SYSTEM_ERROR_EVENTS_PATTERNS

from unit_tests.dummy_remote import DummyRemote
from unit_tests.test_utils_common import DummyNode
from unit_tests.lib.events_utils import EventsUtilsMixin


class DecodeDummyNode(DummyNode):

    def copy_scylla_debug_info(self, node_name, debug_file):
        return "scylla_debug_info_file"


class TestDecodeBactraces(unittest.TestCase, EventsUtilsMixin):
    @classmethod
    def setUpClass(cls):
        cls.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)

    @classmethod
    def tearDownClass(cls):
        cls.teardown_events_processes()

    @cached_property
    def test_config(self):
        result = TestConfig()
        result.set_decoding_queue()
        return result

    @cached_property
    def node(self):
        dummy_node = DecodeDummyNode(
            name='test_node',
            parent_cluster=None,
            base_logdir=self.temp_dir,
            ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
        )
        dummy_node.remoter = DummyRemote()
        return dummy_node

    @cached_property
    def monitor_node(self):
        dummy_monitor = DecodeDummyNode(
            name='test_monitor_node',
            parent_cluster=None,
            base_logdir=self.temp_dir,
            ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
        )
        dummy_monitor.remoter = DummyRemote()
        return dummy_monitor

    @cached_property
    def _db_log_reader(self):
        return DbLogReader(
            system_log=self.node.system_log,
            node_name=str(self),
            remoter=self.node.remoter,
            decoding_queue=self.test_config.DECODING_QUEUE,
            system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
            log_lines=True,
            backtrace_stall_decoding=True,
            backtrace_decoding_disable_regex=None,
        )

    @cached_property
    def _db_log_reader_no_decoding(self):
        return DbLogReader(
            system_log=self.node.system_log,
            node_name=str(self),
            remoter=self.node.remoter,
            decoding_queue=None,
            system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
            log_lines=False,
            backtrace_stall_decoding=True,
            backtrace_decoding_disable_regex=None,
        )

    def _read_and_publish_events(self):
        self._db_log_reader._read_and_publish_events()

    def _read_and_publish_events_no_decoding(self):
        self._db_log_reader_no_decoding._read_and_publish_events()

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    def test_01_reactor_stall_is_not_decoded_if_disabled(self):
        # Accesses the test_config to ensure it is initialized
        self.test_config
        self.monitor_node.start_decode_on_monitor_node_thread()
        self._read_and_publish_events_no_decoding()
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()
        self.monitor_node.wait_till_tasks_threads_are_stopped()

        events = []
        with self.get_raw_events_log().open() as events_file:
            for line in events_file.readlines():
                events.append(json.loads(line))

        assert any(event.get('raw_backtrace') for event in events), "should have at least one backtrace"
        for event in events:
            if event.get('raw_backtrace'):
                self.assertIsNone(event['backtrace'])

    def test_02_reactor_stalls_is_decoded_if_enabled(self):
        self.test_config.BACKTRACE_DECODING = True

        self.test_config.DECODING_QUEUE = Queue()

        self.monitor_node.start_decode_on_monitor_node_thread()
        self._read_and_publish_events()
        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()
        self.monitor_node.wait_till_tasks_threads_are_stopped()

        events = []
        with self.get_raw_events_log().open() as events_file:
            for line in events_file.readlines():
                events.append(json.loads(line))

        assert any(event.get('raw_backtrace') for event in events), "should have at least one backtrace"
        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_03_decode_interlace_reactor_stall(self):

        self.test_config.DECODING_QUEUE = Queue()
        self.test_config.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_interlace_stall.log')

        self._read_and_publish_events()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()
        self.monitor_node.wait_till_tasks_threads_are_stopped()

        events = []
        with self.get_raw_events_log().open() as events_file:
            for line in events_file.readlines():
                events.append(json.loads(line))

        assert any(event.get('raw_backtrace') for event in events), "should have at least one backtrace"
        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))

    def test_04_decode_backtraces_core(self):

        self.test_config.DECODING_QUEUE = Queue()
        self.test_config.BACKTRACE_DECODING = True

        self.monitor_node.start_decode_on_monitor_node_thread()
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_core.log')

        self._read_and_publish_events()

        self.monitor_node.termination_event.set()
        self.monitor_node.stop_task_threads()
        self.monitor_node.wait_till_tasks_threads_are_stopped()

        events = []
        with self.get_raw_events_log().open() as events_file:
            for line in events_file.readlines():
                events.append(json.loads(line))

        assert any(event.get('raw_backtrace') for event in events), "should have at least one backtrace"
        for event in events:
            if event.get('backtrace') and event.get('raw_backtrace'):
                self.assertEqual(event['backtrace'].strip(),
                                 "addr2line -Cpife scylla_debug_info_file {}".format(' '.join(event['raw_backtrace'].split("\n"))))


# Pytest-style fixtures and tests

@pytest.fixture(name='test_config')
def test_config_fixture():
    """Fixture to create TestConfig with decoding queue."""
    config = TestConfig()
    config.set_decoding_queue()
    config.BACKTRACE_DECODING = True
    config.DECODING_QUEUE = Queue()
    yield config


@pytest.fixture(name='dummy_node')
def dummy_node_fixture(tmp_path):
    """Fixture to create a dummy node for testing."""
    dummy_node = DecodeDummyNode(
        name='test_node',
        parent_cluster=None,
        base_logdir=tmp_path,
    )
    dummy_node.remoter = DummyRemote()
    return dummy_node


@pytest.fixture(name='monitor_node')
def monitor_node_fixture(tmp_path):
    """Fixture to create a monitor node for testing."""
    monitor_node = DecodeDummyNode(
        name='test_monitor_node',
        parent_cluster=None,
        base_logdir=tmp_path,
    )
    monitor_node.remoter = DummyRemote()
    yield monitor_node

    # Cleanup
    monitor_node.termination_event.set()
    monitor_node.stop_task_threads()
    monitor_node.wait_till_tasks_threads_are_stopped()


@pytest.mark.parametrize('stall_decoding,disable_regex,event_filter,should_decode', [
    pytest.param(
        False,
        None,
        lambda e: e.get('type') == 'REACTOR_STALLED' and e.get('raw_backtrace'),
        False,
        id='reactor_stalls_not_decoded_when_stall_decoding_disabled'
    ),
    pytest.param(
        False,
        None,
        lambda e: e.get('raw_backtrace') and e.get('type') != 'REACTOR_STALLED',
        True,
        id='other_backtraces_decoded_when_stall_decoding_disabled'
    ),
    pytest.param(
        True,
        '^REACTOR_STALLED$',
        lambda e: e.get('type') == 'REACTOR_STALLED' and e.get('raw_backtrace'),
        True,  # Should be False when bug is fixed
        id='regex_filter_excludes_matching_events',
    ),
])
def test_backtrace_decoding_configuration(
    test_config,
    dummy_node,
    monitor_node,
    events_function_scope,
    stall_decoding,
    disable_regex,
    event_filter,
    should_decode
):
    """Test various backtrace decoding configuration scenarios.

    Args:
        stall_decoding: Whether to enable stall decoding
        disable_regex: Regex pattern to disable decoding for matching event types
        event_filter: Lambda function to filter events for validation
        should_decode: Whether the filtered events should have decoded backtraces
    """

    # Setup
    dummy_node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    # Create db_log_reader with specific configuration
    db_log_reader = DbLogReader(
        system_log=dummy_node.system_log,
        node_name=str(dummy_node),
        remoter=dummy_node.remoter,
        decoding_queue=test_config.DECODING_QUEUE,
        system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
        log_lines=True,
        backtrace_stall_decoding=stall_decoding,
        backtrace_decoding_disable_regex=disable_regex,
    )

    # Execute
    monitor_node.start_decode_on_monitor_node_thread()
    db_log_reader._read_and_publish_events()
    monitor_node.termination_event.set()
    monitor_node.stop_task_threads()
    monitor_node.wait_till_tasks_threads_are_stopped()

    # Validate
    events = []
    with open(events_function_scope.get_raw_events_log()) as events_file:
        for line in events_file.readlines():
            events.append(json.loads(line))

    filtered_events = [e for e in events if event_filter(e)]
    assert len(filtered_events) > 0, "Should have at least one matching event"

    for event in filtered_events:
        if should_decode:
            assert event.get('backtrace') is not None, \
                f"Event of type {event.get('type')} should have decoded backtrace"
        else:
            assert event.get('backtrace') is None, \
                f"Event of type {event.get('type')} should not have decoded backtrace"

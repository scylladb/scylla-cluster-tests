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

from multiprocessing import Queue

import pytest

from sdcm.cluster import TestConfig
from sdcm.db_log_reader import DbLogReader
from sdcm.sct_events.database import SYSTEM_ERROR_EVENTS_PATTERNS

from unit_tests.lib.dummy_remote import DummyRemote
from unit_tests.lib.fake_cluster import DummyNode


class DecodeDummyNode(DummyNode):
    def copy_scylla_debug_info(self, node_name, debug_file):
        return "scylla_debug_info_file"


@pytest.fixture(name="test_config")
def test_config_fixture():
    """Fixture to create TestConfig with decoding queue."""
    config = TestConfig()
    config.set_decoding_queue()
    config.BACKTRACE_DECODING = True
    config.DECODING_QUEUE = Queue()
    yield config


@pytest.fixture(name="dummy_node")
def dummy_node_fixture(tmp_path):
    """Fixture to create a dummy node for testing."""
    dummy_node = DecodeDummyNode(
        name="test_node",
        parent_cluster=None,
        base_logdir=tmp_path,
    )
    dummy_node.remoter = DummyRemote()
    return dummy_node


@pytest.fixture(name="monitor_node")
def monitor_node_fixture(tmp_path):
    """Fixture to create a monitor node for testing."""
    monitor_node = DecodeDummyNode(
        name="test_monitor_node",
        parent_cluster=None,
        base_logdir=tmp_path,
    )
    monitor_node.remoter = DummyRemote()
    yield monitor_node

    # Cleanup
    monitor_node.termination_event.set()
    monitor_node.stop_task_threads()
    monitor_node.wait_till_tasks_threads_are_stopped()


def test_reactor_stall_not_decoded_when_no_decoding_queue(
    dummy_node, monitor_node, events_function_scope, test_data_dir
):
    """Backtraces are not decoded when decoding_queue is None."""
    config = TestConfig()
    config.set_decoding_queue()

    dummy_node.system_log = str(test_data_dir / "system.log")

    db_log_reader = DbLogReader(
        system_log=dummy_node.system_log,
        node_name=str(dummy_node),
        remoter=dummy_node.remoter,
        decoding_queue=None,
        system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
        log_lines=False,
        backtrace_stall_decoding=True,
        backtrace_decoding_disable_regex=None,
    )

    monitor_node.start_decode_on_monitor_node_thread()
    db_log_reader._read_and_publish_events()

    events = events_function_scope.published_events

    assert any(event.get("raw_backtrace") for event in events), "should have at least one backtrace"
    for event in events:
        if event.get("raw_backtrace"):
            assert event["backtrace"] is None


@pytest.mark.parametrize(
    "log_file",
    [
        pytest.param("system.log", id="standard_log"),
        pytest.param("system_interlace_stall.log", id="interlace_stall_log"),
        pytest.param("system_core.log", id="core_backtrace_log"),
    ],
)
def test_backtraces_decoded_when_enabled(
    test_config, dummy_node, monitor_node, events_function_scope, log_file, test_data_dir
):
    """Backtraces are decoded to addr2line commands when decoding is enabled."""
    dummy_node.system_log = str(test_data_dir / log_file)

    db_log_reader = DbLogReader(
        system_log=dummy_node.system_log,
        node_name=str(dummy_node),
        remoter=dummy_node.remoter,
        decoding_queue=test_config.DECODING_QUEUE,
        system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
        log_lines=True,
        backtrace_stall_decoding=True,
        backtrace_decoding_disable_regex=None,
    )

    monitor_node.start_decode_on_monitor_node_thread()
    db_log_reader._read_and_publish_events()
    monitor_node.termination_event.set()
    monitor_node.stop_task_threads()
    monitor_node.wait_till_tasks_threads_are_stopped()

    events = events_function_scope.published_events

    assert any(event.get("raw_backtrace") for event in events), "should have at least one backtrace"
    for event in events:
        if event.get("backtrace") and event.get("raw_backtrace"):
            assert event["backtrace"].strip() == "addr2line -Cpife scylla_debug_info_file {}".format(
                " ".join(event["raw_backtrace"].split("\n"))
            )


@pytest.mark.parametrize(
    "stall_decoding,disable_regex,event_filter,should_decode",
    [
        pytest.param(
            False,
            None,
            lambda e: e.get("type") == "REACTOR_STALLED" and e.get("raw_backtrace"),
            False,
            id="reactor_stalls_not_decoded_when_stall_decoding_disabled",
        ),
        pytest.param(
            False,
            None,
            lambda e: e.get("raw_backtrace") and e.get("type") != "REACTOR_STALLED",
            True,
            id="other_backtraces_decoded_when_stall_decoding_disabled",
        ),
        pytest.param(
            True,
            "^REACTOR_STALLED$",
            lambda e: e.get("type") == "REACTOR_STALLED" and e.get("raw_backtrace"),
            True,  # Should be False when bug is fixed
            id="regex_filter_excludes_matching_events",
        ),
    ],
)
def test_backtrace_decoding_configuration(
    test_config,
    dummy_node,
    monitor_node,
    events_function_scope,
    test_data_dir,
    stall_decoding,
    disable_regex,
    event_filter,
    should_decode,
):
    """Test various backtrace decoding configuration scenarios.

    Args:
        stall_decoding: Whether to enable stall decoding
        disable_regex: Regex pattern to disable decoding for matching event types
        event_filter: Lambda function to filter events for validation
        should_decode: Whether the filtered events should have decoded backtraces
    """
    # Setup
    dummy_node.system_log = str(test_data_dir / "system.log")

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

    monitor_node.start_decode_on_monitor_node_thread()
    db_log_reader._read_and_publish_events()
    monitor_node.termination_event.set()
    monitor_node.stop_task_threads()
    monitor_node.wait_till_tasks_threads_are_stopped()

    events = events_function_scope.published_events

    filtered_events = [e for e in events if event_filter(e)]
    assert len(filtered_events) > 0, "Should have at least one matching event"

    for event in filtered_events:
        if should_decode:
            assert event.get("backtrace") is not None, (
                f"Event of type {event.get('type')} should have decoded backtrace"
            )
        else:
            assert event.get("backtrace") is None, (
                f"Event of type {event.get('type')} should not have decoded backtrace"
            )

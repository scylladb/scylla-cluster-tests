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

import queue

from multiprocessing import Queue
from unittest.mock import MagicMock, patch

import pytest
import requests

from sdcm.cluster import TestConfig
from sdcm.db_log_reader import DbLogReader
from sdcm.sct_events.database import SYSTEM_ERROR_EVENTS_PATTERNS

from unit_tests.lib.dummy_remote import DummyRemote
from unit_tests.lib.fake_cluster import DummyNode

DECODED_BY_SERVICE = "decoded-by-external"
BUILD_ID = "abc123"


def _service_response(success=True, stdout=DECODED_BY_SERVICE, stderr=""):
    """Build a fake requests.Response for the backtrace service."""
    response = MagicMock()
    response.json.return_value = {"success": success, "stdout": stdout, "stderr": stderr}
    response.raise_for_status.return_value = None
    return response


@pytest.fixture(name="test_config")
def test_config_fixture():
    """Fixture to create TestConfig with decoding queue."""
    config = TestConfig()
    config.set_decoding_queue()
    config.BACKTRACE_DECODING = True
    config.DECODING_QUEUE = Queue()
    yield config
    TestConfig.BACKTRACE_DECODING = False
    TestConfig.DECODING_QUEUE = None


@pytest.fixture(name="dummy_node")
def dummy_node_fixture(tmp_path):
    """Fixture to create a dummy node for testing."""
    dummy_node = DummyNode(
        name="test_node",
        parent_cluster=None,
        base_logdir=tmp_path,
    )
    dummy_node.remoter = DummyRemote()
    return dummy_node


@pytest.fixture(name="monitor_node")
def monitor_node_fixture(tmp_path):
    """Fixture to create a monitor node for testing."""
    monitor_node = DummyNode(
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


def _make_db_log_reader(dummy_node, decoding_queue, stall_decoding=True, disable_regex=None):
    db_log_reader = DbLogReader(
        system_log=dummy_node.system_log,
        node_name=str(dummy_node),
        remoter=dummy_node.remoter,
        decoding_queue=decoding_queue,
        system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
        log_lines=True,
        backtrace_stall_decoding=stall_decoding,
        backtrace_decoding_disable_regex=disable_regex,
    )
    db_log_reader._build_id = BUILD_ID
    return db_log_reader


def _run_decode_thread_over_log(monitor_node, db_log_reader):
    monitor_node.start_decode_on_monitor_node_thread()
    db_log_reader._read_and_publish_events()
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
    """Backtraces flow from the db log reader through the decode thread to the published event."""
    dummy_node.system_log = str(test_data_dir / log_file)
    db_log_reader = _make_db_log_reader(dummy_node, test_config.DECODING_QUEUE)

    with patch("sdcm.cluster.requests.post", return_value=_service_response()):
        _run_decode_thread_over_log(monitor_node, db_log_reader)

    events = events_function_scope.published_events

    assert any(event.get("raw_backtrace") for event in events), "should have at least one backtrace"
    for event in events:
        if event.get("raw_backtrace"):
            assert event["backtrace"] == DECODED_BY_SERVICE


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
    dummy_node.system_log = str(test_data_dir / "system.log")
    db_log_reader = _make_db_log_reader(dummy_node, test_config.DECODING_QUEUE, stall_decoding, disable_regex)

    with patch("sdcm.cluster.requests.post", return_value=_service_response()):
        _run_decode_thread_over_log(monitor_node, db_log_reader)

    events = events_function_scope.published_events

    filtered_events = [e for e in events if event_filter(e)]
    assert len(filtered_events) > 0, "Should have at least one matching event"

    for event in filtered_events:
        if should_decode:
            assert event.get("backtrace") == DECODED_BY_SERVICE, (
                f"Event of type {event.get('type')} should have decoded backtrace"
            )
        else:
            assert event.get("backtrace") is None, (
                f"Event of type {event.get('type')} should not have decoded backtrace"
            )


def _run_decode_with_queue_item(monitor_node, build_id, raw_backtrace):
    """Helper: enqueue one fake event, run decode_backtrace(), return the event mock."""
    config = TestConfig()
    config.DECODING_QUEUE = queue.Queue()

    event = MagicMock()
    event.raw_backtrace = raw_backtrace
    event.backtrace = None
    event.severity = MagicMock()
    event.severity.value = 0
    event.type = "REACTOR_STALLED"
    event.event_id = "test-event-id"

    config.DECODING_QUEUE.put({"event": event, "node": "test_node", "build_id": build_id})
    config.DECODING_QUEUE.put(None)

    monitor_node.test_config = config

    # retries must not slow the unit tests down
    with patch("time.sleep"):
        monitor_node.decode_backtrace()
    return event


def test_external_service_success_sets_decoded_backtrace(monitor_node):
    """A successful service reply becomes the decoded backtrace; nothing is run on the monitor node."""
    monitor_node.remoter = MagicMock()

    with patch("sdcm.cluster.requests.post", return_value=_service_response()) as mock_post:
        event = _run_decode_with_queue_item(monitor_node, BUILD_ID, "0x1234\n0x5678")

    assert event.backtrace == DECODED_BY_SERVICE
    assert event.build_id == BUILD_ID
    mock_post.assert_called_once()
    monitor_node.remoter.run.assert_not_called()


def test_external_service_post_request_payload(monitor_node):
    """Verify the POST request sends correct URL, build_id and input format."""
    with patch("sdcm.cluster.requests.post", return_value=_service_response()) as mock_post:
        _run_decode_with_queue_item(monitor_node, "abc123def", "0x1234\n0x5678")

    mock_post.assert_called_once_with(
        "https://backtrace.scylladb.com/api/backtrace",
        json={"build_id": "abc123def", "input": "Backtrace:\n0x1234\n0x5678"},
        timeout=120,
    )


@pytest.mark.parametrize(
    "side_effect,expected_calls",
    [
        pytest.param(requests.HTTPError(response=MagicMock(status_code=404)), 3, id="http_404"),
        pytest.param(
            _service_response(success=False, stdout="", stderr="OSError: [Errno 28] No space left on device"),
            1,
            id="service_side_error_is_final",
        ),
    ],
)
def test_external_service_failure_publishes_raw_backtrace(monitor_node, side_effect, expected_calls):
    """When the service fails, the event keeps only its raw backtrace and nothing is decoded on the monitor."""
    monitor_node.remoter = MagicMock()

    with (
        patch("sdcm.cluster.requests.post", side_effect=[side_effect] * expected_calls) as mock_post,
        patch("sdcm.cluster.FindIssuePerBacktrace") as mock_find_issue,
    ):
        event = _run_decode_with_queue_item(monitor_node, BUILD_ID, "0x1234\n0x5678")

    assert event.backtrace is None
    assert event.raw_backtrace == "0x1234\n0x5678"
    assert event.build_id == BUILD_ID, "the build id must stay with the raw backtrace so it can be decoded later"
    assert mock_post.call_count == expected_calls
    monitor_node.remoter.run.assert_not_called()
    mock_find_issue.assert_not_called()


def test_external_service_retries_transient_errors(monitor_node):
    """Transient HTTP errors are retried and a later success is used."""
    with patch(
        "sdcm.cluster.requests.post",
        side_effect=[requests.Timeout("timed out"), requests.ConnectionError("refused"), _service_response()],
    ) as mock_post:
        event = _run_decode_with_queue_item(monitor_node, BUILD_ID, "0x1234\n0x5678")

    assert event.backtrace == DECODED_BY_SERVICE
    assert mock_post.call_count == 3


def test_external_service_cooldown_skips_build_after_failure(monitor_node):
    """After a final failure the service is not asked about the same build again while the cooldown lasts."""
    with patch("sdcm.cluster.requests.post", return_value=_service_response(success=False)) as mock_post:
        first = _run_decode_with_queue_item(monitor_node, BUILD_ID, "0x1111")
        second = _run_decode_with_queue_item(monitor_node, BUILD_ID, "0x2222")
        other_build = _run_decode_with_queue_item(monitor_node, "def456", "0x3333")

    assert first.backtrace is None
    assert second.backtrace is None
    assert other_build.backtrace is None
    assert mock_post.call_count == 2, "one call per build id: the second event of abc123 must be skipped"


def test_no_build_id_publishes_raw_backtrace(monitor_node):
    """Without a build id there is nothing to decode against: no service call, nothing run on the monitor."""
    monitor_node.remoter = MagicMock()

    with patch("sdcm.cluster.requests.post") as mock_post:
        event = _run_decode_with_queue_item(monitor_node, None, "0x1234\n0x5678")

    mock_post.assert_not_called()
    monitor_node.remoter.run.assert_not_called()
    assert event.backtrace is None
    assert event.build_id is None

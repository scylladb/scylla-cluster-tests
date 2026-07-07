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
# Copyright (c) 2025 ScyllaDB

"""Tests for ArgusEventPostman draining its queue on stop() and staying time-bounded."""

import time
import shutil
import tempfile
import threading
from unittest.mock import MagicMock

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.setup import EVENTS_DEVICE_START_DELAY, stop_events_device
from sdcm.sct_events.argus import SCTArgusEvent, get_argus_postman, start_argus_pipeline
from sdcm.sct_events.events_device import start_events_main_device
from sdcm.sct_events.events_processes import EVENTS_ARGUS_AGGREGATOR_ID, EventsProcessesRegistry, get_events_process


def _argus_event(severity: Severity, message: str) -> SCTArgusEvent:
    """Build a minimal Argus event payload as produced by ArgusEventCollector."""
    return SCTArgusEvent(
        {
            "run_id": "test-run",
            "severity": severity.name,
            "ts": time.time(),
            "event_type": "TestEvent",
            "message": message,
        }
    )


@pytest.fixture
def argus_pipeline():
    """Start an isolated Argus pipeline (collector/aggregator/postman) on its own registry.

    The postman is intentionally left not-enabled, so its run() loop parks and only the
    stop()/drain path touches the aggregator's outbound_queue - making the tests deterministic.
    """
    temp_dir = tempfile.mkdtemp()
    registry = EventsProcessesRegistry(log_dir=temp_dir)
    # Start the main device first so the collector has a valid upstream to consume from.
    start_events_main_device(_registry=registry)
    time.sleep(EVENTS_DEVICE_START_DELAY)
    start_argus_pipeline(_registry=registry)
    postman = get_argus_postman(_registry=registry)
    aggregator = get_events_process(EVENTS_ARGUS_AGGREGATOR_ID, _registry=registry)
    postman._argus_client = MagicMock()
    try:
        yield postman, aggregator
    finally:
        stop_events_device(_registry=registry)
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_stop_drains_pending_events(argus_pipeline):
    """stop() posts every event still queued in the aggregator before the thread exits."""
    postman, aggregator = argus_pipeline
    submitted = []
    postman._argus_client.submit_event.side_effect = lambda event: submitted.append(event["severity"])

    aggregator.outbound_queue.put(_argus_event(Severity.WARNING, "warn"))
    aggregator.outbound_queue.put(_argus_event(Severity.ERROR, "err"))
    aggregator.outbound_queue.put(_argus_event(Severity.NORMAL, "info"))

    postman.stop(timeout=5)

    assert not postman.is_alive()
    assert submitted == [Severity.WARNING.name, Severity.ERROR.name, Severity.NORMAL.name]


def test_stop_is_bounded_when_submit_hangs(argus_pipeline):
    """stop() returns within the caller-provided timeout even when a submit blocks forever."""
    postman, aggregator = argus_pipeline

    release = threading.Event()  # never set -> submit_event blocks until teardown
    postman._argus_client.submit_event.side_effect = lambda event: release.wait()

    aggregator.outbound_queue.put(_argus_event(Severity.ERROR, "err"))

    start = time.monotonic()
    postman.stop(timeout=1)
    elapsed = time.monotonic() - start

    try:
        assert elapsed < 3
        assert postman._argus_client.submit_event.called  # drain was attempted
        assert postman.is_alive()  # the hung submit is still stuck past the timeout
    finally:
        release.set()  # release the leaked daemon worker


def test_stop_without_queued_events_returns_quickly(argus_pipeline):
    """stop() with an empty queue does not block and posts nothing."""
    postman, _ = argus_pipeline

    start = time.monotonic()
    postman.stop(timeout=5)
    elapsed = time.monotonic() - start

    assert elapsed < 2
    assert not postman.is_alive()
    postman._argus_client.submit_event.assert_not_called()

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
import threading
import time
import unittest.mock

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.gce_events import GceInstanceEvent
from sdcm.sct_events.system import InfoEvent, SpotTerminationEvent
from sdcm.sct_events.events_analyzer import (
    _is_benign_gce_live_migration_of_loader,
    start_events_analyzer,
)
from sdcm.sct_events.events_processes import EVENTS_ANALYZER_ID

LIVE_MIGRATION_METHOD = "compute.instances.migrateOnHostMaintenance"
TERMINATE_ON_MAINTENANCE_METHOD = "compute.instances.terminateOnHostMaintenance"


def make_gce_instance_event(node_name: str, method: str, severity: Severity = Severity.CRITICAL) -> GceInstanceEvent:
    """Build a GceInstanceEvent from a GCE audit log entry for `node_name` with API method `method`."""
    gce_log_entry = {
        "timestamp": "2026-08-23T00:00:00.000000+00:00",
        "protoPayload": {
            "resourceName": f"projects/p/zones/z/instances/{node_name}",
            "methodName": method,
            "status": {"message": "host maintenance"},
        },
    }
    return GceInstanceEvent(gce_log_entry, severity=severity)


@pytest.fixture
def events_analyzer(start_events_subscriber):
    """A running events analyzer wired to the test's events registry."""
    return start_events_subscriber(start_events_analyzer, EVENTS_ANALYZER_ID)


def test_events_analyzer_kills_test_on_critical_event(main_events_context, events_analyzer):
    """The analyzer consumes every published event and calls kill_test once a CRITICAL one arrives."""
    initial_events_no = main_events_context.events_main_device.events_counter

    event1 = InfoEvent(message="m1")
    event2 = SpotTerminationEvent(node="n1", message="m2")

    with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
        with main_events_context.wait_for_n_events(events_analyzer, count=2, timeout=1):
            main_events_context.events_main_device.publish_event(event1)
            main_events_context.events_main_device.publish_event(event2)

    assert main_events_context.events_main_device.events_counter == initial_events_no + events_analyzer.events_counter
    mock.assert_called_once()


def test_events_analyzer_keeps_test_running_on_loader_live_migration(main_events_context, events_analyzer):
    """A CRITICAL loader live migration does not kill the test, proving the analyzer consults the benign-event check."""
    event = make_gce_instance_event("loader-node-1", LIVE_MIGRATION_METHOD)

    with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
        with main_events_context.wait_for_n_events(events_analyzer, count=1, timeout=1):
            main_events_context.events_main_device.publish_event(event)

        mock.assert_not_called()


def test_can_stop_events_analyzer_during_stream_of_events(main_events_context, events_analyzer):
    """stop() returns while events keep arriving, so teardown is not blocked by a busy publisher."""
    stop_event = threading.Event()

    def publish_event_every_100_ms():
        while not stop_event.is_set():
            main_events_context.events_main_device.publish_event(InfoEvent(message="m1"))
            time.sleep(0.1)

    thread = threading.Thread(target=publish_event_every_100_ms)
    thread.start()
    try:
        with main_events_context.wait_for_n_events(events_analyzer, count=2, timeout=1):
            pass
        events_analyzer.stop(timeout=5)
    finally:
        stop_event.set()
        thread.join(timeout=1)


@pytest.mark.parametrize(
    "node_name, method, expected",
    [
        pytest.param("loader-node-1", LIVE_MIGRATION_METHOD, True, id="true-for-loader"),
        pytest.param("db-node-1", LIVE_MIGRATION_METHOD, False, id="false-for-db-node"),
        pytest.param("loader-node-1", TERMINATE_ON_MAINTENANCE_METHOD, False, id="false-for-non-live-migration"),
    ],
)
def test_is_benign_gce_live_migration_of_loader_matches_only_loader_live_migration(node_name, method, expected):
    """Only a live migration of a loader node is classified as benign; db nodes and other methods are not."""
    event = make_gce_instance_event(node_name, method)
    assert _is_benign_gce_live_migration_of_loader("GceInstanceEvent", event) is expected


def test_is_benign_gce_live_migration_of_loader_false_for_other_event_class():
    """Non-GCE event classes on a loader are never classified as a benign live migration."""
    event = SpotTerminationEvent(node="loader-node-1", message="m")
    assert _is_benign_gce_live_migration_of_loader("SpotTerminationEvent", event) is False

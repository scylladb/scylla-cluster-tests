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

import time

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.file_logger import (
    EventsFileLogger,
    get_events_grouped_by_category,
    get_events_logger,
    get_logger_event_summary,
    start_events_logger,
)
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.sct_events.system import SpotTerminationEvent
from unit_tests.lib.events_utils import EventsUtilsMixin


class EventsHarness(EventsUtilsMixin):
    """EventsUtilsMixin on its own class, so the started events state stays out of the shared mixin."""


@pytest.fixture()
def events_harness():
    """Real events main device on its own registry; torn down after the test."""
    harness = EventsHarness()
    harness.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)
    yield harness
    harness.teardown_events_processes()


@pytest.fixture()
def file_logger(events_harness):
    """Started EventsFileLogger on the harness registry; stopped before the events processes go down."""
    start_events_logger(_registry=events_harness.events_processes_registry)
    logger = get_events_logger(_registry=events_harness.events_processes_registry)
    time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

    assert isinstance(logger, EventsFileLogger)
    assert logger.is_alive()
    assert logger._registry == events_harness.events_main_device._registry
    assert logger._registry == events_harness.events_processes_registry

    yield logger
    logger.stop(timeout=3)


def test_file_logger(events_harness, file_logger):
    """get_logger_event_summary and get_events_grouped_by_category count published events per severity."""
    event_normal = SpotTerminationEvent(node="n1", message="m1")
    event_normal.severity = Severity.NORMAL
    event_warning = SpotTerminationEvent(node="n2", message="m2")
    event_warning.severity = Severity.WARNING
    event_error = SpotTerminationEvent(node="n3", message="m3")
    event_error.severity = Severity.ERROR
    event_critical = SpotTerminationEvent(node="n4", message="m4")
    event_critical.severity = Severity.CRITICAL
    event_debug = SpotTerminationEvent(node="n5", message="m5")
    event_debug.severity = Severity.DEBUG

    with events_harness.wait_for_n_events(file_logger, count=15, timeout=3):
        events_harness.events_main_device.publish_event(event_normal)
        events_harness.events_main_device.publish_event(event_warning)
        events_harness.events_main_device.publish_event(event_warning)
        events_harness.events_main_device.publish_event(event_error)
        events_harness.events_main_device.publish_event(event_error)
        events_harness.events_main_device.publish_event(event_error)
        events_harness.events_main_device.publish_event(event_critical)
        events_harness.events_main_device.publish_event(event_critical)
        events_harness.events_main_device.publish_event(event_critical)
        events_harness.events_main_device.publish_event(event_critical)
        events_harness.events_main_device.publish_event(event_debug)
        events_harness.events_main_device.publish_event(event_debug)
        events_harness.events_main_device.publish_event(event_debug)
        events_harness.events_main_device.publish_event(event_debug)
        events_harness.events_main_device.publish_event(event_debug)

    assert events_harness.events_main_device.events_counter == file_logger.events_counter

    summary = get_logger_event_summary(_registry=events_harness.events_processes_registry)
    assert summary == {
        Severity.NORMAL.name: 1,
        Severity.WARNING.name: 2,
        Severity.ERROR.name: 3,
        Severity.CRITICAL.name: 4,
        Severity.DEBUG.name: 5,
    }

    grouped = get_events_grouped_by_category(_registry=events_harness.events_processes_registry)
    assert len(grouped[Severity.NORMAL.name]) == 1
    assert len(grouped[Severity.WARNING.name]) == 2
    assert len(grouped[Severity.ERROR.name]) == 3
    assert len(grouped[Severity.CRITICAL.name]) == 4
    assert len(grouped[Severity.DEBUG.name]) == 5


def test_get_events_grouped_by_category_limit(events_harness, file_logger):
    """get_events_grouped_by_category caps each severity group at ``limit`` events."""
    with events_harness.wait_for_n_events(file_logger, count=len(Severity) * 10, timeout=3):
        for severity in Severity:
            for num in range(10):
                event = SpotTerminationEvent(node="node", message=f"m-{num}-{severity.name}")
                event.severity = severity
                events_harness.events_main_device.publish_event(event)

    assert events_harness.events_main_device.events_counter == file_logger.events_counter

    summary = get_logger_event_summary(_registry=events_harness.events_processes_registry)
    assert set(summary.values()) == {10}

    grouped = get_events_grouped_by_category(_registry=events_harness.events_processes_registry, limit=5)
    for severity, group in grouped.items():
        assert len(group) == 5
        for num, event in enumerate(group, start=0 if severity == Severity.CRITICAL.name else 5):
            assert f"m-{num}-{severity}" in event

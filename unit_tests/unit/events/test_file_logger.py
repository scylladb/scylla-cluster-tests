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

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.sct_events.events_processes import EVENTS_FILE_LOGGER_ID
from sdcm.sct_events.file_logger import (
    start_events_logger,
    get_events_grouped_by_category,
    get_logger_event_summary,
)


@pytest.fixture
def file_logger(start_events_subscriber):
    """A running EventsFileLogger wired to the test's events registry."""
    return start_events_subscriber(start_events_logger, EVENTS_FILE_LOGGER_ID)


def test_file_logger_counts_and_groups_events_by_severity(main_events_context, file_logger) -> None:
    """The file logger counts and groups every published event by severity, which feeds the test result summary."""
    published_counts = {
        Severity.NORMAL: 1,
        Severity.WARNING: 2,
        Severity.ERROR: 3,
        Severity.CRITICAL: 4,
        Severity.DEBUG: 5,
    }

    with main_events_context.wait_for_n_events(file_logger, count=sum(published_counts.values()), timeout=3):
        for node_num, (severity, count) in enumerate(published_counts.items(), start=1):
            event = SpotTerminationEvent(node=f"n{node_num}", message=f"m{node_num}")
            event.severity = severity
            for _ in range(count):
                main_events_context.events_main_device.publish_event(event)

    assert main_events_context.events_main_device.events_counter == file_logger.events_counter

    summary = get_logger_event_summary(_registry=main_events_context.events_processes_registry)
    assert summary == {
        Severity.NORMAL.name: 1,
        Severity.WARNING.name: 2,
        Severity.ERROR.name: 3,
        Severity.CRITICAL.name: 4,
        Severity.DEBUG.name: 5,
    }

    grouped = get_events_grouped_by_category(_registry=main_events_context.events_processes_registry)
    assert len(grouped[Severity.NORMAL.name]) == 1
    assert len(grouped[Severity.WARNING.name]) == 2
    assert len(grouped[Severity.ERROR.name]) == 3
    assert len(grouped[Severity.CRITICAL.name]) == 4
    assert len(grouped[Severity.DEBUG.name]) == 5


def test_get_events_grouped_by_category_limit(main_events_context, file_logger) -> None:
    """The limit keeps the first CRITICAL events and the latest events of other severities, so reports stay short."""
    with main_events_context.wait_for_n_events(file_logger, count=len(Severity) * 10, timeout=3):
        for severity in Severity:
            for num in range(10):
                event = SpotTerminationEvent(node="node", message=f"m-{num}-{severity.name}")
                event.severity = severity
                main_events_context.events_main_device.publish_event(event)

    assert main_events_context.events_main_device.events_counter == file_logger.events_counter

    summary = get_logger_event_summary(_registry=main_events_context.events_processes_registry)
    assert set(summary.values()) == {10}

    grouped = get_events_grouped_by_category(_registry=main_events_context.events_processes_registry, limit=5)
    for severity, group in grouped.items():
        assert len(group) == 5
        # get_events_by_category() keeps the first `limit` CRITICAL events and the last `limit` of every
        # other severity, so out of the 10 events published above CRITICAL yields m-0..m-4 and the rest m-5..m-9.
        for num, event in enumerate(group, start=0 if severity == Severity.CRITICAL.name else 5):
            assert f"m-{num}-{severity}" in event

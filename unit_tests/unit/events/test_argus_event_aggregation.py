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

"""Tests for how events are keyed when the Argus aggregator dedups them."""

import pytest

from sdcm.sct_events.argus import ArgusEventAggregator, ArgusEventCollector
from sdcm.sct_events.base import SctEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.system import InfoEvent
from sdcm.utils.argus import Argus


@pytest.fixture
def make_log_event():
    """Build a DatabaseLogEvent the way the db log reader does: cloned from one template per event type.

    Clones keep the template's `event_id`, so every event of a type shares one id, as in production.
    """
    templates = {}

    def make(event_type, backtrace, line_number=1, node="node"):
        template = templates.setdefault(event_type, event_type())
        event = template.clone().add_info(
            node=node, line="seastar_memory - oversized allocation: 262144 bytes", line_number=line_number
        )
        event.raw_backtrace = backtrace
        return event

    return make


def drain(queue):
    """Return everything currently waiting in the queue."""
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items


def test_argus_aggregation_id_defaults_to_event_id():
    """Event types that do not override the aggregation id keep deduping on their `event_id`."""
    event = InfoEvent(message="info")

    assert event.argus_aggregation_id() == event.event_id


@pytest.mark.parametrize(
    "event_type",
    [
        DatabaseLogEvent.OVERSIZED_ALLOCATION,
        DatabaseLogEvent.TOPOLOGY_BARRIER_STALL,
    ],
)
def test_argus_aggregation_id_equal_backtraces_share_id_distinct_differ(event_type, make_log_event):
    """Events with the same backtrace must dedup in Argus, whichever node or line they came from."""
    first = make_log_event(event_type, "0x1\n0x2")
    same_trace = make_log_event(event_type, "0x1\n0x2", line_number=9)
    other_trace = make_log_event(event_type, "0x1\n0x4")

    assert first.argus_aggregation_id() == same_trace.argus_aggregation_id()
    assert first.argus_aggregation_id() != other_trace.argus_aggregation_id()


def test_argus_aggregation_id_decoded_backtrace_takes_precedence_over_raw(make_log_event):
    """Raw addresses can differ for one code path; the decoded frames are the stable identity."""
    first = make_log_event(DatabaseLogEvent.OVERSIZED_ALLOCATION, "0x1")
    second = make_log_event(DatabaseLogEvent.OVERSIZED_ALLOCATION, "0x2")
    first.backtrace = second.backtrace = "foo() at bar.cc:1"

    assert first.argus_aggregation_id() == second.argus_aggregation_id()


def test_argus_aggregation_id_without_backtrace_falls_back_to_event_id(make_log_event):
    """Events lacking any backtrace keep their own id rather than all hashing to one value."""
    event = make_log_event(DatabaseLogEvent.OVERSIZED_ALLOCATION, "")

    assert event.argus_aggregation_id() == event.event_id


@pytest.mark.parametrize(
    "event_type",
    [
        DatabaseLogEvent.OVERSIZED_ALLOCATION,
        DatabaseLogEvent.TOPOLOGY_BARRIER_STALL,
    ],
)
def test_argus_pipeline_posts_distinct_backtraces_and_collapses_identical_ones(
    event_type, events_function_scope, monkeypatch, make_log_event
):
    """Events of one type share an `event_id`; the pipeline must still post each distinct backtrace once.

    node-A and node-B hit the same backtrace, so only node-A is posted; node-C hits another one and is posted too.
    """
    monkeypatch.setattr(Argus, "INSTANCE", None)
    registry = SctEvent._events_processes_registry
    events = [
        make_log_event(event_type, "0x1\n0x2", node="node-A"),
        make_log_event(event_type, "0x1\n0x2", node="node-B"),
        make_log_event(event_type, "0x3", node="node-C"),
    ]
    collector = ArgusEventCollector(_registry=registry)
    monkeypatch.setattr(collector, "inbound_events", lambda: iter([(event_type.__name__, e) for e in events]))
    aggregator = ArgusEventAggregator(_registry=registry)

    collector.run()
    monkeypatch.setattr(aggregator, "inbound_events", lambda: iter(drain(collector.outbound_queue)))
    aggregator.run()

    assert [event["node"] for event in drain(aggregator.outbound_queue)] == ["node-A", "node-C"]

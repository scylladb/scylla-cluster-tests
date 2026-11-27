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

import ctypes
import multiprocessing
import threading

import pytest
import zmq

from sdcm.sct_events.events_device import EventsDevice, get_events_main_device, start_events_main_device
from sdcm.sct_events.events_processes import EventsProcessesRegistry
from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.wait import wait_for


@pytest.fixture
def events_processes_registry(tmp_path):
    return EventsProcessesRegistry(log_dir=str(tmp_path))


@pytest.fixture
def events_device(events_processes_registry):
    return EventsDevice(_registry=events_processes_registry)


def test_defaults(events_device, events_processes_registry):
    assert events_device.events_counter == 0
    assert events_device.events_log_base_dir == events_processes_registry.log_dir / "events_log"
    assert events_device.raw_events_log == events_device.events_log_base_dir / "raw_events.log"


def test_publish_subscribe(events_device):
    event1 = ClusterHealthValidatorEvent.NodeStatus()
    event2 = ClusterHealthValidatorEvent.NodePeersNulls()

    # Put events to the publish queue.
    events_device.publish_event(event1)
    events_device.publish_event(event2)

    stop_event = threading.Event()
    counter = multiprocessing.Value(ctypes.c_uint32, 0)

    threading.Timer(interval=1, function=stop_event.set).start()  # stop subscriber in 1 second.
    events_device.start_delay = 0.5
    events_device.start()

    try:
        events_generator = events_device.outbound_events(stop_event=stop_event, events_counter=counter)

        event1_class, event1_received = next(events_generator)
        assert event1_class == "ClusterHealthValidatorEvent"
        assert event1_received == event1

        event2_class, event2_received = next(events_generator)
        assert event2_class == "ClusterHealthValidatorEvent"
        assert event2_received == event2

        with pytest.raises(StopIteration):
            next(events_generator)
    finally:
        events_device.stop(timeout=5)

    assert events_device.events_counter == counter.value
    assert counter.value == 2


def test_start_get_events_main_device(events_processes_registry):
    assert get_events_main_device(_registry=events_processes_registry) is None
    start_events_main_device(_registry=events_processes_registry)
    events_device = get_events_main_device(_registry=events_processes_registry)
    wait_for(func=events_device.is_alive, timeout=5)
    try:
        assert isinstance(events_device, EventsDevice)
        assert events_device.events_counter == 0
        assert events_device.is_alive()
        assert events_device.subscribe_address
    finally:
        events_device.stop(timeout=5)


def test_event_is_counted_when_subscriber_asks_for_next_one(events_device):
    """The device counts an event once the subscriber asks for the next one, after it has processed the event."""
    events_device.publish_event(ClusterHealthValidatorEvent.NodeStatus())
    events_device.publish_event(ClusterHealthValidatorEvent.NodePeersNulls())

    stop_event = threading.Event()
    counter = multiprocessing.Value(ctypes.c_uint32, 0)
    events_device.start_delay = 0.5
    events_device.start()

    try:
        events_generator = events_device.outbound_events(stop_event=stop_event, events_counter=counter)
        next(events_generator)
        assert counter.value == 0
        next(events_generator)
        assert counter.value == 1
        stop_event.set()
        with pytest.raises(StopIteration):
            next(events_generator)
        assert counter.value == 2
    finally:
        events_device.stop(timeout=5)


def test_wait_for_subscribers_returns_once_subscribed(events_device):
    """wait_for_subscribers() returns once the device has received the subscription of a new SUB socket."""
    events_device.start()

    try:
        with zmq.Context() as ctx, ctx.socket(zmq.SUB) as sub:
            sub.connect(events_device.subscribe_address)
            sub.subscribe(b"")
            events_device.wait_for_subscribers(1, timeout=5)
            assert events_device.subscribers == 1
    finally:
        events_device.stop(timeout=5)


def test_wait_for_subscribers_warns_on_timeout(events_device, caplog):
    """wait_for_subscribers() logs a warning and returns when the subscribers do not come in time."""
    events_device.start()

    try:
        events_device.wait_for_subscribers(1, timeout=0.2)
        assert events_device.subscribers == 0
        assert "EventsDevice has 0 of 1 subscribers after 0.2s" in caplog.text
    finally:
        events_device.stop(timeout=5)

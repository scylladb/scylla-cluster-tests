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
import threading
import multiprocessing

import pytest

from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.events_device import EventsDevice, start_events_main_device, get_events_main_device
from sdcm.sct_events.events_processes import EventsProcessesRegistry
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
        events_device.stop(timeout=1)

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
        events_device.stop(timeout=1)

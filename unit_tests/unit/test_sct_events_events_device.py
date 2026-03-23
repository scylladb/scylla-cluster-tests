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
import shutil
import tempfile
import unittest
import threading
import multiprocessing

from sdcm.sct_events.health import ClusterHealthValidatorEvent
from sdcm.sct_events.events_device import EventsDevice, start_events_main_device, get_events_main_device
from sdcm.sct_events.events_processes import EventsProcessesRegistry
from sdcm.wait import wait_for


class TestEventsDevice(unittest.TestCase):
    temp_dir = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = tempfile.mkdtemp()
        cls.events_processes_registry = EventsProcessesRegistry(log_dir=cls.temp_dir)

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.events_device = EventsDevice(_registry=self.events_processes_registry)

    def test_defaults(self):
        self.assertEqual(self.events_device.events_counter, 0)
        self.assertEqual(self.events_device.events_log_base_dir, self.events_processes_registry.log_dir / "events_log")
        self.assertEqual(self.events_device.raw_events_log, self.events_device.events_log_base_dir / "raw_events.log")

    def test_publish_subscribe(self):
        event1 = ClusterHealthValidatorEvent.NodeStatus()
        event2 = ClusterHealthValidatorEvent.NodePeersNulls()

        # Put events to the publish queue.
        self.events_device.publish_event(event1)
        self.events_device.publish_event(event2)

        stop_event = threading.Event()
        counter = multiprocessing.Value(ctypes.c_uint32, 0)

        threading.Timer(interval=1, function=stop_event.set).start()  # stop subscriber in 1 second.
        self.events_device.start_delay = 0.5
        self.events_device.start()

        try:
            events_generator = self.events_device.outbound_events(stop_event=stop_event, events_counter=counter)

            event1_class, event1_received = next(events_generator)
            self.assertEqual(event1_class, "ClusterHealthValidatorEvent")
            self.assertEqual(event1_received, event1)

            event2_class, event2_received = next(events_generator)
            self.assertEqual(event2_class, "ClusterHealthValidatorEvent")
            self.assertEqual(event2_received, event2)

            self.assertRaises(StopIteration, next, events_generator)
        finally:
            self.events_device.stop(timeout=1)

        self.assertEqual(self.events_device.events_counter, counter.value)
        self.assertEqual(counter.value, 2)

    def test_start_get_events_main_device(self):
        self.assertIsNone(get_events_main_device(_registry=self.events_processes_registry))
        start_events_main_device(_registry=self.events_processes_registry)
        events_device = get_events_main_device(_registry=self.events_processes_registry)
        wait_for(func=events_device.is_alive, timeout=5)
        try:
            self.assertIsInstance(events_device, EventsDevice)
            self.assertEqual(events_device.events_counter, 0)
            self.assertTrue(events_device.is_alive())
            self.assertTrue(events_device.subscribe_address)
        finally:
            events_device.stop(timeout=1)

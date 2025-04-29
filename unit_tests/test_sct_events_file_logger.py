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
import unittest

from sdcm.sct_events import Severity
from sdcm.sct_events.system import SpotTerminationEvent
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.sct_events.file_logger import \
    EventsFileLogger, start_events_logger, get_events_logger, get_events_grouped_by_category, get_logger_event_summary

from unit_tests.lib.events_utils import EventsUtilsMixin


class TestFileLogger(unittest.TestCase, EventsUtilsMixin):

    def setUp(self) -> None:
        self.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)
        start_events_logger(_registry=self.events_processes_registry)
        self.file_logger = get_events_logger(_registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        self.assertIsInstance(self.file_logger, EventsFileLogger)
        self.assertTrue(self.file_logger.is_alive())
        self.assertEqual(self.file_logger._registry, self.events_main_device._registry)
        self.assertEqual(self.file_logger._registry, self.events_processes_registry)

    def tearDown(self) -> None:
        self.file_logger.stop(timeout=3)
        self.teardown_events_processes()

    def test_file_logger(self) -> None:
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

        with self.wait_for_n_events(self.file_logger, count=15, timeout=3):
            self.events_main_device.publish_event(event_normal)
            self.events_main_device.publish_event(event_warning)
            self.events_main_device.publish_event(event_warning)
            self.events_main_device.publish_event(event_error)
            self.events_main_device.publish_event(event_error)
            self.events_main_device.publish_event(event_error)
            self.events_main_device.publish_event(event_critical)
            self.events_main_device.publish_event(event_critical)
            self.events_main_device.publish_event(event_critical)
            self.events_main_device.publish_event(event_critical)
            self.events_main_device.publish_event(event_debug)
            self.events_main_device.publish_event(event_debug)
            self.events_main_device.publish_event(event_debug)
            self.events_main_device.publish_event(event_debug)
            self.events_main_device.publish_event(event_debug)

        self.assertEqual(self.events_main_device.events_counter, self.file_logger.events_counter)

        summary = get_logger_event_summary(_registry=self.events_processes_registry)
        self.assertDictEqual(summary, {Severity.NORMAL.name: 1,
                                       Severity.WARNING.name: 2,
                                       Severity.ERROR.name: 3,
                                       Severity.CRITICAL.name: 4,
                                       Severity.DEBUG.name: 5, })

        grouped = get_events_grouped_by_category(_registry=self.events_processes_registry)
        self.assertEqual(len(grouped[Severity.NORMAL.name]), 1)
        self.assertEqual(len(grouped[Severity.WARNING.name]), 2)
        self.assertEqual(len(grouped[Severity.ERROR.name]), 3)
        self.assertEqual(len(grouped[Severity.CRITICAL.name]), 4)
        self.assertEqual(len(grouped[Severity.DEBUG.name]), 5)

    def test_get_events_grouped_by_category_limit(self) -> None:
        with self.wait_for_n_events(self.file_logger, count=len(Severity) * 10, timeout=3):
            for severity in Severity:
                for num in range(10):
                    event = SpotTerminationEvent(node="node", message=f"m-{num}-{severity.name}")
                    event.severity = severity
                    self.events_main_device.publish_event(event)

        self.assertEqual(self.events_main_device.events_counter, self.file_logger.events_counter)

        summary = get_logger_event_summary(_registry=self.events_processes_registry)
        self.assertEqual(set(summary.values()), {10})

        grouped = get_events_grouped_by_category(_registry=self.events_processes_registry, limit=5)
        for severity, group in grouped.items():
            self.assertEqual(len(group), 5)
            for num, event in enumerate(group, start=0 if severity == Severity.CRITICAL.name else 5):
                self.assertIn(f"m-{num}-{severity}", event)

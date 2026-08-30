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

from sdcm.sct_events import Severity
from sdcm.sct_events.gce_events import GceInstanceEvent
from sdcm.sct_events.system import InfoEvent, SpotTerminationEvent
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.sct_events.events_analyzer import (
    EventsAnalyzer,
    _is_benign_gce_live_migration_of_loader,
    start_events_analyzer,
)
from sdcm.sct_events.events_processes import EVENTS_ANALYZER_ID, get_events_process

from unit_tests.lib.events_utils import EventsUtilsMixin


def _gce_instance_event(node_name: str, method: str, severity: Severity = Severity.CRITICAL) -> GceInstanceEvent:
    gce_log_entry = {
        "timestamp": "2026-08-23T00:00:00.000000+00:00",
        "protoPayload": {
            "resourceName": f"projects/p/zones/z/instances/{node_name}",
            "methodName": method,
            "status": {"message": "host maintenance"},
        },
    }
    return GceInstanceEvent(gce_log_entry, severity=severity)


class TestEventsAnalyzer(EventsUtilsMixin):
    @classmethod
    def setup_class(cls) -> None:
        cls.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)

    @classmethod
    def teardown_class(cls) -> None:
        cls.teardown_events_processes()

    def test_events_analyzer(self):
        initial_events_no = self.events_main_device.events_counter  # coming from other tests
        start_events_analyzer(_registry=self.events_processes_registry)
        events_analyzer = get_events_process(name=EVENTS_ANALYZER_ID, _registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            assert isinstance(events_analyzer, EventsAnalyzer)
            assert events_analyzer.is_alive()
            assert events_analyzer._registry == self.events_main_device._registry
            assert events_analyzer._registry == self.events_processes_registry

            event1 = InfoEvent(message="m1")
            event2 = SpotTerminationEvent(node="n1", message="m2")

            with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
                with self.wait_for_n_events(events_analyzer, count=2, timeout=1):
                    self.events_main_device.publish_event(event1)
                    self.events_main_device.publish_event(event2)

            assert self.events_main_device.events_counter == initial_events_no + events_analyzer.events_counter

            mock.assert_called_once()
        finally:
            events_analyzer.stop(timeout=1)

    def test_kill_test_not_called_for_gce_live_migration_of_loader(self):
        """A GCE host-maintenance live migration of a loader node is a known-benign, transient
        cloud event and must not abort the test."""
        start_events_analyzer(_registry=self.events_processes_registry)
        events_analyzer = get_events_process(name=EVENTS_ANALYZER_ID, _registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            event = _gce_instance_event("loader-node-1", "compute.instances.migrateOnHostMaintenance")

            with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
                with self.wait_for_n_events(events_analyzer, count=1, timeout=1):
                    self.events_main_device.publish_event(event)

                mock.assert_not_called()
        finally:
            events_analyzer.stop(timeout=1)

    def test_kill_test_called_for_gce_live_migration_of_db_node(self):
        """The same live-migration event on a db node must still abort the test: the benign-event
        exemption is specific to loader nodes only."""
        start_events_analyzer(_registry=self.events_processes_registry)
        events_analyzer = get_events_process(name=EVENTS_ANALYZER_ID, _registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            event = _gce_instance_event("db-node-1", "compute.instances.migrateOnHostMaintenance")

            with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
                with self.wait_for_n_events(events_analyzer, count=1, timeout=1):
                    self.events_main_device.publish_event(event)

                mock.assert_called_once()
        finally:
            events_analyzer.stop(timeout=1)

    def test_kill_test_called_for_non_live_migration_gce_event_on_loader(self):
        """A CRITICAL GceInstanceEvent on a loader node that is NOT a live migration (e.g. a
        host-maintenance terminate) must still abort the test: the exemption is specific to the
        live-migration scenario, not "any GceInstanceEvent on a loader"."""
        start_events_analyzer(_registry=self.events_processes_registry)
        events_analyzer = get_events_process(name=EVENTS_ANALYZER_ID, _registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            event = _gce_instance_event("loader-node-1", "compute.instances.terminateOnHostMaintenance")

            with unittest.mock.patch("sdcm.sct_events.events_analyzer.EventsAnalyzer.kill_test") as mock:
                with self.wait_for_n_events(events_analyzer, count=1, timeout=1):
                    self.events_main_device.publish_event(event)

                mock.assert_called_once()
        finally:
            events_analyzer.stop(timeout=1)

    def test_can_stop_events_analyzer_during_stream_of_events(self):
        start_events_analyzer(_registry=self.events_processes_registry)
        events_analyzer = get_events_process(name=EVENTS_ANALYZER_ID, _registry=self.events_processes_registry)

        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)
        stop_event = threading.Event()

        def publish_event_every_100_ms():
            while not stop_event.is_set():
                event3 = InfoEvent(message="m1")
                self.events_main_device.publish_event(event3)
                time.sleep(0.1)

        thread = threading.Thread(target=publish_event_every_100_ms)
        thread.start()
        try:
            with self.wait_for_n_events(events_analyzer, count=2, timeout=1):
                # make sure that events_analyzer is alive and processing events
                pass
            events_analyzer.stop(timeout=5)
        finally:
            stop_event.set()
            thread.join(timeout=1)


def test_is_benign_gce_live_migration_of_loader_true_for_loader():
    event = _gce_instance_event("loader-node-1", "compute.instances.migrateOnHostMaintenance")
    assert _is_benign_gce_live_migration_of_loader("GceInstanceEvent", event) is True


def test_is_benign_gce_live_migration_of_loader_false_for_db_node():
    event = _gce_instance_event("db-node-1", "compute.instances.migrateOnHostMaintenance")
    assert _is_benign_gce_live_migration_of_loader("GceInstanceEvent", event) is False


def test_is_benign_gce_live_migration_of_loader_false_for_non_live_migration_event():
    event = _gce_instance_event("loader-node-1", "compute.instances.terminateOnHostMaintenance")
    assert _is_benign_gce_live_migration_of_loader("GceInstanceEvent", event) is False


def test_is_benign_gce_live_migration_of_loader_false_for_other_event_class():
    event = SpotTerminationEvent(node="loader-node-1", message="m")
    assert _is_benign_gce_live_migration_of_loader("SpotTerminationEvent", event) is False

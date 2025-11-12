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
import os

import pickle
import unittest
from textwrap import dedent

from sdcm.sct_events import Severity
from sdcm.sct_events.system import (
    StartupTestEvent,
    TestFrameworkEvent,
    ElasticsearchEvent,
    SpotTerminationEvent,
    ScyllaRepoEvent,
    InfoEvent,
    ThreadFailedEvent,
    CoreDumpEvent,
    TestResultEvent,
    InstanceStatusEvent,
    INSTANCE_STATUS_EVENTS_PATTERNS,
)


class TestSystemEvents(unittest.TestCase):
    def test_startup_test_event(self):
        event = StartupTestEvent()
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(StartupTestEvent Severity.NORMAL) period_type=not-set event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_test_framework_event(self):
        event = TestFrameworkEvent(
            source="s1",
            source_method="m1",
            args=(
                "a1",
                "a2",
            ),
            kwargs={
                "k1": "v1",
                "k2": "v2",
            },
            message="msg1",
            exception="e1",
        )
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(TestFrameworkEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e, source=s1.m1(args=('a1', 'a2'), "
            "kwargs={'k1': 'v1', 'k2': 'v2'})"
            " message=msg1\nexception=e1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_elasticsearch_event(self):
        event = ElasticsearchEvent(doc_id="d1", error="e1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(ElasticsearchEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: doc_id=d1 error=e1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_spot_termination_event(self):
        event = SpotTerminationEvent(node="node1", message="m1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(SpotTerminationEvent Severity.CRITICAL) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: node=node1 message=m1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_scylla_repo_event(self):
        event = ScyllaRepoEvent(url="u1", error="e1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(ScyllaRepoEvent Severity.WARNING) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: url=u1 error=e1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_info_event(self):
        event = InfoEvent(message="m1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(InfoEvent Severity.NORMAL) period_type=not-set event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: message=m1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_thread_failed_event(self):
        event = ThreadFailedEvent(message="m1", traceback="t1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(ThreadFailedEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e: message=m1\nt1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_coredump_event(self):
        event = CoreDumpEvent(node="node1", corefile_url="url1", backtrace="b1", download_instructions="d1")
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            "(CoreDumpEvent Severity.ERROR) period_type=one-time "
            "event_id=aff29bce-d75c-4f86-9890-c6d9c1c25d3e node=node1\ncorefile_url=url1\nbacktrace=b1\n"
            "Info about modules can be found in SCT logs by search for 'Coredump Modules info'\n"
            "download_instructions:\nd1\n",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_test_result_event_fail(self):
        event = TestResultEvent(
            test_status="FAILED",
            events={
                "g1": [
                    "e1",
                    "e2",
                ],
                "g2": [
                    "e3",
                ],
            },
        )
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            dedent("""\
                ================================= TEST RESULTS =================================

                ----- LAST g1 EVENT ----------------------------------------------------------
                e1
                e2
                ----- LAST g2 EVENT ----------------------------------------------------------
                e3
                ================================================================================
                FAILED :(
            """),
        )
        loaded_event = pickle.loads(pickle.dumps(event))
        loaded_event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(event, loaded_event)

    def test_test_result_event_ok(self):
        event = TestResultEvent(
            test_status="SUCCESS",
            events={
                "g1": [
                    "e1",
                    "e2",
                ],
                "g2": [
                    "e3",
                ],
            },
        )
        event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(
            str(event),
            dedent("""\
                ================================= TEST RESULTS =================================
                ================================================================================
                SUCCESS :)
            """),
        )
        loaded_event = pickle.loads(pickle.dumps(event))
        loaded_event.event_id = "aff29bce-d75c-4f86-9890-c6d9c1c25d3e"
        self.assertEqual(event, loaded_event)


class TestInstanceStatusEvent(unittest.TestCase):
    def test_known_system_status_events(self):
        self.assertTrue(issubclass(InstanceStatusEvent.STARTUP, InstanceStatusEvent))
        self.assertTrue(issubclass(InstanceStatusEvent.REBOOT, InstanceStatusEvent))
        self.assertTrue(issubclass(InstanceStatusEvent.POWER_OFF, InstanceStatusEvent))

    def test_instance_startup_event(self):
        event1 = InstanceStatusEvent.STARTUP()
        self.assertEqual(event1.severity, Severity.WARNING)

        self.assertIs(event1, event1.add_info(node="n1", line="kernel: Linux version", line_number=0))
        self.assertEqual(event1.severity, Severity.WARNING)
        self.assertEqual(event1.node, "n1")
        self.assertEqual(event1.line, "kernel: Linux version")
        self.assertEqual(event1.line_number, 0)

    def test_instance_reboot_event(self):
        event1 = InstanceStatusEvent.REBOOT()
        self.assertEqual(event1.severity, Severity.WARNING)

        self.assertIs(
            event1, event1.add_info(node="n1", line="Stopped target Host and Network Name Lookups", line_number=0)
        )
        self.assertEqual(event1.severity, Severity.WARNING)
        self.assertEqual(event1.node, "n1")
        self.assertEqual(event1.line, "Stopped target Host and Network Name Lookups")
        self.assertEqual(event1.line_number, 0)

    def test_instance_poweroff_event(self):
        event1 = InstanceStatusEvent.POWER_OFF()
        self.assertEqual(event1.severity, Severity.WARNING)
        self.assertIs(event1, event1.add_info(node="n1", line="Powering Off", line_number=0))
        self.assertEqual(event1.severity, Severity.WARNING)
        self.assertEqual(event1.node, "n1")
        self.assertEqual(event1.line, "Powering Off")
        self.assertEqual(event1.line_number, 0)

    def test_instance_status_events_patterns(self):
        cloned_events = []
        with open(
            os.path.join(os.path.dirname(__file__), "test_data/system_status_events.log"), encoding="utf-8"
        ) as sct_log:
            for index, line in enumerate(sct_log.readlines()):
                for pattern, event in INSTANCE_STATUS_EVENTS_PATTERNS:
                    match = pattern.search(line)
                    if match:
                        cloned_events.append(event.clone().add_info(node=self, line_number=index, line=line))
                        break

        self.assertEqual(len(cloned_events), 3)

        self.assertEqual(cloned_events[0].type, "STARTUP")
        self.assertEqual(cloned_events[0].regex, "kernel: Linux version")
        self.assertEqual(cloned_events[0].severity, Severity.WARNING)

        self.assertEqual(cloned_events[1].type, "REBOOT")
        self.assertEqual(cloned_events[1].regex, "Stopped target Host and Network Name Lookups")
        self.assertEqual(cloned_events[1].severity, Severity.WARNING)

        self.assertEqual(cloned_events[2].type, "POWER_OFF")
        self.assertEqual(cloned_events[2].regex, "Reached target Power-Off")
        self.assertEqual(cloned_events[2].severity, Severity.WARNING)

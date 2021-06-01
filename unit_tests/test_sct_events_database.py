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

import unittest

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.database import \
    DatabaseLogEvent, FullScanEvent, IndexSpecialColumnErrorEvent, TOLERABLE_REACTOR_STALL, SYSTEM_ERROR_EVENTS


class TestDatabaseLogEvent(unittest.TestCase):
    def test_known_system_errors(self):
        self.assertTrue(issubclass(DatabaseLogEvent.NO_SPACE_ERROR, DatabaseLogEvent))
        self.assertTrue(issubclass(DatabaseLogEvent.UNKNOWN_VERB, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.CLIENT_DISCONNECT, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.SEMAPHORE_TIME_OUT, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.SYSTEM_PAXOS_TIMEOUT, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.RESTARTED_DUE_TO_TIME_OUT, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.EMPTY_NESTED_EXCEPTION, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.DATABASE_ERROR, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.BAD_ALLOC, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.SCHEMA_FAILURE, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.RUNTIME_ERROR, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.FILESYSTEM_ERROR, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.STACKTRACE, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.BACKTRACE, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.ABORTING_ON_SHARD, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.SEGMENTATION, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.INTEGRITY_CHECK, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.REACTOR_STALLED, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.BOOT, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.STOP, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.SUPPRESSED_MESSAGES, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.stream_exception, DatabaseLogEvent)),
        self.assertTrue(issubclass(DatabaseLogEvent.POWER_OFF, DatabaseLogEvent)),

    def test_reactor_stalled_severity(self):
        event1 = DatabaseLogEvent.REACTOR_STALLED()
        self.assertEqual(event1.severity, Severity.DEBUG)

        self.assertIs(event1, event1.add_info(node="n1", line=f"{TOLERABLE_REACTOR_STALL-1} ms", line_number=1))
        self.assertEqual(event1.severity, Severity.DEBUG)
        self.assertEqual(event1.node, "n1")
        self.assertEqual(event1.line, f"{TOLERABLE_REACTOR_STALL-1} ms")
        self.assertEqual(event1.line_number, 1)

        event2 = DatabaseLogEvent.REACTOR_STALLED()
        self.assertEqual(event2.severity, Severity.DEBUG)
        self.assertIs(event2, event2.add_info(node="n2", line=f"{TOLERABLE_REACTOR_STALL} ms", line_number=2))
        self.assertEqual(event2.severity, Severity.ERROR)
        self.assertEqual(event2.node, "n2")
        self.assertEqual(event2.line, f"{TOLERABLE_REACTOR_STALL} ms")
        self.assertEqual(event2.line_number, 2)

    def test_system_error_events_list(self):
        self.assertSetEqual(set(dir(DatabaseLogEvent)) - set(dir(LogEvent)),
                            {ev.type for ev in SYSTEM_ERROR_EVENTS})


class TestFullScanEvent(unittest.TestCase):
    def test_no_message(self):
        event = FullScanEvent.start(db_node_ip="127.0.0.1", ks_cf="ks")
        self.assertFalse(hasattr(event, "message"))
        event.event_id = "743c4ad7-7d83-4b07-9602-120bb6c98fd6"
        self.assertEqual(str(event),
                         "(FullScanEvent Severity.NORMAL) period_type=not-set "
                         "event_id=743c4ad7-7d83-4b07-9602-120bb6c98fd6: type=start select_from=ks on "
                         "db_node=127.0.0.1")

    def test_with_message(self):
        event = FullScanEvent.finish(db_node_ip="127.0.0.1", ks_cf="ks", message="m1")
        self.assertEqual(event.message, "m1")
        event.event_id = "743c4ad7-7d83-4b07-9602-120bb6c98fd6"
        self.assertEqual(
            str(event),
            "(FullScanEvent Severity.NORMAL) period_type=not-set event_id=743c4ad7-7d83-4b07-9602-120bb6c98fd6: "
            "type=finish select_from=ks on db_node=127.0.0.1 message=m1"
        )


class TestIndexSpecialColumnErrorEvent(unittest.TestCase):
    def test_msgfmt(self):
        event = IndexSpecialColumnErrorEvent(message="m1")
        event.event_id = "ac449879-485a-4b06-8596-3fbe58881093"
        self.assertEqual(str(event),
                         "(IndexSpecialColumnErrorEvent Severity.ERROR) period_type=not-set "
                         "event_id=ac449879-485a-4b06-8596-3fbe58881093: message=m1")

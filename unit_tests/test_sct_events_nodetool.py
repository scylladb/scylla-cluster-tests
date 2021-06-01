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
from sdcm.sct_events.nodetool import NodetoolEvent


class TestNodetoolEvent(unittest.TestCase):

    def test_nodetool_cmd_no_options(self):
        event = NodetoolEvent(type="scrub --skip-corrupted drop_table_during_repair_ks_0",
                              subtype='start',
                              severity=Severity.NORMAL,
                              node='1.0.0.121',
                              options="")
        event.event_id = "3c8e2362-a987-4eff-953f-9cd1ad2017e5"
        self.assertEqual(
            str(event),
            "(NodetoolEvent Severity.NORMAL) period_type=not-set event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5: "
            "type=scrub subtype=start node=1.0.0.121 options=--skip-corrupted "
            "drop_table_during_repair_ks_0"
        )

        event = NodetoolEvent(type="scrub --skip-corrupted drop_table_during_repair_ks_0",
                              subtype='end',
                              severity=Severity.NORMAL,
                              node='1.0.0.121',
                              duration="20s",
                              options="")
        event.event_id = "3c8e2362-a987-4eff-953f-9cd1ad2017e5"
        self.assertEqual(
            str(event),
            '(NodetoolEvent Severity.NORMAL) period_type=not-set event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5: '
            'type=scrub subtype=end node=1.0.0.121 '
            'options=--skip-corrupted drop_table_during_repair_ks_0 duration=20s'
        )

    def test_nodetool_cmd_with_options(self):
        event = NodetoolEvent(type="scrub --skip-corrupted drop_table_during_repair_ks_0",
                              subtype='start',
                              severity=Severity.NORMAL,
                              node='1.0.0.121',
                              options="more options")
        event.event_id = "ef4aeb1a-c004-40e4-af14-9d87a0526408"
        self.assertEqual(
            str(event),
            '(NodetoolEvent Severity.NORMAL) period_type=not-set event_id=ef4aeb1a-c004-40e4-af14-9d87a0526408: '
            'type=scrub subtype=start node=1.0.0.121 options=--skip-corrupted '
            'drop_table_during_repair_ks_0 more options'
        )

    def test_nodetool_failure(self):
        event = NodetoolEvent(type="scrub --skip-corrupted drop_table_during_repair_ks_0",
                              subtype='end',
                              severity=Severity.ERROR,
                              node='1.0.0.121',
                              error="Failed with status 1",
                              full_traceback="Traceback:")
        event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        self.assertEqual(
            str(event),
            '(NodetoolEvent Severity.ERROR) period_type=not-set event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318: '
            'type=scrub subtype=end node=1.0.0.121 options=--skip-corrupted '
            'drop_table_during_repair_ks_0 error=Failed with status 1\nTraceback:'
        )

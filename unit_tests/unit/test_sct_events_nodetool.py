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
from unittest import mock

from sdcm.sct_events.nodetool import NodetoolEvent


class TestNodetoolEvent(unittest.TestCase):
    def test_nodetool_cmd_no_options(self):
        event = NodetoolEvent(nodetool_command="scrub", node="1.0.0.121", options="", publish_event=False)
        event.event_id = "3c8e2362-a987-4eff-953f-9cd1ad2017e5"
        event.begin_event()
        self.assertEqual(
            str(event),
            "(NodetoolEvent Severity.NORMAL) period_type=begin event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5: "
            "nodetool_command=scrub node=1.0.0.121",
        )

        event.duration = 20.325
        event.end_event()
        self.assertEqual(
            str(event),
            "(NodetoolEvent Severity.NORMAL) period_type=end event_id=3c8e2362-a987-4eff-953f-9cd1ad2017e5 "
            "duration=20.325s: nodetool_command=scrub node=1.0.0.121",
        )

    def test_nodetool_cmd_with_options(self):
        begin_event_timestamp = 1623596860.1202102
        event = NodetoolEvent(
            nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
            node="1.0.0.121",
            options="more options",
            publish_event=False,
        )
        event.event_id = "ef4aeb1a-c004-40e4-af14-9d87a0526408"
        event.begin_event()
        event.begin_timestamp = event.event_timestamp = begin_event_timestamp
        self.assertEqual(
            str(event),
            "(NodetoolEvent Severity.NORMAL) period_type=begin event_id=ef4aeb1a-c004-40e4-af14-9d87a0526408: "
            "nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted drop_table_during_repair_ks_0 more options",
        )

        # validate duration calculation
        with mock.patch("time.time", return_value=begin_event_timestamp + 1):
            event.end_event()
            self.assertEqual(
                str(event),
                "(NodetoolEvent Severity.NORMAL) period_type=end event_id=ef4aeb1a-c004-40e4-af14-9d87a0526408 "
                "duration=1s: nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted "
                "drop_table_during_repair_ks_0 more options",
            )

    def test_nodetool_failure(self):
        event = NodetoolEvent(
            nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
            node="1.0.0.121",
            options="more options",
            publish_event=False,
        )
        event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        event.begin_event()

        event.add_error(["Failed with status 1"])
        event.full_traceback = "Traceback:"
        event.duration = 90358
        event.end_event()
        self.assertEqual(
            str(event),
            "(NodetoolEvent Severity.NORMAL) period_type=end event_id=c2561d8b-97ca-44fb-b5b1-8bcc0d437318 "
            "duration=1d1h5m58s: nodetool_command=scrub node=1.0.0.121 options=--skip-corrupted "
            "drop_table_during_repair_ks_0 more "
            "options errors=['Failed with status 1']\nTraceback:",
        )

    def test_nodetool_serialization_to_json(self):
        event = NodetoolEvent(
            nodetool_command="scrub --skip-corrupted drop_table_during_repair_ks_0",
            node="1.0.0.121",
            options="more options",
            publish_event=False,
        )
        event.event_id = "c2561d8b-97ca-44fb-b5b1-8bcc0d437318"
        event.begin_event()
        self.assertTrue(event.to_json())

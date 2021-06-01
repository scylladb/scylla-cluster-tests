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

import pickle
import re
import unittest

from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.operator import ScyllaOperatorLogEvent


class TestOperatorEvents(unittest.TestCase):
    def test_scylla_operator_log_event(self):
        event = ScyllaOperatorLogEvent(
            namespace="scylla", cluster="c1", message="m1", error="e1", trace_id="tid1")
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        self.assertEqual(
            str(event),
            "(ScyllaOperatorLogEvent Severity.ERROR) period_type=one-time "
            "event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: line_number=0 tid1 scylla/c1: m1, e1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        self.assertTrue(hasattr(event, "TLS_HANDSHAKE_ERROR"))
        self.assertTrue(hasattr(event, "OPERATOR_STARTED_INFO"))

    def test_scylla_operator_log_event_tls_handshake_error(self):
        log_record = "2020/12/22 10:32:23 http: TLS handshake error from 172.17.0.1:50882: EOF"
        event = ScyllaOperatorLogEvent.TLS_HANDSHAKE_ERROR()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        self.assertEqual(
            "(ScyllaOperatorLogEvent Severity.WARNING) period_type=one-time "
            "event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: "
            "type=TLS_HANDSHAKE_ERROR regex=TLS handshake error from .*: EOF "
            f"line_number=0 node=N/A\n{log_record} None None: None, None",
            str(event),
        )

    def test_scylla_operator_log_event_operator_started_info(self):
        log_record = (
            """{"L":"INFO","T":"2021-04-16T11:55:44.857Z","""
            """"M":"Starting the operator...","_trace_id":"37Dr3r67TNudJCiA3UL9ww"}"""
        )
        event = ScyllaOperatorLogEvent.OPERATOR_STARTED_INFO()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        self.assertEqual(
            "(ScyllaOperatorLogEvent Severity.NORMAL) period_type=one-time "
            "event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: "
            "type=OPERATOR_STARTED_INFO regex=Starting the operator... "
            f"line_number=0 node=N/A\n{log_record} None None: None, None",
            str(event),
        )

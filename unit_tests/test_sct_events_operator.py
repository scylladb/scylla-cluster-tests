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
        event = ScyllaOperatorLogEvent(message="tid1 scylla/c1: m1, e1")
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        self.assertEqual(
            '(ScyllaOperatorLogEvent Severity.NORMAL) period_type=one-time '
            'event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9::  tid1 scylla/c1: m1, e1',
            str(event)
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        self.assertTrue(hasattr(event, "TLS_HANDSHAKE_ERROR"))
        self.assertTrue(hasattr(event, "OPERATOR_STARTED_INFO"))

    def test_scylla_operator_log_event_tls_handshake_error(self):
        log_record = "I0628 15:53:02.269804       1 operator/operator.go:133] http: " \
                     "TLS handshake error from 172.17.0.1:50882: EOF"
        event = ScyllaOperatorLogEvent.TLS_HANDSHAKE_ERROR()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        assert event.timestamp == 1624895582.269804
        self.assertEqual(
            '(ScyllaOperatorLogEvent Severity.WARNING) period_type=one-time '
            'event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: '
            'type=TLS_HANDSHAKE_ERROR regex=TLS handshake error from .*:'
            ' operator/operator.go:133] http: TLS handshake error from 172.17.0.1:50882: EOF',
            str(event),
        )

    def test_scylla_operator_log_event_operator_started_info(self):
        log_record = (
            "I0628 16:07:43.572294       1 scyllacluster/controller.go:203] \"Starting controller\" controller=\"ScyllaCluster\""
        )
        event = ScyllaOperatorLogEvent.OPERATOR_STARTED_INFO()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        assert event.timestamp == 1624896463.572294
        self.assertEqual(
            '(ScyllaOperatorLogEvent Severity.NORMAL) period_type=one-time '
            'event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: type=OPERATOR_STARTED_INFO '
            'regex="Starting controller" controller="ScyllaCluster": scyllacluster/controller.go:203] '
            '"Starting controller" controller="ScyllaCluster"',
            str(event),
        )

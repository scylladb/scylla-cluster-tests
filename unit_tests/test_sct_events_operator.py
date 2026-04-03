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
import datetime

from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.operator import ScyllaOperatorLogEvent


class TestOperatorEvents(unittest.TestCase):
    maxDiff = None

    def test_scylla_operator_log_event_tls_handshake_error(self):
        log_record = (
            "I0628 15:53:02.269804       1 operator/operator.go:133] http: "
            "TLS handshake error from 172.17.0.1:50882: EOF"
        )
        event = ScyllaOperatorLogEvent.TLS_HANDSHAKE_ERROR()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        expected_timestamp = datetime.datetime(
            year=datetime.datetime.now().year,
            month=6,
            day=28,
            hour=15,
            minute=53,
            second=2,
            microsecond=269804,
            tzinfo=datetime.timezone.utc,
        ).timestamp()
        assert event.timestamp == expected_timestamp
        self.assertEqual(
            "(ScyllaOperatorLogEvent Severity.WARNING) period_type=one-time "
            "event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: type=TLS_HANDSHAKE_ERROR regex=TLS handshake error "
            "from .* node=N/A\n"
            "I0628 15:53:02.269804       1 operator/operator.go:133] http: TLS handshake error from 172.17.0.1:50882: "
            "EOF",
            str(event),
        )

    def test_scylla_operator_log_event_operator_started_info(self):
        log_record = 'I0628 16:07:43.572294       1 scyllacluster/controller.go:203] "Starting controller" controller="ScyllaCluster"'
        event = ScyllaOperatorLogEvent.OPERATOR_STARTED_INFO()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        expected_timestamp = datetime.datetime(
            year=datetime.datetime.now().year,
            month=6,
            day=28,
            hour=16,
            minute=7,
            second=43,
            microsecond=572294,
            tzinfo=datetime.timezone.utc,
        ).timestamp()
        assert event.timestamp == expected_timestamp
        self.assertEqual(
            "(ScyllaOperatorLogEvent Severity.NORMAL) period_type=one-time "
            'event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: type=OPERATOR_STARTED_INFO regex="Starting controller" '
            'controller="ScyllaCluster" node=N/A\n'
            'I0628 16:07:43.572294       1 scyllacluster/controller.go:203] "Starting controller" '
            'controller="ScyllaCluster"',
            str(event),
        )

    def test_scylla_operator_log_event_wrong_scheduled_info(self):
        log_record = (
            "I0830 12:35:39              Not allowed pods are scheduled on Scylla node found: kube-proxy-7kjnf "
            "(ip-10-0-1-200.eu-north-1.compute.internal node)"
        )
        event = ScyllaOperatorLogEvent.WRONG_SCHEDULED_PODS()
        pattern = re.compile(event.regex, re.IGNORECASE)

        self.assertTrue(isinstance(event, LogEvent))
        self.assertTrue(pattern.search(log_record))
        event.add_info(node="N/A", line=log_record, line_number=0)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))
        event.event_id = "9bb2980a-5940-49a7-8b08-d5c323b46aa9"
        self.assertEqual(
            "(ScyllaOperatorLogEvent Severity.WARNING) period_type=one-time "
            "event_id=9bb2980a-5940-49a7-8b08-d5c323b46aa9: type=WRONG_SCHEDULED_PODS "
            "regex=Not allowed pods are scheduled on Scylla node found node=N/A\n"
            "I0830 12:35:39              Not allowed pods are scheduled on Scylla node found: kube-proxy-7kjnf "
            "(ip-10-0-1-200.eu-north-1.compute.internal node)",
            str(event),
        )

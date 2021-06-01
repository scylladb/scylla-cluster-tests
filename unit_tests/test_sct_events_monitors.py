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
import unittest

from sdcm.sct_events import Severity
from sdcm.sct_events.monitors import PrometheusAlertManagerEvent


RAW_ALERT = dict(
    annotations=dict(
        description="[10.0.201.178] has been down for more than 30 seconds.",
        summary="Instance [10.0.201.178] down",
    ),
    endsAt="2019-12-26T06:21:09.591Z",
    startsAt="2019-12-24T17:00:09.591Z",
    updatedAt="2019-12-26T06:18:09.593Z",
    labels=dict(
        alertname="InstanceDown",
        instance="[10.0.201.178]",
        job="scylla",
        monitor="scylla-monitor",
        severity="2",
    ),
)


class TestPrometheusAlertManagerEvent(unittest.TestCase):
    def test_msgfmt(self):
        event = PrometheusAlertManagerEvent.start(raw_alert=RAW_ALERT)
        event.event_id = "536eaf22-3d8f-418a-9381-fe0bcdce7ad9"
        self.assertEqual(
            str(event),
            "(PrometheusAlertManagerEvent Severity.WARNING) period_type=not-set "
            "event_id=536eaf22-3d8f-418a-9381-fe0bcdce7ad9: alert_name=InstanceDown type=start"
            " start=2019-12-24T17:00:09.591Z end=2019-12-26T06:21:09.591Z"
            " description=[10.0.201.178] has been down for more than 30 seconds. updated=2019-12-26T06:18:09.593Z"
            " state= fingerprint=None labels={'alertname': 'InstanceDown', 'instance': '[10.0.201.178]',"
            " 'job': 'scylla', 'monitor': 'scylla-monitor', 'severity': '2'}"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_sct_severity(self):
        event = PrometheusAlertManagerEvent.end(raw_alert=dict(labels=dict(sct_severity="CRITICAL")))
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertTrue(str(event).startswith("(PrometheusAlertManagerEvent Severity.CRITICAL)"))
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_sct_severity_wrong(self):
        event = PrometheusAlertManagerEvent.end(raw_alert=dict(labels=dict(sct_severity="WRONG")))
        self.assertEqual(event.severity, Severity.WARNING)
        self.assertTrue(str(event).startswith("(PrometheusAlertManagerEvent Severity.WARNING)"))
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

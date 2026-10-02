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

import pytest

from sdcm.sct_events import Severity
from sdcm.sct_events.monitors import PrometheusAlertManagerEvent


RAW_ALERT = {
    "annotations": {
        "description": "[10.0.201.178] has been down for more than 30 seconds.",
        "summary": "Instance [10.0.201.178] down",
    },
    "endsAt": "2019-12-26T06:21:09.591Z",
    "startsAt": "2019-12-24T17:00:09.591Z",
    "updatedAt": "2019-12-26T06:18:09.593Z",
    "labels": {
        "alertname": "InstanceDown",
        "instance": "[10.0.201.178]",
        "job": "scylla",
        "monitor": "scylla-monitor",
        "sct_severity": "ERROR",
    },
}


def test_prometheus_alert_manager_event_msgfmt():
    event = PrometheusAlertManagerEvent(raw_alert=RAW_ALERT)
    event.event_id = "536eaf22-3d8f-418a-9381-fe0bcdce7ad9"
    event.publish_event = False
    event.begin_event()
    assert str(event) == (
        "(PrometheusAlertManagerEvent Severity.ERROR) period_type=begin "
        "event_id=536eaf22-3d8f-418a-9381-fe0bcdce7ad9: alert_name=InstanceDown node=[10.0.201.178] "
        "start=2019-12-24T17:00:09.591Z end=2019-12-26T06:21:09.591Z "
        "description=[10.0.201.178] has been down for more than 30 seconds. updated=2019-12-26T06:18:09.593Z "
        "state= fingerprint=None labels={'alertname': 'InstanceDown', 'instance': '[10.0.201.178]', 'job': 'scylla'"
        ", 'monitor': 'scylla-monitor', 'sct_severity': 'ERROR'}"
    )
    assert event == pickle.loads(pickle.dumps(event))


@pytest.mark.parametrize(
    "sct_severity, expected_severity",
    [
        pytest.param("CRITICAL", Severity.CRITICAL, id="critical"),
        pytest.param("WRONG", Severity.NORMAL, id="wrong-defaults-to-normal"),
    ],
)
def test_prometheus_alert_manager_event_sct_severity(sct_severity, expected_severity):
    event = PrometheusAlertManagerEvent(raw_alert={"labels": {"sct_severity": sct_severity}})
    event.publish_event = False
    event.end_event()
    assert event.severity == expected_severity
    assert str(event).startswith(f"(PrometheusAlertManagerEvent {expected_severity})")
    assert event == pickle.loads(pickle.dumps(event))

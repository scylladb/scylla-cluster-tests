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

import re
import json

from sdcm.sct_events.base import SctEvent, Severity


class PrometheusAlertManagerEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    _from_str_regexp = re.compile(
        "[^:]+: alert_name=(?P<alert_name>[^ ]+) type=(?P<type>[^ ]+) start=(?P<start>[^ ]+) "
        f"end=(?P<end>[^ ]+) description=(?P<description>[^ ]+) updated=(?P<updated>[^ ]+) state=(?P<state>[^ ]+) "
        f"fingerprint=(?P<fingerprint>[^ ]+) labels=(?P<labels>[^ ]+)")

    def __init__(self,  # pylint: disable=too-many-arguments
                 raw_alert=None, event_str=None, sct_event_str=None, event_type: str = None, severity=Severity.WARNING):
        super().__init__()
        self.severity = severity
        self.type = event_type
        if raw_alert:
            self._load_from_raw_alert(**raw_alert)
        elif event_str:
            self._load_from_event_str(event_str)
        elif sct_event_str:
            self._load_from_sctevent_str(sct_event_str)

    def __str__(self):
        return f"{super().__str__()}: alert_name={self.alert_name} type={self.type} start={self.start} "\
               f"end={self.end} description={self.description} updated={self.updated} state={self.state} "\
               f"fingerprint={self.fingerprint} labels={self.labels}"

    def _load_from_sctevent_str(self, data: str):
        result = self._from_str_regexp.match(data)
        if not result:
            return False
        tmp = result.groupdict()
        if not tmp:
            return False
        tmp['labels'] = json.loads(tmp['labels'])
        for name, value in tmp:
            setattr(self, name, value)
        return True

    def _load_from_event_str(self, data: str):
        try:
            tmp = json.loads(data)
        except Exception:  # pylint: disable=broad-except
            return None
        for name, value in tmp.items():
            if name not in ['annotations', 'description', 'start', 'end', 'updated', 'fingerprint', 'status', 'labels',
                            'state', 'alert_name', 'severity', 'type', 'timestamp', 'severity']:
                return False
            setattr(self, name, value)
        if isinstance(self.severity, str):
            self.severity = getattr(Severity, self.severity)
        return True

    def _load_from_raw_alert(self,  # pylint: disable=too-many-arguments,invalid-name,unused-argument
                             annotations: dict = None, startsAt=None, endsAt=None, updatedAt=None, fingerprint=None,
                             status=None, labels=None, **kwargs):
        self.annotations = annotations
        if self.annotations:
            self.description = self.annotations.get('description', self.annotations.get('summary', ''))
        else:
            self.description = ''
        self.start = startsAt
        self.end = endsAt
        self.updated = updatedAt
        self.fingerprint = fingerprint
        self.status = status
        self.labels = labels
        if self.status:
            self.state = self.status.get('state', '')
        else:
            self.state = ''
        if self.labels:
            self.alert_name = self.labels.get('alertname', '')
        else:
            self.alert_name = ''
        sct_severity = self.labels.get('sct_severity')
        if sct_severity:
            self.severity = Severity.__dict__.get(sct_severity, Severity.WARNING)

    def __eq__(self, other):
        for name in ['alert_name', 'type', 'start', 'end', 'description', 'updated', 'state', 'fingerprint', 'labels']:
            other_value = getattr(other, name, None)
            value = getattr(self, name, None)
            if value != other_value:
                return False
        return True

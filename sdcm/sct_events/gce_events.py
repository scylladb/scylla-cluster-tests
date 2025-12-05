from typing import Dict

from dateutil import parser

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, EventPeriod


class GceInstanceEvent(SctEvent):
    def __init__(self, gce_log_entry: Dict, severity=Severity.ERROR):
        self.date = str(parser.parse(gce_log_entry["timestamp"]).astimezone())
        self.node = gce_log_entry["protoPayload"]["resourceName"].split("/")[-1]
        self.method = gce_log_entry["protoPayload"]["methodName"]
        self.message = gce_log_entry["protoPayload"]["status"]["message"]
        self.period_type = EventPeriod.INFORMATIONAL.value
        super().__init__(severity=severity)

    @property
    def msgfmt(self):
        return super().msgfmt + ": {0.method} on node {0.node} at {0.date}: {0.message}"

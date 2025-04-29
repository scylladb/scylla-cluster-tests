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
import datetime
import re
import logging
import time

from typing import List, Tuple, Type

from sdcm.sct_events import Severity
from sdcm.utils.remote_logger import KubernetesWrongSchedulingLogger
from sdcm.sct_events.base import LogEvent, LogEventProtocol, T_log_event


LOGGER = logging.getLogger(__name__)


class ScyllaOperatorLogEvent(LogEvent):
    REAPPLY: Type[LogEventProtocol]
    TLS_HANDSHAKE_ERROR: Type[LogEventProtocol]
    OPERATOR_STARTED_INFO: Type[LogEventProtocol]
    WRONG_SCHEDULED_PODS: Type[LogEventProtocol]

    def __init__(self, regex: str = None, severity=Severity.NORMAL):
        super().__init__(regex=regex, severity=severity)
        self.line_number = None

    def add_info(self: T_log_event, node, line: str, line_number: int) -> T_log_event:
        # I0628 15:53:02.269804       1 operator/operator.go:133] <message>
        # I - Log level = INFO
        # 06 - Month
        # 28 - Day
        splits = line.split(maxsplit=4)
        if len(splits) != 5 or len(splits[0]) != 5:
            return self
        type_month_date, time_string, *_ = splits
        try:
            hour_minute_second, milliseconds = time_string.split('.')
            hour, minute, second = hour_minute_second.split(':')
            year = datetime.datetime.now().year
            month = int(type_month_date[1:3])
            day = int(type_month_date[3:5])
            self.source_timestamp = datetime.datetime(
                year=year, month=month, day=day, hour=int(hour), minute=int(minute), second=int(second),
                microsecond=int(milliseconds), tzinfo=datetime.timezone.utc).timestamp()
        except Exception:  # noqa: BLE001
            pass
        self.event_timestamp = time.time()
        self.node = str(node)
        self.line = line
        self._ready_to_publish = True

        return self


ScyllaOperatorLogEvent.add_subevent_type(
    "REAPPLY", severity=Severity.WARNING,
    regex="please apply your changes to the latest version and try again")
ScyllaOperatorLogEvent.add_subevent_type(
    "TLS_HANDSHAKE_ERROR", severity=Severity.WARNING,
    regex="TLS handshake error from .*")
ScyllaOperatorLogEvent.add_subevent_type(
    "OPERATOR_STARTED_INFO", severity=Severity.NORMAL,
    regex='"Starting controller" controller="ScyllaCluster"')
ScyllaOperatorLogEvent.add_subevent_type(
    "WRONG_SCHEDULED_PODS", severity=Severity.WARNING,
    regex=KubernetesWrongSchedulingLogger.WRONG_SCHEDULED_PODS_MESSAGE)


SCYLLA_OPERATOR_EVENTS = [
    ScyllaOperatorLogEvent.REAPPLY(),
    ScyllaOperatorLogEvent.TLS_HANDSHAKE_ERROR(),
    ScyllaOperatorLogEvent.OPERATOR_STARTED_INFO(),
    ScyllaOperatorLogEvent.WRONG_SCHEDULED_PODS(),
]


SCYLLA_OPERATOR_EVENT_PATTERNS: List[Tuple[re.Pattern, LogEventProtocol]] = \
    [(re.compile(event.regex, re.IGNORECASE), event) for event in SCYLLA_OPERATOR_EVENTS]


__all__ = ("ScyllaOperatorLogEvent", "SCYLLA_OPERATOR_EVENT_PATTERNS")

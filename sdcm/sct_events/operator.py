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
import logging
from datetime import datetime
from typing import List, Tuple, Type, Optional

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent, LogEventProtocol, T_log_event


LOGGER = logging.getLogger(__name__)


class ScyllaOperatorLogEvent(LogEvent):
    REAPPLY: Type[LogEventProtocol]
    TLS_HANDSHAKE_ERROR: Type[LogEventProtocol]
    OPERATOR_STARTED_INFO: Type[LogEventProtocol]
    timestamp: str
    namespace: str
    cluster: str
    message: str
    error: str
    trace_id: str

    event_data_mapping = {
        'T': 'timestamp',
        'N': 'namespace',
        'M': 'message',
        'cluster': 'cluster',
        'error': 'error',
        '_trace_id': 'trace_id'
    }

    # pylint: disable=too-many-arguments
    def __init__(self, timestamp=None, namespace=None, cluster=None, message=None, error=None, trace_id=None,
                 regex=None, severity=Severity.ERROR):
        super().__init__(regex, severity=severity)
        self.set_params(timestamp=timestamp, namespace=namespace, cluster=cluster, message=message, error=error,
                        trace_id=trace_id)

    @property
    def msgfmt(self):
        cluster = f"/{self.cluster}" if self.cluster else ""
        return super().msgfmt + " {0.trace_id} {0.namespace}" + cluster + ": {0.message}, {0.error}"

    # pylint: disable=too-many-arguments
    def set_params(self, timestamp=None, namespace=None, cluster=None, message=None, error=None, trace_id=None):
        self.namespace = namespace
        self.cluster = cluster
        self.message = message
        self.timestamp = timestamp
        self.error = error
        self.trace_id = trace_id

    def load_from_json_string(self: T_log_event, data: str) -> Optional[T_log_event]:
        try:
            log_data = json.loads(data)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error('Failed to interpret following log line:\n%s\n, due to the: %s', data, exc)
            return None
        event_data = {}
        for log_en_name, attr_name in self.event_data_mapping.items():
            log_en_data = log_data.get(log_en_name, None)
            if log_en_data is None:
                continue
            if attr_name == 'timestamp':
                log_en_data = datetime.strptime(log_en_data, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            event_data[attr_name] = log_en_data
        try:
            self.set_params(**event_data)
        except Exception as exc:  # pylint: disable=broad-except
            LOGGER.error('Failed to set event parameters %s\n, due to the: %s', data, exc)
            return None
        return self


ScyllaOperatorLogEvent.add_subevent_type(
    "REAPPLY", severity=Severity.WARNING,
    regex="please apply your changes to the latest version and try again")
ScyllaOperatorLogEvent.add_subevent_type(
    "TLS_HANDSHAKE_ERROR", severity=Severity.WARNING,
    regex="TLS handshake error from .*: EOF")
ScyllaOperatorLogEvent.add_subevent_type(
    "OPERATOR_STARTED_INFO", severity=Severity.NORMAL,
    regex="Starting the operator...")


SCYLLA_OPERATOR_EVENTS = [
    ScyllaOperatorLogEvent.REAPPLY(),
    ScyllaOperatorLogEvent.TLS_HANDSHAKE_ERROR(),
    ScyllaOperatorLogEvent.OPERATOR_STARTED_INFO(),
]


SCYLLA_OPERATOR_EVENT_PATTERNS: List[Tuple[re.Pattern, LogEventProtocol]] = \
    [(re.compile(event.regex, re.IGNORECASE), event) for event in SCYLLA_OPERATOR_EVENTS]


__all__ = ("ScyllaOperatorLogEvent", "SCYLLA_OPERATOR_EVENT_PATTERNS")

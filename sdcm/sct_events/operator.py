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

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent


class ScyllaOperatorLogEvent(SctEvent):
    def __init__(self, timestamp=None, namespace=None, cluster=None, message=None, error=None, trace_id=None):
        super().__init__(severity=Severity.ERROR)

        self.namespace = namespace
        self.cluster = cluster
        self.message = message
        self.timestamp = timestamp
        self.error = error
        self.trace_id = trace_id

    @property
    def msgfmt(self):
        cluster = f"/{self.cluster}" if self.cluster else ""
        return super().msgfmt + " {0.trace_id} {0.namespace}" + cluster + ": {0.message}, {0.error}"


class ScyllaOperatorRestartEvent(SctEvent):
    def __init__(self, restart_count):
        super().__init__(severity=Severity.ERROR)

        self.restart_count = restart_count

    @property
    def msgfmt(self):
        return super().msgfmt + ": Scylla Operator has been restarted, restart_count={0.restart_count}"


__all__ = ("ScyllaOperatorLogEvent", "ScyllaOperatorRestartEvent", )

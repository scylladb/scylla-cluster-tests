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
from functools import cached_property

from sdcm.sct_events import Severity
from sdcm.sct_events.continuous_event import ContinuousEvent


class DisruptionEvent(ContinuousEvent):
    def __init__(self,
                 nemesis_name,
                 node,
                 severity=Severity.NORMAL,
                 publish_event=True):
        self.nemesis_name = nemesis_name
        self.node = str(node)
        self.duration = None
        self.full_traceback = ""
        super().__init__(severity=severity, publish_event=publish_event)

    @cached_property
    def subcontext_fields(self) -> list:
        return ['event_id', 'base', 'nemesis_name', 'node', 'timestamp']

    @cached_property
    def subcontext_fmt(self):
        return {'nemesis_name': self.nemesis_name}

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": nemesis_name={0.nemesis_name} target_node={0.node}"
        if self.is_skipped:
            fmt += " skipped"
        if self.skip_reason:
            fmt += " skip_reason={0.skip_reason}"
        if self.errors:
            fmt += " errors={0.errors_formatted}"
        if self.full_traceback:
            fmt += "\n{0.full_traceback}"
        return fmt

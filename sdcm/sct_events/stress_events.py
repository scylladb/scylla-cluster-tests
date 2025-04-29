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
# Copyright (c) 2021 ScyllaDB
from typing import Any, Optional, List, Protocol

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.continuous_event import ContinuousEvent


class BaseStressEvent(ContinuousEvent, abstract=True):

    @classmethod
    def add_stress_subevents(cls,
                             failure: Optional[Severity] = None,
                             error: Optional[Severity] = None,
                             timeout: Optional[Severity] = None,
                             start: Optional[Severity] = Severity.NORMAL,
                             finish: Optional[Severity] = Severity.NORMAL,
                             warning: Optional[Severity] = None) -> None:
        if failure is not None:
            cls.add_subevent_type("failure", severity=failure)
        if error is not None:
            cls.add_subevent_type("error", severity=error)
        if warning is not None:
            cls.add_subevent_type("warning", severity=warning)
        if timeout is not None:
            cls.add_subevent_type("timeout", severity=timeout)
        if start is not None:
            cls.add_subevent_type("start", severity=start)
        if finish is not None:
            cls.add_subevent_type("finish", severity=finish)


class StressEventProtocol(SctEventProtocol, Protocol):
    node: str
    stress_cmd: Optional[str]
    log_file_name: Optional[str]
    errors: Optional[List[str]]

    @property
    def errors_formatted(self):
        ...


class StressEvent(BaseStressEvent, abstract=True):

    def __init__(self,
                 node: Any,
                 stress_cmd: Optional[str] = None,
                 log_file_name: Optional[str] = None,
                 errors: Optional[List[str]] = None,
                 severity: Severity = Severity.NORMAL,
                 publish_event: bool = True):
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        super().__init__(severity=severity, publish_event=publish_event, errors=errors)

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ":"
        if self.type:
            fmt += " type={0.type}"
        if self.node:
            fmt += " node={0.node}"
        if self.stress_cmd:
            fmt += "\nstress_cmd={0.stress_cmd}"
        if self.errors:
            fmt += "\nerrors:\n\n{0.errors_formatted}"
        return fmt

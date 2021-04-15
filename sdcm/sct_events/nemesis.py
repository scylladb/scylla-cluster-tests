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


class DisruptionEvent(SctEvent):
    def __init__(self,
                 type,
                 subtype,
                 status,
                 node=None,
                 start=None,
                 end=None,
                 duration=None,
                 error=None,
                 full_traceback=None,
                 **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__(severity=Severity.NORMAL if status else Severity.ERROR)

        self.type = type
        self.subtype = subtype
        self.node = str(node)
        self.start = start
        self.end = end
        self.duration = duration

        self.error = None
        self.full_traceback = ""
        if error:
            self.error = error
            self.full_traceback = str(full_traceback)

        self.__dict__.update(kwargs)

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": type={0.type} subtype={0.subtype} target_node={0.node} duration={0.duration}"
        if self.severity == Severity.ERROR:
            fmt += " error={0.error}\n{0.full_traceback}"
        return fmt

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

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent


class NodetoolEvent(SctEvent):
    def __init__(self,
                 type,
                 subtype,
                 severity,
                 options=None,
                 node=None,
                 start=None,
                 end=None,
                 duration=None,
                 error=None,
                 full_traceback=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__(severity=severity)

        # handle the case if command is like "snapshot -kc keyspace1"
        nodetool_cmd = type.split()[0]
        if len(type.split()) > 1:
            options_to_list = [options] if options else []
            options = ' '.join(type.split()[1:] + options_to_list)

        self.type = nodetool_cmd
        self.subtype = subtype
        self.options = options
        self.node = str(node)
        self.start = start
        self.end = end
        self.duration = duration

        self.error = None
        self.full_traceback = ""
        if error:
            self.error = error
            self.full_traceback = str(full_traceback)

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": type={0.type} subtype={0.subtype} node={0.node}"
        if self.options:
            fmt += " options={0.options}"
        if self.duration:
            fmt += " duration={0.duration}"
        if self.error:
            fmt += " error={0.error}"
        if self.full_traceback:
            fmt += "\n{0.full_traceback}"
        return fmt

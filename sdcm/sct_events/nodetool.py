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
from sdcm.sct_events.base import ContinuousEvent


# pylint: disable=too-many-instance-attributes
class NodetoolEvent(ContinuousEvent):
    def __init__(self,  # pylint: disable=too-many-arguments
                 nodetool_command,
                 severity=Severity.NORMAL,
                 node=None,
                 options=None,
                 publish_event=True):
        super().__init__(severity=severity, publish_event=publish_event)

        # handle the case if command is like "snapshot -kc keyspace1"
        cmd_splitted = nodetool_command.split()
        if len(cmd_splitted) > 1:
            options_to_list = [options] if options else []
            options = ' '.join(cmd_splitted[1:] + options_to_list)

        if cmd_splitted:
            self.nodetool_command = cmd_splitted[0]
        self.options = options
        self.node = str(node)
        self.full_traceback = None

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": nodetool_command={0.nodetool_command}"
        if self.node:
            fmt += " node={0.node}"
        if self.options:
            fmt += " options={0.options}"
        if self.errors:
            fmt += " errors={0.errors}"
        if self.full_traceback:
            fmt += "\n{0.full_traceback}"
        return fmt

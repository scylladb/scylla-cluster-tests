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
from dataclasses import dataclass, asdict
from json import JSONEncoder
from typing import Any, Type

from sdcm.sct_events import Severity
from sdcm.sct_events.continuous_event import ContinuousEvent


@dataclass
class NodetoolCommand:
    cmd: str
    options: str

    def __post_init__(self):
        cmd_split = self.cmd.split()
        if len(cmd_split) > 1:
            options_to_list = [self.options] if self.options else []
            self.options = ' '.join(cmd_split[1:] + options_to_list)
            self.cmd = cmd_split[0]


class NodetoolEventEncoder(JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, NodetoolCommand):
            return asdict(o)
        else:
            return super().default(o)


class NodetoolEvent(ContinuousEvent):
    def __init__(self,
                 nodetool_command,
                 severity=Severity.NORMAL,
                 node=None,
                 options=None,
                 publish_event=True):
        self.nodetool_command = NodetoolCommand(cmd=nodetool_command, options=options)
        self.node = str(node)
        self.full_traceback = None
        super().__init__(severity=severity, publish_event=publish_event)

    @property
    def msgfmt(self) -> str:
        fmt = super().msgfmt + ": nodetool_command={0.nodetool_command.cmd}"
        if self.node:
            fmt += " node={0.node}"
        if self.nodetool_command.options:
            fmt += " options={0.nodetool_command.options}"
        if self.errors:
            fmt += " errors={0.errors}"
        if self.full_traceback:
            fmt += "\n{0.full_traceback}"
        return fmt

    def to_json(self, encoder: Type[JSONEncoder] = NodetoolEventEncoder) -> str:
        return super().to_json(encoder=encoder)

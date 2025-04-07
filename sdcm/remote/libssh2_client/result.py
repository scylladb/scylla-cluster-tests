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

from dataclasses import dataclass, field


@dataclass
class Result:
    """
    A copy-cat from invoke.runners.Result

    """
    stdout: str
    stderr: str
    encoding: str = 'utf8'
    command: str = ''
    shell: str = ''
    exited: int = None
    env: dict = field(default_factory=dict)
    pty: bool = False
    hide: tuple = tuple()

    @property
    def return_code(self) -> int:
        return self.exited

    def __nonzero__(self):
        return self.ok

    def __bool__(self):
        return self.__nonzero__()

    def __str__(self) -> str:
        if self.exited is not None:
            desc = "Command exited with status {}.".format(self.exited)
        else:
            desc = "Command was not fully executed due to watcher error."
        ret = [desc]
        for stream in ("stdout", "stderr"):
            val = getattr(self, stream)
            ret.append(
                """=== {} ===
{}
""".format(
                    stream, val.rstrip()
                )
                if val
                else "(no {})".format(stream)
            )
        return "\n".join(ret)

    def __repr__(self) -> str:
        template = "<Result cmd={!r} exited={}>"
        return template.format(self.command, self.exited)

    @property
    def ok(self) -> bool:
        return self.exited == 0

    @property
    def failed(self) -> bool:
        return not self.ok

    def tail(self, stream: str, count: int = 10) -> str:
        return "\n\n" + "\n".join(getattr(self, stream).splitlines()[-count:])

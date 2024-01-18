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

import enum
import json
import logging
from typing import Protocol, Optional, Type, runtime_checkable


class Severity(enum.Enum):
    SUPPRESS = -2  # those won't be generating events for
    DEBUG = -1  # this should only be through test case configuration!!!
    UNKNOWN = 0
    NORMAL = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


@runtime_checkable
class SctEventProtocol(Protocol):
    base: str
    type: Optional[str]
    subtype: Optional[str]
    event_timestamp: Optional[float]
    severity: Severity

    # pylint: disable=super-init-not-called
    def __init__(self, *args, **kwargs):
        ...

    @classmethod
    def is_abstract(cls) -> bool:
        ...

    @classmethod
    def add_subevent_type(cls,
                          name: str,
                          /, *,
                          abstract: bool = False,
                          mixin: Optional[Type] = None,
                          **kwargs) -> None:
        ...

    @property
    def formatted_event_timestamp(self) -> str:
        ...

    @property
    def timestamp(self) -> float:
        ...

    def publish(self, warn_not_ready: bool = True) -> None:
        ...

    def publish_or_dump(self, default_logger: Optional[logging.Logger] = None, warn_not_ready: bool = True) -> None:
        ...

    def to_json(self) -> str:
        ...


# Monkey patch JSONEncoder make enums jsonable
_SAVED_DEFAULT = json.JSONEncoder().default  # save default method.


def _new_default(self, obj):  # pylint: disable=unused-argument
    if isinstance(obj, enum.Enum):
        return obj.name  # could also be obj.value
    else:
        return _SAVED_DEFAULT


json.JSONEncoder.default = _new_default  # set new default method.


__all__ = ("SctEventProtocol", "Severity", )

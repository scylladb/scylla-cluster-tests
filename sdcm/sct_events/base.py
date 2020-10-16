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
import time
import logging
import datetime
from typing import Optional

from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events.events_processes import EventsProcessesRegistry


LOGGER = logging.getLogger(__name__)


class Severity(enum.Enum):
    UNKNOWN = 0
    NORMAL = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


class SctEvent:
    _registry: Optional[EventsProcessesRegistry] = None

    severity = Severity.NORMAL
    type = None
    subtype = None

    def __init__(self):
        self.timestamp = time.time()

        # Populate __dict__ with default values.
        self.severity = self.severity
        self.type = self.type
        self.subtype = self.subtype

    @property
    def formatted_timestamp(self) -> str:
        try:
            return datetime.datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            LOGGER.exception("Failed to format a timestamp: %d", self.timestamp)
            return "0000-00-00 <UnknownTimestamp>"

    def publish(self) -> None:
        get_events_main_device(_registry=self._registry).publish_event(self)

    def publish_or_dump(self, default_logger: Optional[logging.Logger] = None) -> None:
        if proc := get_events_main_device(_registry=self._registry):
            if proc.is_alive():
                self.publish()
            else:
                # pylint: disable=import-outside-toplevel; to avoid cyclic imports
                from sdcm.sct_events.file_logger import get_events_logger
                get_events_logger(_registry=self._registry).write_event(self)
        elif default_logger:
            default_logger.error(str(self))

    def __str__(self):
        return f"({type(self).__name__} {self.severity})"

    def to_json(self) -> str:
        return json.dumps({"__class__": type(self).__name__, **self.__dict__})

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return False
        return self.__dict__ == other.__dict__

    def __getstate__(self):
        state = self.__dict__.copy()

        # Remove non-picklable stuff.
        state.pop("_registry", None)

        return state


__all__ = ("Severity", "SctEvent", )

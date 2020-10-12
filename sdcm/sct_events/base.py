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
import time
import logging
import datetime
from typing import TypeVar, Generic

from sdcm.sct_events.json import json
from sdcm.sct_events.events_processes import EVENTS_PROCESSES


LOGGER = logging.getLogger(__name__)


class Severity(enum.Enum):
    NORMAL = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


EventType = TypeVar('EventType', bound='SctEvent')


class SctEvent(Generic[EventType]):
    def __init__(self):
        self.timestamp = time.time()
        self.severity = getattr(self, 'severity', Severity.NORMAL)

    @property
    def formatted_timestamp(self):
        try:
            return datetime.datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            LOGGER.exception("failed to format timestamp:[%d]", self.timestamp)
            return '<UnknownTimestamp>'

    def publish(self, guaranteed=True):
        if guaranteed:
            return EVENTS_PROCESSES['MainDevice'].publish_event_guaranteed(self)
        return EVENTS_PROCESSES['MainDevice'].publish_event(self)

    def publish_or_dump(self, default_logger=None):
        if 'MainDevice' in EVENTS_PROCESSES:
            if EVENTS_PROCESSES['MainDevice'].is_alive():
                self.publish()
            else:
                EVENTS_PROCESSES['EVENTS_FILE_LOGGER'].dump_event_into_files(self)
            return
        if default_logger:
            default_logger.error(str(self))

    def __str__(self):
        return "({} {})".format(self.__class__.__name__, self.severity)

    def to_json(self):
        return json.dumps(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__


__all__ = ("Severity", "EventType", "SctEvent", )

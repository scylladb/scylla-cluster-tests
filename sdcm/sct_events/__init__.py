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

import threading
import multiprocessing
from typing import Union

from sdcm.sct_events.events_processes import EVENTS_PROCESSES


StopEvent = Union[multiprocessing.Event, threading.Event]


def subscribe_events(stop_event: StopEvent):
    return EVENTS_PROCESSES["MainDevice"].subscribe_events(stop_event=stop_event)


def get_events_grouped_by_category(limit=0) -> dict:
    return EVENTS_PROCESSES['EVENTS_FILE_LOGGER'].get_events_by_category(limit)


__all__ = ("StopEvent", "subscribe_events", "get_events_grouped_by_category", )

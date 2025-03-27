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
# Copyright (c) 2023 ScyllaDB

import re
import logging
import threading

from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4
from typing import Tuple, Callable, Any, cast
from functools import partial

from sdcm.sct_events.base import T_log_event

from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events.events_processes import \
    EVENTS_COUNTER_ID, EventsProcessesRegistry, BaseEventsProcess, \
    start_events_process, get_events_process, verbose_suppress

STALL_INTERVALS = [10, 20, 30, 50, 100, 200, 1000, 2000]

LOGGER = logging.getLogger(__name__)
REACTOR_MS_REGEX = re.compile(r'Reactor stalled for\s(\d+)\sms')
EVENT_COUNTER_DIR = "event_stats"


@dataclass
class EventStatData:
    event_types: tuple
    save_dir: Path
    stats: dict


class EventStatHandler:  # pylint: disable=too-few-public-methods
    def __init__(self, event: T_log_event, save_dir: str | Path):
        self._event = event
        self._event_name = self._event.__class__.__name__
        self._save_dir = Path(save_dir) / Path(self._event_name)

    def update_stat(self, stat: dict):
        if not stat:
            return {"event": self._event_name,
                    "counter": 1}
        return {
            "event": self._event_name,
            "counter": stat["counter"] + 1
        }

    def _save_event(self):
        node = self._event.node
        if not self._save_dir.exists():
            self._save_dir.mkdir(parents=True, exist_ok=True)
        file_path = self._save_dir / Path(node)
        with open(file_path, "a+", encoding="utf-8") as event_writer:
            event_writer.write(str(self._event.line) + "\n")


class ReactorStallEventStatHandler(EventStatHandler):  # pylint: disable=too-few-public-methods
    intervals = STALL_INTERVALS

    def _get_interval(self, stall_ms: int):
        if not stall_ms:
            return 0
        for boundary in self.intervals:
            if stall_ms < boundary:
                return boundary
        return stall_ms

    def update_stat(self, stat: dict):
        self._save_event()
        if not stat:
            stat = {"event": self._event_name,
                    "counter": 0,
                    "ms": {}}
        stall_ms = 0
        if match := REACTOR_MS_REGEX.search(self._event.line):
            stall_ms = int(match.group(1))
        interval = self._get_interval(stall_ms)
        stat["ms"].update({interval: stat["ms"].get(interval, 0) + 1})
        stat["counter"] += 1
        return {"event": self._event_name,
                "counter": stat["counter"],
                "ms": stat["ms"]}


EventStatHandleMapper = {
    "DatabaseLogEvent.REACTOR_STALLED": ReactorStallEventStatHandler,
}


class EventsCounter(BaseEventsProcess[Tuple[str, Any], None], threading.Thread):

    def __init__(self, _registry: EventsProcessesRegistry):
        base_dir: Path = get_events_main_device(_registry=_registry).events_log_base_dir
        self.events_stat_dir = base_dir / Path(EVENT_COUNTER_DIR)
        self.counters_register = dict()
        super().__init__(_registry=_registry)

    def run(self) -> None:
        LOGGER.debug("Counting events is running")
        for event_tuple in self.inbound_events():
            if not self.counters_register:
                continue

            with verbose_suppress("EventsFileLogger failed to process %s", event_tuple):
                _, event = event_tuple  # try to unpack event from EventsDevice

                for cm_id, event_stat_data in self.counters_register.items():
                    if event_stat_data.event_types and event.__class__ in event_stat_data.event_types:
                        handler = EventStatHandleMapper.get(event.__class__.__name__, EventStatHandler)
                        stat = event_stat_data.stats.setdefault(event.__class__.__name__, {})
                        event_stat_data.stats.update({event.__class__.__name__: handler(
                            event, event_stat_data.save_dir).update_stat(stat)})
                        self.counters_register[cm_id] = event_stat_data

    def add_counter(self, counter_id: str, event_stat_data: EventStatData):
        self.counters_register[counter_id] = event_stat_data

    def get_counter(self, counter_id: str) -> EventStatData | None:
        if counter_id in self.counters_register:
            return self.counters_register[counter_id]
        return None

    def remove_counter(self, counter_id: str):
        if counter_id in self.counters_register:
            del self.counters_register[counter_id]


start_events_counter = partial(start_events_process, EVENTS_COUNTER_ID, EventsCounter)
get_events_counter = cast(Callable[..., EventsCounter], partial(get_events_process, EVENTS_COUNTER_ID))


class EventCounterContextManager:
    def __init__(self, event_type, name, _registry=None):
        self._events_stat_dir = get_events_counter(_registry=_registry).events_stat_dir
        self._id = f"{uuid4()}"

        if not isinstance(event_type, tuple):
            event_type = (event_type, )

        self._event_type = event_type
        self._counter_device = get_events_counter(_registry=_registry)
        self._statistics = {}
        self._save_file_path = self._events_stat_dir / Path(name)

    def start_event_counter(self) -> None:
        LOGGER.debug("Start count events type %s by CounterEvent with id %s",
                     self._event_type, self._id)
        event_stat_data = EventStatData(event_types=self._event_type,
                                        save_dir=self._save_file_path,
                                        stats={})
        self._counter_device.add_counter(self._id, event_stat_data)

    def stop_event_counter(self):
        if counter_data := self._counter_device.get_counter(self._id):
            self._statistics = counter_data.stats
            self._counter_device.remove_counter(self._id)

    def get_stats(self):
        if counter_data := self._counter_device.get_counter(self._id):
            self._statistics = counter_data.stats
        return self._statistics

    def __enter__(self):
        self.start_event_counter()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.stop_event_counter()


__all__ = ("EventsCounter",
           "start_events_counter", "get_events_counter", "STALL_INTERVALS")

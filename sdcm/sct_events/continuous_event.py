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
from __future__ import annotations

import logging
import time
import traceback
from typing import Optional, List, Iterable, Any

from dateutil.relativedelta import relativedelta

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, EventPeriod
from sdcm.utils.metaclasses import Singleton

LOGGER = logging.getLogger(__name__)


class ContinuousEventRegistryException(Exception):
    pass


class ContinuousEventsRegistry(metaclass=Singleton):
    def __init__(self):
        self.hashed_continuous_events: dict[int, list[ContinuousEvent]] = {}

    @property
    def continuous_events(self) -> Iterable[ContinuousEvent]:
        for hash_bucket in self.hashed_continuous_events.values():
            for event in hash_bucket:
                yield event

    def add_event(self, event: ContinuousEvent):
        if not issubclass(type(event), ContinuousEvent):
            msg = f"Event: {event} is not a ContinuousEvent"
            raise ContinuousEventRegistryException(msg)
        hash_bucket = self.hashed_continuous_events.get(event.continuous_hash, None)
        if hash_bucket is None:
            hash_bucket = []
            self.hashed_continuous_events[event.continuous_hash] = hash_bucket
        elif hash_bucket:
            LOGGER.error(
                'Continues event %s with hash %s (%s) is not suppose to have duplicates, '
                'while there is another event with same hash - %s',
                hash_bucket[-1],
                event.continuous_hash,
                event.continuous_hash_dict,
                event,
            )
        hash_bucket.append(event)

    def del_event(self, event: ContinuousEvent):
        hash_bucket = self.hashed_continuous_events.get(event.continuous_hash, [])
        if event in hash_bucket:
            hash_bucket.remove(event)

    def cleanup_registry(self):
        self.hashed_continuous_events.clear()

    def find_continuous_events_by_hash(self, continuous_hash: int) -> list[ContinuousEvent]:
        return self.hashed_continuous_events.get(continuous_hash, []).copy()

    def find_running_disruption_events(self):
        running_nemesis_events = [event[0] for event in self.hashed_continuous_events.values()
                                  if event and event[0].base == "DisruptionEvent"]
        return running_nemesis_events


class ContinuousEvent(SctEvent, abstract=True):
    # Event filter does not create object of the class (not initialize it), so "_duration" attribute should
    # exist without initialization
    _duration: Optional[int] = None
    continuous_hash_fields: tuple[str] = ('event_id',)
    log_file_name: str = None

    def __init__(self,
                 severity: Severity = Severity.UNKNOWN,
                 publish_event: bool = True, errors: list[str] = None):
        super().__init__(severity=severity)
        self.errors = errors if errors else []
        self.publish_event = publish_event
        self._ready_to_publish = publish_event
        self.begin_timestamp = None
        self.end_timestamp = None
        self.is_skipped = False
        self.skip_reason = ""

    @property
    def continuous_hash(self):
        """Returns hash that is used to find similar events, i.e. related start event"""
        return hash(self.continuous_hash_tuple)

    @classmethod
    def get_continuous_hash_from_dict(cls, data: dict):
        if "shard" in data:
            data["shard"] = int(data["shard"])
        return hash((cls.__name__,) + tuple(data.get(attr_name) for attr_name in cls.continuous_hash_fields))

    @property
    def continuous_hash_tuple(self) -> tuple[Any | None]:
        return (self.__class__.__name__,) + \
            tuple(getattr(self, attr_name, None) for attr_name in self.continuous_hash_fields)

    @property
    def continuous_hash_dict(self) -> dict[str, str]:
        return {attr_name: getattr(self, attr_name, None) for attr_name in self.continuous_hash_fields}

    def __enter__(self):
        return self.begin_event()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None and isinstance(exc_val, Exception):
            if not isinstance(self.errors, list):
                self.errors = []

            if self.is_skipped:
                self.severity = Severity.NORMAL
            else:
                self.severity = Severity.ERROR if self.severity.value <= Severity.ERROR.value else self.severity
                self.errors.append(traceback.format_exc(limit=None, chain=True))

        self.end_event()

    def skip(self, skip_reason):
        self.is_skipped = True
        self.skip_reason = skip_reason

    @property
    def msgfmt(self):
        fmt = super().msgfmt
        if self.duration is not None:
            fmt += " duration={0.duration_formatted}"
        return fmt

    @property
    def errors_formatted(self):
        return "\n".join(self.errors) if self.errors else ""

    @property
    def duration(self):
        if self._duration is None:
            if self.begin_timestamp is not None and self.end_timestamp is not None:
                self._duration = int(self.end_timestamp - self.begin_timestamp)
        return self._duration

    @duration.setter
    def duration(self, duration: int):
        self._duration = duration

    @property
    def duration_formatted(self):
        duration = ''
        if self.duration is None:
            return duration

        delta = relativedelta(seconds=self.duration)
        days, hours, minutes, sec = (int(delta.days), int(delta.hours), int(delta.minutes), delta.seconds)
        if days:
            duration += f"{days}d"

        if days or hours:
            duration += f"{hours}h"

        if (hours or days) or (not hours and minutes > 0):
            duration += f"{minutes}m"

        duration += f"{sec}s"

        return duration

    # TODO: rename function to "begin" after the refactor will be done
    def begin_event(self, publish: bool = True) -> ContinuousEvent:
        self.begin_timestamp = self.event_timestamp = time.time()
        self.period_type = EventPeriod.BEGIN.value
        ContinuousEventsRegistry().add_event(self)
        self.add_subcontext()
        if publish and self.publish_event:
            self._ready_to_publish = True
            self.publish()
        return self

    # TODO: rename function to "end" after the refactor will be done
    def end_event(self, publish: bool = True) -> None:
        self.end_timestamp = self.event_timestamp = time.time()
        self.period_type = EventPeriod.END.value
        ContinuousEventsRegistry().del_event(self)
        # clean all subcontext (it's subcontext of "begin" event)
        self.subcontext = []

        self.add_subcontext()
        if publish and self.publish_event:
            self._ready_to_publish = True
            self.publish()

    def add_error(self, errors: Optional[List[str]]) -> None:
        if not isinstance(self.errors, list):
            self.errors = []

        self.errors.extend(errors)

    # TODO: rename function to "error" after the refactor will be done
    def event_error(self):
        self.event_timestamp = time.time()
        self.period_type = EventPeriod.INFORMATIONAL.value
        self.duration = None
        if self.publish_event:
            self._ready_to_publish = True
            self.publish()

    def publish(self, warn_not_ready: bool = True) -> None:
        super().publish(warn_not_ready=warn_not_ready)

    def publish_or_dump(self, default_logger: Optional[logging.Logger] = None, warn_not_ready: bool = True) -> None:
        super().publish_or_dump(default_logger=default_logger, warn_not_ready=warn_not_ready)

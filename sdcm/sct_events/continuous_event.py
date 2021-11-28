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
import uuid
from typing import Optional, List, Union, Type, Any

from dateutil.relativedelta import relativedelta

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, EventPeriod
from sdcm.utils.metaclasses import Singleton

LOGGER = logging.getLogger(__name__)


class ContinuousEventRegistryException(BaseException):
    pass


class ContinuousEventsRegistry(metaclass=Singleton):
    def __init__(self):
        self._continuous_events: List[ContinuousEvent] = []

    @property
    def continuous_events(self):
        return self._continuous_events

    def add_event(self, event: ContinuousEvent):
        if not issubclass(type(event), ContinuousEvent):
            msg = f"Event: {event} is not a ContinuousEvent"
            raise ContinuousEventRegistryException(msg)

        if self._find_event_by_id(event.event_id):
            msg = f"Event with id: {event.event_id} is already present. Event ids in the registry must be unique."
            raise ContinuousEventRegistryException(msg)

        self.continuous_events.append(event)

    def get_event_by_id(self, event_id: Union[uuid.UUID, str]) -> Optional[ContinuousEvent]:
        found_events = self._find_event_by_id(event_id)

        if not found_events:
            LOGGER.warning("Couldn't find continuous event with id: {event_id} in registry.".format(event_id=event_id))

            return None

        return found_events[0]

    def get_events_by_type(self, event_type: Type[ContinuousEvent]) -> List[ContinuousEvent]:
        event_filter = self.get_registry_filter()
        found_events = event_filter \
            .filter_by_type(event_type=event_type) \
            .get_filtered()

        if not found_events:
            LOGGER.warning("No continuous events of type: {event_type} found in registry."
                           .format(event_type=event_type))

        return found_events

    def get_events_by_period(self,
                             period_type: EventPeriod) -> List[ContinuousEvent]:
        event_filter = self.get_registry_filter()
        found_events = event_filter \
            .filter_by_period(period_type=period_type.value) \
            .get_filtered()

        if not found_events:
            LOGGER.warning("No continuous events with period type: {period_type} found in registry."
                           .format(period_type=period_type))

        return found_events

    def get_events_by_node(self, node: str) -> List[ContinuousEvent]:
        event_filter = self.get_registry_filter()
        found_events = event_filter \
            .filter_by_node(node=node) \
            .get_filtered()

        if not found_events:
            LOGGER.warning("No continuous event with associated with node: {node_name} found in registry"
                           .format(node_name=node))

        return found_events

    def get_registry_filter(self) -> ContinuousRegistryFilter:
        registry_filter = ContinuousRegistryFilter(registry=self.continuous_events)

        return registry_filter

    def _find_event_by_id(self, event_id: Union[uuid.UUID, str]) -> List[ContinuousEvent]:
        event_filter = self.get_registry_filter()
        found_events = event_filter.filter_by_id(event_id).get_filtered()

        return found_events


# pylint: disable=too-many-instance-attributes
class ContinuousEvent(SctEvent, abstract=True):
    # Event filter does not create object of the class (not initialize it), so "_duration" attribute should
    # exist without initialization
    _duration: Optional[int] = None

    def __init__(self,
                 severity: Severity = Severity.UNKNOWN,
                 publish_event: bool = True):
        super().__init__(severity=severity)
        self.log_file_name = None
        self.errors = []
        self.publish_event = publish_event
        self._ready_to_publish = publish_event
        self.begin_timestamp = None
        self.end_timestamp = None
        self.is_skipped = False
        self.skip_reason = ""
        self._continuous_event_registry = ContinuousEventsRegistry()
        self.register_event()

    def __enter__(self):
        event = self.begin_event()
        return event

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_tb is not None:
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
        return "\n".join(self.errors) if self.errors is not None else ""

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
    def begin_event(self) -> ContinuousEvent:
        self.begin_timestamp = self.event_timestamp = time.time()
        self.period_type = EventPeriod.BEGIN.value
        self.severity = Severity.NORMAL
        if self.publish_event:
            self._ready_to_publish = True
            self.publish()
        return self

    # TODO: rename function to "end" after the refactor will be done
    def end_event(self) -> None:
        self.end_timestamp = self.event_timestamp = time.time()
        self.period_type = EventPeriod.END.value
        if self.publish_event:
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

    def register_event(self):
        self._continuous_event_registry.add_event(self)


class ContinuousRegistryFilter:
    def __init__(self, registry: List[Any]):
        self._registry = registry
        self._output = registry.copy()

    def filter_by_id(self, event_id: str) -> ContinuousRegistryFilter:
        self._output = [event for event in self._output if event.event_id == event_id]

        return self

    def filter_by_node(self, node: str) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if getattr(item, 'node', None) == node]
        return self

    def filter_by_type(self, event_type: Type[ContinuousEvent]) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if isinstance(item, event_type)]

        return self

    def filter_by_period(self, period_type: EventPeriod) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if item.period_type == period_type]

        return self

    def filter_by_shard(self, shard: int) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if item.shard == shard]

        return self

    def filter_by_attr(self, **kwargs) -> ContinuousRegistryFilter:
        output = []
        for event in self._output:
            selected = False
            for attr, value in kwargs.items():
                if getattr(event, attr, None) == value:
                    selected = True
                else:
                    selected = False
                    break

            if selected:
                output.append(event)

        self._output = output

        return self

    def get_filtered(self) -> List[Any]:
        return self._output

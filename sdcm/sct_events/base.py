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

# pylint: disable=too-many-instance-attributes, broad-except, protected-access

from __future__ import annotations

import json
import time
import traceback
import uuid
import pickle
import fnmatch
import logging
from enum import Enum
from json import JSONEncoder
from types import new_class
from typing import \
    Any, Optional, Type, Dict, List, Tuple, Callable, Generic, TypeVar, Protocol, runtime_checkable, Union
from keyword import iskeyword
from weakref import proxy as weakproxy
from datetime import datetime
from functools import partialmethod

import yaml
import dateutil.parser
from dateutil.relativedelta import relativedelta

from sdcm import sct_abs_path
from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.events_processes import EventsProcessesRegistry
from sdcm.utils.metaclasses import Singleton

DEFAULT_SEVERITIES = sct_abs_path("defaults/severities.yaml")

LOGGER = logging.getLogger(__name__)


class ContinuousRegistryFilter:
    def __init__(self, registry: List[Any], by_base: str = None):
        """
        str by_base: return events only with event.base value = by_base.
                     For example: NodetoolEvent.base = "NodetoolEvent"
        """
        if not by_base:
            self._registry = registry
        else:
            self._registry = [event for event in registry if event.base == by_base]
        self._output = self._registry.copy()

    def filter_by_id(self, event_id: str) -> ContinuousRegistryFilter:
        self._output = [event for event in self._output if event.event_id == event_id]

        return self

    def filter_by_node(self, node: str) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if item.node == node]

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

    def filter_by_alert(self, alert: str) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if item.alert_name == alert]

        return self

    def filter_by_starts_at(self, starts_at: str) -> ContinuousRegistryFilter:
        self._output = [item for item in self._output if item.starts_at == starts_at]

        return self

    def get_filtered(self) -> List[Any]:
        return self._output


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

    def get_registry_filter(self, by_base=None) -> ContinuousRegistryFilter:
        registry_filter = ContinuousRegistryFilter(registry=self.continuous_events, by_base=by_base)

        return registry_filter

    def _find_event_by_id(self, event_id: Union[uuid.UUID, str]) -> List[ContinuousEvent]:
        event_filter = self.get_registry_filter()
        found_events = event_filter.filter_by_id(event_id).get_filtered()

        return found_events


class SctEventTypesRegistry(Dict[str, Type["SctEvent"]]):  # pylint: disable=too-few-public-methods
    def __init__(self, severities_conf: str = DEFAULT_SEVERITIES):
        super().__init__()
        with open(severities_conf) as fobj:
            self.max_severities = {event_t: Severity[sev] for event_t, sev in yaml.safe_load(fobj).items()}
        self.limit_rules = []

    def __setitem__(self, key: str, value: Type[SctEvent]):
        if not value.is_abstract() and key not in self.max_severities:
            raise ValueError(f"There is no max severity configured for {key}")
        super().__setitem__(key, weakproxy(value))  # pylint: disable=no-member; pylint doesn't know about Dict

    def __set_name__(self, owner: Type[SctEvent], name: str) -> None:
        self[owner.__name__] = owner  # add owner class to the registry.


class EventPeriod(Enum):
    BEGIN = "begin"
    END = "end"
    INFORMATIONAL = "one-time"  # this is not interval event. It's for one point of time event
    NOT_DEFINED = "not-set"


class SctEvent:
    _sct_event_types_registry: SctEventTypesRegistry = SctEventTypesRegistry()
    _events_processes_registry: Optional[EventsProcessesRegistry] = None

    _abstract: bool = True  # this attribute set by __init_subclass__()
    base: str = "SctEvent"  # this attribute set by __init_subclass__()
    type: Optional[str] = None  # this attribute set by add_subevent_type()
    subtype: Optional[str] = None  # this attribute set by add_subevent_type()

    period_type: str = EventPeriod.NOT_DEFINED.value  # attribute possible values are from EventTypes enum

    formatter: Callable[[str, SctEvent], str] = staticmethod(str.format)
    msgfmt: str = "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id}"

    timestamp: Optional[float] = None  # actual value should be set using __init__()
    severity: Severity = Severity.UNKNOWN  # actual value should be set using __init__()

    _ready_to_publish: bool = False  # set it to True in __init__() and to False in publish() to prevent double-publish

    def __init_subclass__(cls, abstract: bool = False):
        # pylint: disable=unsupported-membership-test; pylint doesn't know about Dict
        if cls.__name__ in cls._sct_event_types_registry:
            raise TypeError(f"Name {cls.__name__} is already used")
        cls.base = cls.__name__.split(".", 1)[0]
        cls._abstract = bool(abstract)
        cls._sct_event_types_registry[cls.__name__] = cls

    # Do it this way because abc.ABC doesn't prevent the instantiation if there are no abstract methods or properties.
    def __new__(cls, *_, **__):
        if cls.is_abstract():
            raise TypeError(f"Class {cls.__name__} may not be instantiated directly")
        return super().__new__(cls)

    def __init__(self, severity: Severity = Severity.UNKNOWN):
        self.timestamp = time.time()
        self.severity = severity
        self._ready_to_publish = True
        self.event_id = str(uuid.uuid4())

    @classmethod
    def is_abstract(cls) -> bool:
        return cls._abstract

    @classmethod
    def add_subevent_type(cls,
                          name: str,
                          /, *,
                          abstract: bool = False,
                          mixin: Optional[Type] = None,
                          **kwargs) -> None:

        # Check if we can add a new sub-event type:
        #   1) only 2 levels of sub-events allowed (i.e., `Event.TYPE.subtype')
        assert len(cls.__name__.split(".")) < 3, "max level of the event's nesting is already reached"

        #   2) name of sub-event should be a correct Python identifier.
        assert name.isidentifier() and not iskeyword(name), \
            "name of an SCT event type should be a valid Python identifier and not a keyword"

        #   3) Base event shouldn't have an attribute with same name.
        assert not hasattr(cls, name), f"SCT event type {cls} already has attribute `{name}'"

        bases = (cls, ) if mixin is None else (mixin, cls, )
        init_index = 0 if mixin is None or "__init__" in vars(mixin) else 1  # check if mixin has own `__init__()'
        nesting_level = "type" if cls.type is None else "subtype"

        # Create a new type with `__init__()' based on the parent class using `partialmethod()' from `functools'.
        event_t = new_class(
            name=f"{cls.__name__}.{name}",
            bases=bases,
            kwds={"abstract": bool(abstract)},
            exec_body=lambda ns: ns.update({
                nesting_level: name,
                "__init__": partialmethod(bases[init_index].__init__, **kwargs),
            })
        )

        # For pickling to work, the __module__ variable needs to be set to the same as base class.
        event_t.__module__ = cls.__module__

        # Make the new class available in the base class as an attribute (i.e., `cls.name')
        setattr(cls, name, event_t)

    @property
    def formatted_timestamp(self) -> str:
        try:
            return datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except (TypeError, OverflowError, OSError,):
            LOGGER.exception("Failed to format a timestamp: %r", self.timestamp)
            return "0000-00-00 <UnknownTimestamp>"

    def publish(self, warn_not_ready: bool = True) -> None:
        # pylint: disable=import-outside-toplevel; to avoid cyclic imports
        from sdcm.sct_events.events_device import get_events_main_device

        if not self._ready_to_publish:
            if warn_not_ready:
                LOGGER.warning("[SCT internal warning] %s is not ready to be published", self)
            return
        get_events_main_device(_registry=self._events_processes_registry).publish_event(self)
        self._ready_to_publish = False

    def publish_or_dump(self, default_logger: Optional[logging.Logger] = None, warn_not_ready: bool = True) -> None:
        # pylint: disable=import-outside-toplevel; to avoid cyclic imports
        from sdcm.sct_events.events_device import get_events_main_device

        if not self._ready_to_publish:
            if warn_not_ready:
                LOGGER.warning("[SCT internal warning] %s is not ready to be published", self)
            return
        try:
            proc = get_events_main_device(_registry=self._events_processes_registry)
        except RuntimeError:
            LOGGER.exception("Unable to get events main device")
            proc = None
        if proc:
            if proc.is_alive():
                self.publish()
            else:
                from sdcm.sct_events.file_logger import get_events_logger
                get_events_logger(_registry=self._events_processes_registry).write_event(self)
        elif default_logger:
            default_logger.error(str(self))
        self._ready_to_publish = False

    def dont_publish(self):
        self._ready_to_publish = False
        LOGGER.debug("%s marked to not publish", self)

    def to_json(self, encoder: Type[JSONEncoder] = JSONEncoder) -> str:
        return json.dumps({
            "base": self.base,
            "type": self.type,
            "subtype": self.subtype,
            **self.__getstate__(),
        }, cls=encoder)

    def __getstate__(self):
        # Remove everything from the __dict__ that starts with "_".
        return {attr: value for attr, value in self.__dict__.items() if not attr.startswith("_")}

    def __str__(self):
        return self.formatter(self.msgfmt, self)

    def __eq__(self, other):
        return (isinstance(other, type(self)) or isinstance(self, type(other))) \
            and self.__getstate__() == other.__getstate__()

    def __del__(self):
        if self._ready_to_publish:
            warning = f"[SCT internal warning] {self} has not been published or dumped, maybe you missed .publish()"
            try:
                LOGGER.warning(warning)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"Exception while printing {warning}. Full exception: {exc}")


class InformationalEvent(SctEvent, abstract=True):

    def __init__(self, severity: Severity = Severity.UNKNOWN):
        super().__init__(severity=severity)
        self.period_type = EventPeriod.INFORMATIONAL.value


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
        return self

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
        self.timestamp = time.time()
        self.begin_timestamp = self.timestamp
        self.period_type = EventPeriod.BEGIN.value
        self.severity = Severity.NORMAL
        if self.publish_event:
            self._ready_to_publish = True
            self.publish()
        return self

    # TODO: rename function to "end" after the refactor will be done
    def end_event(self) -> None:
        self.timestamp = time.time()
        self.end_timestamp = self.timestamp
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
        self.timestamp = time.time()
        self.period_type = EventPeriod.INFORMATIONAL.value
        self.duration = None
        if self.publish_event:
            self._ready_to_publish = True
            self.publish()

    def register_event(self):
        self._continuous_event_registry.add_event(self)


def add_severity_limit_rules(rules: List[str]) -> None:
    for rule in rules:
        if not rule:
            continue
        try:
            pattern, severity = rule.split("=", 1)
            severity = Severity[severity.strip()]
            SctEvent._sct_event_types_registry.limit_rules.insert(0, (pattern.strip(), severity))  # keep it reversed
        except Exception:
            LOGGER.exception("Unable to add a max severity limit rule `%s'", rule)


def _max_severity(keys: Tuple[str, ...], name: str) -> Severity:
    for pattern, severity in SctEvent._sct_event_types_registry.limit_rules:
        if fnmatch.filter(keys, pattern):
            return severity
    return SctEvent._sct_event_types_registry.max_severities[name]


def max_severity(event: SctEvent) -> Severity:
    return _max_severity(
        keys=(event.base, f"{event.base}.{event.type}", f"{event.base}.{event.type}.{event.subtype}", ),
        name=type(event).__name__,
    )


def print_critical_events() -> None:
    critical_event_lines = []
    for event_name in SctEvent._sct_event_types_registry.max_severities:
        if _max_severity(keys=(event_name, ), name=event_name) == Severity.CRITICAL:
            critical_event_lines.append(f"  * {event_name}")
    LOGGER.info("The run can be interrupted by following critical events:\n%s\n\n", "\n".join(critical_event_lines))


class SystemEvent(SctEvent, abstract=True):
    pass


class BaseFilter(SystemEvent, abstract=True):
    def __init__(self, severity: Severity = Severity.NORMAL):
        super().__init__(severity=severity)

        self.uuid = str(uuid.uuid4())
        self.clear_filter = False
        self.expire_time = None

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return self.uuid == other.uuid

    def cancel_filter(self) -> None:
        self.clear_filter = True
        self._ready_to_publish = True
        self.publish()

    def __enter__(self):
        self.publish()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.cancel_filter()

    def eval_filter(self, event: SctEventProtocol) -> bool:
        raise NotImplementedError()


T_log_event = TypeVar("T_log_event", bound="LogEvent")  # pylint: disable=invalid-name


@runtime_checkable
class LogEventProtocol(SctEventProtocol, Protocol[T_log_event]):
    regex: str
    node: Any
    line: Optional[str]
    line_number: int
    backtrace: Optional[str]
    raw_backtrace: Optional[str]

    def add_info(self: T_log_event, node, line: str, line_number: int) -> T_log_event:
        ...

    def clone(self: T_log_event) -> T_log_event:
        ...


class LogEvent(Generic[T_log_event], InformationalEvent, abstract=True):
    def __init__(self, regex: str, severity=Severity.ERROR):
        super().__init__(severity=severity)

        self.regex = regex
        self.node = None
        self.line = None
        self.line_number = 0
        self.backtrace = None
        self.raw_backtrace = None

        self._ready_to_publish: bool = False  # set it to True in `.add_info()'

    def add_info(self: T_log_event, node, line: str, line_number: int) -> T_log_event:
        """Update the event info from the log line.

        Set `self._ready_to_publish' flag and return self.
        """

        try:
            splitted_line = line.split()
            if "T" in splitted_line[0]:
                # Cover messages log time format. Example:
                # 2021-04-06T13:03:28  ...
                event_time = splitted_line[0]
            else:
                # Cover ScyllaBench event time format. Example:
                # 2021/04/06 13:03:28 Operation timed out for scylla_bench.test - received only 1 responses from ...
                #
                # And regular log time. Example:
                # 2021-04-06 13:03:28  ...
                event_time = " ".join(splitted_line[:2])

            self.timestamp = dateutil.parser.parse(event_time).timestamp()
        except ValueError:
            self.timestamp = time.time()
        self.node = str(node)
        self.line = line
        self.line_number = line_number

        self._ready_to_publish = True  # this property not included to the clones, so need to call `.add_info()' first.

        return self

    def clone(self: T_log_event) -> T_log_event:
        return pickle.loads(pickle.dumps(self))

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ":"
        if self.type is not None:
            fmt += " type={0.type}"
        if self.regex is not None:
            fmt += " regex={0.regex}"
        if self.line_number is not None:
            fmt += " line_number={0.line_number}"
        if self.node is not None:
            fmt += " node={0.node}"
        if self.line is not None:
            fmt += "\n{0.line}"
        if self.backtrace:
            fmt += "\n{0.backtrace}"
        elif self.raw_backtrace:
            fmt += "\n{0.raw_backtrace}"
        return fmt


class BaseStressEvent(ContinuousEvent, abstract=True):
    # pylint: disable=too-many-arguments
    @classmethod
    def add_stress_subevents(cls,
                             failure: Optional[Severity] = None,
                             error: Optional[Severity] = None,
                             timeout: Optional[Severity] = None,
                             start: Optional[Severity] = Severity.NORMAL,
                             finish: Optional[Severity] = Severity.NORMAL,
                             warning: Optional[Severity] = None) -> None:
        if failure is not None:
            cls.add_subevent_type("failure", severity=failure)
        if error is not None:
            cls.add_subevent_type("error", severity=error)
        if warning is not None:
            cls.add_subevent_type("warning", severity=warning)
        if timeout is not None:
            cls.add_subevent_type("timeout", severity=timeout)
        if start is not None:
            cls.add_subevent_type("start", severity=start)
        if finish is not None:
            cls.add_subevent_type("finish", severity=finish)


class StressEventProtocol(SctEventProtocol, Protocol):
    node: str
    stress_cmd: Optional[str]
    log_file_name: Optional[str]
    errors: Optional[List[str]]

    @property
    def errors_formatted(self):
        ...


class StressEvent(BaseStressEvent, abstract=True):
    # pylint: disable=too-many-arguments
    def __init__(self,
                 node: Any,
                 stress_cmd: Optional[str] = None,
                 log_file_name: Optional[str] = None,
                 errors: Optional[List[str]] = None,
                 severity: Severity = Severity.NORMAL,
                 publish_event: bool = True):
        super().__init__(severity=severity, publish_event=publish_event)

        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.errors = errors

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ":"
        if self.type:
            fmt += " type={0.type}"
        if self.node:
            fmt += " node={0.node}"
        if self.stress_cmd:
            fmt += "\nstress_cmd={0.stress_cmd}"
        if self.errors:
            fmt += "\nerrors:\n\n{0.errors_formatted}"
        return fmt


__all__ = ("SctEvent", "SctEventProtocol", "SystemEvent", "BaseFilter",
           "LogEvent", "LogEventProtocol", "T_log_event",
           "BaseStressEvent", "StressEvent", "StressEventProtocol",
           "add_severity_limit_rules", "max_severity", "print_critical_events",
           "ContinuousEvent", "InformationalEvent", "ContinuousEventsRegistry",
           "ContinuousEventRegistryException", "EventPeriod")

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

from __future__ import annotations

import json
import time
import uuid
import pickle
import fnmatch
import logging
from enum import Enum
from json import JSONEncoder
from types import new_class
from typing import \
    Any, Optional, Type, Dict, List, Tuple, Callable, Generic, TypeVar, Protocol, runtime_checkable
from keyword import iskeyword
from weakref import proxy as weakproxy
from datetime import datetime, timezone
from functools import partialmethod, cached_property

import yaml
import dateutil.parser

from sdcm import sct_abs_path
from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.events_processes import EventsProcessesRegistry

DEFAULT_SEVERITIES = sct_abs_path("defaults/severities.yaml")
FILTER_EVENT_DECAY_TIME = 600.0
LOGGER = logging.getLogger(__name__)


class SctEventTypesRegistry(Dict[str, Type["SctEvent"]]):
    def __init__(self, severities_conf: str = DEFAULT_SEVERITIES):
        super().__init__()
        with open(severities_conf, encoding="utf-8") as fobj:
            self.max_severities = {event_t: Severity[sev] for event_t, sev in yaml.safe_load(fobj).items()}
        self.limit_rules = []

    def __setitem__(self, key: str, value: Type[SctEvent]):
        if not value.is_abstract() and key not in self.max_severities:
            raise ValueError(f"There is no max severity configured for {key}")
        super().__setitem__(key, weakproxy(value))

    def __set_name__(self, owner: Type[SctEvent], name: str) -> None:
        self[owner.__name__] = owner  # add owner class to the registry.


class EventPeriod(Enum):
    BEGIN = "begin"
    END = "end"
    INFORMATIONAL = "one-time"  # this is not interval event. It's for one point of time event
    NOT_DEFINED = "not-set"


LOG_LEVEL_MAPPING = {
    Severity.WARNING: logging.DEBUG,
    Severity.CRITICAL: logging.CRITICAL,
    Severity.ERROR: logging.ERROR,
    Severity.DEBUG: logging.DEBUG,
    Severity.NORMAL: logging.INFO,
    Severity.UNKNOWN: logging.WARNING,
}


class SctEvent:
    _sct_event_types_registry: SctEventTypesRegistry = SctEventTypesRegistry()
    _events_processes_registry: Optional[EventsProcessesRegistry] = None

    _abstract: bool = True  # this attribute set by __init_subclass__()
    base: str = "SctEvent"  # this attribute set by __init_subclass__()
    type: Optional[str] = None  # this attribute set by add_subevent_type()
    subtype: Optional[str] = None  # this attribute set by add_subevent_type()
    subcontext: Optional[List[dict]] = None  # this attribute keeps context of event

    period_type: str = EventPeriod.NOT_DEFINED.value  # attribute possible values are from EventTypes enum

    formatter: Callable[[str, SctEvent], str] = staticmethod(str.format)

    event_timestamp: Optional[float] = None  # actual value should be set using __init__()
    source_timestamp: Optional[float] = None
    severity: Severity = Severity.UNKNOWN  # actual value should be set using __init__()

    _ready_to_publish: bool = False  # set it to True in __init__() and to False in publish() to prevent double-publish
    publish_to_grafana: bool = True
    publish_to_argus: bool = True
    save_to_files: bool = True

    def __init_subclass__(cls, abstract: bool = False):
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
        self.event_timestamp = time.time()
        self.source_timestamp: Optional[float] = None
        self.severity = severity
        self._ready_to_publish = True
        self.event_id = str(uuid.uuid4())
        self.log_level = LOG_LEVEL_MAPPING.get(severity, logging.ERROR)
        self.subcontext = []

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

    @staticmethod
    def _formatted_timestamp(timestamp: float) -> str:
        try:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except (TypeError, OverflowError, OSError,):
            LOGGER.exception("Failed to format a timestamp: %r", timestamp)
            return "0000-00-00 <UnknownTimestamp>"

    @property
    def formatted_event_timestamp(self) -> str:
        return self._formatted_timestamp(self.event_timestamp)

    @property
    def formatted_source_timestamp(self) -> str:
        return self._formatted_timestamp(self.source_timestamp)

    @property
    def timestamp(self) -> float:
        """
        Some events are sourced from the outside of SCT, for such events we try to catch original time when
          it was created and store it into source_timestamp
        This property returns source_timestamp if it is present and timestamp if it is not
        """
        return self.source_timestamp or self.event_timestamp

    @cached_property
    def subcontext_fmt(self):
        return {}

    def concatenate_subcontext_for_message(self, context_name):
        concatenated_context = []
        for event_context in self.subcontext or []:
            if isinstance(event_context, dict) and context_name in event_context:
                concatenated_context.append(event_context[context_name])
            elif isinstance(event_context, SctEvent) and context_name in event_context.subcontext_fmt:
                concatenated_context.append(event_context.subcontext_fmt[context_name])
        return ','.join(concatenated_context)

    @property
    def msgfmt(self):
        fmt = "({0.base} {0.severity}) period_type={0.period_type} event_id={0.event_id}"

        if during_nemesis := self.concatenate_subcontext_for_message(context_name='nemesis_name'):
            fmt += f" during_nemesis={during_nemesis}"

        return fmt

    def add_subcontext(self):
        from sdcm.sct_events.continuous_event import ContinuousEventsRegistry  # noqa: PLC0415
        # Add subcontext for event with ERROR and CRITICAL severity only
        if self.severity.value < 3:
            return

        # Issue https://github.com/scylladb/scylla-cluster-tests/issues/5544
        # Print out event content for understanding which event does not initialize self.subcontext
        if self.subcontext is None:
            self.subcontext = []
            LOGGER.error("'self.subcontext' was not initialized. Event: %s", self)

        # Add nemesis info if event happened during nemesis
        if self.base != "DisruptionEvent":
            running_disruption_events = ContinuousEventsRegistry().find_running_disruption_events()
            if not running_disruption_events:
                return

            # Issue https://github.com/scylladb/scylla-cluster-tests/issues/5974.
            # Prevent printing previously running nemeses
            if len(running_disruption_events) == 1:
                self.subcontext = []

            for nemesis in running_disruption_events:
                # To prevent multiplying of subcontext for LogEvent
                if nemesis not in self.subcontext:
                    self.subcontext.append(nemesis)

    def publish(self, warn_not_ready: bool = True) -> None:
        from sdcm.sct_events.events_device import get_events_main_device  # noqa: PLC0415

        if not self._ready_to_publish:
            if warn_not_ready:
                LOGGER.warning("[SCT internal warning] %s is not ready to be published", self)
            return
        get_events_main_device(_registry=self._events_processes_registry).publish_event(self)
        self._ready_to_publish = False

    def publish_or_dump(self, default_logger: Optional[logging.Logger] = None, warn_not_ready: bool = True) -> None:
        from sdcm.sct_events.events_device import get_events_main_device  # noqa: PLC0415

        if not self._ready_to_publish:
            if warn_not_ready:
                LOGGER.warning("[SCT internal warning] %s is not ready to be published", self)
            return
        try:
            proc = get_events_main_device(_registry=self._events_processes_registry)
        except RuntimeError as exc:
            LOGGER.warning("Unable to get events main device: %s", exc)
            proc = None
        if proc:
            if proc.is_alive():
                self.publish()
            else:
                from sdcm.sct_events.file_logger import get_events_logger  # noqa: PLC0415
                get_events_logger(_registry=self._events_processes_registry).write_event(self)
        elif default_logger:
            default_logger.error(str(self))
        self._ready_to_publish = False

    def dont_publish(self):
        self._ready_to_publish = False
        LOGGER.debug("%s marked to not publish", self)

    def ready_to_publish(self):
        self._ready_to_publish = True

    @cached_property
    def subcontext_fields(self) -> list:
        """
        List of event fields that will be saved in raw_event.log for subcontext event
        """
        return []

    def to_json(self, encoder: Type[JSONEncoder] = JSONEncoder) -> str:
        return json.dumps({
            **self.attribute_with_value_for_json(attributes_list=["base", "type", "subtype"]),
            **self.__getstate__(),
        }, cls=encoder)

    def attribute_with_value_for_json(self, attributes_list: list, event: SctEvent = None) -> dict:
        return dict(zip(attributes_list, [getattr(event or self, field) for field in attributes_list]))

    def __getstate__(self):
        # Remove everything from the __dict__ that starts with "_".
        attr_list = [attr for attr in self.__dict__ if not attr.startswith("_") and "subcontext" not in attr]
        attrs = self.attribute_with_value_for_json(attributes_list=attr_list)

        if subcontext := getattr(self, "subcontext"):
            attrs["subcontext"] = [self.attribute_with_value_for_json(attributes_list=event.subcontext_fields,
                                                                      event=event) for event in subcontext]
        else:
            attrs["subcontext"] = []
        return attrs

    def __str__(self):
        return self.formatter(self.msgfmt, self)

    __hash__ = False

    def __eq__(self, other):
        return (isinstance(other, type(self)) or isinstance(self, type(other))) \
            and self.__getstate__() == other.__getstate__()

    def __del__(self):
        if self._ready_to_publish:
            warning = f"[SCT internal warning] {self} has not been published or dumped, maybe you missed .publish()"
            try:
                LOGGER.warning(warning)
            except Exception as exc:  # noqa: BLE001
                print(f"Exception while printing {warning}. Full exception: {exc}")


class InformationalEvent(SctEvent, abstract=True):

    def __init__(self, severity: Severity = Severity.UNKNOWN):
        super().__init__(severity=severity)
        self.period_type = EventPeriod.INFORMATIONAL.value
        self.add_subcontext()


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
    expire_time = None
    clear_filter = False

    def __init__(self, severity: Severity = Severity.NORMAL):
        super().__init__(severity=severity)
        self.uuid = str(uuid.uuid4())

    __hash__ = False

    def __eq__(self, other):
        if not isinstance(self, type(other)):
            return False
        return self.uuid == other.uuid

    def cancel_filter(self) -> None:
        if self.expire_time is None:
            self.expire_time = time.time()
        self.clear_filter = True
        self._ready_to_publish = True
        self.publish()

    def is_deceased(self) -> bool:
        """
        When filter is expired we keep it in memory in case of getting events with
          timestamp shifted back in time
        This property is signaling that filter could be cleaned up
        """
        return self.expire_time and time.time() >= self.expire_time + FILTER_EVENT_DECAY_TIME

    def __enter__(self):
        self.publish()
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.cancel_filter()

    def eval_filter(self, event: SctEventProtocol) -> bool:
        raise NotImplementedError()


T_log_event = TypeVar("T_log_event", bound="LogEvent")


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

        # .* patterns works extremely slow, on big log message pattern evaluation can take >1s
        #   in order to keep log reading fast we must avoid them at all costs
        if regex:
            assert '.*'.count(regex) < 2
        self.regex = regex
        self.node = None
        self.line = None
        self.line_number = 0
        self.backtrace = None
        self.raw_backtrace = None
        self.subcontext = []
        self.known_issue = None

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

            self.source_timestamp = dateutil.parser.parse(event_time).replace(tzinfo=timezone.utc) .timestamp()
        except ValueError:
            pass
        self.event_timestamp = time.time()
        self.node = str(node)
        self.line = line
        self.line_number = line_number

        self.add_subcontext()

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
        if self.known_issue:
            fmt += " known_issue={0.known_issue}"
        if self.line is not None:
            fmt += "\n{0.line}"
        if self.backtrace:
            fmt += "\n{0.backtrace}"
        elif self.raw_backtrace:
            fmt += "\n{0.raw_backtrace}"
        return fmt


__all__ = ("SctEvent", "SctEventProtocol", "SystemEvent", "BaseFilter",
           "LogEvent", "LogEventProtocol", "T_log_event",
           "add_severity_limit_rules", "max_severity", "print_critical_events",
           "InformationalEvent", "EventPeriod")

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

import re
import logging
from functools import partial
from typing import Type, List, Tuple, Generic, Optional, NamedTuple, Pattern, Callable, Match

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import SctEvent, LogEvent, LogEventProtocol, T_log_event, InformationalEvent, \
    EventPeriod

from sdcm.sct_events.continuous_event import ContinuousEventsRegistry, ContinuousEvent
from sdcm.sct_events.system import TestFrameworkEvent

TOLERABLE_REACTOR_STALL: int = 1000  # ms

LOGGER = logging.getLogger(__name__)


class DatabaseLogEvent(LogEvent, abstract=True):
    NO_SPACE_ERROR: Type[LogEventProtocol]
    UNKNOWN_VERB: Type[LogEventProtocol]
    CLIENT_DISCONNECT: Type[LogEventProtocol]
    SEMAPHORE_TIME_OUT: Type[LogEventProtocol]
    SYSTEM_PAXOS_TIMEOUT: Type[LogEventProtocol]
    GATE_CLOSED: Type[LogEventProtocol]
    RESTARTED_DUE_TO_TIME_OUT: Type[LogEventProtocol]
    EMPTY_NESTED_EXCEPTION: Type[LogEventProtocol]
    DATABASE_ERROR: Type[LogEventProtocol]
    BAD_ALLOC: Type[LogEventProtocol]
    SCHEMA_FAILURE: Type[LogEventProtocol]
    RUNTIME_ERROR: Type[LogEventProtocol]
    FILESYSTEM_ERROR: Type[LogEventProtocol]
    STACKTRACE: Type[LogEventProtocol]

    # REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
    REACTOR_STALLED: Type[LogEventProtocol]
    KERNEL_CALLSTACK: Type[LogEventProtocol]
    BACKTRACE: Type[LogEventProtocol]
    ABORTING_ON_SHARD: Type[LogEventProtocol]
    SEGMENTATION: Type[LogEventProtocol]
    INTEGRITY_CHECK: Type[LogEventProtocol]
    SUPPRESSED_MESSAGES: Type[LogEventProtocol]
    stream_exception: Type[LogEventProtocol]


MILLI_RE = re.compile(r"(\d+) ms")


# pylint: disable=too-few-public-methods
class ReactorStalledMixin(Generic[T_log_event]):
    tolerable_reactor_stall: int = TOLERABLE_REACTOR_STALL

    def add_info(self: T_log_event, node, line: str, line_number: int) -> T_log_event:
        try:
            # Dynamically handle reactor stalls severity.
            if int(MILLI_RE.findall(line)[0]) >= self.tolerable_reactor_stall:
                self.severity = Severity.ERROR
        except (ValueError, IndexError, ):
            LOGGER.warning("failed to read REACTOR_STALLED line=[%s] ", line)
        return super().add_info(node=node, line=line, line_number=line_number)


DatabaseLogEvent.add_subevent_type("NO_SPACE_ERROR", severity=Severity.ERROR,
                                   regex="No space left on device")
DatabaseLogEvent.add_subevent_type("UNKNOWN_VERB", severity=Severity.WARNING,
                                   regex="unknown verb exception")
DatabaseLogEvent.add_subevent_type("CLIENT_DISCONNECT", severity=Severity.WARNING,
                                   regex=r"cql_server - exception while processing connection:")
DatabaseLogEvent.add_subevent_type("SEMAPHORE_TIME_OUT", severity=Severity.WARNING,
                                   regex="semaphore_timed_out")
# This scylla WARNING includes "exception" word and reported as ERROR. To prevent it I add the subevent below and locate
# it before DATABASE_ERROR. Message example:
# storage_proxy - Failed to apply mutation from 10.0.2.108#8: exceptions::mutation_write_timeout_exception
# (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.)
DatabaseLogEvent.add_subevent_type("SYSTEM_PAXOS_TIMEOUT", severity=Severity.WARNING,
                                   regex="(mutation_write_|Operation timed out for system.paxos|"
                                         "Operation failed for system.paxos)")
DatabaseLogEvent.add_subevent_type("GATE_CLOSED", severity=Severity.WARNING,
                                   regex="exception \"gate closed\" in no_wait handler ignored")
DatabaseLogEvent.add_subevent_type("RESTARTED_DUE_TO_TIME_OUT", severity=Severity.WARNING,
                                   regex="scylla-server.service.*State 'stop-sigterm' timed out")
DatabaseLogEvent.add_subevent_type("EMPTY_NESTED_EXCEPTION", severity=Severity.WARNING,
                                   regex=r"cql_server - exception while processing connection: "
                                         r"seastar::nested_exception \(seastar::nested_exception\)$")
DatabaseLogEvent.add_subevent_type("DATABASE_ERROR", severity=Severity.ERROR,
                                   regex="Exception ")
DatabaseLogEvent.add_subevent_type("BAD_ALLOC", severity=Severity.ERROR,
                                   regex="std::bad_alloc")
DatabaseLogEvent.add_subevent_type("SCHEMA_FAILURE", severity=Severity.ERROR,
                                   regex="Failed to load schema version")
DatabaseLogEvent.add_subevent_type("RUNTIME_ERROR", severity=Severity.ERROR,
                                   regex="std::runtime_error")
DatabaseLogEvent.add_subevent_type("FILESYSTEM_ERROR", severity=Severity.ERROR,
                                   regex="filesystem_error")
DatabaseLogEvent.add_subevent_type("STACKTRACE", severity=Severity.ERROR,
                                   regex="stacktrace")

# REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
DatabaseLogEvent.add_subevent_type("REACTOR_STALLED", mixin=ReactorStalledMixin, severity=Severity.DEBUG,
                                   regex="Reactor stalled")
DatabaseLogEvent.add_subevent_type("KERNEL_CALLSTACK", severity=Severity.DEBUG,
                                   regex="kernel callstack: 0x.{16}")
DatabaseLogEvent.add_subevent_type("BACKTRACE", severity=Severity.ERROR,
                                   regex="backtrace")
DatabaseLogEvent.add_subevent_type("ABORTING_ON_SHARD", severity=Severity.ERROR,
                                   regex="Aborting on shard")
DatabaseLogEvent.add_subevent_type("SEGMENTATION", severity=Severity.ERROR,
                                   regex="segmentation")
DatabaseLogEvent.add_subevent_type("INTEGRITY_CHECK", severity=Severity.ERROR,
                                   regex="integrity check failed")
DatabaseLogEvent.add_subevent_type("SUPPRESSED_MESSAGES", severity=Severity.WARNING,
                                   regex="journal: Suppressed")
DatabaseLogEvent.add_subevent_type("stream_exception", severity=Severity.ERROR,
                                   regex="stream_exception")


SYSTEM_ERROR_EVENTS = (
    DatabaseLogEvent.NO_SPACE_ERROR(),
    DatabaseLogEvent.UNKNOWN_VERB(),
    DatabaseLogEvent.CLIENT_DISCONNECT(),
    DatabaseLogEvent.SEMAPHORE_TIME_OUT(),
    DatabaseLogEvent.SYSTEM_PAXOS_TIMEOUT(),
    DatabaseLogEvent.GATE_CLOSED(),
    DatabaseLogEvent.RESTARTED_DUE_TO_TIME_OUT(),
    DatabaseLogEvent.EMPTY_NESTED_EXCEPTION(),
    DatabaseLogEvent.DATABASE_ERROR(),
    DatabaseLogEvent.BAD_ALLOC(),
    DatabaseLogEvent.SCHEMA_FAILURE(),
    DatabaseLogEvent.RUNTIME_ERROR(),
    DatabaseLogEvent.FILESYSTEM_ERROR(),
    DatabaseLogEvent.STACKTRACE(),

    # REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
    DatabaseLogEvent.REACTOR_STALLED(),
    DatabaseLogEvent.KERNEL_CALLSTACK(),
    DatabaseLogEvent.BACKTRACE(),
    DatabaseLogEvent.ABORTING_ON_SHARD(),
    DatabaseLogEvent.SEGMENTATION(),
    DatabaseLogEvent.INTEGRITY_CHECK(),
    DatabaseLogEvent.SUPPRESSED_MESSAGES(),
    DatabaseLogEvent.stream_exception(),
)
SYSTEM_ERROR_EVENTS_PATTERNS: List[Tuple[re.Pattern, LogEventProtocol]] = \
    [(re.compile(event.regex, re.IGNORECASE), event) for event in SYSTEM_ERROR_EVENTS]
BACKTRACE_RE = re.compile(r'(?P<other_bt>/lib.*?\+0x[0-f]*\n)|(?P<scylla_bt>0x[0-f]*\n)', re.IGNORECASE)


class ScyllaHelpErrorEvent(SctEvent, abstract=True):
    duplicate: Type[SctEventProtocol]
    filtered: Type[SctEventProtocol]
    message: str

    def __init__(self, message: Optional[str] = None, severity=Severity.ERROR):
        super().__init__(severity=severity)

        # Don't include `message' to the state if it's None.
        if message is not None:
            self.message = message

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ": type={0.type}"
        if hasattr(self, "message"):
            fmt += " message={0.message}"
        return fmt


ScyllaHelpErrorEvent.add_subevent_type("duplicate")
ScyllaHelpErrorEvent.add_subevent_type("filtered")


class IndexSpecialColumnErrorEvent(InformationalEvent):
    def __init__(self, message: str, severity: Severity = Severity.ERROR):
        super().__init__(severity=severity)

        self.message = message

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"


class ScyllaServerEventPattern(NamedTuple):
    pattern: Pattern
    period_func: Callable


class ScyllaServerEventPatternFuncs(NamedTuple):
    pattern: Pattern
    event_class: Type[ContinuousEvent]
    period_func: Callable


class ScyllaDatabaseContinuousEvent(ContinuousEvent, abstract=True):
    begin_pattern: str = NotImplemented
    end_pattern: str = NotImplemented

    def __init__(self, node: str, shard: int = None, severity=Severity.UNKNOWN, publish_event=True):
        super().__init__(severity=severity, publish_event=publish_event)
        self.node = node
        self.shard = shard

    @property
    def msgfmt(self):
        node = " node={0.node}" if self.node else ""
        shard = " shard={0.shard}" if self.shard is not None else ""
        fmt = f"{super().msgfmt}{node}{shard}"

        return fmt


class ScyllaServerStatusEvent(ScyllaDatabaseContinuousEvent):
    begin_pattern = r'Starting Scylla Server'
    end_pattern = r'Stopping Scylla Server|Failed to start Scylla Server'

    def __init__(self, node: str, severity=Severity.NORMAL, **__):
        super().__init__(node=node, severity=severity)


class BootstrapEvent(ScyllaDatabaseContinuousEvent):
    begin_pattern = r'Starting to bootstrap'
    end_pattern = r'Bootstrap succeeded'

    def __init__(self, node: str, severity=Severity.NORMAL, **__):
        super().__init__(node=node, severity=severity)


class FullScanEvent(ScyllaDatabaseContinuousEvent):
    def __init__(self, node: str, ks_cf: str, message: Optional[str] = None, severity=Severity.NORMAL, **__):
        super().__init__(node=node, severity=severity)
        self.ks_cf = ks_cf
        self.message = message

    @property
    def msgfmt(self):
        fmt = super().msgfmt + " select_from={0.ks_cf}"
        if self.message:
            fmt += " message={0.message}"
        return fmt


class RepairEvent(ScyllaDatabaseContinuousEvent):
    begin_pattern = r'Repair 1 out of \d+ ranges, id=\[id=\d+, uuid=[\d\w-]{36}\], shard=(?P<shard>\d+)'
    end_pattern = r'repair id \[id=\d+, uuid=[\d\w-]{36}\] on shard (?P<shard>\d+) completed'

    def __init__(self, node: str, shard: int, severity=Severity.NORMAL, **__):
        super().__init__(node=node, shard=shard, severity=severity)
        self.log_level = logging.DEBUG


class CompactionEvent(ScyllaDatabaseContinuousEvent):
    begin_pattern = r'\[shard (?P<shard>\d+)\] compaction - \[Compact (?P<table>\w+.\w+) ' \
                    r'(?P<compaction_process_id>.+)\] Compacting '
    end_pattern = r'\[shard (?P<shard>\d+)\] compaction - \[Compact (?P<table>\w+.\w+) ' \
                  r'(?P<compaction_process_id>.+)\] Compacted '

    def __init__(self, node: str, shard: int, table: str, compaction_process_id: str,  # pylint: disable=too-many-arguments
                 severity=Severity.NORMAL, **__):
        super().__init__(node=node, shard=shard, severity=severity)
        self.table = table
        self.compaction_process_id = compaction_process_id
        self.log_level = logging.DEBUG

    @property
    def msgfmt(self):
        table = " table={0.table}" if self.table is not None else ""
        compaction_process_id = " compaction_process_id={0.compaction_process_id}" \
            if self.compaction_process_id is not None else ""
        fmt = f"{super().msgfmt}{table}{compaction_process_id}"

        return fmt


class JMXServiceEvent(ScyllaDatabaseContinuousEvent):
    begin_pattern = r'Starting the JMX server'
    end_pattern = r'Stopped Scylla JMX'

    def __init__(self, node: str, severity=Severity.NORMAL, **__):
        super().__init__(node=node, severity=severity)


SCYLLA_DATABASE_CONTINUOUS_EVENTS = [
    ScyllaServerStatusEvent,
    BootstrapEvent,
    RepairEvent,
    JMXServiceEvent,
    CompactionEvent
]


def get_pattern_to_event_to_func_mapping(node: str) \
        -> List[ScyllaServerEventPatternFuncs]:
    """
    This function maps regex patterns, event classes and begin / end
    functions into ScyllaServerEventPatternFuncs object. Helper
    functions are delegated to find the event that should be the
    target of the start / stop action, or creating a new one.
    """
    mapping = []
    event_registry = ContinuousEventsRegistry()

    def _add_event(event_type: Type[ScyllaDatabaseContinuousEvent], match: Match):
        kwargs = match.groupdict()
        if "shard" in kwargs:
            kwargs["shard"] = int(kwargs["shard"])

        new_event = event_type(node=node, **kwargs)
        new_event.begin_event()

    def _end_event(event_type: Type[ScyllaDatabaseContinuousEvent], match: Match):
        kwargs = match.groupdict()

        event_filter = event_registry.get_registry_filter()
        event_filter \
            .filter_by_node(node=node) \
            .filter_by_type(event_type=event_type) \
            .filter_by_period(period_type=EventPeriod.BEGIN.value)

        if kwargs.get("shard"):
            event_filter.filter_by_shard(int(kwargs["shard"]))

        if kwargs.get("table"):
            event_filter.filter_by_attr(base="CompactionEvent", table=kwargs["table"])

        if kwargs.get("compaction_process_id"):
            event_filter.filter_by_attr(base="CompactionEvent", compaction_process_id=kwargs["compaction_process_id"])

        begun_events = event_filter.get_filtered()

        if not begun_events:
            TestFrameworkEvent(
                source=event_type.__name__,
                message="Did not find any events of type {event_type}"
                        " with period type {period_type}, event data: {event_data}".format(
                            event_type=event_type,
                            period_type=EventPeriod.BEGIN.value,
                            event_data=kwargs,
                        ),
                severity=Severity.ERROR
            ).publish_or_dump()
            return

        if len(begun_events) > 1:
            LOGGER.debug("Found %s events of type %s with period %s. "
                         "Will apply the function to most recent event by default.",
                         len(begun_events), event_type, EventPeriod.BEGIN.value)
        event = begun_events[-1]
        event.end_event()

    for event in SCYLLA_DATABASE_CONTINUOUS_EVENTS:
        mapping.append(ScyllaServerEventPatternFuncs(pattern=re.compile(event.begin_pattern),
                                                     event_class=event,
                                                     period_func=partial(_add_event, event_type=event)))
        mapping.append(ScyllaServerEventPatternFuncs(pattern=re.compile(event.end_pattern), event_class=event,
                                                     period_func=partial(_end_event, event_type=event)))

    return mapping

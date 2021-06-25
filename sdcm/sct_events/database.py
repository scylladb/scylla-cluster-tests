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
from typing import Type, List, Tuple, Generic, Optional

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import SctEvent, LogEvent, LogEventProtocol, T_log_event


TOLERABLE_REACTOR_STALL: int = 1000  # ms

LOGGER = logging.getLogger(__name__)


class DatabaseLogEvent(LogEvent, abstract=True):
    NO_SPACE_ERROR: Type[LogEventProtocol]
    UNKNOWN_VERB: Type[LogEventProtocol]
    CLIENT_DISCONNECT: Type[LogEventProtocol]
    SEMAPHORE_TIME_OUT: Type[LogEventProtocol]
    SYSTEM_PAXOS_TIMEOUT: Type[LogEventProtocol]
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
    BACKTRACE: Type[LogEventProtocol]
    ABORTING_ON_SHARD: Type[LogEventProtocol]
    SEGMENTATION: Type[LogEventProtocol]
    INTEGRITY_CHECK: Type[LogEventProtocol]
    BOOT: Type[LogEventProtocol]
    STOP: Type[LogEventProtocol]
    SUPPRESSED_MESSAGES: Type[LogEventProtocol]
    stream_exception: Type[LogEventProtocol]
    POWER_OFF: Type[LogEventProtocol]


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
                                   regex=r"\!INFO.*cql_server - exception while processing connection:.*")
DatabaseLogEvent.add_subevent_type("SEMAPHORE_TIME_OUT", severity=Severity.WARNING,
                                   regex="semaphore_timed_out")
# This scylla WARNING includes "exception" word and reported as ERROR. To prevent it I add the subevent below and locate
# it before DATABASE_ERROR. Message example:
# storage_proxy - Failed to apply mutation from 10.0.2.108#8: exceptions::mutation_write_timeout_exception
# (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.)
DatabaseLogEvent.add_subevent_type("SYSTEM_PAXOS_TIMEOUT", severity=Severity.WARNING,
                                   regex=".*mutation_write_*|.*Operation timed out for system.paxos.*|"
                                         ".*Operation failed for system.paxos.*")
DatabaseLogEvent.add_subevent_type("RESTARTED_DUE_TO_TIME_OUT", severity=Severity.WARNING,
                                   regex="scylla-server.service.*State 'stop-sigterm' timed out.*Killing")
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
DatabaseLogEvent.add_subevent_type("BACKTRACE", severity=Severity.ERROR,
                                   regex="backtrace")
DatabaseLogEvent.add_subevent_type("ABORTING_ON_SHARD", severity=Severity.ERROR,
                                   regex="Aborting on shard")
DatabaseLogEvent.add_subevent_type("SEGMENTATION", severity=Severity.ERROR,
                                   regex="segmentation")
DatabaseLogEvent.add_subevent_type("INTEGRITY_CHECK", severity=Severity.ERROR,
                                   regex="integrity check failed")
DatabaseLogEvent.add_subevent_type("BOOT", severity=Severity.NORMAL,
                                   regex="Starting Scylla Server")
DatabaseLogEvent.add_subevent_type("STOP", severity=Severity.NORMAL,
                                   regex="Stopping Scylla Server")
DatabaseLogEvent.add_subevent_type("SUPPRESSED_MESSAGES", severity=Severity.WARNING,
                                   regex="journal: Suppressed")
DatabaseLogEvent.add_subevent_type("stream_exception", severity=Severity.ERROR,
                                   regex="stream_exception")
DatabaseLogEvent.add_subevent_type("POWER_OFF", severity=Severity.CRITICAL, regex="Powering Off")


SYSTEM_ERROR_EVENTS = (
    DatabaseLogEvent.NO_SPACE_ERROR(),
    DatabaseLogEvent.UNKNOWN_VERB(),
    DatabaseLogEvent.CLIENT_DISCONNECT(),
    DatabaseLogEvent.SEMAPHORE_TIME_OUT(),
    DatabaseLogEvent.SYSTEM_PAXOS_TIMEOUT(),
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
    DatabaseLogEvent.BACKTRACE(),
    DatabaseLogEvent.ABORTING_ON_SHARD(),
    DatabaseLogEvent.SEGMENTATION(),
    DatabaseLogEvent.INTEGRITY_CHECK(),
    DatabaseLogEvent.BOOT(),
    DatabaseLogEvent.STOP(),
    DatabaseLogEvent.SUPPRESSED_MESSAGES(),
    DatabaseLogEvent.stream_exception(),
    DatabaseLogEvent.POWER_OFF(),
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


class FullScanEvent(SctEvent, abstract=True):
    start: Type[SctEventProtocol]
    finish: Type[SctEventProtocol]

    message: str

    def __init__(self, db_node_ip: str, ks_cf, message: Optional[str] = None, severity=Severity.NORMAL):
        super().__init__(severity=severity)

        self.db_node_ip = db_node_ip
        self.ks_cf = ks_cf

        # Don't include `message' to the state if it's None.
        if message is not None:
            self.message = message

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ": type={0.type} select_from={0.ks_cf} on db_node={0.db_node_ip}"
        if hasattr(self, "message"):
            fmt += " message={0.message}"
        return fmt


FullScanEvent.add_subevent_type("start")
FullScanEvent.add_subevent_type("finish")


class IndexSpecialColumnErrorEvent(SctEvent):
    def __init__(self, message: str, severity: Severity = Severity.ERROR):
        super().__init__(severity=severity)

        self.message = message

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"

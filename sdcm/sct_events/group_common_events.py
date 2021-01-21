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

from contextlib import contextmanager, ExitStack

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.filters import DbEventsFilter, EventsSeverityChangerFilter, EventsFilter
from sdcm.sct_events.loaders import YcsbStressEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.monitors import PrometheusAlertManagerEvent


@contextmanager
def ignore_alternator_client_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=PrometheusAlertManagerEvent,
            regex=".*YCSBTooManyErrors.*",
            extra_time_to_expiration=60,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=PrometheusAlertManagerEvent,
            regex=".*YCSBTooManyVerifyErrors.*",
            extra_time_to_expiration=60,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=YcsbStressEvent,
            regex=r".*Cannot achieve consistency level.*",
            extra_time_to_expiration=30,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=YcsbStressEvent,
            regex=r".*Operation timed out.*",
            extra_time_to_expiration=30,
        ))
        yield


@contextmanager
def ignore_operation_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=LogEvent,
            regex=r".*Operation timed out.*",
            extra_time_to_expiration=30,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=LogEvent,
            regex=r".*Operation failed for system.paxos.*",
            extra_time_to_expiration=30,
        ))
        yield


@contextmanager
def ignore_upgrade_schema_errors():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Failed to load schema",
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.SCHEMA_FAILURE,
            line="Failed to load schema",
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Failed to pull schema",
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.RUNTIME_ERROR,
            line="Failed to load schema",
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Could not retrieve CDC streams with timestamp",
        ))
        # This error message occurs during version rating only for the Drain operating system.
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="cql_server - exception while processing connection: seastar::nested_exception "
                 "(seastar::nested_exception)",
        ))
        yield


@contextmanager
def ignore_no_space_errors(node):
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.NO_SPACE_ERROR,
            node=node,
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.BACKTRACE,
            line="No space left on device",
            node=node,
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="No space left on device",
            node=node,
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.FILESYSTEM_ERROR,
            line="No space left on device",
            node=node,
        ))
        yield


@contextmanager
def ignore_mutation_write_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=LogEvent,
            regex=r".*mutation_write_*",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=LogEvent,
            regex=r".*Operation timed out for system.paxos.*",
            extra_time_to_expiration=30,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=LogEvent,
            regex=r".*Operation failed for system.paxos.*",
            extra_time_to_expiration=30,
        ))
        yield


@contextmanager
def ignore_ycsb_connection_refused():
    with EventsFilter(event_class=YcsbStressEvent, regex='.*Unable to execute HTTP request: Connection refused.*'):
        yield


@contextmanager
def ignore_scrub_invalid_errors():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Skipping invalid clustering row fragment",
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Skipping invalid partition",
        ))
        yield

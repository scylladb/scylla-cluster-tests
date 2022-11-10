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

from contextlib import contextmanager, ExitStack, ContextDecorator
from functools import wraps
from typing import ContextManager, Callable, Sequence

from sdcm.sct_events import Severity
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
            event_class=DatabaseLogEvent,
            regex=r".*Operation timed out.*",
            extra_time_to_expiration=30,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
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
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="No such file or directory",
            node=node,
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.FILESYSTEM_ERROR,
            line="No such file or directory",
            node=node,
        ))
        yield


@contextmanager
def ignore_mutation_write_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*mutation_write_",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*Operation timed out for system.paxos.*",
            extra_time_to_expiration=30,
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*Operation failed for system.paxos.*",
            extra_time_to_expiration=30,
        ))
        yield


@contextmanager
def ignore_ycsb_connection_refused():
    with EventsFilter(event_class=YcsbStressEvent, regex='.*Unable to execute HTTP request: .*Connection refused'):
        yield


@contextmanager
def ignore_abort_requested_errors():
    with DbEventsFilter(db_event=DatabaseLogEvent.RUNTIME_ERROR, line="abort requested"):
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


@contextmanager
def ignore_compaction_stopped_exceptions():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="was stopped due to: user request"
        ))
        yield


@contextmanager
def ignore_view_error_gate_closed_exception():
    with EventsFilter(event_class=DatabaseLogEvent, regex='.*view - Error applying view update.*gate_closed_exception'):
        yield


@contextmanager
def ignore_stream_mutation_fragments_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*Failed to handle STREAM_MUTATION_FRAGMENTS.*",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r"storage_proxy \- exception during mutation write.*gate_closed_exception",
            extra_time_to_expiration=30
        ))
        yield


def decorate_with_context(context_list: list[Callable | ContextManager] | Callable | ContextManager):
    """
    helper to decorate a function to run with a list of callables that return context managers
    """
    context_list = context_list if isinstance(context_list, Sequence) else [context_list]
    for context in context_list:
        assert callable(context) or isinstance(context, ContextManager), \
            f"{context} - Should be contextmanager or callable that returns one"
        assert not isinstance(context, ContextDecorator), \
            f"{context} - ContextDecorator shouldn't be used, since they are usable only one"

    def inner_decorator(func):
        @wraps(func)
        def inner_func(*args, **kwargs):
            with ExitStack() as stack:
                for context_manager in context_list:
                    if callable(context_manager):
                        cmanger = context_manager()
                        assert isinstance(cmanger, ContextManager)
                        stack.enter_context(cmanger)
                    else:
                        stack.enter_context(context_manager)

                return func(*args, **kwargs)
        return inner_func
    return inner_decorator

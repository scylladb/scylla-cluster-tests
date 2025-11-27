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

from sdcm.cluster import TestConfig
from sdcm.sct_events import Severity
from sdcm.sct_events.filters import DbEventsFilter, EventsSeverityChangerFilter, EventsFilter
from sdcm.sct_events.loaders import YcsbStressEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.monitors import PrometheusAlertManagerEvent
from sdcm.utils.issues import SkipPerIssues


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
def ignore_topology_change_coordinator_errors():
    with ExitStack() as stack:
        if SkipPerIssues(
                issues=[
                    "https://github.com/scylladb/scylladb/issues/20754",
                    "https://github.com/scylladb/scylladb/issues/20950",
                ],
                params=TestConfig().tester_obj().params,
        ):
            # @piodul:
            #
            #   The upgrade-to-view-build-status-on-raft procedure runs right after the VIEW_BUILD_STATUS_ON_GROUP0
            #   feature is enabled.  Enabling a cluster feature on raft requires all nodes to be alive.  Most likely
            #   the node being restarted wasn't yet seen as such, but the upgrade procedure started anyway.
            #
            #   The error is not critical.  The topology coordinator node will retry the operation in short intervals
            #   until it succeeds.  The operation shouldn't have any harmful side effects if it fails, so it's mostly
            #   bad UX because we can avoid the busywork and error messages by appropriately delaying the moment when
            #   the operation is executed.
            #
            #   Therefore, it is OK to ignore this particular error until a proper fix is merged.
            stack.enter_context(DbEventsFilter(
                db_event=DatabaseLogEvent.DATABASE_ERROR,
                line=r".*raft_topology - topology change coordinator fiber got error exceptions::unavailable_exception "
                     r"\(Cannot achieve consistency level for cl ALL\.",
            ))
            stack.enter_context(DbEventsFilter(
                db_event=DatabaseLogEvent.RUNTIME_ERROR,
                line=r".*raft_topology - drain rpc failed, proceed to fence old writes:.*connection is closed",
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
        stack.enter_context(ignore_topology_change_coordinator_errors())
        yield


@contextmanager
def ignore_no_space_errors(node):
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.NO_SPACE_ERROR,
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.BACKTRACE,
            line="No space left on device",
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="No space left on device",
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.FILESYSTEM_ERROR,
            line="No space left on device",
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="No such file or directory",
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.FILESYSTEM_ERROR,
            line="No such file or directory",
            node=node,
            extra_time_to_expiration=1800,  # 30min
        ))
        yield


@contextmanager
def ignore_disk_quota_exceeded_errors(node):
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.BACKTRACE,
            line="Disk quota exceeded",
            node=node,
            extra_time_to_expiration=3600,  # one hour
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.FILESYSTEM_ERROR,
            line="Disk quota exceeded",
            node=node,
            extra_time_to_expiration=3600,  # one hour
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DATABASE_ERROR,
            line="Disk quota exceeded",
            node=node,
            extra_time_to_expiration=3600,  # one hour
        ))
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.DISK_ERROR,
            line="Disk quota exceeded",
            node=node,
            extra_time_to_expiration=3600,  # one hour
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
def ignore_reactor_stall_errors():
    with EventsSeverityChangerFilter(new_severity=Severity.WARNING, event_class=DatabaseLogEvent,
                                     regex=r".*Reactor stalled for"):
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
def ignore_large_collection_warning():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.WARNING,
            line="Writing large collection"
        ))
        yield


@contextmanager
def ignore_max_memory_for_unlimited_query_soft_limit():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(
            db_event=DatabaseLogEvent.WARNING,
            line="mutation_partition - Memory usage of unpaged query exceeds soft limit"
        ))
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
            regex=r".*storage_proxy \- exception during mutation write.*gate_closed_exception",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*storage_service \- .*Operation failed",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*node_ops - decommission.*Operation failed.*std::runtime_error.*aborted_by_user=true, failed_because=N\/A",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*node_ops - decommission.*Operation failed.*std::runtime_error.*failed_because=seastar::rpc::closed_error",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*node_ops - decommission.*Operation failed.*seastar::abort_requested_exception",
            extra_time_to_expiration=30
        ))
        yield


@contextmanager
def ignore_raft_topology_cmd_failing():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - raft_topology_cmd.* failed with: seastar::abort_requested_exception \(abort requested\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - raft_topology_cmd failed with: raft::request_aborted \(Request is aborted by a caller\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is decommissioning\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is rebuilding\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is replacing\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - send_raft_topology_cmd\(stream_ranges\) failed with exception \(node state is bootstrapping\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - topology change coordinator fiber got error.*connection is closed",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - topology change coordinator fiber got error.*failed status returned from",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - raft_topology_cmd.*failed with: service::raft_group_not_found",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - raft_topology_cmd.*failed with: service::wait_for_ip_timeout",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - raft_topology_cmd.*failed with:.*abort requested",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - drain rpc failed, proceed to fence old writes:.*failed status returned from",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - drain rpc failed, proceed to fence old writes.*connection is closed",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - topology change coordinator fiber got error std::runtime_error.*connection is closed",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft_topology - transition_state::write_both_read_new, global_token_metadata_barrier failed.*connection is closed",
            extra_time_to_expiration=30
        ))
        yield


@contextmanager
def ignore_raft_transport_failing():
    # Example of scenario when we should ignore this error: https://github.com/scylladb/scylladb/issues/15713#issuecomment-2217376031
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft::transport_error \(.*rpc::closed_error \(connection is closed\)",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*raft - .* Transferring snapshot.*not found",
            extra_time_to_expiration=30
        ))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r"raft - \[[0-9a-f-]+\] Transferring snapshot to [0-9a-f-]+ failed with: seastar::rpc::remote_verb_error",
            extra_time_to_expiration=30
        ))
        yield


@contextmanager
def ignore_take_snapshot_failing():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*api - take_snapshot failed: std::filesystem::__cxx11::filesystem_error.*No such file or directory",
            extra_time_to_expiration=60))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*api - take_snapshot failed: std::runtime_error \(Keyspace.*snapshot.*already exists",
            extra_time_to_expiration=60))
        yield


@contextmanager
def ignore_ipv6_failure_to_assign():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*init - Startup failed:.*Cannot assign requested address",
            extra_time_to_expiration=60))
        stack.enter_context(EventsSeverityChangerFilter(
            new_severity=Severity.WARNING,
            event_class=DatabaseLogEvent,
            regex=r".*init - Could not start Prometheus API server.*Cannot assign requested address",
            extra_time_to_expiration=60))
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


def decorate_with_context_if_issues_open(
        contexts:  list[Callable | ContextManager] | Callable | ContextManager, issue_refs: list[str]):
    """
    Helper to decorate a function, to apply the provided contexts only if referenced GitHub issues are opened.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if SkipPerIssues(issue_refs, TestConfig().tester_obj().params):
                decorated_func = decorate_with_context(contexts)(func)
                return decorated_func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator

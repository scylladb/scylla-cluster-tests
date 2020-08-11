from contextlib import contextmanager, ExitStack

from sdcm.sct_events import DbEventsFilter, Severity, DatabaseLogEvent, EventsSeverityChangerFilter, YcsbStressEvent, \
    PrometheusAlertManagerEvent


@contextmanager
def ignore_alternator_client_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=PrometheusAlertManagerEvent, regex=".*YCSBTooManyErrors.*", severity=Severity.WARNING,
            extra_time_to_expiration=60))
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=PrometheusAlertManagerEvent, regex=".*YCSBTooManyVerifyErrors.*", severity=Severity.WARNING,
            extra_time_to_expiration=60))
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=YcsbStressEvent, regex=r".*Cannot achieve consistency level.*", severity=Severity.WARNING,
            extra_time_to_expiration=30))
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=YcsbStressEvent, regex=r".*Operation timed out.*", severity=Severity.WARNING,
            extra_time_to_expiration=30))
        yield


@contextmanager
def ignore_operation_errors():
    with ExitStack() as stack:
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=DatabaseLogEvent, regex=r".*Operation timed out.*", severity=Severity.WARNING,
            extra_time_to_expiration=30))
        stack.enter_context(EventsSeverityChangerFilter(
            event_class=DatabaseLogEvent, regex=r'.*Operation failed for system.paxos.*', severity=Severity.WARNING,
            extra_time_to_expiration=30))
        yield


@contextmanager
def ignore_upgrade_schema_errors():
    with ExitStack() as stack:
        stack.enter_context(DbEventsFilter(type='DATABASE_ERROR', line='Failed to load schema'))
        stack.enter_context(DbEventsFilter(type='SCHEMA_FAILURE', line='Failed to load schema'))
        stack.enter_context(DbEventsFilter(type='DATABASE_ERROR', line='Failed to pull schema'))
        stack.enter_context(DbEventsFilter(type='RUNTIME_ERROR', line='Failed to load schema'))
        yield


@contextmanager
def ignore_no_space_errors(node):
    with DbEventsFilter(type='NO_SPACE_ERROR', node=node), \
            DbEventsFilter(type='BACKTRACE', line='No space left on device', node=node), \
            DbEventsFilter(type='DATABASE_ERROR', line='No space left on device', node=node), \
            DbEventsFilter(type='FILESYSTEM_ERROR', line='No space left on device', node=node):
        yield

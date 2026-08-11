import logging

from sdcm.sct_events import Severity
from sdcm.sct_events.system import TestFrameworkEvent
from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionHandler, ExceptionStrategy

LOGGER = logging.getLogger(__name__)

__all__ = [
    "EventExceptionHandler",
]


class EventExceptionHandler(ExceptionHandler):
    """Publish collector failures as SCT events so they are visible in Argus, not only in sct.log.

    The default :class:`ExceptionHandler` only logs, which means a collector that fails for a whole
    run leaves no trace outside ``sct.log``. This handler is the opt-in alternative: pass it to a
    collector that runs inside an SCT test and its failures become ``TestFrameworkEvent``s.

    It lives here rather than in the manager so the generic diagnostics layer
    (:mod:`sdcm.utils.diagnostic_collector` and :mod:`sdcm.utils.diagnostic_collector.manager`)
    keeps no dependency on the SCT event bus, and so unit tests need no event device.

    Severity defaults to ``WARNING``: diagnostics are best-effort, and a transient CQL failure while
    collecting must not fail an otherwise healthy test run. Pass ``severity=Severity.ERROR`` for a
    collector whose data the test actually depends on.
    """

    def __init__(
        self,
        severity: Severity = Severity.WARNING,
        strategy: ExceptionStrategy = ExceptionStrategy.CONTINUE,
    ):
        self.severity = severity
        self.strategy = strategy

    def _publish(self, collector: DiagnosticCollector, exception: Exception, stage: str) -> ExceptionStrategy:
        # publish_or_dump() falls back to plain logging when no events device is running, so the
        # same handler works inside a test, in unit tests and in any non-SCT consumer.
        TestFrameworkEvent(
            source=collector.name,
            source_method=f"diagnostics.{stage}",
            message=f"Diagnostics collector '{collector.name}' failed during {stage}",
            exception=exception,
            severity=self.severity,
        ).publish_or_dump(default_logger=LOGGER)
        return self.strategy

    def handle_exception_during_collecting(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Publish an event for a failure in the collecting stage."""
        return self._publish(collector, exception, "collect")

    def handle_exception_during_storing(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Publish an event for a failure in the storing stage."""
        return self._publish(collector, exception, "store")

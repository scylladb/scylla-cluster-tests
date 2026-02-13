import logging

from dataclasses import dataclass
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Any, Sequence

from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionHandler, ExceptionStrategy

LOGGER = logging.getLogger(__name__)


@dataclass
class DiagnosticResult:
    """Container for Diagnostic results."""

    collector_name: str
    timestamp: datetime
    data: Any
    collected: bool
    stored: bool
    error: Exception | None = None


class DiagnosticManager(Thread):
    """Manages the collection and storage of diagnostics data from multiple collectors."""

    def __init__(
        self,
        collectors: Sequence[DiagnosticCollector],
        interval: float = 60.0,
        exception_handler: ExceptionHandler | None = None,
        name: str = "DiagnosticManager",
    ):
        super().__init__(name=name)
        self.daemon = True
        self._collectors = collectors
        self._interval = interval
        self._exception_handler = exception_handler or ExceptionHandler()
        self._stop_event = Event()
        self._results: list[DiagnosticResult] = []
        self._results_lock = Lock()
        self._is_collecting = Event()

    @property
    def is_collecting(self) -> bool:
        return self._is_collecting.is_set()

    def run(self):
        LOGGER.debug("Starting collect diagnostics data every %s seconds", self._interval)
        while not self._stop_event.is_set():
            LOGGER.debug("Waiting for next collection interval...")
            if self._stop_event.wait(self._interval):
                break
            self._collecting_diagnostics()

        # Perform final collection before stopping to avoid missing diagnostics
        LOGGER.debug("Performing final diagnostics collection before stopping.")
        self._collecting_diagnostics()
        LOGGER.debug("Diagnostic collection thread stopped.")

    def _collecting_diagnostics(self):
        """Collect diagnostics data from all collectors."""
        self._is_collecting.set()
        LOGGER.debug("Starting diagnostics collection from %d collectors", len(self._collectors))
        for collector in self._collectors:
            if self._stop_event.is_set():
                LOGGER.debug("Stopping diagnostics collection due to stop event.")
                break
            collected = stored = True
            data = None
            error = None
            try:
                LOGGER.debug("Collecting diagnostics from collector '%s'", collector.name)
                data = collector.collect()
            except Exception as e:  # noqa: BLE001
                error = e
                strategy = self._exception_handler.handle_exception_during_collecting(collector, e)
                if strategy == ExceptionStrategy.STOP:
                    LOGGER.debug("Stopping diagnostics collection due to exception during collecting.")
                    self._stop_event.set()
                    break
                continue

            try:
                LOGGER.debug("Storing diagnostics for collector '%s'", collector.name)
                collector.store(data)
            except Exception as e:  # noqa: BLE001
                strategy = self._exception_handler.handle_exception_during_storing(collector, e)
                error = e
                stored = False
                if strategy == ExceptionStrategy.STOP:
                    LOGGER.debug("Stopping diagnostics storing due to exception during storing.")
                    self._stop_event.set()
                    break

            diagnostics_result = DiagnosticResult(
                collector_name=collector.name,
                timestamp=datetime.now(),
                data=data,
                collected=collected,
                stored=stored,
                error=error,
            )
            with self._results_lock:
                self._results.append(diagnostics_result)

        self._is_collecting.clear()
        LOGGER.debug("Finished diagnostics collection cycle.")

    def start_collecting(self):
        """Start the diagnostics collection thread."""
        LOGGER.debug("Starting diagnostics collection thread.")
        self._stop_event.clear()
        if not self.is_alive():
            self.start()

    def stop_collecting(self):
        """Stop the diagnostics collection thread."""
        LOGGER.debug("Stopping diagnostics collection thread.")
        self._stop_event.set()
        if self.is_alive():
            self.join()
        self._is_collecting.clear()
        for collector in self._collectors:
            try:
                collector.clean()
            except Exception as e:  # noqa: BLE001
                LOGGER.error("Exception occurred while cleaning collector '%s': %s", collector.name, e)

    def get_results(self) -> list[DiagnosticResult]:
        """Get the collected diagnostics results."""
        with self._results_lock:
            return self._results.copy()

    def clear_results(self):
        """Clear the collected diagnostics results."""
        with self._results_lock:
            self._results.clear()

    def __enter__(self) -> DiagnosticManager:
        self.start_collecting()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and stop collecting diagnostics."""
        self.stop_collecting()
        if exc_type is not None:
            LOGGER.error(
                "Exception occurred in DiagnosticManager context: %s", exc_val, exc_info=(exc_type, exc_val, exc_tb)
            )

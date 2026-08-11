import contextlib
import logging

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from threading import Event, Lock, Thread
from typing import Any, Iterator, Sequence

from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionStrategy

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


class DiagnosticManager:
    """Manages the collection and storage of diagnostics data from multiple collectors.

    The manager owns an internal worker thread (composition) instead of subclassing ``Thread``.
    This allows the manager to be started again after it was stopped, and lets it monitor the
    worker's execution: liveness via :attr:`is_running` and crash detection via :attr:`error`.

    Each collector carries its own :class:`ExceptionHandler`, so the manager dispatches failure
    handling through the failing collector (``collector.exception_handler``). This lets every
    collector use a different, runtime-swappable policy.

    The in-memory history is a bounded ring buffer (:attr:`max_history`): once full, the oldest
    cycle is dropped automatically so a long-running manager cannot grow memory without bound. Pass
    ``max_history=None`` for an unbounded history (and manage :meth:`clear_results` yourself).
    """

    def __init__(
        self,
        collectors: Sequence[DiagnosticCollector],
        interval: float = 60.0,
        name: str = "DiagnosticManager",
        max_history: int | None = 10_000,
    ):
        self._collectors = collectors
        self._interval = interval
        self._name = name
        self._stop_event = Event()
        # Bounded ring buffer: once full, the oldest cycle is dropped automatically so a
        # long-running manager cannot grow memory without bound. Pass max_history=None for an
        # unbounded history (legacy behavior) when the caller manages clear_results() itself.
        self._results: deque[DiagnosticResult] = deque(maxlen=max_history)
        self._results_lock = Lock()
        self._is_collecting = Event()
        self._thread: Thread | None = None
        self._lifecycle_lock = Lock()
        self._thread_error: BaseException | None = None

    @property
    def is_collecting(self) -> bool:
        """True while a single collection cycle is in progress."""
        return self._is_collecting.is_set()

    @property
    def is_running(self) -> bool:
        """True while the background worker thread is alive."""
        return self._thread is not None and self._thread.is_alive()

    @property
    def error(self) -> BaseException | None:
        """The exception that crashed the worker thread, if any (else ``None``)."""
        return self._thread_error

    def is_healthy(self) -> bool:
        """True if the worker is running and has not crashed."""
        return self.is_running and self._thread_error is None

    def _thread_main(self):
        """Top-level body of the owned thread: run the loop and record any crash."""
        try:
            self._run()
        except BaseException as e:  # noqa: BLE001  # monitor crashes, then re-expose on stop
            self._thread_error = e
            LOGGER.exception("Diagnostics worker thread '%s' crashed: %s", self._name, e)
        finally:
            self._is_collecting.clear()

    def _run(self):
        LOGGER.debug("Starting collect diagnostics data every %s seconds", self._interval)
        while not self._stop_event.is_set():
            LOGGER.debug("Waiting for next collection interval...")
            if self._stop_event.wait(self._interval):
                break
            self._collecting_diagnostics()

        # Perform final collection before stopping to avoid missing diagnostics. The stop event is
        # already set at this point, so force the cycle to run instead of aborting at the guard.
        LOGGER.debug("Performing final diagnostics collection before stopping.")
        self._collecting_diagnostics(final=True)
        LOGGER.debug("Diagnostic collection thread stopped.")

    def _record_result(
        self, collector: DiagnosticCollector, data: Any, collected: bool, stored: bool, error: Exception | None
    ):
        """Append a DiagnosticResult to the history in a thread-safe way."""
        result = DiagnosticResult(
            collector_name=collector.name,
            timestamp=datetime.now(),
            data=data,
            collected=collected,
            stored=stored,
            error=error,
        )
        with self._results_lock:
            self._results.append(result)

    def _collecting_diagnostics(self, final: bool = False):
        """Collect diagnostics data from all collectors.

        Args:
            final: When True this is the final collection performed on stop. The stop event is
                already set by then, so the per-collector stop guard is skipped to ensure the
                cycle actually runs.
        """
        self._is_collecting.set()
        LOGGER.debug("Starting diagnostics collection from %d collectors", len(self._collectors))
        for collector in self._collectors:
            if not final and self._stop_event.is_set():
                LOGGER.debug("Stopping diagnostics collection due to stop event.")
                break
            collected = stored = True
            data = None
            error = None
            try:
                LOGGER.debug("Collecting diagnostics from collector '%s'", collector.name)
                data = collector.collect()
            except Exception as e:  # noqa: BLE001
                strategy = collector.exception_handler.handle_exception_during_collecting(collector, e)
                # Always record the failed cycle so callers can see which collector failed and why
                # instead of silently dropping it from get_results().
                self._record_result(collector, data=None, collected=False, stored=False, error=e)
                if strategy == ExceptionStrategy.STOP:
                    LOGGER.debug("Stopping diagnostics collection due to exception during collecting.")
                    self._stop_event.set()
                    break
                continue

            stop_after_store = False
            try:
                LOGGER.debug("Storing diagnostics for collector '%s'", collector.name)
                collector.store(data)
            except Exception as e:  # noqa: BLE001
                strategy = collector.exception_handler.handle_exception_during_storing(collector, e)
                error = e
                stored = False
                if strategy == ExceptionStrategy.STOP:
                    LOGGER.debug("Stopping diagnostics storing due to exception during storing.")
                    self._stop_event.set()
                    stop_after_store = True

            self._record_result(collector, data=data, collected=collected, stored=stored, error=error)
            if stop_after_store:
                break

        self._is_collecting.clear()
        LOGGER.debug("Finished diagnostics collection cycle.")

    def start_collecting(self):
        """Start (or restart) the background collection thread.

        A brand-new ``Thread`` is created on each start, so a manager that was previously stopped
        can be started again without the ``RuntimeError: threads can only be started once`` that a
        ``Thread`` subclass would raise.

        The check-and-create is serialized by ``_lifecycle_lock`` so concurrent callers can never
        spawn more than one worker thread.
        """
        with self._lifecycle_lock:
            if self.is_running:
                LOGGER.debug("Diagnostics collection already running; ignoring start request.")
                return
            LOGGER.debug("Starting diagnostics collection thread.")
            self._stop_event.clear()
            self._thread_error = None
            self._thread = Thread(target=self._thread_main, name=self._name, daemon=True)
            self._thread.start()

    def stop_collecting(self, timeout: float | None = None):
        """Stop the worker, run collector cleanup, and surface a crashed worker.

        Serialized by ``_lifecycle_lock`` so it cannot race with a concurrent ``start_collecting()``.

        Args:
            timeout: Optional maximum number of seconds to wait for the worker thread to stop.
                If it does not stop in time, an error is logged and collector cleanup is skipped
                (cleaning collectors while the worker may still be running would race with it)
                instead of blocking forever.

        Raises:
            RuntimeError: If the worker thread crashed with an unexpected exception.
        """
        with self._lifecycle_lock:
            LOGGER.debug("Stopping diagnostics collection thread.")
            self._stop_event.set()
            if self._thread is not None and self._thread.is_alive():
                self._thread.join(timeout=timeout)
            self._is_collecting.clear()
            if self._thread is not None and self._thread.is_alive():
                LOGGER.error(
                    "Diagnostics worker thread '%s' did not stop within %s seconds. Skipping collectors cleanup",
                    self._name,
                    timeout,
                )
            else:
                for collector in self._collectors:
                    try:
                        collector.clean()
                    except Exception as e:  # noqa: BLE001
                        LOGGER.error("Exception occurred while cleaning collector '%s': %s", collector.name, e)

            # Monitoring: re-raise an unexpected worker crash so callers are not left unaware.
            if self._thread_error is not None:
                raise RuntimeError("Diagnostics worker thread crashed") from self._thread_error

    def get_results(self) -> list[DiagnosticResult]:
        """Get the collected diagnostics results."""
        with self._results_lock:
            return list(self._results)

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


@contextlib.contextmanager
def collect_diagnostics(*collectors: DiagnosticCollector, interval: float = 60.0) -> Iterator[DiagnosticManager]:
    """Run the given diagnostic collectors in the background for the duration of the ``with`` block.

    Generic helper that works with any ``DiagnosticCollector``, so no per-collector facade is needed.

    Example:
        with collect_diagnostics(MVDiagnosticCollector(session)):
            ...
    """
    with DiagnosticManager(collectors, interval=interval) as manager:
        yield manager

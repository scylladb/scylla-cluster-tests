"""Unit tests for DiagnosticManager."""

import logging
import threading
import time

import pytest

from sdcm.utils.diagnostic_collector import ExceptionStrategy
from sdcm.utils.diagnostic_collector.manager import DiagnosticManager, DiagnosticResult, collect_diagnostics

from unit_tests.unit.diagnostic_collector import MockDiagnosticCollector, MockExceptionHandler


# --- Test DiagnosticManager Collection ---


def test_diagnostic_manager_single_collection():
    """Test manager collects and stores diagnostics from all collectors."""
    collectors = [
        MockDiagnosticCollector(name="Collector1", collect_data="data1"),
        MockDiagnosticCollector(name="Collector2", collect_data="data2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    # Trigger single collection manually
    manager._collecting_diagnostics()

    # Verify all collectors were called
    assert collectors[0].collect_count == 1, f"Collector1 should be collected once, got {collectors[0].collect_count}"
    assert collectors[0].store_count == 1, f"Collector1 should be stored once, got {collectors[0].store_count}"
    assert collectors[1].collect_count == 1, f"Collector2 should be collected once, got {collectors[1].collect_count}"
    assert collectors[1].store_count == 1, f"Collector2 should be stored once, got {collectors[1].store_count}"

    # Verify results were stored
    results = manager.get_results()
    assert len(results) == 2, f"Expected 2 results (one per collector), got {len(results)}"
    assert results[0].collector_name == "Collector1", (
        f"First result should be from Collector1, got {results[0].collector_name}"
    )
    assert results[1].collector_name == "Collector2", (
        f"Second result should be from Collector2, got {results[1].collector_name}"
    )
    assert results[0].collected is True, f"First result should be marked collected, got {results[0].collected}"
    assert results[0].stored is True, f"First result should be marked stored, got {results[0].stored}"


def test_diagnostic_manager_collection_with_collect_failure():
    """Test manager handles collection failures properly."""
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.CONTINUE)
    collectors = [
        MockDiagnosticCollector(name="FailingCollector", should_fail_collect=True, exception_handler=handler),
        MockDiagnosticCollector(name="SuccessCollector", collect_data="success"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    # First collector should fail to collect
    assert collectors[0].collect_count == 1, (
        f"FailingCollector should be collected once, got {collectors[0].collect_count}"
    )
    assert collectors[0].store_count == 0, (
        f"FailingCollector should not be stored after collect failure, got {collectors[0].store_count}"
    )

    # Second collector should succeed
    assert collectors[1].collect_count == 1, (
        f"SuccessCollector should be collected once, got {collectors[1].collect_count}"
    )
    assert collectors[1].store_count == 1, f"SuccessCollector should be stored once, got {collectors[1].store_count}"

    # Verify exception was handled
    assert len(handler.collect_exceptions) == 1, (
        f"Expected exactly 1 handled collect exception, got {len(handler.collect_exceptions)}"
    )
    assert handler.collect_exceptions[0][0].name == "FailingCollector", (
        f"Handled exception should be from FailingCollector, got {handler.collect_exceptions[0][0].name}"
    )

    # The failed cycle must be recorded (not silently dropped) so callers can see which collector
    # failed and why.
    results = manager.get_results()
    assert len(results) == 2, f"Expected 2 results (failed + succeeded collector), got {len(results)}"
    failed_result = next(r for r in results if r.collector_name == "FailingCollector")
    assert failed_result.collected is False, "Failed collector result should be marked not collected"
    assert failed_result.stored is False, "Failed collector result should be marked not stored"
    assert failed_result.error is not None, "Failed collector result should record the collect error"
    assert failed_result.data is None, "Failed collector result should carry no data"


def test_diagnostic_manager_collection_with_store_failure():
    """Test manager handles storage failures properly."""
    handler = MockExceptionHandler(store_strategy=ExceptionStrategy.CONTINUE)
    collectors = [
        MockDiagnosticCollector(name="StoreFailCollector", should_fail_store=True, exception_handler=handler),
        MockDiagnosticCollector(name="SuccessCollector", collect_data="success"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    # First collector should collect but fail to store
    assert collectors[0].collect_count == 1, (
        f"StoreFailCollector should be collected once, got {collectors[0].collect_count}"
    )
    assert collectors[0].store_count == 1, (
        f"StoreFailCollector store should be attempted once, got {collectors[0].store_count}"
    )

    # Second collector should succeed
    assert collectors[1].collect_count == 1, (
        f"SuccessCollector should be collected once, got {collectors[1].collect_count}"
    )
    assert collectors[1].store_count == 1, f"SuccessCollector should be stored once, got {collectors[1].store_count}"

    # Verify exception was handled
    assert len(handler.store_exceptions) == 1, (
        f"Expected exactly 1 handled store exception, got {len(handler.store_exceptions)}"
    )
    assert handler.store_exceptions[0][0].name == "StoreFailCollector", (
        f"Handled exception should be from StoreFailCollector, got {handler.store_exceptions[0][0].name}"
    )

    # Verify results show storage failure
    results = manager.get_results()
    assert len(results) == 2, f"Expected 2 results (one per collector), got {len(results)}"
    assert results[0].collected is True, f"First result should be marked collected, got {results[0].collected}"
    assert results[0].stored is False, (
        f"First result should be marked not stored after store failure, got {results[0].stored}"
    )
    assert results[0].error is not None, "First result should record the store error, got None"


def test_diagnostic_manager_stop_on_collect_exception():
    """Test manager stops collection when handler returns STOP strategy."""
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP)
    collectors = [
        MockDiagnosticCollector(name="Collector1", should_fail_collect=True, exception_handler=handler),
        MockDiagnosticCollector(name="Collector2"),
        MockDiagnosticCollector(name="Collector3"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    # First collector fails and triggers stop
    assert collectors[0].collect_count == 1, (
        f"Collector1 should be collected once before stop, got {collectors[0].collect_count}"
    )
    # Subsequent collectors should not be called
    assert collectors[1].collect_count == 0, (
        f"Collector2 should not be collected after STOP strategy, got {collectors[1].collect_count}"
    )
    assert collectors[2].collect_count == 0, (
        f"Collector3 should not be collected after STOP strategy, got {collectors[2].collect_count}"
    )

    # Stop event should be set
    assert manager._stop_event.is_set(), "Stop event should be set after collect handler returns STOP"


def test_diagnostic_manager_stop_on_store_exception():
    """Test manager stops collection when storage handler returns STOP strategy."""
    handler = MockExceptionHandler(store_strategy=ExceptionStrategy.STOP)
    collectors = [
        MockDiagnosticCollector(name="Collector1", should_fail_store=True, exception_handler=handler),
        MockDiagnosticCollector(name="Collector2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    # First collector collects but fails to store
    assert collectors[0].collect_count == 1, f"Collector1 should be collected once, got {collectors[0].collect_count}"
    assert collectors[0].store_count == 1, f"Collector1 store should be attempted once, got {collectors[0].store_count}"
    # Second collector should not be called
    assert collectors[1].collect_count == 0, (
        f"Collector2 should not be collected after STOP strategy, got {collectors[1].collect_count}"
    )

    # Stop event should be set
    assert manager._stop_event.is_set(), "Stop event should be set after store handler returns STOP"


def test_diagnostic_manager_per_collector_handlers_mixed_strategies():
    """Per-collector handlers: two CONTINUE collectors and one STOP collector (DI, path 1).

    Mirrors the 3-collector scenario: C1/C2 use CONTINUE handlers, C3 uses a STOP handler. C3's
    failure must halt the run, while C1/C2 (ordered before C3) still complete the cycle.
    """
    continue_handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.CONTINUE)
    stop_handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP)
    collectors = [
        MockDiagnosticCollector(name="C1", exception_handler=continue_handler),
        MockDiagnosticCollector(name="C2", exception_handler=continue_handler),
        MockDiagnosticCollector(name="C3", should_fail_collect=True, exception_handler=stop_handler),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    # C1 and C2 precede the failing STOP collector, so they complete this cycle.
    assert collectors[0].collect_count == 1 and collectors[0].store_count == 1, "C1 should fully run before the STOP"
    assert collectors[1].collect_count == 1 and collectors[1].store_count == 1, "C2 should fully run before the STOP"
    # C3 fails and its STOP handler halts the run.
    assert collectors[2].collect_count == 1, "C3 should be attempted once"
    assert manager._stop_event.is_set(), "C3's STOP handler must set the stop event"

    # The shared CONTINUE handler saw no failures; the STOP handler saw exactly C3's failure.
    assert continue_handler.collect_exceptions == [], "CONTINUE handler should not have handled any failure"
    assert len(stop_handler.collect_exceptions) == 1, "STOP handler should have handled exactly C3's failure"
    assert stop_handler.collect_exceptions[0][0].name == "C3", "STOP handler should have handled C3"


def test_diagnostic_manager_runtime_handler_swap():
    """A collector's exception_handler can be swapped at runtime to change its failure policy."""
    collector = MockDiagnosticCollector(
        name="Swappable",
        should_fail_collect=True,
        exception_handler=MockExceptionHandler(collect_strategy=ExceptionStrategy.CONTINUE),
    )
    manager = DiagnosticManager(collectors=[collector], interval=1.0)

    # First cycle: CONTINUE policy -> run is not stopped.
    manager._collecting_diagnostics()
    assert not manager._stop_event.is_set(), "CONTINUE handler must not stop the run"

    # Swap the policy on the live collector to STOP and collect again.
    collector.exception_handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP)
    manager._collecting_diagnostics()
    assert manager._stop_event.is_set(), "After swapping to a STOP handler, the run must stop"


# --- Test DiagnosticManager Thread Management ---


def test_diagnostic_manager_start_stop():
    """Test manager can start and stop collection thread."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    # Start collection
    manager.start_collecting()
    assert manager.is_running, "Manager thread should be alive after start_collecting()"

    # Give it time to run at least once
    time.sleep(0.2)

    # Stop collection
    manager.stop_collecting()
    assert not manager.is_running, "Manager thread should not be alive after stop_collecting()"

    # Verify clean was called
    assert collectors[0].clean_count == 1, (
        f"Collector clean() should be called once on stop, got {collectors[0].clean_count}"
    )


def test_diagnostic_manager_multiple_collections():
    """Test manager performs multiple collections over time."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    deadline = time.time() + 2.0
    try:
        while collectors[0].collect_count < 2 and time.time() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop_collecting()

    # Verify multiple collections occurred (at least 2, could be 3-4 depending on timing)
    assert collectors[0].collect_count >= 2, (
        f"Expected at least 2 collections over the run, got {collectors[0].collect_count}"
    )


def test_diagnostic_manager_is_collecting_flag():
    """Test is_collecting flag is set during collection."""
    collectors = [MockDiagnosticCollector(name="Collector1", collect_delay=0.05)]
    manager = DiagnosticManager(collectors=collectors, interval=0.5)

    assert manager.is_collecting is False, "is_collecting should be False before start"

    manager.start_collecting()
    time.sleep(0.1)  # Wait for first collection to start

    # During collection, flag might be set
    # Note: This is timing-dependent, so we just verify it's a bool
    collecting_status = manager.is_collecting
    assert isinstance(collecting_status, bool), f"is_collecting should return a bool, got {type(collecting_status)}"

    manager.stop_collecting()
    assert manager.is_collecting is False, "is_collecting should be False after stop"


def test_diagnostic_manager_final_collection_on_stop():
    """Test manager performs final collection when stopped."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=10.0)  # Long interval

    manager.start_collecting()
    time.sleep(0.1)  # Not enough time for interval-based collection
    initial_count = collectors[0].collect_count
    manager.stop_collecting()

    # Exactly one final collection must run on stop (regression guard: a missing/broken final
    # collection would leave the count unchanged).
    assert collectors[0].collect_count == initial_count + 1, (
        f"Exactly one final collection should run on stop "
        f"(initial={initial_count}, final={collectors[0].collect_count})"
    )


# --- Test DiagnosticManager Context Manager ---


def test_diagnostic_manager_context_manager():
    """Test manager works as context manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    with manager as mgr:
        assert mgr is manager, "Context manager should yield the manager instance itself"
        assert manager.is_running, "Manager thread should be alive inside the context"
        time.sleep(0.15)

    # After context exit, manager should be stopped
    assert not manager.is_running, "Manager thread should be stopped after context exit"
    assert collectors[0].clean_count == 1, (
        f"Collector clean() should be called once on context exit, got {collectors[0].clean_count}"
    )


def test_diagnostic_manager_context_manager_with_exception(caplog):
    """Test manager handles exceptions in context manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            with manager:
                raise ValueError("Test exception")

    # Manager should still be stopped
    assert not manager.is_running, "Manager thread should be stopped even when context body raises"
    assert "Exception occurred in DiagnosticManager context" in caplog.text, (
        "Expected the context exception to be logged at ERROR level"
    )


# --- Test collect_diagnostics Helper ---


def test_collect_diagnostics_helper_runs_and_cleans():
    """Test the generic collect_diagnostics helper collects in the background and cleans up on exit."""
    collector = MockDiagnosticCollector(name="Collector1")

    with collect_diagnostics(collector, interval=0.1) as manager:
        assert manager.is_running, "collect_diagnostics() should start a live background manager"
        time.sleep(0.15)

    assert not manager.is_running, "Manager should be stopped after collect_diagnostics() context exit"
    assert collector.collect_count >= 1, f"Collector should be collected at least once, got {collector.collect_count}"
    assert collector.clean_count == 1, f"Collector clean() should be called once on exit, got {collector.clean_count}"


def test_collect_diagnostics_helper_multiple_collectors():
    """Test the helper accepts multiple collectors as varargs."""
    collectors = [MockDiagnosticCollector(name="Collector1"), MockDiagnosticCollector(name="Collector2")]

    with collect_diagnostics(*collectors, interval=0.1):
        time.sleep(0.15)

    for collector in collectors:
        assert collector.collect_count >= 1, (
            f"{collector.name} should be collected at least once, got {collector.collect_count}"
        )
        assert collector.clean_count == 1, (
            f"{collector.name} clean() should be called once on exit, got {collector.clean_count}"
        )


# --- Test DiagnosticManager Results Management ---


def test_diagnostic_manager_get_results():
    """Test getting results from manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    results = manager.get_results()

    assert len(results) == 1, f"Expected exactly 1 result after one collection, got {len(results)}"
    assert isinstance(results[0], DiagnosticResult), f"Result should be a DiagnosticResult, got {type(results[0])}"
    assert results[0].collector_name == "Collector1", (
        f"Result should be from Collector1, got {results[0].collector_name}"
    )


def test_diagnostic_manager_get_results_returns_copy():
    """Test get_results returns a copy, not the original list."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    results1 = manager.get_results()
    results2 = manager.get_results()

    # Should be equal but not the same object
    assert results1 == results2, "Two get_results() calls should return equal contents"
    assert results1 is not results2, "get_results() should return a new copy each call, not the same list object"


def test_diagnostic_manager_clear_results():
    """Test clearing results from manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    assert len(manager.get_results()) == 1, f"Expected 1 result before clear, got {len(manager.get_results())}"

    manager.clear_results()
    assert len(manager.get_results()) == 0, (
        f"Expected 0 results after clear_results(), got {len(manager.get_results())}"
    )


def test_diagnostic_manager_accumulates_results():
    """Test results accumulate over multiple collections."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.25)  # Allow 2-3 collections
    manager.stop_collecting()

    results = manager.get_results()
    # Should have multiple results (at least 2)
    assert len(results) >= 2, f"Expected results to accumulate to at least 2, got {len(results)}"

    # Each result should be for the same collector
    for result in results:
        assert result.collector_name == "Collector1", (
            f"All accumulated results should be from Collector1, got {result.collector_name}"
        )


def test_diagnostic_manager_full_history_two_collectors_multiple_cycles():
    """Two collectors over several cycles: manager keeps the full, ordered, independent history.

    Drives the collection cycle deterministically (no timing) so the number of cycles is exact,
    and guards against history corruption by asserting each cycle's snapshot is preserved.
    """
    collectors = [
        MockDiagnosticCollector(name="Collector1", collect_data="data1"),
        MockDiagnosticCollector(name="Collector2", collect_data="data2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    cycles = 4  # within the requested 3-5 range
    for _ in range(cycles):
        manager._collecting_diagnostics()

    # Each collector was collected and stored exactly once per cycle.
    for collector in collectors:
        assert collector.collect_count == cycles, (
            f"{collector.name} should be collected {cycles} times, got {collector.collect_count}"
        )
        assert collector.store_count == cycles, (
            f"{collector.name} should be stored {cycles} times, got {collector.store_count}"
        )

    results = manager.get_results()
    # Full history: one DiagnosticResult per collector per cycle.
    assert len(results) == cycles * len(collectors), (
        f"Manager should keep full history of {cycles * len(collectors)} results, got {len(results)}"
    )

    # Per collector, every cycle's snapshot must be an INDEPENDENT object whose data reflects that
    # cycle (regression guard: a shared/overwritten dict would make all counts equal the latest).
    for name in ("Collector1", "Collector2"):
        per_collector = [r for r in results if r.collector_name == name]
        assert len(per_collector) == cycles, f"{name} should have {cycles} results in history, got {len(per_collector)}"
        counts = [r.data["count"] for r in per_collector]
        assert counts == list(range(1, cycles + 1)), (
            f"{name} history must preserve each cycle's snapshot {list(range(1, cycles + 1))}, got {counts}"
        )


def test_diagnostic_manager_history_survives_stop_and_clean():
    """Recorded history (and its data) stays intact after stop_collecting() runs collector.clean()."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    cycles = 3
    for _ in range(cycles):
        manager._collecting_diagnostics()

    manager.stop_collecting()  # triggers clean() on every collector

    assert collectors[0].clean_count == 1, f"clean() should run once on stop, got {collectors[0].clean_count}"
    results = manager.get_results()
    assert len(results) == cycles, f"Full history of {cycles} results should remain after stop, got {len(results)}"
    counts = [r.data["count"] for r in results]
    assert counts == list(range(1, cycles + 1)), (
        f"History data must be intact and independent after clean(), got {counts}"
    )


def test_diagnostic_manager_clean_on_stop():
    """Test clean is called on all collectors when manager stops."""
    collectors = [
        MockDiagnosticCollector(name="Collector1"),
        MockDiagnosticCollector(name="Collector2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.15)
    manager.stop_collecting()

    # Clean should be called on all collectors
    assert collectors[0].clean_count == 1, (
        f"Collector1 clean() should be called once on stop, got {collectors[0].clean_count}"
    )
    assert collectors[1].clean_count == 1, (
        f"Collector2 clean() should be called once on stop, got {collectors[1].clean_count}"
    )


def test_diagnostic_manager_clean_handles_exceptions(caplog):
    """Test manager handles exceptions during clean gracefully."""

    class FailingCleanCollector(MockDiagnosticCollector):
        def clean(self):
            super().clean()
            raise RuntimeError("Clean failed")

    collectors = [
        FailingCleanCollector(name="Collector1"),
        MockDiagnosticCollector(name="Collector2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    with caplog.at_level(logging.ERROR):
        manager.start_collecting()
        time.sleep(0.15)
        manager.stop_collecting()

    # Both clean methods should be called despite exception
    assert collectors[0].clean_count == 1, (
        f"FailingCleanCollector clean() should still be called once, got {collectors[0].clean_count}"
    )
    assert collectors[1].clean_count == 1, (
        f"Collector2 clean() should be called once despite earlier failure, got {collectors[1].clean_count}"
    )
    assert "Exception occurred while cleaning collector 'Collector1'" in caplog.text, (
        "Expected the clean() failure to be logged at ERROR level"
    )


# --- Test Edge Cases ---


def test_diagnostic_manager_empty_collectors():
    """Test manager handles empty collector list."""
    manager = DiagnosticManager(collectors=[], interval=1.0)

    manager._collecting_diagnostics()
    results = manager.get_results()

    assert len(results) == 0, f"Empty collector list should produce no results, got {len(results)}"


def test_diagnostic_manager_stop_before_start():
    """Test stopping manager before starting doesn't cause issues."""
    collectors = [MockDiagnosticCollector()]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    # Stop without starting should not raise exception
    manager.stop_collecting()
    assert not manager.is_running, "Manager should not be alive when stopped before ever starting"


def test_diagnostic_manager_double_start():
    """Test starting manager twice doesn't create issues."""
    collectors = [MockDiagnosticCollector()]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    assert manager.is_running, "Manager thread should be alive after first start_collecting()"

    # Second start should not crash
    manager.start_collecting()
    time.sleep(0.15)

    manager.stop_collecting()
    assert not manager.is_running, "Manager thread should be stopped after stop_collecting()"


def test_diagnostic_manager_concurrent_start_creates_single_thread():
    """Test concurrent start_collecting() calls never spawn more than one worker thread."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    barrier = threading.Barrier(8)
    created_threads = set()
    created_lock = threading.Lock()

    def racer():
        barrier.wait()  # release all callers at once to maximize the race
        manager.start_collecting()
        if manager._thread is not None:
            with created_lock:
                created_threads.add(manager._thread.ident)

    threads = [threading.Thread(target=racer) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    try:
        assert manager.is_running, "Exactly one worker should be running after concurrent starts"
        # The lifecycle lock must serialize the check-and-create, so only a single worker ident
        # is ever observed across all racing callers.
        assert len(created_threads) == 1, (
            f"Concurrent start_collecting() must create a single worker thread, got {created_threads}"
        )
    finally:
        manager.stop_collecting()
    assert not manager.is_running, "Manager thread should be stopped after stop_collecting()"


def test_diagnostic_manager_restart_after_stop():
    """Test a stopped manager can be started again (owned thread is recreated on each start)."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.15)
    manager.stop_collecting()
    assert not manager.is_running, "Manager thread should be stopped after stop_collecting()"
    first_run_count = collectors[0].collect_count
    assert first_run_count >= 1, "First run should have collected at least once"

    # Restart must work (no RuntimeError) and resume collecting on a fresh thread.
    manager.start_collecting()
    assert manager.is_running, "Manager should be running again after restart"
    time.sleep(0.15)
    manager.stop_collecting()
    assert not manager.is_running, "Manager thread should be stopped after second stop_collecting()"
    assert collectors[0].collect_count > first_run_count, (
        f"Restart should resume collecting (first={first_run_count}, after restart={collectors[0].collect_count})"
    )


def test_diagnostic_manager_monitors_worker_crash():
    """Test an unexpected crash in the worker is captured and re-raised on stop."""

    class ExplodingManager(DiagnosticManager):
        def _run(self):
            raise RuntimeError("boom in worker")

    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = ExplodingManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.1)

    # The worker crashed: not healthy, and the captured error is exposed.
    assert manager.is_healthy() is False, "A crashed worker should not be reported as healthy"
    assert isinstance(manager.error, RuntimeError), f"Worker error should be captured, got {manager.error!r}"

    # stop_collecting() surfaces the crash to the caller instead of swallowing it.
    with pytest.raises(RuntimeError, match="Diagnostics worker thread crashed"):
        manager.stop_collecting()


def test_diagnostic_manager_thread_safety():
    """Test results can be read concurrently while collection is happening, without errors."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.05)

    manager.start_collecting()

    # Access results from main thread while the collection thread is running.
    # We don't assert on counts here (that is timing-dependent); we only verify that
    # concurrent reads always return a valid, non-decreasing snapshot list and never raise.
    previous_len = 0
    for _ in range(20):
        results = manager.get_results()
        assert isinstance(results, list), f"get_results() should always return a list, got {type(results)}"
        # get_results returns a copy, so the snapshot length must never go backwards
        assert len(results) >= previous_len, (
            f"Concurrent get_results() snapshot length must not decrease (previous={previous_len}, current={len(results)})"
        )
        previous_len = len(results)
        time.sleep(0.01)

    manager.stop_collecting()

    # After stopping, a final collection is guaranteed, so there must be at least one result.
    final_results = manager.get_results()
    assert final_results, "Expected at least one result after stop (final collection is guaranteed)"

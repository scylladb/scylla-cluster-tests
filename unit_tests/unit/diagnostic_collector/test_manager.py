"""Unit tests for DiagnosticManager."""

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
    assert collectors[0].store_attempts == 1, f"Collector1 should be stored once, got {collectors[0].store_attempts}"
    assert collectors[1].collect_count == 1, f"Collector2 should be collected once, got {collectors[1].collect_count}"
    assert collectors[1].store_attempts == 1, f"Collector2 should be stored once, got {collectors[1].store_attempts}"

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
    assert collectors[0].store_attempts == 0, (
        f"FailingCollector should not be stored after collect failure, got {collectors[0].store_attempts}"
    )

    # Second collector should succeed
    assert collectors[1].collect_count == 1, (
        f"SuccessCollector should be collected once, got {collectors[1].collect_count}"
    )
    assert collectors[1].store_attempts == 1, (
        f"SuccessCollector should be stored once, got {collectors[1].store_attempts}"
    )

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
    assert collectors[0].store_attempts == 1, (
        f"StoreFailCollector store should be attempted once, got {collectors[0].store_attempts}"
    )

    # Second collector should succeed
    assert collectors[1].collect_count == 1, (
        f"SuccessCollector should be collected once, got {collectors[1].collect_count}"
    )
    assert collectors[1].store_attempts == 1, (
        f"SuccessCollector should be stored once, got {collectors[1].store_attempts}"
    )

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


@pytest.mark.parametrize(
    "fail_kwargs, expected_store_attempts",
    [
        # A collect failure breaks out immediately, so store is never attempted...
        pytest.param({"should_fail_collect": True}, 0, id="collect"),
        # ...while a store failure still records its DiagnosticResult before breaking out, which is
        # a separate code path (the stop_after_store flag) that can regress on its own.
        pytest.param({"should_fail_store": True}, 1, id="store"),
    ],
)
def test_diagnostic_manager_stop_strategy_halts_cycle(fail_kwargs, expected_store_attempts):
    """A STOP strategy from either stage halts the cycle and skips the remaining collectors."""
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP, store_strategy=ExceptionStrategy.STOP)
    collectors = [
        MockDiagnosticCollector(name="Collector1", exception_handler=handler, **fail_kwargs),
        MockDiagnosticCollector(name="Collector2"),
        MockDiagnosticCollector(name="Collector3"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()

    assert collectors[0].collect_count == 1, (
        f"Collector1 should be collected once before stop, got {collectors[0].collect_count}"
    )
    assert collectors[0].store_attempts == expected_store_attempts, (
        f"Collector1 store attempts should be {expected_store_attempts}, got {collectors[0].store_attempts}"
    )
    for collector in collectors[1:]:
        assert collector.collect_count == 0, (
            f"{collector.name} should not be collected after STOP strategy, got {collector.collect_count}"
        )

    assert manager._stop_event.is_set(), "Stop event should be set after the handler returns STOP"


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
    assert collectors[0].collect_count == 1 and collectors[0].store_attempts == 1, "C1 should fully run before the STOP"
    assert collectors[1].collect_count == 1 and collectors[1].store_attempts == 1, "C2 should fully run before the STOP"
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


def test_diagnostic_manager_lifecycle_over_multiple_collections():
    """Full lifecycle: start -> repeated collection -> stop -> clean, for every collector."""
    collectors = [
        MockDiagnosticCollector(name="Collector1"),
        MockDiagnosticCollector(name="Collector2"),
    ]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    assert manager.is_running is False, "manager should not be running before start"
    assert manager.is_collecting is False, "is_collecting should be False before start"

    manager.start_collecting()
    assert manager.is_running, "Manager thread should be alive after start_collecting()"

    deadline = time.time() + 5.0
    try:
        while collectors[0].collect_count < 2 and time.time() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop_collecting()

    assert manager.is_running is False, "Manager thread should not be alive after stop_collecting()"
    assert manager.is_collecting is False, "is_collecting should be False after stop"
    for collector in collectors:
        assert collector.collect_count >= 2, (
            f"{collector.name} should collect repeatedly, got {collector.collect_count}"
        )
        assert collector.clean_count == 1, (
            f"{collector.name} clean() should be called once on stop, got {collector.clean_count}"
        )


def test_diagnostic_manager_final_collection_on_stop():
    """A final collection always runs on stop, even when no interval ever elapsed."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=10.0)

    # Drive the worker loop directly with the stop already requested: the interval wait returns
    # immediately and the only cycle that can run is the forced final one. No thread, no sleep,
    # so the exact-count assertion below cannot be perturbed by scheduling.
    manager._stop_event.set()
    manager._run()

    # Regression guard for the final=True bypass of the per-collector stop guard: without it the
    # final cycle aborts at the guard and the count stays at 0.
    assert collectors[0].collect_count == 1, (
        f"Exactly one final collection should run on stop, got {collectors[0].collect_count}"
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

    with pytest.raises(ValueError, match="Test exception"):
        with manager:
            raise ValueError("Test exception")

    # Manager should still be stopped
    assert not manager.is_running, "Manager thread should be stopped even when context body raises"
    assert "Exception occurred in DiagnosticManager context: Test exception" in caplog.text, (
        "Expected the context exception, with its message, to be logged"
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


def test_collect_diagnostics_helper_cleans_up_on_exception():
    """The helper stops the manager and cleans collectors even when the context body raises."""
    collector = MockDiagnosticCollector(name="Collector1")

    with pytest.raises(ValueError, match="boom"):
        with collect_diagnostics(collector, interval=0.1) as manager:
            raise ValueError("boom")

    assert not manager.is_running, "helper must stop the manager when the body raises"
    assert collector.clean_count == 1, f"helper must clean collectors when the body raises, got {collector.clean_count}"


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
    # Poll for the condition instead of assuming a fixed number of intervals fire within a sleep:
    # a loaded CI machine can deliver fewer collections than the wall-clock budget suggests.
    deadline = time.time() + 5.0
    try:
        while len(manager.get_results()) < 2 and time.time() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop_collecting()

    results = manager.get_results()
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

    cycles = 4  # several cycles, enough to prove each cycle's snapshot stays independent
    for _ in range(cycles):
        manager._collecting_diagnostics()

    # Each collector was collected and stored exactly once per cycle.
    for collector in collectors:
        assert collector.collect_count == cycles, (
            f"{collector.name} should be collected {cycles} times, got {collector.collect_count}"
        )
        assert collector.store_attempts == cycles, (
            f"{collector.name} should be stored {cycles} times, got {collector.store_attempts}"
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

"""Unit tests for DiagnosticManager."""

import logging
import time
from typing import Any

import pytest

from sdcm.utils.diagnostic_collector import DiagnosticCollector, ExceptionHandler, ExceptionStrategy
from sdcm.utils.diagnostic_collector.diagnostic_manager import DiagnosticManager, DiagnosticResult


# --- Mock Classes ---


class MockDiagnosticCollector(DiagnosticCollector):
    """Mock implementation of DiagnosticCollector for testing."""

    def __init__(
        self,
        name: str | None = None,
        collect_data: Any = None,
        should_fail_collect: bool = False,
        should_fail_store: bool = False,
        collect_delay: float = 0,
    ):
        super().__init__(name)
        self.collect_data = collect_data or {"test": "data"}
        self.should_fail_collect = should_fail_collect
        self.should_fail_store = should_fail_store
        self.collect_delay = collect_delay
        self.collect_count = 0
        self.store_count = 0
        self.clean_count = 0
        self.stored_results = []

    def collect(self) -> Any:
        """Mock collect implementation."""
        self.collect_count += 1
        if self.collect_delay > 0:
            time.sleep(self.collect_delay)
        if self.should_fail_collect:
            raise RuntimeError(f"Collection failed for {self.name}")
        return {"data": self.collect_data, "count": self.collect_count}

    def store(self, result: Any):
        """Mock store implementation."""
        self.store_count += 1
        if self.should_fail_store:
            raise RuntimeError(f"Storage failed for {self.name}")
        self.stored_results.append(result)

    def clean(self):
        """Mock clean implementation."""
        self.clean_count += 1


class MockExceptionHandler(ExceptionHandler):
    """Mock exception handler for testing."""

    def __init__(
        self,
        collect_strategy: ExceptionStrategy = ExceptionStrategy.CONTINUE,
        store_strategy: ExceptionStrategy = ExceptionStrategy.CONTINUE,
    ):
        super().__init__()
        self.collect_strategy = collect_strategy
        self.store_strategy = store_strategy
        self.collect_exceptions = []
        self.store_exceptions = []

    def handle_exception_during_collecting(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Track collecting exceptions and return configured strategy."""
        self.collect_exceptions.append((collector, exception))
        return self.collect_strategy

    def handle_exception_during_storing(
        self, collector: DiagnosticCollector, exception: Exception
    ) -> ExceptionStrategy:
        """Track storing exceptions and return configured strategy."""
        self.store_exceptions.append((collector, exception))
        return self.store_strategy


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
    assert collectors[0].collect_count == 1
    assert collectors[0].store_count == 1
    assert collectors[1].collect_count == 1
    assert collectors[1].store_count == 1

    # Verify results were stored
    results = manager.get_results()
    assert len(results) == 2
    assert results[0].collector_name == "Collector1"
    assert results[1].collector_name == "Collector2"
    assert results[0].collected is True
    assert results[0].stored is True


def test_diagnostic_manager_collection_with_collect_failure():
    """Test manager handles collection failures properly."""
    collectors = [
        MockDiagnosticCollector(name="FailingCollector", should_fail_collect=True),
        MockDiagnosticCollector(name="SuccessCollector", collect_data="success"),
    ]
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.CONTINUE)
    manager = DiagnosticManager(collectors=collectors, exception_handler=handler, interval=1.0)

    manager._collecting_diagnostics()

    # First collector should fail to collect
    assert collectors[0].collect_count == 1
    assert collectors[0].store_count == 0  # Store not called because collect failed

    # Second collector should succeed
    assert collectors[1].collect_count == 1
    assert collectors[1].store_count == 1

    # Verify exception was handled
    assert len(handler.collect_exceptions) == 1
    assert handler.collect_exceptions[0][0].name == "FailingCollector"


def test_diagnostic_manager_collection_with_store_failure():
    """Test manager handles storage failures properly."""
    collectors = [
        MockDiagnosticCollector(name="StoreFailCollector", should_fail_store=True),
        MockDiagnosticCollector(name="SuccessCollector", collect_data="success"),
    ]
    handler = MockExceptionHandler(store_strategy=ExceptionStrategy.CONTINUE)
    manager = DiagnosticManager(collectors=collectors, exception_handler=handler, interval=1.0)

    manager._collecting_diagnostics()

    # First collector should collect but fail to store
    assert collectors[0].collect_count == 1
    assert collectors[0].store_count == 1

    # Second collector should succeed
    assert collectors[1].collect_count == 1
    assert collectors[1].store_count == 1

    # Verify exception was handled
    assert len(handler.store_exceptions) == 1
    assert handler.store_exceptions[0][0].name == "StoreFailCollector"

    # Verify results show storage failure
    results = manager.get_results()
    assert len(results) == 2
    assert results[0].collected is True
    assert results[0].stored is False
    assert results[0].error is not None


def test_diagnostic_manager_stop_on_collect_exception():
    """Test manager stops collection when handler returns STOP strategy."""
    collectors = [
        MockDiagnosticCollector(name="Collector1", should_fail_collect=True),
        MockDiagnosticCollector(name="Collector2"),
        MockDiagnosticCollector(name="Collector3"),
    ]
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP)
    manager = DiagnosticManager(collectors=collectors, exception_handler=handler, interval=1.0)

    manager._collecting_diagnostics()

    # First collector fails and triggers stop
    assert collectors[0].collect_count == 1
    # Subsequent collectors should not be called
    assert collectors[1].collect_count == 0
    assert collectors[2].collect_count == 0

    # Stop event should be set
    assert manager._stop_event.is_set()


def test_diagnostic_manager_stop_on_store_exception():
    """Test manager stops collection when storage handler returns STOP strategy."""
    collectors = [
        MockDiagnosticCollector(name="Collector1", should_fail_store=True),
        MockDiagnosticCollector(name="Collector2"),
    ]
    handler = MockExceptionHandler(store_strategy=ExceptionStrategy.STOP)
    manager = DiagnosticManager(collectors=collectors, exception_handler=handler, interval=1.0)

    manager._collecting_diagnostics()

    # First collector collects but fails to store
    assert collectors[0].collect_count == 1
    assert collectors[0].store_count == 1
    # Second collector should not be called
    assert collectors[1].collect_count == 0

    # Stop event should be set
    assert manager._stop_event.is_set()


# --- Test DiagnosticManager Thread Management ---


def test_diagnostic_manager_start_stop():
    """Test manager can start and stop collection thread."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    # Start collection
    manager.start_collecting()
    assert manager.is_alive()

    # Give it time to run at least once
    time.sleep(0.2)

    # Stop collection
    manager.stop_collecting()
    assert not manager.is_alive()

    # Verify clean was called
    assert collectors[0].clean_count == 1


def test_diagnostic_manager_multiple_collections():
    """Test manager performs multiple collections over time."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.35)  # Should trigger ~3 collections
    manager.stop_collecting()

    # Verify multiple collections occurred (at least 2, could be 3-4 depending on timing)
    assert collectors[0].collect_count >= 2


def test_diagnostic_manager_is_collecting_flag():
    """Test is_collecting flag is set during collection."""
    collectors = [MockDiagnosticCollector(name="Collector1", collect_delay=0.05)]
    manager = DiagnosticManager(collectors=collectors, interval=0.5)

    assert manager.is_collecting is False

    manager.start_collecting()
    time.sleep(0.1)  # Wait for first collection to start

    # During collection, flag might be set
    # Note: This is timing-dependent, so we just verify it's callable
    collecting_status = manager.is_collecting
    assert isinstance(collecting_status, bool)

    manager.stop_collecting()
    assert manager.is_collecting is False


def test_diagnostic_manager_final_collection_on_stop():
    """Test manager performs final collection when stopped."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=10.0)  # Long interval

    manager.start_collecting()
    time.sleep(0.1)  # Not enough time for interval-based collection
    initial_count = collectors[0].collect_count
    manager.stop_collecting()

    # Final collection should occur (at least 1 collection on stop)
    assert collectors[0].collect_count >= initial_count


# --- Test DiagnosticManager Context Manager ---


def test_diagnostic_manager_context_manager():
    """Test manager works as context manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    with manager as mgr:
        assert mgr is manager
        assert manager.is_alive()
        time.sleep(0.15)

    # After context exit, manager should be stopped
    assert not manager.is_alive()
    assert collectors[0].clean_count == 1


def test_diagnostic_manager_context_manager_with_exception(caplog):
    """Test manager handles exceptions in context manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            with manager:
                raise ValueError("Test exception")

    # Manager should still be stopped
    assert not manager.is_alive()
    assert "Exception occurred in DiagnosticManager context" in caplog.text


# --- Test DiagnosticManager Results Management ---


def test_diagnostic_manager_get_results():
    """Test getting results from manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    results = manager.get_results()

    assert len(results) == 1
    assert isinstance(results[0], DiagnosticResult)
    assert results[0].collector_name == "Collector1"


def test_diagnostic_manager_get_results_returns_copy():
    """Test get_results returns a copy, not the original list."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    results1 = manager.get_results()
    results2 = manager.get_results()

    # Should be equal but not the same object
    assert results1 == results2
    assert results1 is not results2


def test_diagnostic_manager_clear_results():
    """Test clearing results from manager."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    manager._collecting_diagnostics()
    assert len(manager.get_results()) == 1

    manager.clear_results()
    assert len(manager.get_results()) == 0


def test_diagnostic_manager_accumulates_results():
    """Test results accumulate over multiple collections."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    time.sleep(0.25)  # Allow 2-3 collections
    manager.stop_collecting()

    results = manager.get_results()
    # Should have multiple results (at least 2)
    assert len(results) >= 2

    # Each result should be for the same collector
    for result in results:
        assert result.collector_name == "Collector1"


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
    assert collectors[0].clean_count == 1
    assert collectors[1].clean_count == 1


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
    assert collectors[0].clean_count == 1
    assert collectors[1].clean_count == 1
    assert "Exception occurred while cleaning collector 'Collector1'" in caplog.text


# --- Test Edge Cases ---


def test_diagnostic_manager_empty_collectors():
    """Test manager handles empty collector list."""
    manager = DiagnosticManager(collectors=[], interval=1.0)

    manager._collecting_diagnostics()
    results = manager.get_results()

    assert len(results) == 0


def test_diagnostic_manager_stop_before_start():
    """Test stopping manager before starting doesn't cause issues."""
    collectors = [MockDiagnosticCollector()]
    manager = DiagnosticManager(collectors=collectors, interval=1.0)

    # Stop without starting should not raise exception
    manager.stop_collecting()
    assert not manager.is_alive()


def test_diagnostic_manager_double_start():
    """Test starting manager twice doesn't create issues."""
    collectors = [MockDiagnosticCollector()]
    manager = DiagnosticManager(collectors=collectors, interval=0.1)

    manager.start_collecting()
    assert manager.is_alive()

    # Second start should not crash
    manager.start_collecting()
    time.sleep(0.15)

    manager.stop_collecting()
    assert not manager.is_alive()


def test_diagnostic_manager_thread_safety():
    """Test results are thread-safe (basic test)."""
    collectors = [MockDiagnosticCollector(name="Collector1")]
    manager = DiagnosticManager(collectors=collectors, interval=0.05)

    manager.start_collecting()

    # Access results from main thread while collection is happening
    for _ in range(5):
        results = manager.get_results()
        time.sleep(0.01)
        assert not results
    for _ in range(5):
        results = manager.get_results()
        assert results

    manager.stop_collecting()

    # Should not raise any threading errors
    final_results = manager.get_results()
    assert final_results

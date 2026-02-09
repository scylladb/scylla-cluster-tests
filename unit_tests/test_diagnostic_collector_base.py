"""Unit tests for diagnostic_collector base classes and utilities."""

import logging
from typing import Any

import pytest

from sdcm.utils.diagnostic_collector import (
    DiagnosticCollector,
    DiagnosticException,
    ExceptionHandler,
    ExceptionStrategy,
)


# --- Mock Classes ---


class MockDiagnosticCollector(DiagnosticCollector):
    """Mock implementation of DiagnosticCollector for testing."""

    def __init__(self, name: str | None = None, collect_data: Any = None, should_fail_collect: bool = False):
        super().__init__(name)
        self.collect_data = collect_data or {"test": "data"}
        self.should_fail_collect = should_fail_collect
        self.collect_called = False
        self.store_called = False
        self.clean_called = False
        self.stored_data = None

    def collect(self) -> Any:
        """Mock collect implementation."""
        self.collect_called = True
        if self.should_fail_collect:
            raise DiagnosticException("Collection failed")
        return self.collect_data

    def store(self, result: Any):
        """Mock store implementation."""
        self.store_called = True
        self.stored_data = result

    def clean(self):
        """Mock clean implementation."""
        self.clean_called = True


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


def test_diagnostic_collector_workflow():
    """Test typical collector workflow: collect -> store -> clean."""
    collector = MockDiagnosticCollector(collect_data={"workflow": "data"})

    # Collect
    data = collector.collect()
    assert data == {"workflow": "data"}
    assert collector.collect_called is True

    # Store
    collector.store(data)
    assert collector.store_called is True
    assert collector.stored_data == data

    # Clean
    collector.clean()
    assert collector.clean_called is True


# --- Test ExceptionHandler ---


def test_exception_handler_handle_collecting_exception(caplog):
    """Test exception handler handles collecting exceptions."""
    handler = ExceptionHandler()
    collector = MockDiagnosticCollector(name="TestCollector")
    exception = Exception("Test exception")

    with caplog.at_level(logging.ERROR):
        strategy = handler.handle_exception_during_collecting(collector, exception)

    assert strategy == ExceptionStrategy.CONTINUE
    assert "Exception occurred in collector 'TestCollector'" in caplog.text
    assert "Test exception" in caplog.text


def test_exception_handler_handle_storing_exception(caplog):
    """Test exception handler handles storing exceptions."""
    handler = ExceptionHandler()
    collector = MockDiagnosticCollector(name="TestCollector")
    exception = Exception("Store failed")

    with caplog.at_level(logging.ERROR):
        strategy = handler.handle_exception_during_storing(collector, exception)

    assert strategy == ExceptionStrategy.CONTINUE
    assert "Exception occurred while storing diagnostics for collector 'TestCollector'" in caplog.text
    assert "Store failed" in caplog.text


def test_mock_exception_handler_tracks_collect_exceptions():
    """Test mock exception handler tracks collecting exceptions."""
    handler = MockExceptionHandler(collect_strategy=ExceptionStrategy.STOP)
    collector = MockDiagnosticCollector(name="TestCollector")
    exception = Exception("Test exception")

    strategy = handler.handle_exception_during_collecting(collector, exception)

    assert strategy == ExceptionStrategy.STOP
    assert len(handler.collect_exceptions) == 1
    assert handler.collect_exceptions[0][0] == collector
    assert handler.collect_exceptions[0][1] == exception


def test_mock_exception_handler_tracks_store_exceptions():
    """Test mock exception handler tracks storing exceptions."""
    handler = MockExceptionHandler(store_strategy=ExceptionStrategy.STOP)
    collector = MockDiagnosticCollector(name="TestCollector")
    exception = Exception("Store failed")

    strategy = handler.handle_exception_during_storing(collector, exception)

    assert strategy == ExceptionStrategy.STOP
    assert len(handler.store_exceptions) == 1
    assert handler.store_exceptions[0][0] == collector
    assert handler.store_exceptions[0][1] == exception


@pytest.mark.parametrize(
    "strategy",
    [
        ExceptionStrategy.CONTINUE,
        ExceptionStrategy.STOP,
    ],
)
def test_mock_exception_handler_strategies(strategy):
    """Test mock exception handler returns configured strategies."""
    handler = MockExceptionHandler(collect_strategy=strategy, store_strategy=strategy)
    collector = MockDiagnosticCollector()
    exception = Exception("Test")

    collect_result = handler.handle_exception_during_collecting(collector, exception)
    store_result = handler.handle_exception_during_storing(collector, exception)

    assert collect_result == strategy
    assert store_result == strategy

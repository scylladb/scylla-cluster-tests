"""Unit tests for diagnostic_collector base classes and utilities."""

import logging
from unittest.mock import MagicMock

import pytest

from sdcm.utils.diagnostic_collector import ExceptionHandler, ExceptionStrategy

from unit_tests.unit.diagnostic_collector import MockDiagnosticCollector, MockExceptionHandler


# --- Test DiagnosticCollector ---


def test_diagnostic_collector_workflow():
    """Test typical collector workflow: collect -> store -> clean."""
    collector = MockDiagnosticCollector(collect_data={"workflow": "data"})
    collector.store = MagicMock(wraps=collector.store)

    # Collect
    data = collector.collect()
    assert data == {"data": {"workflow": "data"}, "count": 1}
    assert collector.collect_count == 1

    # Store
    collector.store(data)
    collector.store.assert_called_once_with(data)

    # Clean
    collector.clean()
    assert collector.clean_count == 1


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

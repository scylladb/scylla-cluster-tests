"""Unit tests for diagnostic_collector base classes and utilities."""

from unittest.mock import MagicMock

from sdcm.utils.diagnostic_collector import ExceptionHandler, ExceptionStrategy

from unit_tests.unit.diagnostic_collector import MockDiagnosticCollector


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


def test_exception_handler_handle_collecting_exception(mock_collector, caplog):
    """Test exception handler handles collecting exceptions."""
    handler = ExceptionHandler()
    exception = Exception("Test exception")

    strategy = handler.handle_exception_during_collecting(mock_collector, exception)

    assert strategy == ExceptionStrategy.CONTINUE
    assert "Exception occurred in collector 'TestCollector'" in caplog.text
    assert "Test exception" in caplog.text


def test_exception_handler_handle_storing_exception(mock_collector, caplog):
    """Test exception handler handles storing exceptions."""
    handler = ExceptionHandler()
    exception = Exception("Store failed")

    strategy = handler.handle_exception_during_storing(mock_collector, exception)

    assert strategy == ExceptionStrategy.CONTINUE
    assert "Exception occurred while storing diagnostics for collector 'TestCollector'" in caplog.text
    assert "Store failed" in caplog.text


# MockExceptionHandler is a test double, so it is not tested here directly: its recording and
# strategy behaviour is exercised for real by the manager tests (test_manager.py), which would fail
# loudly if it broke.

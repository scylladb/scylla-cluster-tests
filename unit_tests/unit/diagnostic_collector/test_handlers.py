"""Unit tests for EventExceptionHandler."""

from unittest.mock import patch

import pytest

from sdcm.sct_events import Severity
from sdcm.utils.diagnostic_collector import ExceptionStrategy
from sdcm.utils.diagnostic_collector.handlers import EventExceptionHandler


@pytest.fixture
def no_events_device():
    """Simulate an environment without a running events device (unit tests, non-SCT consumers)."""
    with patch("sdcm.sct_events.events_device.get_events_main_device", side_effect=RuntimeError("no device")):
        yield


@pytest.mark.parametrize(
    "method, stage",
    [
        pytest.param("handle_exception_during_collecting", "collect", id="collect"),
        pytest.param("handle_exception_during_storing", "store", id="store"),
    ],
)
def test_event_exception_handler_publishes_event(mock_collector, events_function_scope, method, stage):
    """Each stage publishes a real TestFrameworkEvent naming the failing collector, stage and error."""
    handler = EventExceptionHandler()

    strategy = getattr(handler, method)(mock_collector, RuntimeError("boom"))

    published = events_function_scope.get_events_by_category()[Severity.WARNING.name]
    assert len(published) == 1, f"exactly one WARNING event should be published, got {published}"
    event = published[0]
    assert "TestFrameworkEvent" in event, f"a TestFrameworkEvent should be published, got {event}"
    assert f"source=TestCollector.diagnostics.{stage}()" in event, (
        f"the event should name the failing collector and stage, got {event}"
    )
    assert f"Diagnostics collector 'TestCollector' failed during {stage}" in event, (
        f"the event message should describe the failure, got {event}"
    )
    assert "exception=boom" in event, f"the event should carry the original exception, got {event}"
    assert strategy == ExceptionStrategy.CONTINUE, f"Publishing must not halt the run by default, got {strategy}"


def test_event_exception_handler_severity_and_strategy_are_configurable(mock_collector, events_function_scope):
    """A collector whose data the test depends on can escalate severity and halt the run."""
    handler = EventExceptionHandler(severity=Severity.ERROR, strategy=ExceptionStrategy.STOP)

    strategy = handler.handle_exception_during_collecting(mock_collector, RuntimeError("boom"))

    by_category = events_function_scope.get_events_by_category()
    assert len(by_category[Severity.ERROR.name]) == 1, (
        f"the failure should be published at the configured severity, got {by_category}"
    )
    assert by_category[Severity.WARNING.name] == [], "the default severity should not be used once configured"
    assert strategy == ExceptionStrategy.STOP, f"configured strategy should be returned, got {strategy}"


def test_event_exception_handler_falls_back_to_logging_without_events_device(mock_collector, caplog, no_events_device):
    """Without an events device the event is dumped to the logger instead of raising."""
    handler = EventExceptionHandler()

    strategy = handler.handle_exception_during_collecting(mock_collector, RuntimeError("boom"))

    assert strategy == ExceptionStrategy.CONTINUE, "fallback path must not change the strategy"
    assert "Diagnostics collector 'TestCollector' failed during collect" in caplog.text, (
        "the event must be dumped to the logger when no events device is available"
    )

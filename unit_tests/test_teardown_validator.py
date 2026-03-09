import logging
from unittest.mock import Mock, patch, mock_open

import pytest

from sdcm.teardown_validators.events import ErrorEventsValidator, Severity
from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


FAILING_EVENTS = {
    "only_class": {
        "event_class": "Event1",
    },
    "class_and_type": {
        "event_class": "Event2",
        "event_type": "Type2",
    },
    "class_and_type_and_regex": {
        "event_class": "Event3",
        "event_type": "Type3",
        "regex": ".*failing event.*",
    },
}


class FakeSCTConfiguration(SCTConfiguration):
    failing_events: list

    def __init__(self, failing_events: list):
        super().__init__(failing_events=failing_events)

    def _load_environment_variables(self):
        return {"teardown_validators": {"test_error_events": {"enabled": True, "failing_events": self.failing_events}}}


@pytest.fixture
def tester_mock():
    return Mock()


def setup_validator(tester_mock, failing_events):
    params = FakeSCTConfiguration(failing_events)
    validator = ErrorEventsValidator(params, tester_mock)
    return params, validator


def setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=None):
    get_events_main_device_mock.return_value.raw_events_log = "raw_events.log"
    tester_mock.get_event_summary.return_value = {Severity.CRITICAL.name: critical_events}
    return mock_open(read_data=event_data)


@patch("sdcm.teardown_validators.events.get_events_main_device")
def test_validate_no_failing_events_no_critical_events(get_events_main_device_mock, tester_mock):
    _, validator = setup_validator(tester_mock, list(FAILING_EVENTS.values()))
    event_data = (
        '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n'
        '{"severity": "ERROR", "base": "Event4", "type": "Type1", "line": "failing event line"}\n'
    )
    open_mock = setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=0)

    with patch("builtins.open", open_mock):
        validator.validate()
    assert tester_mock.get_test_status() == "SUCCESS"


@patch("sdcm.teardown_validators.events.get_events_main_device")
def test_validate_with_failing_event_class(get_events_main_device_mock, tester_mock):
    _, validator = setup_validator(tester_mock, [FAILING_EVENTS["only_class"]])
    event_data = (
        '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n'
        '{"severity": "ERROR", "base": "Event1", "type": "null", "line": "null"}\n'
    )
    open_mock = setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=0)

    with patch("builtins.open", open_mock):
        validator.validate()
    assert tester_mock.get_test_status() == "FAILED"


@patch("sdcm.teardown_validators.events.get_events_main_device")
def test_validate_with_failing_event_class_and_type(get_events_main_device_mock, tester_mock):
    _, validator = setup_validator(tester_mock, [FAILING_EVENTS["class_and_type"]])
    event_data = (
        '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n'
        '{"severity": "ERROR", "base": "Event2", "type": "Type2", "line": "null"}\n'
    )
    open_mock = setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=0)

    with patch("builtins.open", open_mock):
        validator.validate()
    assert tester_mock.get_test_status() == "FAILED"


@patch("sdcm.teardown_validators.events.get_events_main_device")
def test_validate_with_failing_event_class_and_type_and_regex(get_events_main_device_mock, tester_mock):
    _, validator = setup_validator(tester_mock, [FAILING_EVENTS["class_and_type_and_regex"]])
    event_data = (
        '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n'
        '{"severity": "ERROR", "base": "Event3", "type": "Type3", "line": "This is failing event line"}\n'
    )
    open_mock = setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=0)

    with patch("builtins.open", open_mock):
        validator.validate()
    assert tester_mock.get_test_status() == "FAILED"


@patch("sdcm.teardown_validators.events.get_events_main_device")
def test_validate_with_failing_events_with_critical_events(get_events_main_device_mock, tester_mock):
    _, validator = setup_validator(tester_mock, [FAILING_EVENTS["class_and_type_and_regex"]])
    event_data = (
        '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n'
        '{"severity": "ERROR", "base": "Event3", "type": "Type3", "line": "This is failing event line"}\n'
    )
    open_mock = setup_mocks(tester_mock, get_events_main_device_mock, event_data, critical_events=1)

    with patch("builtins.open", open_mock):
        validator.validate()
    assert tester_mock.get_test_status() == "FAILED"

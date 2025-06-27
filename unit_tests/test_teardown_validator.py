import logging
import unittest
from unittest.mock import Mock, patch, mock_open

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
    def __init__(self, failing_events: list):
        self.failing_events = failing_events
        super().__init__()

    def _load_environment_variables(self):
        return {"teardown_validators": {"test_error_events": {"enabled": True, "failing_events": self.failing_events}}}


class TestErrorEventsValidator(unittest.TestCase):
    def setUp(self):
        self.tester_mock = Mock()

    def setup_validator(self, failing_events):
        self.params = FakeSCTConfiguration(failing_events)
        self.validator = ErrorEventsValidator(self.params, self.tester_mock)

    def setup_mocks(self, get_events_main_device_mock, event_data, critical_events=None):
        get_events_main_device_mock.return_value.raw_events_log = "raw_events.log"
        self.tester_mock.get_event_summary.return_value = {Severity.CRITICAL.name: critical_events}
        open_mock = mock_open(read_data=event_data)
        return open_mock

    @patch("sdcm.teardown_validators.events.get_events_main_device")
    def test_validate_no_failing_events_no_critical_events(self, get_events_main_device_mock):
        self.setup_validator(list(FAILING_EVENTS.values()))
        event_data = '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n{"severity": "ERROR", "base": "Event4", "type": "Type1", "line": "failing event line"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch("builtins.open", open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), "SUCCESS")

    @patch("sdcm.teardown_validators.events.get_events_main_device")
    def test_validate_with_failing_event_class(self, get_events_main_device_mock):
        self.setup_validator([FAILING_EVENTS["only_class"]])
        event_data = '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n{"severity": "ERROR", "base": "Event1", "type": "null", "line": "null"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch("builtins.open", open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), "FAILED")

    @patch("sdcm.teardown_validators.events.get_events_main_device")
    def test_validate_with_failing_event_class_and_type(self, get_events_main_device_mock):
        self.setup_validator([FAILING_EVENTS["class_and_type"]])
        event_data = '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n{"severity": "ERROR", "base": "Event2", "type": "Type2", "line": "null"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch("builtins.open", open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), "FAILED")

    @patch("sdcm.teardown_validators.events.get_events_main_device")
    def test_validate_with_failing_event_class_and_type_and_regex(self, get_events_main_device_mock):
        self.setup_validator([FAILING_EVENTS["class_and_type_and_regex"]])
        event_data = '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n{"severity": "ERROR", "base": "Event3", "type": "Type3", "line": "This is failing event line"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch("builtins.open", open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), "FAILED")

    @patch("sdcm.teardown_validators.events.get_events_main_device")
    def test_validate_with_failing_events_with_critical_events(self, get_events_main_device_mock):
        self.setup_validator([FAILING_EVENTS["class_and_type_and_regex"]])
        event_data = '{"severity": "WARNING", "base": "Event1", "type": "Type1", "line": "failing event line"}\n{"severity": "ERROR", "base": "Event3", "type": "Type3", "line": "This is failing event line"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=1)

        with patch("builtins.open", open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), "FAILED")

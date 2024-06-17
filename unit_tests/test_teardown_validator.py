import logging
import unittest
from unittest.mock import Mock, patch, mock_open

from sdcm.teardown_validators.events import ErrorEventsValidator, Severity
from sdcm.sct_config import SCTConfiguration

LOGGER = logging.getLogger(__name__)


class FakeSCTConfiguration(SCTConfiguration):
    def _load_environment_variables(self):
        return {
            'teardown_validators': {
                'test_error_events': {
                    'enabled': True,
                    'failing_events': ['event1', 'EveNT2']
                }
            }
        }


class TestErrorEventsValidator(unittest.TestCase):
    def setUp(self):
        self.tester_mock = Mock()
        self.params = FakeSCTConfiguration()
        self.validator = ErrorEventsValidator(self.params, self.tester_mock)

    def setup_mocks(self, get_events_main_device_mock, event_data, critical_events=None):
        get_events_main_device_mock.return_value.raw_events_log = 'raw_events.log'
        self.tester_mock.get_event_summary.return_value = {Severity.CRITICAL.name: critical_events}
        open_mock = mock_open(read_data=event_data)
        return open_mock

    @patch('sdcm.teardown_validators.events.get_events_main_device')
    def test_validate_no_failing_events_no_critical_events(self, get_events_main_device_mock):
        event_data = '{"severity": "WARNING", "base": "event1"}\n{"severity": "ERROR", "base": "event3"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch('builtins.open', open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), 'SUCCESS')

    @patch('sdcm.teardown_validators.events.get_events_main_device')
    def test_validate_with_failing_events_no_critical_events(self, get_events_main_device_mock):
        event_data = '{"severity": "WARNING", "base": "event1"}\n{"severity": "ERROR", "base": "evENT2"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=0)

        with patch('builtins.open', open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), 'FAILED')

    @patch('sdcm.teardown_validators.events.get_events_main_device')
    def test_validate_no_failing_events_with_critical_events(self, get_events_main_device_mock):
        event_data = '{"severity": "WARNING", "base": "event1"}\n{"severity": "ERROR", "base": "event3"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=1)

        with patch('builtins.open', open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), 'FAILED')

    @patch('sdcm.teardown_validators.events.get_events_main_device')
    def test_validate_with_failing_events_with_critical_events(self, get_events_main_device_mock):
        event_data = '{"severity": "WARNING", "base": "event1"}\n{"severity": "ERROR", "base": "EVENT2"}\n'
        open_mock = self.setup_mocks(get_events_main_device_mock, event_data, critical_events=1)

        with patch('builtins.open', open_mock):
            self.validator.validate()
        self.assertEqual(self.tester_mock.get_test_status(), 'FAILED')

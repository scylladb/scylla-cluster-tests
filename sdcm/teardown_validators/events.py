import json
import logging

from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events import Severity
from sdcm.teardown_validators.base import TeardownValidator

LOGGER = logging.getLogger(__name__)


class ErrorEventsValidator(TeardownValidator):  # pylint: disable=too-few-public-methods
    """
    A teardown validator that checks test events log for presence of specific error events and critical-level events.
    If the test doesn't have any of the defined failing error events or critical events, its status
    defaults to 'SUCCESS'.

    Usage:
        An instance of ErrorEventsValidator can be used during teardown of a test, where specific error events
        should not be considered as test failures.
        This is particularly useful in complex testing environments where multiple parallel operations may lead to
        non-critical errors that should not directly influence the overall test outcome.
    """
    validator_name = 'test_error_events'

    def validate(self):
        LOGGER.info("Running validation of ERROR events for parallel nemeses")
        failing_event_types = [et.lower() for et in self.configuration.get("failing_events")]
        raw_events_log = get_events_main_device(_registry=self.tester.events_processes_registry).raw_events_log

        with open(raw_events_log, encoding='utf-8') as events_file:
            events = (json.loads(line) for line in events_file)
            failing_error_events = [
                event for event in events
                if event['severity'] == Severity.ERROR.name and event['base'].lower() in failing_event_types]

        critical_events = self.tester.get_event_summary().get(Severity.CRITICAL.name, 0)
        self.tester.get_test_status = lambda: 'FAILED' if (failing_error_events or critical_events) else 'SUCCESS'

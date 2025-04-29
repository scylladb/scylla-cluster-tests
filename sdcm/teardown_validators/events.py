import json
import logging
import re
from functools import reduce
from typing import Optional, Union

from sdcm.sct_events.events_device import get_events_main_device
from sdcm.sct_events import Severity
from sdcm.teardown_validators.base import TeardownValidator

LOGGER = logging.getLogger(__name__)


class FailingEventsFilter:
    def __init__(
            self,
            event_class: str,
            event_type: Optional[str] = None,
            regex: Optional[Union[str, re.Pattern]] = None):
        """Filter to match only events that qualify as valid reason to fail a test."""
        self.event_class = event_class
        self.event_type = event_type
        self.regex = re.compile(regex) if isinstance(regex, str) else regex

    def filter_events(self, events: list[dict]) -> list[dict]:
        filtered_events = []
        for event in events:
            if self._matches(event):
                filtered_events.append(event)
        return filtered_events

    def _matches(self, event: dict) -> bool:
        match = (
            (event['severity'] == Severity.ERROR.name) and
            (event['base'] == self.event_class) and
            (self.event_type is None or event['type'] == self.event_type) and
            (self.regex is None or self.regex.search(event['line']))
        )
        return match


class ErrorEventsValidator(TeardownValidator):
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
        LOGGER.info("Running validation of failing events for parallel nemeses")
        filters = [FailingEventsFilter(**event) for event in self.configuration.get("failing_events")]
        raw_events_log = get_events_main_device(_registry=self.tester.events_processes_registry).raw_events_log

        with open(raw_events_log, encoding='utf-8') as events_file:
            initial_events = (json.loads(line) for line in events_file)
            failing_events = reduce(lambda events, f: f.filter_events(events), filters, initial_events)

        critical_events = self.tester.get_event_summary().get(Severity.CRITICAL.name, 0)
        self.tester.get_test_status = lambda: 'FAILED' if (failing_events or critical_events) else 'SUCCESS'

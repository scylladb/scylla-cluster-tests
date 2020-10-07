import sys

from sdcm.sct_events import Severity
from sdcm.services.base import DetachThreadService
from sdcm.services.event_device import EventsDevice


class TestFailure(Exception):
    pass


class EventsAnalyzer(DetachThreadService):
    _interval = 0
    stop_priority = 60
    _events_to_fail_the_test = ['CassandraStressEvent', 'ScyllaBenchEvent', 'YcsbStressEvent', 'NdbenchStressEvent']

    def __init__(self, events_device: EventsDevice, setup_info):
        self._events_device = events_device
        self._setup_info = setup_info
        super().__init__()

    def _service_body(self):
        for event_class, message_data in self._events_device.subscribe_events(stop_event=self._termination_event):
            try:
                if event_class == 'TestResultEvent':
                    # don't send kill test cause of those event, test is already done when those are sent out
                    continue

                if event_class in self._events_to_fail_the_test and message_data.type == 'failure':
                    raise TestFailure(f"Stress Command failed: {message_data}")

                if message_data.severity == Severity.CRITICAL:
                    raise TestFailure(f"Got critical event: {message_data}")

            except TestFailure:
                self.kill_test(sys.exc_info())

            except Exception:  # pylint: disable=broad-except
                self._log.exception("analyzer logic failed")

    def kill_test(self, backtrace_with_reason):
        self.stop()
        if self._setup_info and self._setup_info.tester_obj():
            self._setup_info.tester_obj().kill_test(backtrace_with_reason)
        else:
            self._log.error("no test was register using 'Setup.set_tester_obj()', not killing")

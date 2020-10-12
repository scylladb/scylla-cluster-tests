# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import sys
import logging
import threading

from sdcm.cluster import Setup
from sdcm.sct_events import subscribe_events
from sdcm.sct_events.base import Severity
from sdcm.sct_events.events_processes import EVENTS_PROCESSES
from sdcm.sct_events.decorators import raise_event_on_failure


LOADERS_EVENTS = {"CassandraStressEvent", "ScyllaBenchEvent", "YcsbStressEvent", "NdbenchStressEvent", }

LOGGER = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class EventsAnalyzer(threading.Thread):
    def __init__(self):
        self.stop_event = threading.Event()
        super().__init__(daemon=True)

    @raise_event_on_failure
    def run(self):
        for event_class, message_data in subscribe_events(stop_event=self.stop_event):
            try:
                if event_class == 'TestResultEvent':
                    # don't send kill test cause of those event, test is already done when those are sent out
                    continue

                if event_class in LOADERS_EVENTS and message_data.type == 'failure':
                    raise TestFailure(f"Stress command failed: {message_data}")

                if message_data.severity == Severity.CRITICAL:
                    raise TestFailure(f"Got critical event: {message_data}")

            except TestFailure:
                self.kill_test(sys.exc_info())

            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("analyzer logic failed")

    def terminate(self):
        self.stop_event.set()

    def stop(self, timeout: float = None):
        self.stop_event.set()
        self.join(timeout)

    def kill_test(self, backtrace_with_reason):
        self.terminate()
        if not Setup.tester_obj():
            LOGGER.error("no test was register using 'Setup.set_tester_obj()', not killing")
            return
        Setup.tester_obj().kill_test(backtrace_with_reason)


def stop_events_analyzer():
    if analyzer := EVENTS_PROCESSES.get("EVENTS_ANALYZER"):
        analyzer.terminate()
        analyzer.join(timeout=60)


__all__ = ("EventsAnalyzer", "TestFailure", "stop_events_analyzer", )

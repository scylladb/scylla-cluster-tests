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
from typing import Tuple, Any, Optional
from functools import partial

from sdcm.cluster import TestConfig
from sdcm.sct_events import Severity
from sdcm.sct_events.events_processes import \
    EVENTS_ANALYZER_ID, EventsProcessesRegistry, BaseEventsProcess, \
    start_events_process, get_events_process, verbose_suppress


LOADERS_EVENTS = \
    {"CassandraStressEvent", "ScyllaBenchEvent", "YcsbStressEvent", "NdbenchStressEvent", "CDCReaderStressEvent"}

LOGGER = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class EventsAnalyzer(BaseEventsProcess[Tuple[str, Any], None], threading.Thread):
    def run(self) -> None:
        for event_tuple in self.inbound_events():
            with verbose_suppress("EventsAnalyzer failed to process %s", event_tuple):
                event_class, event = event_tuple  # try to unpack event from EventsDevice

                # Don't kill the test cause of TestResultEvent: it was done already when this event was sent out.
                if event_class == "TestResultEvent" or event.severity != Severity.CRITICAL:
                    continue

                try:
                    if event_class in LOADERS_EVENTS:
                        raise TestFailure(f"Stress command failed: {event}")
                    raise TestFailure(f"Got critical event: {event}")
                except TestFailure:
                    self.kill_test(sys.exc_info())

    def kill_test(self, backtrace_with_reason, memo={}) -> None:  # pylint: disable=dangerous-default-value  # noqa: B006
        self.terminate()
        if tester := TestConfig().tester_obj():
            if memo:
                # NOTE: in some cases we may get flooded with the CRITICAL events.
                #       And we should call 'kill_test' only once to avoid following:
                #       - Long awaiting of the events handler closing in the tearDown stage.
                #         It won't return until all the events are processed causing job timeout.
                #       - Catching old python bug: https://bugs.python.org/issue24283
                #       Also, in general, it makes no sense to kill test more than once.
                return
            memo["kill_test_has_run"] = True
            tester.kill_test(backtrace_with_reason)
        else:
            LOGGER.error("No test was registered using `TestConfig.set_tester_obj()', do not kill")


start_events_analyzer = partial(start_events_process, EVENTS_ANALYZER_ID, EventsAnalyzer)


def stop_events_analyzer(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    if analyzer := get_events_process(EVENTS_ANALYZER_ID, _registry=_registry):
        analyzer.stop(timeout=60)


__all__ = ("start_events_analyzer", "stop_events_analyzer", )

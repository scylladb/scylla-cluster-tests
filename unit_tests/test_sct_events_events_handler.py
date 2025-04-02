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
# Copyright (c) 2022 ScyllaDB
import time
import unittest
import unittest.mock


from sdcm.sct_events.event_handler import start_events_handler
from sdcm.sct_events.events_processes import get_events_process, EVENTS_HANDLER_ID
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.sct_events.setup import EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.test_config import TestConfig
from unit_tests.lib.events_utils import EventsUtilsMixin


class TestEventsHandler(unittest.TestCase, EventsUtilsMixin):

    @classmethod
    def setUpClass(cls) -> None:
        cls.setup_events_processes(events_device=False, events_main_device=True, registry_patcher=False)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.teardown_events_processes()

    def test_events_handler(self):
        with unittest.mock.patch("sdcm.sct_events.handlers.schema_disagreement.SchemaDisagreementHandler.handle", spec=True) as mock:
            start_events_handler(_registry=self.events_processes_registry)
            events_handler = get_events_process(name=EVENTS_HANDLER_ID, _registry=self.events_processes_registry)
            time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

            TestConfig().set_tester_obj("abc")
            time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

            try:
                assert events_handler.is_alive()
                assert events_handler._registry == self.events_main_device._registry
                assert events_handler._registry == self.events_processes_registry
                event1 = CassandraStressLogEvent.SchemaDisagreement()
                with self.wait_for_n_events(events_handler, count=1, timeout=1):
                    self.events_main_device.publish_event(event1)
                mock.assert_called_once()
                assert mock.call_args.kwargs["event"] == event1
            finally:
                events_handler.stop(timeout=1)

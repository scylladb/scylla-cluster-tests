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
import unittest.mock

import pytest

from sdcm.sct_events.event_handler import start_events_handler
from sdcm.sct_events.events_processes import get_events_process, EVENTS_HANDLER_ID
from sdcm.sct_events.loaders import CassandraStressLogEvent
from sdcm.sct_events.setup import EVENTS_PROCESS_STOP_TIMEOUT, EVENTS_SUBSCRIBERS_START_DELAY
from sdcm.test_config import TestConfig


@pytest.fixture
def tester_obj():
    """Provide a tester object to code reading it off the TestConfig singleton.

    TestConfig keeps the tester object on a class attribute and set_tester_obj() assigns it only once,
    so a test that sets it can never unset it and leaks into every test that runs afterwards.
    Patching the accessor gives the same value without touching the singleton state.
    """
    with unittest.mock.patch.object(TestConfig, "tester_obj", return_value="abc") as mock:
        yield mock


def test_events_handler(main_events_context, tester_obj):
    with unittest.mock.patch(
        "sdcm.sct_events.handlers.schema_disagreement.SchemaDisagreementHandler.handle", spec=True
    ) as mock:
        start_events_handler(_registry=main_events_context.events_processes_registry)
        events_handler = get_events_process(
            name=EVENTS_HANDLER_ID, _registry=main_events_context.events_processes_registry
        )
        time.sleep(EVENTS_SUBSCRIBERS_START_DELAY)

        try:
            assert events_handler.is_alive()
            assert events_handler._registry == main_events_context.events_main_device._registry
            assert events_handler._registry == main_events_context.events_processes_registry
            event1 = CassandraStressLogEvent.SchemaDisagreement()
            with main_events_context.wait_for_n_events(events_handler, count=1, timeout=1):
                main_events_context.events_main_device.publish_event(event1)
            mock.assert_called_once()
            assert mock.call_args.kwargs["event"] == event1
            assert mock.call_args.kwargs["tester_obj"] == tester_obj.return_value
        finally:
            events_handler.stop(timeout=EVENTS_PROCESS_STOP_TIMEOUT)

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

import logging
import threading
from functools import partial
from typing import Tuple, Any

from sdcm.cluster import TestConfig
from sdcm.sct_events.events_processes import \
    EVENTS_HANDLER_ID, BaseEventsProcess, \
    start_events_process, verbose_suppress
from sdcm.sct_events.handlers.schema_disagreement import SchemaDisagreementHandler

LOGGER = logging.getLogger(__name__)


class TestFailure(Exception):
    pass


class EventsHandler(BaseEventsProcess[Tuple[str, Any], None], threading.Thread):
    """Runs handlers for events (according to EventsHandler.handlers mapping dict).

     Handlers are created to gather additional information for issue investigation purposes."""
    handlers = {
        "CassandraStressLogEvent.SchemaDisagreement": SchemaDisagreementHandler(),
    }

    def run(self) -> None:
        LOGGER.debug("Started events handler")
        for event_tuple in self.inbound_events():
            with verbose_suppress("EventsHandler failed to process %s", event_tuple):
                event_class, event = event_tuple  # try to unpack event from EventsDevice
                full_class_name = event_class + f".{event.type}" if event.type else event_class
                handler = self.handlers.get(full_class_name)
                if handler is None:
                    continue
                tester_obj = TestConfig().tester_obj()
                if not tester_obj:
                    LOGGER.error("Tester object was not initialized, skipping handling event: %s", event_class)
                try:
                    LOGGER.debug("Found handler %s for %s event. Running it.",
                                 handler.__class__.__name__, full_class_name)
                    handler.handle(event=event, tester_obj=tester_obj)
                except Exception as exc:
                    LOGGER.exception("failed to handle event using event handler: %s", exc)


start_events_handler = partial(start_events_process, EVENTS_HANDLER_ID, EventsHandler)
__all__ = ("start_events_handler",)

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
# Copyright (c) 2025 ScyllaDB

import time
import logging
import threading
from typing import NewType, Dict, Any, Tuple, Optional, Callable, cast
from functools import partial
from collections import defaultdict

from argus.common.sct_types import RawEventPayload
from sdcm.sct_events.events_processes import \
    EVENTS_ARGUS_ANNOTATOR_ID, EVENTS_ARGUS_AGGREGATOR_ID, EVENTS_ARGUS_POSTMAN_ID, \
    EventsProcessesRegistry, BaseEventsProcess, EventsProcessPipe, \
    start_events_process, get_events_process, verbose_suppress
from sdcm.utils.argus import Argus


ARGUS_EVENT_AGGREGATOR_TIME_WINDOW: float = 90  # seconds
LOGGER = logging.getLogger(__name__)


SCTArgusEvent = NewType("SCTArgusEvent", RawEventPayload)
SCTArgusEventKey = NewType("SCTArgusEventKey", Tuple[str, ...])


class ArgusEventCollector(EventsProcessPipe[Tuple[str, Any], SCTArgusEvent]):
    def run(self) -> None:
        if Argus.get() and (client := Argus.get().client):
            run_id = client.run_id
        else:
            run_id = None
        for event_tuple in self.inbound_events():
            with verbose_suppress("ArgusEventCollector failed to process %s", event_tuple):
                event_class, event = event_tuple  # try to unpack event from EventsDevice
                if not event.publish_to_argus:
                    continue
                evt = SCTArgusEvent({
                    "run_id": run_id,
                    "severity": event.severity.name,
                    "ts": event.timestamp,
                    "duration": getattr(event, "duration", None),
                    "event_type": event_class,
                    "message": str(event),
                    "known_issue": getattr(event, "known_issue", None),
                    "nemesis_name": getattr(event, "nemesis_name", None),
                    "nemesis_status": getattr(event, "nemesis_status", None),
                    "node": getattr(event, "node", None),
                    "received_timestamp": getattr(event, "received_timestamp", None),
                    "target_node": getattr(event, "target_node", None),
                })
                self.outbound_queue.put(evt)


class ArgusEventAggregator(EventsProcessPipe[SCTArgusEvent, SCTArgusEvent]):
    inbound_events_process = EVENTS_ARGUS_ANNOTATOR_ID
    time_window = ARGUS_EVENT_AGGREGATOR_TIME_WINDOW

    def run(self) -> None:
        time_window_counters: Dict[SCTArgusEventKey, int] = defaultdict(int)

        for event in self.inbound_events():
            with verbose_suppress("ArgusEventAggregator failed to process an event %s", event):
                event_key = self.unique_key(event)
                if time.perf_counter() - time_window_counters.get(event_key, 0) > ARGUS_EVENT_AGGREGATOR_TIME_WINDOW:
                    # not seen event from sometime or ever
                    time_window_counters[event_key] = time.perf_counter()
                else:
                    # recently seen
                    continue

                # Put the event to the posting queue.
                LOGGER.debug("Event moving to posting queue: %s", event)
                self.outbound_queue.put(event)

    @staticmethod
    def unique_key(event: SCTArgusEvent) -> SCTArgusEventKey:
        return SCTArgusEventKey(tuple([event["run_id"], event["severity"], event["event_type"]]))


class ArgusEventPostman(BaseEventsProcess[SCTArgusEvent, None], threading.Thread):
    inbound_events_process = EVENTS_ARGUS_AGGREGATOR_ID

    def __init__(self, _registry: EventsProcessesRegistry):
        self.enabled = threading.Event()
        self._argus_client = None
        super().__init__(_registry=_registry)

    def run(self) -> None:
        self.enabled.wait()

        for event in self.inbound_events():  # events from ArgusAggregator
            with verbose_suppress("ArgusEventPostman failed to post an event to '%s' "
                                  "endpoint.\nEvent: %s", self._argus_client.Routes.SUBMIT_EVENT, event):
                if self._argus_client:
                    self._argus_client.submit_event(event)

    def enable_argus_posting(self) -> None:
        self._argus_client = Argus.get().client

    def start_posting_argus_events(self):
        self.enabled.set()

    def terminate(self) -> None:
        super().terminate()
        self.enabled.set()


start_argus_event_collector = partial(start_events_process, EVENTS_ARGUS_ANNOTATOR_ID, ArgusEventCollector)
start_argus_aggregator = partial(start_events_process, EVENTS_ARGUS_AGGREGATOR_ID, ArgusEventAggregator)
start_argus_postman = partial(start_events_process, EVENTS_ARGUS_POSTMAN_ID, ArgusEventPostman)
get_argus_postman = cast(Callable[..., ArgusEventPostman], partial(get_events_process, EVENTS_ARGUS_POSTMAN_ID))


def start_argus_pipeline(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    start_argus_event_collector(_registry=_registry)
    start_argus_aggregator(_registry=_registry)
    start_argus_postman(_registry=_registry)


def enable_argus_posting(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    get_argus_postman(_registry=_registry).enable_argus_posting()


def start_posting_argus_events(_registry: Optional[EventsProcessesRegistry] = None) -> None:
    get_argus_postman(_registry=_registry).start_posting_argus_events()


__all__ = ("start_argus_pipeline", "enable_argus_posting", "start_posting_argus_events")

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

import time
import queue
import ctypes
import pickle
import logging
import multiprocessing
from typing import Optional, Generator, Any, Tuple, Callable, cast, Dict
from pathlib import Path
from functools import cached_property, partial
from uuid import UUID

from sdcm.utils.action_logger import get_action_logger
from sdcm.sct_events import Severity

from sdcm.sct_events.base import max_severity
from sdcm.sct_events.system import SystemEvent
from sdcm.sct_events.filters import BaseFilter

import zmq

from sdcm.sct_events.events_processes import \
    EVENTS_MAIN_DEVICE_ID, StopEvent, EventsProcessesRegistry, \
    start_events_process, get_events_process, verbose_suppress, suppress_interrupt


EVENTS_DEVICE_START_DELAY: float = 0  # seconds
EVENTS_DEVICE_START_TIMEOUT: float = 30  # seconds
SUB_POLLING_TIMEOUT: int = 1000  # milliseconds
PUB_QUEUE_WAIT_TIMEOUT: float = 1  # seconds
PUB_QUEUE_EVENTS_RATE: float = 0  # seconds
PUBLISH_EVENT_TIMEOUT: float = 5  # seconds
FILTERS_GC_PERIOD: float = 60  # Cleanup old filters once in a while

EVENTS_LOG_DIR: str = "events_log"
RAW_EVENTS_LOG: str = "raw_events.log"

LOGGER = logging.getLogger(__name__)
ACTION_LOGGER = get_action_logger("event")


class EventsDevice(multiprocessing.Process):
    start_delay = EVENTS_DEVICE_START_DELAY
    start_timeout = EVENTS_DEVICE_START_TIMEOUT
    sub_polling_timeout = SUB_POLLING_TIMEOUT
    pub_queue_wait_timeout = PUB_QUEUE_WAIT_TIMEOUT
    pub_queue_events_rate = PUB_QUEUE_EVENTS_RATE

    def __init__(self, _registry: EventsProcessesRegistry):
        self._registry = _registry
        self._events_counter = multiprocessing.Value(ctypes.c_uint32, 0)

        self._running = multiprocessing.Event()
        self._sub_port = multiprocessing.Value(ctypes.c_uint16, 0)
        self._queue = multiprocessing.Queue()
        self._raw_events_lock = multiprocessing.RLock()
        self.events_log_base_dir.mkdir(parents=True, exist_ok=True)

        super().__init__(daemon=True)

    @property
    def events_counter(self):
        return self._events_counter.value

    @cached_property
    def events_log_base_dir(self) -> Path:
        return self._registry.log_dir / EVENTS_LOG_DIR

    @cached_property
    def raw_events_log(self) -> Path:
        return self.events_log_base_dir / RAW_EVENTS_LOG

    def stop(self, timeout: Optional[float] = None) -> None:
        self._running.clear()
        self.join(timeout)

    @property
    def subscribe_address(self) -> str:
        if self._running.wait(timeout=self.start_timeout):
            return f"tcp://localhost:{self._sub_port.value}"
        raise RuntimeError("EventsDevice is not ready to send events.")

    def run(self):
        with suppress_interrupt(), verbose_suppress("EventsDevice failed"):
            with zmq.Context() as ctx, ctx.socket(zmq.PUB) as pub, ctx.socket(zmq.SUB) as sub:
                self._sub_port.value = pub.bind_to_random_port("tcp://*")
                self._running.set()

                LOGGER.debug("EventsDevice listen on %s", self.subscribe_address)

                # Delivery verification subscriber.
                sub.connect(self.subscribe_address)
                sub.subscribe(b"")

                time.sleep(self.start_delay)

                while self._running.is_set() or not self._queue.empty():
                    try:
                        event = self._queue.get(timeout=self.pub_queue_wait_timeout)
                        try:
                            pub.send(event)
                        except zmq.ZMQError:
                            LOGGER.exception("EventsDevice failed to send %s", pickle.loads(event))
                        else:
                            try:
                                if sub.poll(timeout=self.sub_polling_timeout) and sub.recv(zmq.NOBLOCK) == event:
                                    continue  # everything is OK, we can go to send next event in the queue.
                            except zmq.ZMQError:
                                pass
                            LOGGER.error("EventsDevice failed to verify delivery of %s", pickle.loads(event))
                        time.sleep(self.pub_queue_events_rate)
                    except queue.Empty:
                        pass

    def publish_event(self, event, timeout=PUBLISH_EVENT_TIMEOUT) -> None:
        with verbose_suppress("%s: failed to write %s to %s", self, event, self.raw_events_log):
            with self._raw_events_lock, open(self.raw_events_log, "ab+", buffering=0) as log_file:
                log_file.write(event.to_json().encode("utf-8") + b"\n")

        with verbose_suppress("%s: failed to publish %s", self, event):
            self._queue.put(pickle.dumps(event), timeout=timeout)
            self._events_counter.value += 1

    def _sub_socket(self, ctx: zmq.Context) -> zmq.Socket:
        LOGGER.debug("Subscribe to %s", self.subscribe_address)
        sub = ctx.socket(zmq.SUB)
        sub.connect(self.subscribe_address)
        sub.subscribe(b"")
        return sub

    def inbound_events(self, stop_event: StopEvent) -> Generator[Any, None, None]:
        with zmq.Context() as ctx, self._sub_socket(ctx) as sub:
            while not stop_event.is_set():
                if sub.poll(timeout=self.sub_polling_timeout):
                    yield sub.recv_pyobj(flags=zmq.NOBLOCK)

    def outbound_events(self,
                        stop_event: StopEvent,
                        events_counter: multiprocessing.Value) -> Generator[Tuple[str, Any], None, None]:
        filters: Dict[UUID, BaseFilter] = {}
        filters_gc_next_hit = time.perf_counter() + FILTERS_GC_PERIOD

        with suppress_interrupt():
            for events_counter.value, obj in enumerate(self.inbound_events(stop_event=stop_event), start=1):
                if filters_gc_next_hit < time.perf_counter():
                    # Run filter GC once in FILTERS_GC_PERIOD seconds
                    for filter_key, filter_obj in list(filters.items()):
                        if filter_obj.is_deceased():
                            del filters[filter_key]
                    filters_gc_next_hit = time.perf_counter() + FILTERS_GC_PERIOD

                if isinstance(obj, BaseFilter):
                    if obj.clear_filter and not obj.expire_time:
                        LOGGER.debug("%s: delete filter with uuid=%s", self, obj.uuid)
                        filters.pop(obj.uuid, None)
                    elif obj.clear_filter and obj.expire_time and obj.uuid in filters:
                        LOGGER.debug("%s: set expire_time to %s for filter with uuid=%s",
                                     self, obj.expire_time, obj.uuid)
                        filters[obj.uuid].expire_time = obj.expire_time
                    else:
                        LOGGER.debug("%s: add filter %s with uuid=%s", self, obj, obj.uuid)
                        filters[obj.uuid] = obj

                if isinstance(obj, SystemEvent):
                    continue

                obj_filtered = any(f.eval_filter(obj) for f in filters.values())

                if obj_filtered:
                    continue

                if (obj_max_severity := max_severity(obj)).value < obj.severity.value:
                    LOGGER.warning("Limit %s severity to %s as configured", obj, obj_max_severity)
                    obj.severity = obj_max_severity

                # Log to actions.log (only non-filtered Error/Critical events)
                if obj.severity.value > Severity.WARNING.value:
                    event_type = obj.base
                    if getattr(obj, 'type', None):
                        event_type += f".{obj.type}"
                    if getattr(obj, 'subtype', None):
                        event_type += f".{obj.subtype}"
                    ACTION_LOGGER.error(
                        f"{event_type} {obj.severity.name} Event (id={obj.event_id}) on node: {getattr(obj, 'node', None)}"
                    )

                yield obj.base, obj

    def is_alive(self) -> bool:
        return self._running.is_set()


start_events_main_device = partial(start_events_process, EVENTS_MAIN_DEVICE_ID, EventsDevice)
get_events_main_device = cast(Callable[..., EventsDevice], partial(get_events_process, EVENTS_MAIN_DEVICE_ID))


__all__ = ("EventsDevice", "start_events_main_device", "get_events_main_device", )

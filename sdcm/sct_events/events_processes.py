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

from __future__ import annotations

import abc
import queue
import ctypes
import logging
import threading
import multiprocessing
from typing import Union, Generator, Protocol, TypeVar, Generic, Type, Optional, cast
from pathlib import Path
from contextlib import contextmanager

from weakref import proxy as weakproxy


EVENTS_MAIN_DEVICE_ID = "MainDevice"
EVENTS_FILE_LOGGER_ID = "EVENTS_FILE_LOGGER"
EVENTS_GRAFANA_ANNOTATOR_ID = "EVENTS_GRAFANA_ANNOTATOR"
EVENTS_GRAFANA_AGGREGATOR_ID = "EVENTS_GRAFANA_AGGREGATOR"
EVENTS_GRAFANA_POSTMAN_ID = "EVENTS_GRAFANA_POSTMAN"
EVENTS_ARGUS_ANNOTATOR_ID = "EVENTS_ARGUS_ANNOTATOR"
EVENTS_ARGUS_AGGREGATOR_ID = "EVENTS_ARGUS_AGGREGATOR"
EVENTS_ARGUS_POSTMAN_ID = "EVENTS_ARGUS_POSTMAN"
EVENTS_ANALYZER_ID = "EVENTS_ANALYZER"
EVENTS_HANDLER_ID = "EVENTS_HANDLER"
EVENTS_COUNTER_ID = "EVENTS_COUNTER_ID"

EVENTS_PROCESS_PIPE_OUTBOUND_QUEUE_WAIT_TIMEOUT: float = 1
EVENTS_PROCESS_PIPE_OUTBOUND_QUEUE_EVENTS_RATE: float = 0

LOGGER = logging.getLogger(__name__)


StopEvent = Union[multiprocessing.Event, threading.Event]

T_inbound_event = TypeVar("T_inbound_event")
T_outbound_event = TypeVar("T_outbound_event")
T_outbound_events_protocol = TypeVar("T_outbound_events_protocol")

InboundEventsGenerator = Generator[T_inbound_event, None, None]
OutboundEventsGenerator = Generator[T_outbound_event, None, None]


class OutboundEventsProtocol(Protocol[T_outbound_events_protocol]):
    def outbound_events(self,
                        stop_event: StopEvent,
                        events_counter: multiprocessing.Value) -> Generator[T_outbound_events_protocol, None, None]:
        ...


class BaseEventsProcess(Generic[T_inbound_event, T_outbound_event], abc.ABC):
    inbound_events_process = EVENTS_MAIN_DEVICE_ID
    stop_event: StopEvent

    def __init__(self, _registry: EventsProcessesRegistry):
        self._registry = _registry
        self._events_counter = multiprocessing.Value(ctypes.c_uint32, 0)

        if isinstance(self, threading.Thread):
            self.stop_event = threading.Event()
        elif isinstance(self, multiprocessing.Process):
            self.stop_event = multiprocessing.Event()
        else:
            raise RuntimeError(f"{type(self)} should be subclass of threading.Thread or multiprocessing.Process")

        super().__init__(daemon=True)  # next class in MRO should be threading.Thread or multiprocessing.Process

    @property
    def events_counter(self) -> int:
        return self._events_counter.value

    def inbound_events(self) -> InboundEventsGenerator:
        yield from cast(OutboundEventsProtocol[T_inbound_event],
                        get_events_process(name=self.inbound_events_process, _registry=self._registry)) \
            .outbound_events(stop_event=self.stop_event, events_counter=self._events_counter)

    def outbound_events(self, stop_event: StopEvent,
                        events_counter: multiprocessing.Value) -> OutboundEventsGenerator:
        yield from []

    def terminate(self) -> None:
        self.stop_event.set()

    def stop(self, timeout: float = None) -> None:
        events_process_class_name = self.__class__.__name__
        LOGGER.debug("Stopping events process %s", events_process_class_name)
        self.terminate()
        LOGGER.debug("Waiting for events process %s to stop", events_process_class_name)
        self.join(timeout)
        if self.is_alive():
            LOGGER.error("Events process %s is still alive after timeout", events_process_class_name)
            assert False, f"Events process {events_process_class_name} is still alive after timeout"
        else:
            LOGGER.debug("Events process %s stopped", events_process_class_name)


class EventsProcessPipe(BaseEventsProcess[T_inbound_event, T_outbound_event], threading.Thread):
    outbound_queue: queue.SimpleQueue[T_outbound_event]
    outbound_queue_wait_timeout = EVENTS_PROCESS_PIPE_OUTBOUND_QUEUE_WAIT_TIMEOUT
    outbound_queue_events_rate = EVENTS_PROCESS_PIPE_OUTBOUND_QUEUE_EVENTS_RATE

    def __init__(self, _registry: EventsProcessesRegistry):
        self.outbound_queue = queue.SimpleQueue()

        super().__init__(_registry=_registry)

    def outbound_events(self, stop_event: StopEvent, events_counter: multiprocessing.Value) -> OutboundEventsGenerator:
        while not stop_event.is_set():
            try:
                yield self.outbound_queue.get(timeout=self.outbound_queue_wait_timeout)
                events_counter.value += 1
                stop_event.wait(self.outbound_queue_events_rate)
            except queue.Empty:
                pass


class EventsProcessProcess(BaseEventsProcess[T_inbound_event, T_outbound_event], multiprocessing.Process):
    ...


class EventsProcessThread(BaseEventsProcess[T_inbound_event, T_outbound_event], threading.Thread):
    ...


EventsProcess = Union[EventsProcessProcess, EventsProcessThread]


class EventsProcessesRegistry:
    def __init__(self, log_dir: Union[str, Path], _default: bool = False):
        self.log_dir = Path(log_dir)
        self.default = _default
        self._registry_dict_lock = threading.RLock()
        self._registry_dict = {}
        LOGGER.debug("New events processes registry created: %s", self)

    def start_events_process(self, name: str, klass: Type[EventsProcess]) -> None:
        with self._registry_dict_lock:
            self._registry_dict[name] = process = klass(_registry=weakproxy(self))
        process.start()
        LOGGER.debug("New process `%s' started at %s", name, self)

    def get_events_process(self, name: str) -> EventsProcess:
        with self._registry_dict_lock:
            # this happens too many times during SCT run, leaving it for when debugging is needed
            # LOGGER.debug("Get process `%s' from %s", name, self)
            return self._registry_dict.get(name)

    def __str__(self):
        return f"{type(self).__name__}[log_dir={self.log_dir},id=0x{id(self):x},default={self.default}]"


_EVENTS_PROCESSES_LOCK = threading.RLock()
_EVENTS_PROCESSES: Optional[EventsProcessesRegistry] = None


def create_default_events_process_registry(log_dir: Union[str, Path]):
    global _EVENTS_PROCESSES  # noqa: PLW0603

    with _EVENTS_PROCESSES_LOCK:
        if _EVENTS_PROCESSES is None:
            _EVENTS_PROCESSES = EventsProcessesRegistry(log_dir=log_dir, _default=True)
            return _EVENTS_PROCESSES

    raise RuntimeError("Try to create default EventsProcessRegistry second time")


def get_default_events_process_registry(_registry: Optional[EventsProcessesRegistry] = None) -> EventsProcessesRegistry:
    if _registry is not None:
        return _registry
    with _EVENTS_PROCESSES_LOCK:
        if _EVENTS_PROCESSES is None:
            raise RuntimeError("You should create default EventsProcessRegistry first")
        return _EVENTS_PROCESSES


def start_events_process(name, klass, _registry: Optional[EventsProcessesRegistry] = None) -> None:
    get_default_events_process_registry(_registry=_registry).start_events_process(name=name, klass=klass)


def get_events_process(name: str, _registry: Optional[EventsProcessesRegistry] = None) -> EventsProcess:
    return get_default_events_process_registry(_registry=_registry).get_events_process(name=name)


@contextmanager
def verbose_suppress(*args, **kwargs):
    try:
        yield
    except Exception:
        LOGGER.exception(*args, **kwargs)


@contextmanager
def suppress_interrupt():
    try:
        yield
    except (KeyboardInterrupt, SystemExit) as exc:
        LOGGER.debug("Process %s was halted by %s", multiprocessing.current_process().name, type(exc).__name__)


__all__ = ("EVENTS_MAIN_DEVICE_ID", "EVENTS_FILE_LOGGER_ID", "EVENTS_GRAFANA_ANNOTATOR_ID",
           "EVENTS_GRAFANA_AGGREGATOR_ID", "EVENTS_GRAFANA_POSTMAN_ID", "EVENTS_ANALYZER_ID", "EVENTS_HANDLER_ID",
           "StopEvent", "BaseEventsProcess", "EventsProcessPipe", "EventsProcessesRegistry",
           "create_default_events_process_registry", "start_events_process", "get_events_process",
           "verbose_suppress", "suppress_interrupt",)

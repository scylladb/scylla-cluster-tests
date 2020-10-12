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

# pylint: disable=no-member; disable `no-member' messages because of zmq.

import os
import re
import time
import atexit
import logging
import collections
import multiprocessing
from pathlib import Path

import zmq

from sdcm.sct_events import subscribe_events, StopEvent
from sdcm.sct_events.base import SctEvent, Severity
from sdcm.sct_events.json import json
from sdcm.sct_events.system import SystemEvent, StartupTestEvent, TestResultEvent
from sdcm.sct_events.database import DatabaseLogEvent
from sdcm.sct_events.filters import BaseFilter, EventsFilter, DbEventsFilter
from sdcm.sct_events.events_processes import EVENTS_PROCESSES
from sdcm.sct_events.events_analyzer import EventsAnalyzer
from sdcm.utils.grafana import GrafanaEventAggragator, GrafanaAnnotator
from sdcm.utils.decorators import retrying, timeout as retry_timeout


EVENTS_DEVICE_START_TIMEOUT = 30  # seconds

LOGGER = logging.getLogger(__name__)


class EventsDevice(multiprocessing.Process):
    def __init__(self, log_dir):
        super().__init__(daemon=True)

        self._ready = multiprocessing.Event()
        self._pub_port = multiprocessing.Value('d', 0)
        self._sub_port = multiprocessing.Value('d', 0)

        self.event_log_base_dir = os.path.join(log_dir, 'events_log')
        os.makedirs(self.event_log_base_dir, exist_ok=True)
        self.raw_events_filename = os.path.join(self.event_log_base_dir, 'raw_events.log')

    def run(self):
        context = frontend = backend = None

        try:
            context = zmq.Context(1)

            # Socket facing clients.
            frontend = context.socket(zmq.SUB)
            self._pub_port.value = frontend.bind_to_random_port("tcp://*")
            frontend.setsockopt(zmq.SUBSCRIBE, b"")
            frontend.setsockopt(zmq.LINGER, 0)

            # Socket facing services.
            backend = context.socket(zmq.PUB)
            self._sub_port.value = backend.bind_to_random_port("tcp://*")
            backend.setsockopt(zmq.LINGER, 0)

            self._ready.set()

            LOGGER.info("EventsDevice listen on pub_port=%d, sub_port=%d", self._pub_port.value, self._sub_port.value)
            zmq.proxy(frontend, backend)
        except Exception:
            LOGGER.exception("zmq device failed")
        except (KeyboardInterrupt, SystemExit) as ex:
            LOGGER.debug("EventsDevice was halted by %s", ex.__class__.__name__)
        finally:
            if frontend:
                frontend.close()
            if backend:
                backend.close()
            if context:
                context.term()

    @staticmethod
    @retry_timeout(timeout=120)
    def wait_till_event_loop_is_working(number_of_events):
        """
        It waits 120 seconds till row of {number_of_events} events is delivered with no loss
        """
        for _ in range(number_of_events):
            try:
                StartupTestEvent().publish(guaranteed=True)
            except TimeoutError:
                raise RuntimeError("Event loop is not working properly")

    @property
    def sub_port(self):
        if self._ready.wait(timeout=EVENTS_DEVICE_START_TIMEOUT):
            return self._sub_port
        raise RuntimeError("EventsDevice is not ready to send events.")

    @property
    def pub_port(self):
        if self._ready.wait(timeout=EVENTS_DEVICE_START_TIMEOUT):
            return self._pub_port
        raise RuntimeError("EventsDevice is not ready to receive events.")

    def get_client_socket(self, filter_type=b''):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)  # pylint: disable=no-member
        socket.connect("tcp://localhost:%d" % self.sub_port.value)
        socket.setsockopt(zmq.SUBSCRIBE, filter_type)  # pylint: disable=no-member
        return socket

    def subscribe_events(self, filter_type=b'', stop_event: StopEvent = None):
        # pylint: disable=too-many-nested-blocks,too-many-branches
        LOGGER.info("subscribe to server with port %d", self.sub_port.value)
        socket = self.get_client_socket(filter_type)
        filters = dict()
        reiterate = False
        try:
            while reiterate or stop_event is None or not stop_event.is_set():
                # If there is an object scheduled, reiterate cycle, not looking if stop_event is set
                #   Done in order to make sure that all events end up at the files when stop() is called
                reiterate = socket.poll(timeout=1)
                if reiterate:
                    obj = socket.recv_pyobj()

                    # remove filter objects when log event timestamp on the
                    # specific node is bigger the time filter was canceled
                    for filter_key, filter_obj in list(filters.items()):
                        if filter_obj.expire_time and filter_obj.expire_time < obj.timestamp:
                            if (isinstance(obj, DatabaseLogEvent) and getattr(filter_obj, 'node', '') == obj.node) or \
                                    isinstance(filter_obj, EventsFilter):
                                del filters[filter_key]

                    obj_filtered = any([f.eval_filter(obj) for f in filters.values()])
                    if isinstance(obj, BaseFilter):
                        if not obj.clear_filter:
                            filters[obj.id] = obj
                        else:
                            object_filter = filters.get(obj.id, None)
                            if object_filter is None:
                                filters[obj.id] = obj
                            else:
                                filters[obj.id].expire_time = obj.expire_time
                            if not obj.expire_time:
                                del filters[obj.id]

                    obj_filtered = obj_filtered or isinstance(obj, SystemEvent)
                    if not obj_filtered:
                        yield obj.__class__.__name__, obj
        except (KeyboardInterrupt, SystemExit) as ex:
            LOGGER.debug("%s - subscribe_events was halted by %s",
                         multiprocessing.current_process().name, ex.__class__.__name__)
        socket.close()

    def publish_event(self, event):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)  # pylint: disable=no-member
        socket.connect("tcp://localhost:%d" % self.pub_port.value)
        time.sleep(0.01)

        socket.send_pyobj(event)
        with open(self.raw_events_filename, 'a+') as log_file:
            log_file.write(event.to_json() + '\n')
        socket.close()
        return True

    @retrying(n=3, sleep_time=0, allowed_exceptions=TimeoutError)
    def publish_event_guaranteed(self, event):
        client_socket = self.get_client_socket()
        self.publish_event(event)
        end_time = time.time() + 2
        while time.time() < end_time:
            # This iteration cycle needed to make sure that it does not stop on getting very first event,
            # which could be event not it is looking for, but something that is generated in other Thread
            try:
                if not client_socket.poll(timeout=1):
                    continue
                received_event = client_socket.recv_pyobj(flags=1)
                if event == received_event:
                    return True
            except zmq.ZMQError:
                continue
        raise TimeoutError(f"Event {str(type(event))} was not delivered")


class EventsFileLogger(multiprocessing.Process):  # pylint: disable=too-many-instance-attributes
    def __init__(self, log_dir):
        super().__init__(daemon=True)

        self._test_pid = os.getpid()
        self._stop_event = multiprocessing.Event()
        self.event_log_base_dir = Path(log_dir, 'events_log')
        self.events_filename = Path(self.event_log_base_dir, 'events.log')
        self.critical_events_filename = Path(self.event_log_base_dir, 'critical.log')
        self.error_events_filename = Path(self.event_log_base_dir, 'error.log')
        self.warning_events_filename = Path(self.event_log_base_dir, 'warning.log')
        self.normal_events_filename = Path(self.event_log_base_dir, 'normal.log')
        self.events_summary_filename = Path(self.event_log_base_dir, 'summary.log')

        for log_file in [self.critical_events_filename, self.error_events_filename,
                         self.warning_events_filename, self.normal_events_filename,
                         self.events_summary_filename]:
            log_file.touch()

        self.level_to_file_mapping = {
            Severity.CRITICAL: self.critical_events_filename,
            Severity.ERROR: self.error_events_filename,
            Severity.WARNING: self.warning_events_filename,
            Severity.NORMAL: self.normal_events_filename,
        }
        self.level_summary = collections.defaultdict(int)

    def run(self):
        LOGGER.info("writing to %s", self.events_filename)

        for _, message_data in subscribe_events(stop_event=self._stop_event):
            try:
                msg = self.dump_event_into_files(message_data)
                if not isinstance(message_data, TestResultEvent):
                    LOGGER.info(msg)
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Failed to write event to event.log")

    def dump_event_into_files(self, message_data: SctEvent):
        msg = "{}: {}".format(message_data.formatted_timestamp, str(message_data).strip())
        with open(self.events_filename, 'a+') as log_file:
            log_file.write(msg + '\n')
        # update each level log file
        events_filename = self.level_to_file_mapping[message_data.severity]
        with open(events_filename, 'a+') as events_level_file:
            events_level_file.write(msg + '\n')
        # update the summary file
        self.level_summary[Severity(message_data.severity).name] += 1
        with open(self.events_summary_filename, 'w') as summary_file:
            json.dump(dict(self.level_summary), summary_file, indent=4)
        return msg

    def get_events_by_category(self, limit=0):
        output = {}
        line_start = re.compile('^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ')
        for severity, events_file_path in self.level_to_file_mapping.items():
            try:
                with events_file_path.open() as events_file:
                    output[severity.name] = events_bucket = []
                    event = ''
                    for event_data in events_file:
                        event_data = event_data.strip(' \n\t\r')
                        if not event_data:
                            continue
                        if not line_start.match(event_data):
                            event += '\n' + event_data
                            continue
                        if event:
                            if limit and len(events_bucket) >= limit:
                                events_bucket.pop(0)
                            events_bucket.append(event)
                        event = event_data
                    if event:
                        if limit and len(events_bucket) >= limit:
                            events_bucket.pop(0)
                        events_bucket.append(event)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.info("failed to read %s file, due to the %s", events_file_path, str(exc))
                if not output.get(severity.name, None):
                    output[severity.name] = [f"failed to read {events_file_path} file, due to the {str(exc)}"]
        return output

    def stop(self, timeout: float = None):
        self._stop_event.set()
        self.join(timeout)


def start_events_device(log_dir):
    EVENTS_PROCESSES['MainDevice'] = EventsDevice(log_dir)
    EVENTS_PROCESSES['MainDevice'].start()

    EVENTS_PROCESSES['EVENTS_FILE_LOGGER'] = EventsFileLogger(log_dir)
    EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'] = GrafanaAnnotator()
    EVENTS_PROCESSES['EVENTS_GRAFANA_AGGRAGATOR'] = GrafanaEventAggragator()
    EVENTS_PROCESSES['EVENTS_ANALYZER'] = EventsAnalyzer()

    EVENTS_PROCESSES['EVENTS_FILE_LOGGER'].start()
    EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'].start()
    EVENTS_PROCESSES['EVENTS_GRAFANA_AGGRAGATOR'].start()
    EVENTS_PROCESSES['EVENTS_ANALYZER'].start()

    try:
        EVENTS_PROCESSES['MainDevice'].wait_till_event_loop_is_working(number_of_events=20)
    except RuntimeError:
        LOGGER.error("EVENTS_PROCESSES['MainDevice'] event loop failed to deliver 20 test events with no loss")
        raise

    # default filters
    EVENTS_PROCESSES['default_filter'] = []
    EVENTS_PROCESSES['default_filter'] += [DbEventsFilter(type='BACKTRACE', line='Rate-limit: supressed')]
    EVENTS_PROCESSES['default_filter'] += [DbEventsFilter(type='BACKTRACE', line='Rate-limit: suppressed')]

    atexit.register(stop_events_device)


def stop_events_device():
    LOGGER.debug("Stopping Events consumers...")
    processes = ['EVENTS_FILE_LOGGER', 'EVENTS_GRAFANA_ANNOTATOR',
                 'EVENTS_GRAFANA_AGGRAGATOR', 'EVENTS_ANALYZER', 'MainDevice']
    LOGGER.debug("Signalling events consumers to terminate...")
    for proc_name in processes:
        if proc_name in EVENTS_PROCESSES:
            if hasattr(EVENTS_PROCESSES[proc_name], 'stop'):
                EVENTS_PROCESSES[proc_name].stop(10)
            else:
                EVENTS_PROCESSES[proc_name].terminate()
    LOGGER.debug("Waiting for Events consumers to finish...")
    for proc_name in processes:
        if proc_name in EVENTS_PROCESSES:
            EVENTS_PROCESSES[proc_name].join(timeout=60)
    LOGGER.debug("All Events consumers stopped.")


def get_logger_event_summary():
    with open(EVENTS_PROCESSES['EVENTS_FILE_LOGGER'].events_summary_filename) as summary_file:
        output = json.load(summary_file)
    return output


__all__ = ("start_events_device", "stop_events_device", "get_logger_event_summary", )

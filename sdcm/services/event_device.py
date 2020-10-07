import multiprocessing
import os
import threading
import time
from typing import Union

import zmq

from sdcm.sct_events import DatabaseLogEvent, EventsFilter, BaseFilter, SystemEvent, StartupTestEvent
from sdcm.utils.decorators import timeout as retry_timeout, retrying
from sdcm.services.base import DetachProcessService


class EventsDevice(DetachProcessService):
    _start_timeout = 30  # seconds
    stop_priority = 92
    tags = ['core']

    def __init__(self, log_dir):
        self._ready = multiprocessing.Event()
        self._pub_port = multiprocessing.Value('d', 0)
        self._sub_port = multiprocessing.Value('d', 0)
        self.event_log_base_dir = os.path.join(log_dir, 'events_log')
        os.makedirs(self.event_log_base_dir, exist_ok=True)
        self.raw_events_filename = os.path.join(self.event_log_base_dir, 'raw_events.log')
        super().__init__()

    def _service_body(self):
        # pylint: disable=no-member; disable `no-member' messages because of zmq.
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
            self._log.info("listen on pub_port=%d, sub_port=%d", self._pub_port.value, self._sub_port.value)
            zmq.proxy(frontend, backend)
        except Exception:  # pylint: disable=broad-except
            self._log.exception("zmq device failed")
        except (KeyboardInterrupt, SystemExit) as ex:
            self._log.debug("was halted by %s", ex.__class__.__name__)
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
        if self._ready.wait(timeout=self._start_timeout):
            return self._sub_port
        raise RuntimeError("EventsDevice is not ready to send events.")

    @property
    def pub_port(self):
        if self._ready.wait(timeout=self._start_timeout):
            return self._pub_port
        raise RuntimeError("EventsDevice is not ready to receive events.")

    def get_client_socket(self, filter_type=b''):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)  # pylint: disable=no-member
        socket.connect("tcp://localhost:%d" % self.sub_port.value)
        socket.setsockopt(zmq.SUBSCRIBE, filter_type)  # pylint: disable=no-member
        return socket

    def subscribe_events(self, filter_type=b'', stop_event: Union[multiprocessing.Event, threading.Event] = None):
        # pylint: disable=too-many-nested-blocks,too-many-branches
        self._log.info("subscribe to server with port %d", self.sub_port.value)
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
            self._log.debug("%s - subscribe_events was halted by %s",
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

    def stop(self):
        super().stop()
        self.kill()

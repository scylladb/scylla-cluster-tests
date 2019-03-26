import os
import re
import logging
import json
import signal
import datetime
import time
from multiprocessing import Process, Value, Event, Manager, current_process
from json import JSONEncoder

import enum
from enum import Enum
import zmq

from sdcm.utils import safe_kill, pid_exists

LOGGER = logging.getLogger(__name__)


manager = Manager()


class EventsDevice(Process):

    def __init__(self):
        super(EventsDevice, self).__init__()
        self.ready_event = Event()
        self.pub_port = Value('d', 0)
        self.sub_port = Value('d', 0)

        self.raw_events_filename = None

    def start(self, log_dir):
        self.raw_events_filename = os.path.join(log_dir, 'raw_events.log')
        super(EventsDevice, self).start()

    def run(self):
        try:
            context = zmq.Context(1)
            # Socket facing clients
            frontend = context.socket(zmq.SUB)
            self.pub_port.value = frontend.bind_to_random_port("tcp://*")
            frontend.setsockopt(zmq.SUBSCRIBE, "")

            # Socket facing services
            backend = context.socket(zmq.PUB)
            self.sub_port.value = backend.bind_to_random_port("tcp://*")
            LOGGER.info("EventDevice Listen on pub_port=%d, sub_port=%d", self.pub_port.value, self.sub_port.value)

            backend.setsockopt(zmq.LINGER, 0)
            frontend.setsockopt(zmq.LINGER, 0)

            self.ready_event.set()
            zmq.proxy(frontend, backend)

        except Exception as e:
            LOGGER.exception("zmq device failed")
        except (KeyboardInterrupt, SystemExit) as ex:
            LOGGER.debug("EventsDevice was halted by %s", ex.__class__.__name__)
        finally:
            frontend.close()
            backend.close()
            context.term()

    def subscribe_events(self, filter_type=''):
        context = zmq.Context()
        LOGGER.info("subscribe to server with port %d", self.sub_port.value)
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://localhost:%d" % self.sub_port.value)
        socket.setsockopt(zmq.SUBSCRIBE, filter_type)

        filters = dict()

        try:
            while True:
                obj = socket.recv_pyobj()

                obj_filtered = any([f.eval_filter(obj) for f in filters.values()])

                if isinstance(obj, DbEventsFilter):
                    if not obj.clear_filter:
                        filters[obj.id] = obj
                    else:
                        del filters[obj.id]

                obj_filtered = obj_filtered or isinstance(obj, SystemEvent)
                if not obj_filtered:
                    yield obj.__class__.__name__, obj
        except (KeyboardInterrupt, SystemExit) as ex:
            LOGGER.debug("%s - subscribe_events was halted by %s", current_process().name, ex.__class__.__name__)

    def publish_event(self, event):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect("tcp://localhost:%d" % self.pub_port.value)
        time.sleep(0.01)

        socket.send_pyobj(event)
        with open(self.raw_events_filename, 'a+') as log_file:
            log_file.write(event.to_json() + '\n')


EVENTS_PROCESS = EventsDevice()

# monkey patch JSONEncoder make enums jsonable
_saved_default = JSONEncoder().default  # Save default method.


def _new_default(self, obj):
    if isinstance(obj, Enum):
        return obj.name  # Could also be obj.value
    else:
        return _saved_default


JSONEncoder.default = _new_default  # Set new default method.


class Severity(enum.Enum):
    NORMAL = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


class SctEvent(object):
    def __init__(self):
        self.timestamp = self.set_timestamp()
        self.severity = Severity.NORMAL

    def set_timestamp(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def publish(self):
        EVENTS_PROCESS.publish_event(self)

    def __str__(self):
        return "{}: ({} {})".format(self.timestamp, self.__class__.__name__, self.severity)

    def to_json(self):
        return json.dumps(self.__dict__)


class SystemEvent(SctEvent):
    pass


class DbEventsFilter(SystemEvent):
    def __init__(self, type, line=None):
        super(DbEventsFilter, self).__init__()
        self.id = id(self)
        self.type = type
        self.line = line
        self.clear_filter = False
        self.publish()

    def cancel_filter(self):
        self.clear_filter = True
        self.publish()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.cancel_filter()

    def eval_filter(self, event):
        name = self.type and self.type == getattr(event, 'type', '')
        string = self.line and self.line in getattr(event, 'line', '')
        return (name and string) if self.line else name


class InfoEvent(SctEvent):
    def __init__(self, message):
        super(InfoEvent, self).__init__()
        self.message = message
        self.severity = Severity.NORMAL
        self.publish()

    def __str__(self):
        return "{}: message={}".format(super(InfoEvent, self).__str__(), self.message)


class CoreDumpEvent(SctEvent):
    def __init__(self, corefile_urls, download_instructions, backtrace):
        super(CoreDumpEvent, self).__init__()
        self.corefile_urls = corefile_urls
        self.download_instructions = download_instructions
        self.backtrace = backtrace
        self.severity = Severity.CRITICAL
        self.publish()


class KillTestEvent(SctEvent):
    def __init__(self, reason):
        super(KillTestEvent, self).__init__()
        self.reason = reason
        self.severity = Severity.CRITICAL
        self.publish()

    def __str__(self):
        return "{}: reason={}".format(super(KillTestEvent, self).__str__(), self.reason)


class DisruptionEvent(SctEvent):
    def __init__(self, name, status, start, end, duration, node=None, error=None, full_traceback=None, **kwargs):
        super(DisruptionEvent, self).__init__()
        self.name = name
        self.start = start
        self.end = end
        self.duration = duration
        self.node = str(node)
        self.severity = Severity.NORMAL if status else Severity.ERROR
        if error:
            self.error = error
            self.full_traceback = full_traceback

        self.__dict__.update(**kwargs)
        self.publish()

    def __str__(self):
        if self.severity == Severity.NORMAL:
            return "{}: name={} node={} duration={}".format(
                super(DisruptionEvent, self).__str__(), self.name, self.node, self.duration
            )
        elif self.severity == Severity.ERROR:
            return "{}: name={} node={} duration={} error={}\n{}".format(
                super(DisruptionEvent, self).__str__(), self.name, self.node, self.duration,
                self.error,
                str(self.full_traceback)
            )


class CassandraStressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None, ):
        super(CassandraStressEvent, self).__init__()
        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors
        self.publish()

    def __str__(self):
        if self.stress_cmd:
            return "{}: type={} node={}\nstress_cmd={}".format(
                super(CassandraStressEvent, self).__str__(), self.type, self.node, self.stress_cmd)
        if self.errors:
            return "{}: type={} node={}\n{}".format(
                super(CassandraStressEvent, self).__str__(), self.type, self.node, "\n".join(self.errors))


class DatabaseLogEvent(SctEvent):
    def __init__(self, type, regex, severity=Severity.CRITICAL):
        super(DatabaseLogEvent, self).__init__()
        self.type = type
        self.regex = regex
        self.line_number = 0
        self.line = None
        self.node = None
        self.severity = severity

    def add_info_and_publish(self, node, line, line_number):
        self.timestamp = self.set_timestamp()
        self.line = line
        self.line_number = line_number
        self.node = str(node)

        # dynamically handle reactor stalls severity
        if self.type == 'REACTOR_STALLED':
            try:
                stall_time = int(re.findall(r'(\d+) ms', line)[0])
                if stall_time <= 2000:
                    self.severity = Severity.NORMAL

            except (ValueError, IndexError):
                LOGGER.exception("failed to read REACTOR_STALLED line=[%s] ", line)

        self.publish()

    def __str__(self):
        return "{}: type={} node={} line_number={} regex={}\n{}".format(
            super(DatabaseLogEvent, self).__str__(), self.type, self.node, self.line_number, self.regex, self.line)


class SpotTerminationEvent(SctEvent):
    def __init__(self, node, aws_message):
        super(SpotTerminationEvent, self).__init__()
        self.severity = Severity.CRITICAL
        self.node = str(node)
        self.aws_message = aws_message
        self.publish()

    def __str__(self):
        return "{}: node={}  aws_message={} ".format(
            super(SpotTerminationEvent, self).__str__(), self.node, self.aws_message)


class TestKiller(Process):
    def __init__(self, timeout_before_kill=2, test_callback=None):
        super(TestKiller, self).__init__()
        self._test_pid = os.getpid()
        self.test_callback = test_callback
        self.timeout_before_kill = timeout_before_kill

    def run(self):
        for event_type, message_data in EVENTS_PROCESS.subscribe_events():
            if event_type == 'KillTestEvent':
                time.sleep(self.timeout_before_kill)
                LOGGER.debug("Killing the test")
                if callable(self.test_callback):
                    self.test_callback(message_data)
                    continue
                if not safe_kill(self._test_pid, signal.SIGTERM) or pid_exists(self._test_pid):
                    safe_kill(self._test_pid, signal.SIGKILL)


class EventsFileLogger(Process):
    def __init__(self):
        super(EventsFileLogger, self).__init__()
        self._test_pid = os.getpid()
        self.events_filename = None
        self.raw_events_filename = None

    def start(self, log_dir):
        self.events_filename = os.path.join(log_dir, 'events.log')
        super(EventsFileLogger, self).start()

    def run(self):
        LOGGER.info("writing to %s", self.events_filename)

        for event_type, message_data in EVENTS_PROCESS.subscribe_events():
            with open(self.events_filename, 'a+') as log_file:
                log_file.write(str(message_data).strip() + '\n')
            LOGGER.info(message_data)


EVENTS_TASK_KILLER = TestKiller()
EVENTS_FILE_LOOGER = EventsFileLogger()


def start_events_device(log_dir):
    EVENTS_PROCESS.start(log_dir)
    EVENTS_PROCESS.ready_event.wait(timeout=1)
    EVENTS_FILE_LOOGER.start(log_dir)
    EVENTS_TASK_KILLER.start()


def stop_events_device():

    EVENTS_FILE_LOOGER.terminate()
    EVENTS_TASK_KILLER.terminate()
    EVENTS_PROCESS.terminate()

    EVENTS_FILE_LOOGER.join()
    EVENTS_TASK_KILLER.join()
    EVENTS_PROCESS.join()


if __name__ == "__main__":
    import traceback
    import unittest
    import tempfile

    class SctEventsTests(unittest.TestCase):

        @classmethod
        def setUpClass(cls):
            cls.temp_dir = tempfile.mkdtemp()

            EVENTS_PROCESS.start(cls.temp_dir)
            EVENTS_PROCESS.ready_event.wait(timeout=5)

            cls.killed = Event()

            def callback(x):
                cls.killed.set()

            cls.test_killer = TestKiller(timeout_before_kill=0, test_callback=callback)
            cls.test_killer.start()
            cls.event_logger = EventsFileLogger()
            cls.event_logger.start(log_dir=cls.temp_dir)

        @classmethod
        def tearDownClass(cls):
            cls.test_killer.terminate()
            cls.event_logger.terminate()
            EVENTS_PROCESS.terminate()

            cls.test_killer.join()
            cls.event_logger.join()
            EVENTS_PROCESS.join()

        def test_event_info(self):
            InfoEvent(message='jkgkjgl')

        def test_cassandra_stress(self):
            str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf"))
            str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf", log_file_name="/filename/"))

        def test_coredump_event(self):
            str(CoreDumpEvent(corefile_urls='http://', backtrace="asfasdfsdf", download_instructions=""))

        def test_scylla_log_event(self):
            str(DatabaseLogEvent(type="A", regex="B"))

        def test_disruption_event(self):
            try:
                1/0
            except ZeroDivisionError:
                _full_traceback = traceback.format_exc()

            str(DisruptionEvent(name="ChaosMonkeyLimited", status=False, error=str(Exception("long one")),
                                full_traceback=_full_traceback, duration=20, start=1, end=2, node='test'))

            str(DisruptionEvent(name="ChaosMonkeyLimited", status=True, duration=20, start=1, end=2, node='test'))

        def test_kill_test_event(self):
            str(KillTestEvent(reason="Don't like this test"))
            LOGGER.info('sent kill')
            self.assertTrue(self.killed.wait(4), "kill wasn't sent")

        def test_filter(self):
            with DbEventsFilter(type="NO_SPACE_ERROR"), DbEventsFilter(type='BACKTRACE', line='No space left on device'):

                DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22, line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")
                DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                        line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

                DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                        line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

                DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                        line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

        def test_stall_severity(self):
            event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
            event.add_info_and_publish(node="A", line_number=22, line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 2000 ms")
            self.assertTrue(event.severity == Severity.NORMAL)

            event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
            event.add_info_and_publish(node="A", line_number=22, line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 5000 ms")
            self.assertTrue(event.severity == Severity.CRITICAL)

        def test_spot_termination(self):
            str(SpotTerminationEvent(node='test', aws_message='{"action": "terminate", "time": "2017-09-18T08:22:00Z"}'))

    logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)
    unittest.main(verbosity=2)

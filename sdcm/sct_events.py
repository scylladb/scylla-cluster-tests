import threading
import os
import re
import logging
import json
import signal
import time
from multiprocessing import Process, Value, Event, Manager, current_process
from json import JSONEncoder
import atexit

import enum
from enum import Enum
import zmq
import requests

from sdcm.utils.common import safe_kill, pid_exists, _fromtimestamp
from sdcm.prometheus import nemesis_metrics_obj

LOGGER = logging.getLogger(__name__)


manager = Manager()


class EventsDevice(Process):

    def __init__(self, log_dir):
        super(EventsDevice, self).__init__()
        self.ready_event = Event()
        self.pub_port = Value('d', 0)
        self.sub_port = Value('d', 0)

        self.event_log_base_dir = os.path.join(log_dir, 'events_log')
        os.makedirs(self.event_log_base_dir)
        self.raw_events_filename = os.path.join(self.event_log_base_dir, 'raw_events.log')

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

    def subscribe_events(self, filter_type='', stop_event=None):
        context = zmq.Context()
        LOGGER.info("subscribe to server with port %d", self.sub_port.value)
        socket = context.socket(zmq.SUB)
        socket.connect("tcp://localhost:%d" % self.sub_port.value)
        socket.setsockopt(zmq.SUBSCRIBE, filter_type)

        filters = dict()

        try:
            while stop_event is None or not stop_event.isSet():
                if socket.poll(timeout=1):
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
        self.timestamp = time.time()
        self.severity = Severity.NORMAL

    def formatted_timestamp(self):
        return _fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    def publish(self):
        EVENTS_PROCESSES['MainDevice'].publish_event(self)

    def __str__(self):
        return "({} {})".format(self.__class__.__name__, self.severity)

    def to_json(self):
        return json.dumps(self.__dict__)


class SystemEvent(SctEvent):
    pass


class DbEventsFilter(SystemEvent):
    def __init__(self, type, line=None, node=None):
        super(DbEventsFilter, self).__init__()
        self.id = id(self)
        self.type = type
        self.line = line
        self.node = str(node) if node else None
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
        line = getattr(event, 'line', '')
        _type = getattr(event, 'type', '')
        node = getattr(event, 'node', '')
        is_name_matching = self.type and _type and self.type == _type
        is_line_matching = self.line and line and self.line in line
        is_node_matching = self.node and node and self.node == node

        result = is_name_matching
        if self.line:
            result = result and is_line_matching
        if self.node:
            result = result and is_node_matching
        return result


class InfoEvent(SctEvent):
    def __init__(self, message):
        super(InfoEvent, self).__init__()
        self.message = message
        self.severity = Severity.NORMAL
        self.publish()

    def __str__(self):
        return "{0}: message={1.message}".format(super(InfoEvent, self).__str__(), self)


class CoreDumpEvent(SctEvent):
    def __init__(self, corefile_url, download_instructions, backtrace, node):
        super(CoreDumpEvent, self).__init__()
        self.corefile_url = corefile_url
        self.download_instructions = download_instructions
        self.backtrace = backtrace
        self.severity = Severity.CRITICAL
        self.node = str(node)
        self.publish()

    def __str__(self):
        return "{0}: node={1.node}\ncorefile_url=\n{1.corefile_url}\nbacktrace={1.backtrace}\ndownload_instructions=\n{1.download_instructions}".format(
            super(CoreDumpEvent, self).__str__(), self)


class KillTestEvent(SctEvent):
    def __init__(self, reason):
        super(KillTestEvent, self).__init__()
        self.reason = reason
        self.severity = Severity.CRITICAL
        self.publish()

    def __str__(self):
        return "{0}: reason={1.reason}".format(super(KillTestEvent, self).__str__(), self)


class DisruptionEvent(SctEvent):
    def __init__(self, type, name, status, start=None, end=None, duration=None, node=None, error=None, full_traceback=None, **kwargs):
        super(DisruptionEvent, self).__init__()
        self.name = name
        self.type = type
        self.start = start
        self.end = end
        self.duration = duration
        self.node = str(node)
        self.severity = Severity.NORMAL if status else Severity.ERROR
        self.error = None
        self.full_traceback = ''
        if error:
            self.error = error
            self.full_traceback = str(full_traceback)

        self.__dict__.update(kwargs)
        self.publish()

    def __str__(self):
        if self.severity == Severity.NORMAL:
            return "{0}: type={1.type} name={1.name} node={1.node} duration={1.duration}".format(
                super(DisruptionEvent, self).__str__(), self)
        elif self.severity == Severity.ERROR:
            return "{0}: type={1.type} name={1.name} node={1.node} duration={1.duration} error={1.error}\n{1.full_traceback}".format(
                super(DisruptionEvent, self).__str__(), self)


class ClusterHealthValidatorEvent(SctEvent):
    def __init__(self, type, name, status=Severity.CRITICAL, node=None, message=None, error=None, **kwargs):
        super(ClusterHealthValidatorEvent, self).__init__()
        self.name = name
        self.type = type
        self.node = str(node)
        self.severity = status
        self.error = error if error else ''
        self.message = message if message else ''

        self.__dict__.update(kwargs)
        self.publish()

    def __str__(self):
        if self.severity == Severity.NORMAL:
            return "{0}: type={1.type} name={1.name} node={1.node} message={1.message}".format(
                super(ClusterHealthValidatorEvent, self).__str__(), self)
        elif self.severity in (Severity.CRITICAL, Severity.ERROR):
            return "{0}: type={1.type} name={1.name} node={1.node} error={1.error}".format(
                super(ClusterHealthValidatorEvent, self).__str__(), self)


class FullScanEvent(SctEvent):
    def __init__(self, type, ks_cf, db_node_ip, severity=Severity.NORMAL, message=None):
        super(FullScanEvent, self).__init__()
        self.type = type
        self.ks_cf = ks_cf
        self.db_node_ip = db_node_ip
        self.severity = severity
        self.msg = "{0}: type={1.type} select_from={1.ks_cf} on db_node={1.db_node_ip}"
        if message:
            self.message = message
            self.msg += " {1.message}"
        self.publish()

    def __str__(self):
        return self.msg.format(super(FullScanEvent, self).__str__(), self)


class CassandraStressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):
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
            return "{0}: type={1.type} node={1.node}\nstress_cmd={1.stress_cmd}".format(
                super(CassandraStressEvent, self).__str__(), self)
        if self.errors:
            return "{0}: type={1.type} node={1.node}\n{2}".format(
                super(CassandraStressEvent, self).__str__(), self, "\n".join(self.errors))


class DatabaseLogEvent(SctEvent):
    def __init__(self, type, regex, severity=Severity.CRITICAL):
        super(DatabaseLogEvent, self).__init__()
        self.type = type
        self.regex = regex
        self.line_number = 0
        self.line = None
        self.node = None
        self.backtrace = None
        self.raw_backtrace = None
        self.severity = severity

    def add_info(self, node, line, line_number):
        self.timestamp = time.time()
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
                LOGGER.warning("failed to read REACTOR_STALLED line=[%s] ", line)

    def add_backtrace_info(self, backtrace=None, raw_backtrace=None):
        if backtrace:
            self.backtrace = backtrace
        if raw_backtrace:
            self.raw_backtrace = raw_backtrace

    def clone_with_info(self, node, line, line_number):
        ret = DatabaseLogEvent(type='', regex='')
        ret.__dict__.update(self.__dict__)
        ret.add_info(node, line, line_number)
        return ret

    def add_info_and_publish(self, node, line, line_number):
        self.add_info(node, line, line_number)
        self.publish()

    def __str__(self):
        if self.backtrace:
            return "{0}: type={1.type} regex={1.regex} line_number={1.line_number} node={1.node}\n{1.line}\n{1.backtrace}".format(
                super(DatabaseLogEvent, self).__str__(), self)

        if self.raw_backtrace:
            return "{0}: type={1.type} regex={1.regex} line_number={1.line_number} node={1.node}\n{1.line}\n{1.raw_backtrace}".format(
                super(DatabaseLogEvent, self).__str__(), self)

        return "{0}: type={1.type} regex={1.regex} line_number={1.line_number} node={1.node}\n{1.line}".format(
            super(DatabaseLogEvent, self).__str__(), self)


class CassandraStressLogEvent(DatabaseLogEvent):
    pass


class SpotTerminationEvent(SctEvent):
    def __init__(self, node, aws_message):
        super(SpotTerminationEvent, self).__init__()
        self.severity = Severity.CRITICAL
        self.node = str(node)
        self.aws_message = aws_message
        self.publish()

    def __str__(self):
        return "{0}: node={1.node} aws_message={1.aws_message}".format(
            super(SpotTerminationEvent, self).__str__(), self)


class TestKiller(Process):
    def __init__(self, timeout_before_kill=2, test_callback=None):
        super(TestKiller, self).__init__()
        self._test_pid = os.getpid()
        self.test_callback = test_callback
        self.timeout_before_kill = timeout_before_kill

    def run(self):
        for event_type, message_data in EVENTS_PROCESSES['MainDevice'].subscribe_events():
            if event_type == 'KillTestEvent':
                time.sleep(self.timeout_before_kill)
                LOGGER.debug("Killing the test")
                if callable(self.test_callback):
                    self.test_callback(message_data)
                    continue
                if not safe_kill(self._test_pid, signal.SIGTERM) or pid_exists(self._test_pid):
                    safe_kill(self._test_pid, signal.SIGKILL)


class EventsFileLogger(Process):
    def __init__(self, log_dir):
        super(EventsFileLogger, self).__init__()
        self._test_pid = os.getpid()
        self.event_log_base_dir = os.path.join(log_dir, 'events_log')
        self.events_filename = os.path.join(self.event_log_base_dir, 'events.log')
        self.critical_events_filename = os.path.join(self.event_log_base_dir, 'critical.log')
        with open(self.critical_events_filename, 'a'):
            pass

    def run(self):
        LOGGER.info("writing to %s", self.events_filename)

        for event_type, message_data in EVENTS_PROCESSES['MainDevice'].subscribe_events():
            msg = "{}: {}".format(message_data.formatted_timestamp(), str(message_data).strip())
            with open(self.events_filename, 'a+') as log_file:
                log_file.write(msg + '\n')

            if (message_data.severity == Severity.CRITICAL or message_data.severity == Severity.ERROR) \
                    and not event_type == 'ClusterHealthValidatorEvent':
                with open(self.critical_events_filename, 'a+') as critical_file:
                    critical_file.write(msg + '\n')

            LOGGER.info(msg)


# This is an example of how we'll send info into Prometheus,
# Currently it's not in use, since the data we want to show, doesn't fit Prometheus model,
# we are using the GrafanaAnnotator
class PrometheusDumper(threading.Thread):
    def __init__(self):
        self.stop_event = threading.Event()
        super(PrometheusDumper, self).__init__()

    def run(self):
        events_gauge = nemesis_metrics_obj().create_gauge('sct_events_gauge',
                                                          'Gauge for sct events',
                                                          ['event_type', 'type', 'severity', 'node'])

        for event_type, message_data in EVENTS_PROCESSES['MainDevice'].subscribe_events(stop_event=self.stop_event):
            events_gauge.labels(event_type,
                                getattr(message_data, 'type', ''),
                                message_data.severity,
                                getattr(message_data, 'node', '')).set(message_data.timestamp)

    def terminate(self):
        self.stop_event.set()


class GrafanaAnnotator(threading.Thread):
    def __init__(self):
        self.stop_event = threading.Event()
        self.url_set = threading.Event()
        self.grafana_base_url = ''

        self.auth = ('admin', 'admin')
        super(GrafanaAnnotator, self).__init__()

    def set_grafana_url(self, grafana_base_url):
        self.grafana_base_url = grafana_base_url
        self.url_set.set()

    def run(self):
        for event_class, message_data in EVENTS_PROCESSES['MainDevice'].subscribe_events(stop_event=self.stop_event):
            # waiting until the monitor url is set, and we can start using the api
            self.url_set.wait()
            if self.grafana_base_url:
                event_type = getattr(message_data, 'type', None)
                tags = [event_class, message_data.severity.name, 'events']
                if event_type:
                    tags += [event_type]
                annotate_data = {
                    "time": int(message_data.timestamp * 1000.0),
                    "tags": tags,
                    "isRegion": False,
                    "text": str(message_data)
                }
                try:
                    res = requests.post(self.grafana_base_url + '/api/annotations', json=annotate_data, auth=self.auth)
                    res.raise_for_status()
                    LOGGER.info(res.text)
                except requests.exceptions.RequestException as ex:
                    LOGGER.warning("Failed to annotate an event in grafana [%s]", str(ex))

    def terminate(self):
        self.url_set.set()
        self.stop_event.set()

    def find_dashboard(self, query='nemesis'):
        res = requests.get(self.grafana_base_url + '/api/search', params={'query': query}, auth=self.auth).json()
        return res[0]

    def find_panel(self, dashboard, panel_title_prefix="Requests Served per"):
        dashboard_data = requests.get(self.grafana_base_url + '/api/dashboards/uid/{}'.format(dashboard['uid']), auth=self.auth).json()
        for row in dashboard_data['dashboard']['rows']:
            for panel in row['panels']:
                if panel['title'].startswith(panel_title_prefix):
                    panel_id = panel['id']
                    return panel_id, panel

        LOGGER.error("Failed to find panel id that match [%s]", panel_title_prefix)


EVENTS_PROCESSES = dict()


def start_events_device(log_dir, timeout=5):
    EVENTS_PROCESSES['MainDevice'] = EventsDevice(log_dir)
    EVENTS_PROCESSES['MainDevice'].start()
    EVENTS_PROCESSES['MainDevice'].ready_event.wait(timeout=timeout)

    EVENTS_PROCESSES['EVENTS_FILE_LOOGER'] = EventsFileLogger(log_dir)
    EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'] = GrafanaAnnotator()

    EVENTS_PROCESSES['EVENTS_FILE_LOOGER'].start()
    EVENTS_PROCESSES['EVENTS_GRAFANA_ANNOTATOR'].start()

    # default filters
    EVENTS_PROCESSES['default_filter'] = []
    EVENTS_PROCESSES['default_filter'] += [DbEventsFilter(type='BACKTRACE', line='Rate-limit: supressed')]
    EVENTS_PROCESSES['default_filter'] += [DbEventsFilter(type='BACKTRACE', line='Rate-limit: suppressed')]


def stop_events_device():
    processes = ['EVENTS_FILE_LOOGER', 'EVENTS_GRAFANA_ANNOTATOR', 'MainDevice']
    for proc_name in processes:
        if proc_name in EVENTS_PROCESSES:
            EVENTS_PROCESSES[proc_name].terminate()
    for proc_name in processes:
        if proc_name in EVENTS_PROCESSES:
            EVENTS_PROCESSES[proc_name].join(timeout=60)


atexit.register(stop_events_device)

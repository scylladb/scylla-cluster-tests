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

# pylint: disable=too-many-lines
from __future__ import absolute_import
from typing import Optional, Generic, TypeVar, Type, List
import os
import re
import logging
import json
from functools import wraps
from traceback import format_exc, format_stack
from json import JSONEncoder
import time
import datetime
from contextlib import contextmanager, ExitStack

import enum
from enum import Enum
from textwrap import dedent
import dateutil.parser


LOGGER = logging.getLogger(__name__)


# monkey patch JSONEncoder make enums jsonable
_SAVED_DEFAULT = JSONEncoder().default  # Save default method.


def _new_default(self, obj):  # pylint: disable=unused-argument
    if isinstance(obj, Enum):
        return obj.name  # Could also be obj.value
    else:
        return _SAVED_DEFAULT


JSONEncoder.default = _new_default  # Set new default method.


class Severity(enum.Enum):
    NORMAL = 1
    WARNING = 2
    ERROR = 3
    CRITICAL = 4


EventType = TypeVar('EventType', bound='SctEvent')


class SctEvent(Generic[EventType]):
    _main_device = None
    _file_logger = None

    def __init__(self):
        self.timestamp = time.time()
        self.severity = getattr(self, 'severity', Severity.NORMAL)

    @classmethod
    def set_main_device(cls, main_device, file_logger):
        cls._main_device = main_device
        cls._file_logger = file_logger

    @property
    def formatted_timestamp(self):
        try:
            return datetime.datetime.fromtimestamp(self.timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            LOGGER.exception("failed to format timestamp:[%d]", self.timestamp)
            return '<UnknownTimestamp>'

    def publish(self, guaranteed=True):
        if guaranteed:
            return self._main_device.publish_event_guaranteed(self)
        return self._main_device.publish_event(self)

    def publish_or_dump(self, default_logger=None):
        if self._main_device:
            if self._main_device.is_service_alive():
                self.publish()
            else:
                self._file_logger.dump_event_into_files(self)
            return
        if default_logger:
            default_logger.error(str(self))

    def __str__(self):
        return "({} {})".format(self.__class__.__name__, self.severity)

    def to_json(self):
        return json.dumps(self.__dict__)

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.__dict__ == other.__dict__


class TestFrameworkEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    __test__ = False  # Mark this class to be not collected by pytest.

    def __init__(self, source, source_method=None,  # pylint: disable=redefined-builtin,too-many-arguments
                 exception=None, message=None, args=None, kwargs=None, severity=None, trace=None):
        super().__init__()
        if severity is None:
            self.severity = Severity.ERROR
        else:
            self.severity = severity
        self.source = str(source) if source else None
        self.source_method = str(source_method) if source_method else None
        self.exception = str(exception) if exception else None
        self.message = str(message) if message else None
        self.trace = ''.join(format_stack(trace)) if trace else None
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        message = f'message={self.message}' if self.message else ''
        message += f'\nexception={self.exception}' if self.exception else ''
        args = f' args={self.args}' if self.args else ''
        kwargs = f' kwargs={self.kwargs}' if self.kwargs else ''
        params = ','.join([args, kwargs]) if kwargs or args else ''
        source_method = f'.{self.source_method}({params})' if self.source_method else ''
        message += f'\nTraceback (most recent call last):\n{self.trace}' if self.trace else ''
        return f"{super().__str__()}, source={self.source}{source_method} {message}"


class TestResultEvent(SctEvent, Exception):
    """An event that is published and raised at the end of the test.
    It holds and displays all errors of the tests and framework happened.
    """
    __test__ = False  # Mark this class to be not collected by pytest.
    _head = f'{"=" * 30} TEST RESULTS {"=" * 35}'
    _ending = "=" * 80

    def __init__(self, test_status: str, events: dict):
        super().__init__()
        self.test_status = test_status
        self.events = events
        self.ok = test_status == 'SUCCESS'
        self.severity = Severity.NORMAL if self.ok else Severity.ERROR

    def __str__(self):
        if self.ok:
            return dedent(f"""
            {self._head}
            {self._ending}
            SUCCESS :)
            """)
        result = f'\n{self._head}\n'
        for event_group, events in self.events.items():
            if not events:
                continue
            result += f'\n{"-" * 5} LAST {event_group} EVENT {"-" * (62 - len(event_group)) }\n'
            result += '\n'.join(events)
        result += f'{self._ending}\n{self.test_status} :('
        return result

    def __reduce__(self):
        """Needed to be able to serialize and deserialize this event via pickle
        """
        return self.__class__, (self.test_status, self.events)

    def __eq__(self, other: 'TestResultEvent'):
        """Needed to be able to find this event in publish_event_guaranteed cycle
        """
        return isinstance(other, type(self)) and self.test_status == other.test_status and \
            self.events == other.events


class SystemEvent(SctEvent):
    pass


class BaseFilter(SystemEvent):
    def __init__(self):
        super().__init__()
        self.id = id(self)  # pylint: disable=invalid-name
        self.clear_filter = False
        self.expire_time = None
        self.publish()

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.id == other.id

    def cancel_filter(self):
        self.clear_filter = True
        self.publish()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.cancel_filter()

    def eval_filter(self, event):
        raise NotImplementedError()


class DbEventsFilter(BaseFilter):
    def __init__(self, type, line=None, node=None):  # pylint: disable=redefined-builtin
        self.type = type
        self.line = line
        self.node = str(node) if node else None
        super(DbEventsFilter, self).__init__()

    def cancel_filter(self):
        if self.node:
            self.expire_time = time.time()
        super().cancel_filter()

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


class EventsFilter(BaseFilter):
    def __init__(self, event_class: Optional[Type[EventType]] = None, regex: Optional[str] = None,
                 extra_time_to_expiration: Optional[int] = None):
        """
        A filter used to stop events to being raised in `subscribe_events()` calls

        :param event_class: the event class to filter
        :param regex: a regular expression to filter (used on the output of __str__ function of the event)
        :param extra_time_to_expiration: extra time to add for the event expriation

        example of usage:
        >>> events_filter = EventsFilter(event_class=CoreDumpEvent, regex=r'.*bash.core.*')
        >>> CoreDumpEvent("code creating coredump")
        >>> events_filter.cancel_filter()

        example as context manager, that will last 30sec more after exiting the context:
        >>> with EventsFilter(event_class=CoreDumpEvent, regex=r'.*bash.core.*', extra_time_to_expiration=30):
        ...     CoreDumpEvent("code creating coredump")
        """

        assert event_class or regex, "Should call with event_class or regex, or both"
        if event_class:
            assert issubclass(event_class, SctEvent), "event_class should be a class inherits from SctEvent"
            self.event_class = str(event_class.__name__)
        else:
            self.event_class = event_class
        self.regex = regex
        self.extra_time_to_expiration = extra_time_to_expiration
        super().__init__()

    def cancel_filter(self):
        if self.extra_time_to_expiration:
            self.expire_time = time.time() + self.extra_time_to_expiration
        super().cancel_filter()

    def eval_filter(self, event):
        is_class_matching = self.event_class and str(event.__class__.__name__) == self.event_class
        is_regex_matching = self.regex and re.match(self.regex, str(event), re.MULTILINE | re.DOTALL) is not None

        result = True
        if self.event_class:
            result = is_class_matching
        if self.regex:
            result = result and is_regex_matching
        return result


class EventsSeverityChangerFilter(EventsFilter):
    def __init__(self, event_class: Optional[Type[EventType]] = None, regex: Optional[str] = None, extra_time_to_expiration: Optional[int] = None, severity: Optional[Severity] = None):
        """
        A filter that if matches, can change the severity of the matched event

        :param event_class: the event class to filter
        :param regex: a regular expression to filter (used on the output of __str__ function of the event)
        :param extra_time_to_expiration: extra time to add for the event expriation
        :param severity: the new sevirity to assign to the matched events

        exmaple of lower all TestFrameworkEvent to WARNING severity
        >>> severity_filter = EventsSeverityChangerFilter(event_class=TestFrameworkEvent, severity=Severity.WARNING)
        >>> TestFrameworkEvent(source='setup', source_method='cluster_setup', severity=Severity.ERROR)
        >>> severity_filter.cancel_filter()

        example as context manager:
        >>> with EventsSeverityChangerFilter(event_class=TestFrameworkEvent, severity=Severity.WARNING):
        ...     TestFrameworkEvent(source='setup', source_method='cluster_setup', severity=Severity.ERROR)
        """

        assert severity, "EventsSeverityChangerFilter can't work without severity configured"
        self.severity = severity
        super().__init__(event_class=event_class, regex=regex, extra_time_to_expiration=extra_time_to_expiration)

    def eval_filter(self, event):
        should_change = super().eval_filter(event)
        if should_change and self.severity:
            event.severity = self.severity
        return False


class StartupTestEvent(SystemEvent):
    def __init__(self):
        super().__init__()
        self.severity = Severity.NORMAL


class InfoEvent(SctEvent):
    def __init__(self, message):
        super(InfoEvent, self).__init__()
        self.message = message
        self.severity = Severity.NORMAL
        self.publish()

    def __str__(self):
        return "{0}: message={1.message}".format(super(InfoEvent, self).__str__(), self)


class IndexSpecialColumnErrorEvent(SctEvent):
    def __init__(self, message):
        super(IndexSpecialColumnErrorEvent, self).__init__()
        self.message = message
        self.severity = Severity.ERROR
        self.publish()

    def __str__(self):
        return f"{super().__str__()}: message={self.message}"


class ThreadFailedEvent(SctEvent):
    def __init__(self, message, traceback):
        super(ThreadFailedEvent, self).__init__()
        self.message = message
        self.severity = Severity.ERROR
        self.traceback = str(traceback)
        self.publish_or_dump()

    def __str__(self):
        return f"{super().__str__()}: message={self.message}\n{self.traceback}"


class CoreDumpEvent(SctEvent):
    def __init__(self, corefile_url, download_instructions,  # pylint: disable=too-many-arguments
                 backtrace, node, timestamp=None):
        super(CoreDumpEvent, self).__init__()
        self.corefile_url = corefile_url
        self.download_instructions = download_instructions
        self.backtrace = backtrace
        self.severity = Severity.ERROR
        self.node = str(node)
        if timestamp is not None:
            self.timestamp = timestamp
        self.publish()

    def __str__(self):
        output = super(CoreDumpEvent, self).__str__()
        for attr_name in ['node', 'corefile_url', 'backtrace', 'download_instructions']:
            attr_value = getattr(self, attr_name, None)
            if attr_value:
                output += f"{attr_name}={attr_value}\n"
        return output


class DisruptionEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    def __init__(self, type, name, status, start=None, end=None, duration=None, node=None, error=None, full_traceback=None, **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
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
        if self.severity == Severity.ERROR:
            return "{0}: type={1.type} name={1.name} node={1.node} duration={1.duration} error={1.error}\n{1.full_traceback}".format(
                super(DisruptionEvent, self).__str__(), self)
        return "{0}: type={1.type} name={1.name} node={1.node} duration={1.duration}".format(
            super(DisruptionEvent, self).__str__(), self)


class ClusterHealthValidatorEvent(SctEvent):
    def __init__(self, type, name, status=Severity.ERROR, node=None, message=None, error=None, publish=True, **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super(ClusterHealthValidatorEvent, self).__init__()
        self.name = name
        self.type = type
        self.node = str(node)
        self.severity = status
        self.error = error if error else ''
        self.message = message if message else ''

        self.__dict__.update(kwargs)

        if publish:
            self.publish()

    def __str__(self):
        if self.severity in (Severity.NORMAL, Severity.WARNING):
            return "{0}: type={1.type} name={1.name} node={1.node} message={1.message}".format(
                super(ClusterHealthValidatorEvent, self).__str__(), self)
        elif self.severity in (Severity.CRITICAL, Severity.ERROR):
            return "{0}: type={1.type} name={1.name} node={1.node} error={1.error}".format(
                super(ClusterHealthValidatorEvent, self).__str__(), self)
        else:
            return super(ClusterHealthValidatorEvent, self).__str__()


class DataValidatorEvent(SctEvent):
    def __init__(self, type, name, status=Severity.ERROR, message=None, error=None, **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super(DataValidatorEvent, self).__init__()
        self.name = name
        self.type = type
        self.severity = status
        self.error = error if error else ''
        self.message = message if message else ''

        self.__dict__.update(kwargs)
        self.publish()

    def __str__(self):
        if self.severity in (Severity.NORMAL, Severity.WARNING):
            return "{0}: type={1.type} name={1.name} message={1.message}".format(
                super(DataValidatorEvent, self).__str__(), self)
        elif self.severity in (Severity.CRITICAL, Severity.ERROR):
            return "{0}: type={1.type} name={1.name} error={1.error}".format(
                super(DataValidatorEvent, self).__str__(), self)
        else:
            return super(DataValidatorEvent, self).__str__()


class FullScanEvent(SctEvent):
    def __init__(self, type, ks_cf, db_node_ip, severity=Severity.NORMAL, message=None):   # pylint: disable=redefined-builtin,too-many-arguments
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


class GeminiEvent(SctEvent):

    def __init__(self, type, cmd, result=None):  # pylint: disable=redefined-builtin
        super(GeminiEvent, self).__init__()
        self.type = type
        self.cmd = cmd
        self.msg = "{0}: type={1.type} gemini_cmd={1.cmd}"
        self.result = ""
        if result:
            self.result += "Exit code: {exit_code}\n"
            if result['stdout']:
                self.result += "Command output: {stdout}\n"
                result['stdout'] = result['stdout'].strip().split('\n')[-2:]
            if result['stderr']:
                self.result += "Command error: {stderr}\n"
            self.result = self.result.format(**result)
            if result['exit_code'] != 0 or result['stderr']:
                self.severity = Severity.CRITICAL
                self.type = 'error'
            self.msg += '\n{1.result}'
        self.publish()

    def __str__(self):
        return self.msg.format(super(GeminiEvent, self).__str__(), self)


class CassandraStressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super(CassandraStressEvent, self).__init__()
        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors
        self.publish()

    def __str__(self):
        if self.errors:
            return "{0}: type={1.type} node={1.node}\n{2}".format(
                super(CassandraStressEvent, self).__str__(), self, "\n".join(self.errors))

        return "{0}: type={1.type} node={1.node}\nstress_cmd={1.stress_cmd}".format(
            super(CassandraStressEvent, self).__str__(), self)


class ScyllaBenchEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super(ScyllaBenchEvent, self).__init__()
        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors
        self.publish()

    def __str__(self):
        if self.errors:
            return "{0}: type={1.type} node={1.node} stress_cmd={1.stress_cmd} error={2}".format(
                super(ScyllaBenchEvent, self).__str__(), self, "\n".join(self.errors))

        return "{0}: type={1.type} node={1.node} stress_cmd={1.stress_cmd}".format(
            super(ScyllaBenchEvent, self).__str__(), self)


class StressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super(StressEvent, self).__init__()
        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors
        self.publish()

    def __str__(self):
        fmt = f"{super(StressEvent, self).__str__()}: type={self.type} node={self.node}\nstress_cmd={self.stress_cmd}"
        if self.errors:
            errors_str = '\n'.join(self.errors)
            return f"{fmt}\nerrors:\n\n{errors_str}"
        return fmt


class YcsbStressEvent(StressEvent):
    pass


class NdbenchStressEvent(StressEvent):
    pass


class CDCReaderStressEvent(YcsbStressEvent):
    pass


class DatabaseLogEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    def __init__(self, type, regex, severity=Severity.ERROR):  # pylint: disable=redefined-builtin
        super(DatabaseLogEvent, self).__init__()
        self.type = type
        self.regex = regex
        self.line_number = 0
        self.line = None
        self.node = None
        self.backtrace = None
        self.raw_backtrace = None
        self.severity = severity

    def add_info(self, node, line: str, line_number: int) -> bool:
        """Update the event info from the log line.

        Return True if an event is ready to be published and False otherwise.
        """
        try:
            log_time = dateutil.parser.parse(line.split()[0])
            self.timestamp = log_time.timestamp()
        except ValueError:
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

        return True

    def add_backtrace_info(self, backtrace=None, raw_backtrace=None):
        if backtrace:
            self.backtrace = backtrace
        if raw_backtrace:
            self.raw_backtrace = raw_backtrace

    def clone_with_info(self, node, line: str, line_number: int) -> "DatabaseLogEvent":
        ret = DatabaseLogEvent(type='', regex='')
        ret.__dict__.update(self.__dict__)
        ret.add_info(node, line, line_number)
        return ret

    def add_info_and_publish(self, node, line: str, line_number: int) -> None:
        if self.add_info(node, line, line_number):
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


class GeminiLogEvent(DatabaseLogEvent):
    SEVERITY_MAPPING = {
        "INFO": "NORMAL",
        "DEBUG": "NORMAL",
        "WARN": "WARNING",
        "ERROR": "ERROR",
        "FATAL": "CRITICAL",
    }

    def __init__(self, verbose=False):
        super().__init__(type="geminievent", regex="", severity=Severity.CRITICAL)
        self.verbose = verbose

    def add_info(self, node, line: str, line_number: int) -> bool:
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            if self.verbose:
                LOGGER.debug("Failed to parse a line: %s", line.rstrip())
            return False
        try:
            self.timestamp = dateutil.parser.parse(data.pop("T")).timestamp()
        except ValueError:
            self.timestamp = time.time()
        self.severity = getattr(Severity, self.SEVERITY_MAPPING[data.pop("L")])
        self.line = data.pop("M")
        if data:
            self.line += " (" + " ".join(f'{key}="{value}"' for key, value in data.items()) + ")"
        self.line_number = line_number
        self.node = str(node)
        return True

    def __str__(self):
        return f"{SctEvent.__str__(self)}: type={self.type} line_number={self.line_number} node={self.node}\n" \
               f"{self.line}"


class SpotTerminationEvent(SctEvent):
    def __init__(self, node, message):
        super(SpotTerminationEvent, self).__init__()
        self.severity = Severity.CRITICAL
        self.node = str(node)
        self.message = message
        self.publish()

    def __str__(self):
        return "{0}: node={1.node} message={1.message}".format(
            super(SpotTerminationEvent, self).__str__(), self)


class PrometheusAlertManagerEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    _from_str_regexp = re.compile(
        "[^:]+: alert_name=(?P<alert_name>[^ ]+) type=(?P<type>[^ ]+) start=(?P<start>[^ ]+) "
        f"end=(?P<end>[^ ]+) description=(?P<description>[^ ]+) updated=(?P<updated>[^ ]+) state=(?P<state>[^ ]+) "
        f"fingerprint=(?P<fingerprint>[^ ]+) labels=(?P<labels>[^ ]+)")

    def __init__(self,  # pylint: disable=too-many-arguments
                 raw_alert=None, event_str=None, sct_event_str=None, event_type: str = None, severity=Severity.WARNING):
        super().__init__()
        self.severity = severity
        self.type = event_type
        if raw_alert:
            self._load_from_raw_alert(**raw_alert)
        elif event_str:
            self._load_from_event_str(event_str)
        elif sct_event_str:
            self._load_from_sctevent_str(sct_event_str)

    def __str__(self):
        return f"{super().__str__()}: alert_name={self.alert_name} type={self.type} start={self.start} "\
               f"end={self.end} description={self.description} updated={self.updated} state={self.state} "\
               f"fingerprint={self.fingerprint} labels={self.labels}"

    def _load_from_sctevent_str(self, data: str):
        result = self._from_str_regexp.match(data)
        if not result:
            return False
        tmp = result.groupdict()
        if not tmp:
            return False
        tmp['labels'] = json.loads(tmp['labels'])
        for name, value in tmp:
            setattr(self, name, value)
        return True

    def _load_from_event_str(self, data: str):
        try:
            tmp = json.loads(data)
        except Exception:  # pylint: disable=broad-except
            return None
        for name, value in tmp.items():
            if name not in ['annotations', 'description', 'start', 'end', 'updated', 'fingerprint', 'status', 'labels',
                            'state', 'alert_name', 'severity', 'type', 'timestamp', 'severity']:
                return False
            setattr(self, name, value)
        if isinstance(self.severity, str):
            self.severity = getattr(Severity, self.severity)
        return True

    def _load_from_raw_alert(self,  # pylint: disable=too-many-arguments,invalid-name,unused-argument
                             annotations: dict = None, startsAt=None, endsAt=None, updatedAt=None, fingerprint=None,
                             status=None, labels=None, **kwargs):
        self.annotations = annotations
        if self.annotations:
            self.description = self.annotations.get('description', self.annotations.get('summary', ''))
        else:
            self.description = ''
        self.start = startsAt
        self.end = endsAt
        self.updated = updatedAt
        self.fingerprint = fingerprint
        self.status = status
        self.labels = labels
        if self.status:
            self.state = self.status.get('state', '')
        else:
            self.state = ''
        if self.labels:
            self.alert_name = self.labels.get('alertname', '')
        else:
            self.alert_name = ''
        sct_severity = self.labels.get('sct_severity')
        if sct_severity:
            self.severity = Severity.__dict__.get(sct_severity, Severity.WARNING)

    def __eq__(self, other):
        for name in ['alert_name', 'type', 'start', 'end', 'description', 'updated', 'state', 'fingerprint', 'labels']:
            other_value = getattr(other, name, None)
            value = getattr(self, name, None)
            if value != other_value:
                return False
        return True


@contextmanager
def apply_log_filters(*event_filters_list: List[BaseFilter]):
    with ExitStack() as stack:
        for event_filter in event_filters_list:
            stack.enter_context(event_filter)  # pylint: disable=no-member
        yield


def raise_event_on_failure(func):
    """
    Decorate a function that is running inside a thread,
    when exception is raised in this function,
    will raise an Error severity event
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = None
        _test_pid = os.getpid()
        try:
            result = func(*args, **kwargs)
        except Exception as ex:  # pylint: disable=broad-except
            ThreadFailedEvent(message=str(ex), traceback=format_exc())

        return result
    return wrapper

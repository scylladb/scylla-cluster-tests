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

from textwrap import dedent
from traceback import format_stack

from sdcm.sct_events.base import SctEvent, Severity


class SystemEvent(SctEvent):
    pass


class StartupTestEvent(SystemEvent):
    severity = Severity.NORMAL


class TestFrameworkEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    __test__ = False  # Mark this class to be not collected by pytest.

    def __init__(self,
                 source,
                 source_method=None,  # pylint: disable=redefined-builtin,too-many-arguments
                 exception=None,
                 message=None,
                 args=None,
                 kwargs=None,
                 severity=None,
                 trace=None):
        super().__init__()

        self.severity = Severity.ERROR if severity is None else severity
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


class SpotTerminationEvent(SctEvent):
    severity = Severity.CRITICAL

    def __init__(self, node, message):
        super().__init__()

        self.node = str(node)
        self.message = message
        self.publish()

    def __str__(self):
        return f"{super().__str__()}: node={self.node} message={self.message}"


class InfoEvent(SctEvent):
    severity = Severity.NORMAL

    def __init__(self, message):
        super().__init__()

        self.message = message
        self.publish()

    def __str__(self):
        return f"{super().__str__()}: message={self.message}"


class ThreadFailedEvent(SctEvent):
    severity = Severity.ERROR

    def __init__(self, message, traceback):
        super().__init__()

        self.message = message
        self.traceback = str(traceback)
        self.publish_or_dump()

    def __str__(self):
        return f"{super().__str__()}: message={self.message}\n{self.traceback}"


class CoreDumpEvent(SctEvent):
    severity = Severity.ERROR

    def __init__(self,
                 corefile_url,
                 download_instructions,
                 backtrace,
                 node,
                 timestamp=None):
        super().__init__()

        self.corefile_url = corefile_url
        self.download_instructions = download_instructions
        self.backtrace = backtrace
        self.node = str(node)
        if timestamp is not None:
            self.timestamp = timestamp
        self.publish()

    def __str__(self):
        output = super().__str__()
        for attr_name in ['node', 'corefile_url', 'backtrace', 'download_instructions']:
            attr_value = getattr(self, attr_name, None)
            if attr_value:
                output += f"{attr_name}={attr_value}\n"
        return output


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

    def __eq__(self, other: 'TestResultEvent'):
        """Needed to be able to find this event in publish_event_guaranteed cycle
        """
        return isinstance(other, type(self)) and self.test_status == other.test_status and \
            self.events == other.events


class DisruptionEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 type,
                 name,
                 status,
                 start=None,
                 end=None,
                 duration=None,
                 node=None,
                 error=None,
                 full_traceback=None,
                 **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

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
                super().__str__(), self)
        return "{0}: type={1.type} name={1.name} node={1.node} duration={1.duration}".format(super().__str__(), self)

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

import re
import time
from typing import Optional, Type, List
from contextlib import contextmanager, ExitStack

from sdcm.sct_events.base import EventType, SctEvent, Severity
from sdcm.sct_events.system import SystemEvent


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
    def __init__(self,
                 event_class: Optional[Type[EventType]] = None,
                 regex: Optional[str] = None,
                 extra_time_to_expiration: Optional[int] = None,
                 severity: Optional[Severity] = None):
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


@contextmanager
def apply_log_filters(*event_filters_list: List[BaseFilter]):
    with ExitStack() as stack:
        for event_filter in event_filters_list:
            stack.enter_context(event_filter)  # pylint: disable=no-member
        yield

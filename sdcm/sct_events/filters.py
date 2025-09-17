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
from typing import Optional, Type, Union
from functools import cached_property

from sdcm.sct_events import Severity
from sdcm.sct_events.base import SctEvent, SctEventProtocol, BaseFilter, LogEventProtocol


class DbEventsFilter(BaseFilter):
    def __init__(
        self,
        db_event: Union[LogEventProtocol, Type[LogEventProtocol]],
        line: Optional[Union[str, re.Pattern]] = None,
        node: Optional = None,
        extra_time_to_expiration: Optional[int] = 0,
    ):
        super().__init__()

        self.filter_type = db_event.type
        self.filter_line = line
        self.filter_node = str(node.name if hasattr(node, "name") else node) if node else None
        self.extra_time_to_expiration = extra_time_to_expiration
        self.regex = None  # Initialize regex to None
        self.regex_flags = 0  # Initialize regex_flags to default value
        if isinstance(line, re.Pattern):
            self.regex = line.pattern
            self.regex_flags = line.flags
        elif isinstance(line, str):
            self.regex = line
            self.regex_flags = re.MULTILINE | re.DOTALL

    @cached_property
    def _regex(self):
        try:
            return self.regex and re.compile(self.regex, self.regex_flags)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f'Compilation of the regexp "{self.regex}" failed with error: {exc}') from None

    def eval_filter(self, event: LogEventProtocol) -> bool:
        if not isinstance(event, LogEventProtocol):
            return False

        if self.expire_time and event.timestamp and self.expire_time < event.timestamp:
            return False

        result = bool(self.filter_type) and self.filter_type == event.type

        if self._regex:
            event_line = getattr(event, "line", "") or ""
            result &= self._regex.search(event_line) is not None

        if self.filter_node:
            result &= self.filter_node in (getattr(event, "node", "") or "").split()

        return result

    def cancel_filter(self) -> None:
        if self.extra_time_to_expiration:
            self.expire_time = time.time() + self.extra_time_to_expiration
        super().cancel_filter()

    @property
    def msgfmt(self) -> str:
        output = ["{0.base}"]
        if self.filter_type:
            output.append("type={0.filter_type}")
        if self._regex:
            output.append(f"line={self._regex.pattern}")
        if self.filter_node:
            output.append("node={0.filter_node}")
        return "(" + (" ".join(output)) + ")"


class EventsFilter(BaseFilter):
    def __init__(
        self,
        event_class: Optional[Type[SctEventProtocol] | Type[SctEvent]] = None,
        regex: Optional[Union[str, re.Pattern]] = None,
        extra_time_to_expiration: Optional[int] = 0,
    ):
        assert event_class or regex, "Should call with event_class or regex, or both"
        assert not event_class or issubclass(event_class, SctEvent), (
            "event_class should be a class inherits from SctEvent"
        )

        super().__init__()

        self.event_class = event_class and event_class.__name__ + "."  # add a sentinel for a prefix match
        if isinstance(regex, re.Pattern):
            self.regex = regex.pattern
            self.regex_flags = regex.flags
        else:
            self.regex = regex
            self.regex_flags = re.MULTILINE | re.DOTALL
        self.extra_time_to_expiration = extra_time_to_expiration

    @cached_property
    def _regex(self):
        try:
            return self.regex and re.compile(self.regex, self.regex_flags)
        except Exception as exc:  # noqa: BLE001
            raise ValueError(f'Compilation of the regexp "{self.regex}" failed with error: {exc}') from None

    def cancel_filter(self) -> None:
        if self.extra_time_to_expiration:
            self.expire_time = time.time() + self.extra_time_to_expiration
        super().cancel_filter()

    def eval_filter(self, event: SctEventProtocol) -> bool:
        if self.expire_time and event.timestamp and self.expire_time < event.timestamp:
            return False

        result = not self.event_class or (type(event).__name__ + ".").startswith(self.event_class)

        if self._regex:
            result &= self._regex.match(str(event)) is not None

        return result

    @property
    def msgfmt(self) -> str:
        output = ["{0.base}"]
        if self.event_class:
            output.append("event_class={0.event_class}")
        if self.regex:
            output.append("regex={0.regex}")
        return "(" + (" ".join(output)) + ")"


class EventsSeverityChangerFilter(EventsFilter):
    def __init__(
        self,
        new_severity: Severity,
        event_class: Optional[Type[SctEvent]] = None,
        regex: Optional[str] = None,
        extra_time_to_expiration: Optional[int] = None,
    ):
        super().__init__(event_class=event_class, regex=regex, extra_time_to_expiration=extra_time_to_expiration)

        self.new_severity = new_severity

    def eval_filter(self, event: SctEventProtocol) -> bool:
        if super().eval_filter(event) and self.new_severity:
            event.severity = self.new_severity
        return False

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
import logging

import dateutil.parser

from sdcm.sct_events.base import SctEvent, Severity


LOGGER = logging.getLogger(__name__)


class ClusterHealthValidatorEvent(SctEvent):
    def __init__(self, type, name, status=Severity.ERROR, node=None, message=None, error=None, publish=True, **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

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
                super().__str__(), self)
        elif self.severity in (Severity.CRITICAL, Severity.ERROR):
            return "{0}: type={1.type} name={1.name} node={1.node} error={1.error}".format(
                super().__str__(), self)
        else:
            return super().__str__()


class DataValidatorEvent(SctEvent):
    def __init__(self, type, name, status=Severity.ERROR, message=None, error=None, **kwargs):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

        self.name = name
        self.type = type
        self.severity = status
        self.error = error if error else ''
        self.message = message if message else ''

        self.__dict__.update(kwargs)

        self.publish()

    def __str__(self):
        if self.severity in (Severity.NORMAL, Severity.WARNING):
            return "{0}: type={1.type} name={1.name} message={1.message}".format(super().__str__(), self)
        elif self.severity in (Severity.CRITICAL, Severity.ERROR):
            return "{0}: type={1.type} name={1.name} error={1.error}".format(super().__str__(), self)
        else:
            return super().__str__()


class FullScanEvent(SctEvent):
    def __init__(self, type, ks_cf, db_node_ip, severity=Severity.NORMAL, message=None):   # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

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
        return self.msg.format(super().__str__(), self)


class DatabaseLogEvent(SctEvent):  # pylint: disable=too-many-instance-attributes
    def __init__(self, type, regex, severity=Severity.ERROR):  # pylint: disable=redefined-builtin
        super().__init__()

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
                super().__str__(), self)

        if self.raw_backtrace:
            return "{0}: type={1.type} regex={1.regex} line_number={1.line_number} node={1.node}\n{1.line}\n{1.raw_backtrace}".format(
                super().__str__(), self)

        return "{0}: type={1.type} regex={1.regex} line_number={1.line_number} node={1.node}\n{1.line}".format(
            super().__str__(), self)


class IndexSpecialColumnErrorEvent(SctEvent):
    def __init__(self, message):
        super().__init__()

        self.message = message
        self.severity = Severity.ERROR

        self.publish()

    def __str__(self):
        return f"{super().__str__()}: message={self.message}"

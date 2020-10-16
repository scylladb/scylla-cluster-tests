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

import json
import time
import logging

import dateutil.parser

from sdcm.sct_events.base import SctEvent, Severity
from sdcm.sct_events.database import DatabaseLogEvent


LOGGER = logging.getLogger(__name__)


class GeminiEvent(SctEvent):
    def __init__(self, type, cmd, result=None):  # pylint: disable=redefined-builtin
        super().__init__()

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
        return self.msg.format(super().__str__(), self)


class CassandraStressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors

        self.publish()

    def __str__(self):
        if self.errors:
            return "{0}: type={1.type} node={1.node}\n{2}".format(super().__str__(), self, "\n".join(self.errors))
        return "{0}: type={1.type} node={1.node}\nstress_cmd={1.stress_cmd}".format(super().__str__(), self)


class ScyllaBenchEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

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
                super().__str__(), self, "\n".join(self.errors))
        return "{0}: type={1.type} node={1.node} stress_cmd={1.stress_cmd}".format(super().__str__(), self)


class StressEvent(SctEvent):
    def __init__(self, type, node, severity=Severity.NORMAL, stress_cmd=None, log_file_name=None, errors=None):  # pylint: disable=redefined-builtin,too-many-arguments
        super().__init__()

        self.type = type
        self.node = str(node)
        self.stress_cmd = stress_cmd
        self.log_file_name = log_file_name
        self.severity = severity
        self.errors = errors

        self.publish()

    def __str__(self):
        fmt = f"{super().__str__()}: type={self.type} node={self.node}\nstress_cmd={self.stress_cmd}"
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

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
# Copyright (c) 2021 ScyllaDB

# pylint: disable=too-many-lines

import json
import logging
import os
import re
from functools import cached_property
from multiprocessing import Process, Event, Queue
from typing import Optional

from sdcm.remote.base import CommandRunner
from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.database import get_pattern_to_event_to_func_mapping, BACKTRACE_RE
from sdcm.sct_events.decorators import raise_event_on_failure
from sdcm.utils.common import make_threads_be_daemonic_by_default

LOGGER = logging.getLogger(__name__)

# we aren't going to process log line which are bigger than
# this value, i.e. not event would be generated base on those,
# but they would still be in the logs
LOG_LINE_MAX_PROCESSING_SIZE = 1024 * 5


class DbLogReader(Process):
    # pylint: disable=too-many-instance-attributes
    EXCLUDE_FROM_LOGGING = [
        ' | sshd[',
        ' | systemd:',
        ' | systemd-logind:',
        ' | sudo:',
        ' | dhclient[',

        # Remove compactions, repair and steaming logs from being logged, we are getting to many of them
        '] compaction - [Compact',
        '] table - Done with off-strategy compaction for',
        '] table - Starting off-strategy compaction for',
        '] repair - Repair',
        'repair id [id=',
        '] stream_session - [Stream ',
        '] storage_proxy - Exception when communicating with',
    ]
    # pylint: disable=too-many-arguments
    BUILD_ID_REGEX = re.compile(r'build-id\s(.*?)\sstarting\s\.\.\.')

    def __init__(self,
                 system_log: str,
                 remoter: CommandRunner,
                 node_name: str,
                 system_event_patterns: list,
                 decoding_queue: Optional[Queue],
                 log_lines: bool,
                 ):
        self._system_log = system_log
        self._system_event_patterns = system_event_patterns
        self._decoding_queue = decoding_queue
        self._log_lines = log_lines
        self._node_name = node_name

        self._terminate_event = Event()
        self._last_error: LogEvent | None = None
        self._last_line_no = -1
        self._last_log_position = 0
        self._remoter = remoter
        self._skipped_end_line = 0
        self._build_id = None
        super().__init__(name=self.__class__.__name__, daemon=True)

    @cached_property
    def _continuous_event_patterns(self):
        return get_pattern_to_event_to_func_mapping(node=self._node_name)

    def _read_and_publish_events(self) -> None:  # noqa: PLR0912
        """Search for all known patterns listed in `sdcm.sct_events.database.SYSTEM_ERROR_EVENTS'."""

        # pylint: disable=too-many-branches,too-many-locals,too-many-statements

        backtraces = []
        index = 0

        if not os.path.exists(self._system_log):
            return

        with open(self._system_log, encoding="utf-8") as db_file:
            if self._last_log_position:
                db_file.seek(self._last_log_position)
            for index, line in enumerate(db_file, start=self._last_line_no + 1):
                if len(line) > LOG_LINE_MAX_PROCESSING_SIZE:
                    # trim to avoid filling the memory when lot of long line is writen
                    line = line[:LOG_LINE_MAX_PROCESSING_SIZE]  # noqa: PLW2901

                # Postpone processing line with no ending in case if half of line is written to the disc
                if line[-1] == '\n' or self._skipped_end_line > 20:
                    self._skipped_end_line = 0
                else:
                    self._skipped_end_line += 1
                    continue
                try:
                    json_log = None
                    if line[0] == '{':
                        try:
                            json_log = json.loads(line)
                        except Exception:  # pylint: disable=broad-except  # noqa: BLE001
                            pass

                    if self._log_lines:
                        line = line.strip()  # noqa: PLW2901
                        for pattern in self.EXCLUDE_FROM_LOGGING:
                            if pattern in line:
                                break
                        else:
                            LOGGER.debug(line)

                    if json_log:
                        continue

                    if match := self.BUILD_ID_REGEX.search(line):
                        self._build_id = match.groups()[0]
                        LOGGER.debug("Found build-id: %s", self._build_id)

                    match = BACKTRACE_RE.search(line)
                    one_line_backtrace = []
                    if match and backtraces:
                        data = match.groupdict()
                        if data['other_bt']:
                            backtraces[-1]['backtrace'] += [data['other_bt'].strip()]
                        if data['scylla_bt']:
                            backtraces[-1]['backtrace'] += [data['scylla_bt'].strip()]
                    elif "backtrace:" in line.lower() and "0x" in line:
                        # This part handles the backtrases are printed in one line.
                        # Example:
                        # [shard 2] seastar - Exceptional future ignored: exceptions::mutation_write_timeout_exception
                        # (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.),
                        # backtrace:   0x3316f4d#012  0x2e2d177#012  0x189d397#012  0x2e76ea0#012  0x2e770af#012
                        # 0x2eaf065#012  0x2ebd68c#012  0x2e48d5d#012  /opt/scylladb/libreloc/libpthread.so.0+0x94e1#012
                        splitted_line = re.split("backtrace:", line, flags=re.IGNORECASE)
                        for trace_line in splitted_line[1].split():
                            if trace_line.startswith('0x') or 'scylladb/lib' in trace_line:
                                one_line_backtrace.append(trace_line)

                    # for each line, if it matches a continuous event pattern,
                    # call the appropriate function with the class tied to that pattern
                    for item in self._continuous_event_patterns:
                        if event_match := item.pattern.search(line):
                            item.period_func(match=event_match)
                            break

                    skip_to_next_line = False
                    # for each line use all regexes to match, and if found send an event
                    for pattern, event in self._system_event_patterns:
                        if pattern.search(line):
                            if event.severity == Severity.SUPPRESS:
                                skip_to_next_line = True
                                break
                            cloned_event = event.clone().add_info(node=self._node_name, line_number=index, line=line)
                            backtraces.append(dict(event=cloned_event, backtrace=[]))
                            break  # Stop iterating patterns to avoid creating two events for one line of the log

                    if skip_to_next_line:
                        continue

                    if one_line_backtrace and backtraces:
                        backtraces[-1]['backtrace'] = one_line_backtrace
                except Exception:  # pylint: disable=broad-except
                    LOGGER.exception('Processing of %s line of %s failed, line content:\n%s',
                                     index, self._system_log, line)

            if index:
                self._last_line_no = index
                self._last_log_position = db_file.tell()

        traces_count = 0
        for backtrace in backtraces:
            backtrace['event'].raw_backtrace = "\n".join(backtrace['backtrace'])
            if backtrace['event'].type == 'BACKTRACE':
                traces_count += 1

        # support interlaced reactor stalled
        for _ in range(traces_count):
            self._last_error = None
            backtraces = list(filter(self.filter_backtraces, backtraces))

        for backtrace in backtraces:
            if not (self._decoding_queue and backtrace["event"].raw_backtrace):
                backtrace["event"].publish()
                continue
            try:
                self._decoding_queue.put({
                    "node": self._node_name,
                    "build_id": self._build_id,
                    "event": backtrace["event"],
                })
            except Exception:  # pylint: disable=broad-except
                backtrace["event"].publish()
                raise

    @raise_event_on_failure
    def run(self):
        """
        Keep reporting new events from db log, every 30 seconds.
        """
        LOGGER.debug('Logging for node %s is started with following configuration:\nsystem_log=%s'
                     '\nlog_lines=%s\ndecoding_queue=%s',
                     self._node_name, self._system_log, self._log_lines, self._decoding_queue is not None)
        make_threads_be_daemonic_by_default()
        while not self._terminate_event.wait(0.1):
            try:
                self._read_and_publish_events()
            except (SystemExit, KeyboardInterrupt) as ex:
                LOGGER.debug("db_log_reader_thread() stopped by %s", ex.__class__.__name__)
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("failed to read db log")

    def filter_backtraces(self, backtrace):
        # A filter function to attach the backtrace to the correct error and not to the backtraces.
        # If the error is within 10 lines and the last isn't backtrace type, the backtrace would be
        # appended to the previous error.
        try:
            if (self._last_error and
                    backtrace['event'].line_number <= self._last_error.line_number + 20
                    and not self._last_error.type == 'BACKTRACE'
                    and backtrace['event'].type == 'BACKTRACE'):
                self._last_error.raw_backtrace = "\n".join(backtrace['backtrace'])
                backtrace['event'].dont_publish()
                return False
            return True
        finally:
            self._last_error = backtrace['event']

    def get_scylla_build_id(self) -> str | None:
        return self._build_id

    def stop(self):
        self._terminate_event.set()

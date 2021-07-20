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

import threading
import logging
import re
from typing import Iterable

from sdcm.utils.file import File
from sdcm.utils.decorators import timeout
from sdcm.sct_events.operator import (
    SCYLLA_OPERATOR_EVENT_PATTERNS,
)


class ScyllaOperatorLogMonitoring(threading.Thread):
    lookup_time = 0.1
    log = logging.getLogger('ScyllaOperatorLogMonitoring')
    patterns = [
        re.compile(r'^\s*{\s*"L"\s*:\s*"ERROR"'),
        re.compile(r'^\s*{\s*"L"\s*:\s*"INFO"'),
    ]

    def __init__(self, kluster):
        self.termination_event = threading.Event()
        self.current_position = 0
        self.kluster = kluster
        super().__init__(daemon=True)

    @timeout(timeout=300, sleep_time=10, message='Wait for file to be available')
    def _get_file_follower(self) -> Iterable[str]:
        return File(self.kluster.scylla_operator_log, 'r').read_lines_filtered(*self.patterns)

    def run(self):
        file_follower = self._get_file_follower()
        while not self.termination_event.wait(self.lookup_time):
            self.check_logs(file_follower)
        self.check_logs(file_follower)

    @staticmethod
    def check_logs(file_follower: Iterable[str]):
        for log_record in file_follower:
            event_to_log = None
            for pattern, event in SCYLLA_OPERATOR_EVENT_PATTERNS:
                if pattern.search(log_record):
                    event_to_log = event.clone().add_info(
                        node="N/A", line=log_record, line_number=0)
                    break
            if event_to_log is None:
                # NOTE: here we have log line that doesn't match any pattern, so, move on
                continue
            if event_to_log := event_to_log.load_from_json_string(log_record):
                event_to_log.publish()

    def stop(self):
        self.termination_event.set()

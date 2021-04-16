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
import json
from typing import Iterable
from datetime import datetime

from sdcm.utils.file import File
from sdcm.utils.decorators import timeout
from sdcm.sct_events.operator import ScyllaOperatorLogEvent, ScyllaOperatorRestartEvent, SCYLLA_OPERATOR_EVENT_PATTERNS


class ScyllaOperatorLogMonitoring(threading.Thread):
    lookup_time = 0.1
    log = logging.getLogger('ScyllaOperatorLogMonitoring')
    patterns = [
        re.compile('^\s*{\s*"L"\s*:\s*"ERROR"'),
        re.compile('^\s*{\s*"L"\s*:\s*"INFO"'),
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

    def check_logs(self, file_follower: Iterable[str]):
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


class ScyllaOperatorStatusMonitoring(threading.Thread):
    status_check_period = 10
    log = logging.getLogger('ScyllaOperatorStatusMonitoring')

    def __init__(self, kluster):
        self.termination_event = threading.Event()
        self.kluster = kluster
        self.last_restart_count = 0
        super().__init__(daemon=True)

    def run(self):
        while not self.termination_event.wait(self.status_check_period):
            self.check_pod_status()
        self.check_pod_status()

    def check_pod_status(self):
        restart_happened = False
        try:
            status = self.kluster.operator_pod_status
            if status is None:
                return
            current_restart_count = status.container_statuses[0].restart_count
            if self.last_restart_count != current_restart_count:
                restart_happened = True
                self.last_restart_count = current_restart_count
        except Exception as exc:
            self.log.warning(f'Failed to get scylla operator status, due to the: {str(exc)}')
        if restart_happened:
            try:
                ScyllaOperatorRestartEvent(self.last_restart_count).publish()
            except Exception as exc:
                self.log.warning(f'Failed to publish event: {str(exc)}')

    def stop(self):
        self.termination_event.set()

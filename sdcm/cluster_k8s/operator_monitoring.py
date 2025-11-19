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
from typing import Iterable

from sdcm.utils.file import File
from sdcm.utils.decorators import timeout
from sdcm.sct_events.operator import (
    SCYLLA_OPERATOR_EVENT_PATTERNS,
)


class ScyllaOperatorLogMonitoring(threading.Thread):
    lookup_time = 0.1
    log = logging.getLogger("ScyllaOperatorLogMonitoring")

    def __init__(self, kluster):
        self.termination_event = threading.Event()
        self.current_position = 0
        self.kluster = kluster
        super().__init__(daemon=True)

    def _get_file_follower(self) -> Iterable[str]:
        follower_message = (
            f"{self.kluster.region_name}: Wait for the '{self.kluster.scylla_operator_log}' file to be available"
        )

        @timeout(timeout=300, sleep_time=10, message=follower_message)
        def _get_file_follower_wrapped(kluster) -> Iterable[str]:
            return File(kluster.scylla_operator_log, "r").iterate_lines()

        return _get_file_follower_wrapped(self.kluster)

    def run(self):
        file_follower = self._get_file_follower()
        while not self.termination_event.wait(self.lookup_time):
            self.check_logs(file_follower)
        self.check_logs(file_follower)

    @staticmethod
    def check_logs(file_follower: Iterable[str]):
        for log_record in file_follower:
            for pattern, event in SCYLLA_OPERATOR_EVENT_PATTERNS:
                if pattern.search(log_record):
                    event_to_log = event.clone().add_info(node="N/A", line=log_record, line_number=0)
                    event_to_log.publish()
                    break

    def stop(self):
        self.termination_event.set()

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


import time
import unittest
import uuid
from pathlib import Path

from sdcm.sct_events.database import BootstrapEvent


class TestBootstrapEvent(unittest.TestCase):
    temp_log_file_path = Path(f"/tmp/{time.time()}.log")

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_log_file_path.touch()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_log_file_path.unlink()

    def setUp(self) -> None:
        self.event_id = uuid.uuid4()
        self.node = "1.2.4.5.6"
        self.bootstrap_event = BootstrapEvent(node=self.node,
                                              log_file_name=str(self.temp_log_file_path),
                                              publish_event=False)
        self.bootstrap_event.event_id = self.event_id

    def test_bootstrap_event_begin(self):
        self.bootstrap_event.begin_event()
        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=begin event_id={self.event_id} " \
                   f"node={self.node}"

        self.assertEqual(actual, expected)

    def test_bootstrap_event_duration(self):
        duration = 60
        duration_fmt = "1m0s"
        self.bootstrap_event.duration = duration
        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=not-set " \
                   f"event_id={self.event_id} duration={duration_fmt} node={self.node}"

        self.assertEqual(actual, expected)

    def test_bootstrap_as_ctx_manager(self):
        duration = 10

        with self.bootstrap_event:
            self.assertEqual(self.bootstrap_event.period_type, "begin")
            time.sleep(duration)

        self.assertEqual(self.bootstrap_event.duration, duration)
        self.assertEqual(self.bootstrap_event.period_type, "end")

    def test_bootstrap_event_failure(self):
        duration = 605
        duration_fmt = "10m5s"
        errors = ["Failed with status 1"]
        self.bootstrap_event.add_error(errors)
        self.bootstrap_event.duration = duration
        self.bootstrap_event.end_event()

        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=end event_id={self.event_id} " \
                   f"duration={duration_fmt} node={self.node} errors=['{errors[0]}']"

        self.assertEqual(actual, expected)

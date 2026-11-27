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

"""RealEventsTest: shared base class for unit tests that need the real events subsystem.

Extracted from test_events.py to break cross-test imports.
Tests that only need to capture events should prefer FakeEventsDevice / make_fake_events()
from unit_tests.lib.fake_events — the real multiprocessing events system started here
incurs ~4 seconds of startup overhead.
"""

from pathlib import Path

from sdcm.prometheus import start_metrics_server
from unit_tests.lib.events_utils import EventsUtilsMixin


class RealEventsTest(EventsUtilsMixin):
    """Base test class running the real events device, with log files in a per-class temp dir."""

    @classmethod
    def setup_class(cls) -> None:
        """Start the metrics server and the real events device with a patched registry."""
        start_metrics_server()
        cls.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)

    @classmethod
    def teardown_class(cls) -> None:
        """Stop the events device and remove its temp dir."""
        cls.teardown_events_processes()

    @classmethod
    def get_event_log_file(cls, name: str) -> str:
        """Return the text of the named events log file, or an empty string when it is missing."""
        if (log_file := Path(cls.temp_dir, "events_log", name)).exists():
            return log_file.read_text(encoding="utf-8")
        return ""

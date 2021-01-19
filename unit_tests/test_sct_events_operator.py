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

import pickle
import unittest

from sdcm.sct_events.operator import ScyllaOperatorLogEvent, ScyllaOperatorRestartEvent


class TestOperatorEvents(unittest.TestCase):
    def test_scylla_operator_log_event(self):
        event = ScyllaOperatorLogEvent(namespace="scylla", cluster="c1", message="m1", error="e1", trace_id="tid1")
        self.assertEqual(
            str(event),
            "(ScyllaOperatorLogEvent Severity.ERROR): line_number=0 tid1 scylla/c1: m1, e1",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_scylla_operator_restart_event(self):
        event = ScyllaOperatorRestartEvent(restart_count=10)
        self.assertEqual(
            str(event),
            "(ScyllaOperatorRestartEvent Severity.ERROR): Scylla Operator has been restarted, restart_count=10",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

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

from invoke.runners import Result

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.loaders import \
    GeminiEvent, CassandraStressEvent, ScyllaBenchEvent, YcsbStressEvent, NdbenchStressEvent, CDCReaderStressEvent, \
    KclStressEvent, CassandraStressLogEvent, ScyllaBenchLogEvent, GeminiLogEvent, \
    CS_ERROR_EVENTS, SCYLLA_BENCH_ERROR_EVENTS, CS_ERROR_EVENTS_PATTERNS


class TestGeminiEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertFalse(hasattr(GeminiEvent, "failure"))
        self.assertTrue(issubclass(GeminiEvent.error, GeminiEvent))
        self.assertFalse(hasattr(GeminiEvent, "timeout"))
        self.assertTrue(issubclass(GeminiEvent.start, GeminiEvent))
        self.assertTrue(issubclass(GeminiEvent.finish, GeminiEvent))

    def test_without_result(self):
        event = GeminiEvent.start(cmd="cat")
        self.assertEqual(event.severity, Severity.NORMAL)
        self.assertEqual(event.cmd, "cat")
        self.assertEqual(event.result, "")
        self.assertEqual(str(event), "(GeminiEvent Severity.NORMAL): type=start gemini_cmd=cat")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_result(self):
        event = GeminiEvent.error(cmd="cat",
                                  result=Result(stdout="  \n\nline1\n  line2  \nline3\n  ", stderr="\terr\t", exited=1))
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.cmd, "cat")

        result = "Exit code: 1\nCommand output: ['  line2  ', 'line3']\nCommand error: \terr\t\n"
        self.assertEqual(event.result, result)
        self.assertEqual(str(event), "(GeminiEvent Severity.CRITICAL): type=error gemini_cmd=cat\n" + result)

        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestCassandraStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(CassandraStressEvent.failure, CassandraStressEvent))
        self.assertTrue(issubclass(CassandraStressEvent.error, CassandraStressEvent))
        self.assertFalse(hasattr(CassandraStressEvent, "timeout"))
        self.assertTrue(issubclass(CassandraStressEvent.start, CassandraStressEvent))
        self.assertTrue(issubclass(CassandraStressEvent.finish, CassandraStressEvent))

    def test_without_errors(self):
        event = CassandraStressEvent.error(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(CassandraStressEvent Severity.ERROR): type=error node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = CassandraStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(CassandraStressEvent Severity.CRITICAL): type=failure node=node1\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestScyllaBenchEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(ScyllaBenchEvent.failure, ScyllaBenchEvent))
        self.assertTrue(issubclass(ScyllaBenchEvent.error, ScyllaBenchEvent))
        self.assertTrue(issubclass(ScyllaBenchEvent.timeout, ScyllaBenchEvent))
        self.assertTrue(issubclass(ScyllaBenchEvent.start, ScyllaBenchEvent))
        self.assertTrue(issubclass(ScyllaBenchEvent.finish, ScyllaBenchEvent))

    def test_without_errors(self):
        event = ScyllaBenchEvent.timeout(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(ScyllaBenchEvent Severity.ERROR): type=timeout node=[] stress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = ScyllaBenchEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(ScyllaBenchEvent Severity.CRITICAL): type=failure node=node1 stress_cmd=None error=e1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestYcsbStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(YcsbStressEvent.failure, YcsbStressEvent))
        self.assertTrue(issubclass(YcsbStressEvent.error, YcsbStressEvent))
        self.assertFalse(hasattr(YcsbStressEvent, "timeout"))
        self.assertTrue(issubclass(YcsbStressEvent.start, YcsbStressEvent))
        self.assertTrue(issubclass(YcsbStressEvent.finish, YcsbStressEvent))

    def test_without_errors(self):
        event = YcsbStressEvent.error(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(YcsbStressEvent Severity.ERROR): type=error node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = YcsbStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(YcsbStressEvent Severity.CRITICAL): type=failure node=node1\nstress_cmd=None\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestCDCReaderStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(CDCReaderStressEvent.failure, CDCReaderStressEvent))
        self.assertTrue(issubclass(CDCReaderStressEvent.error, CDCReaderStressEvent))
        self.assertFalse(hasattr(CDCReaderStressEvent, "timeout"))
        self.assertTrue(issubclass(CDCReaderStressEvent.start, CDCReaderStressEvent))
        self.assertTrue(issubclass(CDCReaderStressEvent.finish, CDCReaderStressEvent))

    def test_without_errors(self):
        event = CDCReaderStressEvent.start(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.NORMAL)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(CDCReaderStressEvent Severity.NORMAL): type=start node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = CDCReaderStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(CDCReaderStressEvent Severity.CRITICAL): type=failure node=node1\nstress_cmd=None\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestNdbenchStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(NdbenchStressEvent.failure, NdbenchStressEvent))
        self.assertTrue(issubclass(NdbenchStressEvent.error, NdbenchStressEvent))
        self.assertFalse(hasattr(NdbenchStressEvent, "timeout"))
        self.assertTrue(issubclass(NdbenchStressEvent.start, NdbenchStressEvent))
        self.assertTrue(issubclass(NdbenchStressEvent.finish, NdbenchStressEvent))

    def test_without_errors(self):
        event = NdbenchStressEvent.error(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(NdbenchStressEvent Severity.ERROR): type=error node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = NdbenchStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(NdbenchStressEvent Severity.CRITICAL): type=failure node=node1\nstress_cmd=None\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestKclStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(KclStressEvent.failure, KclStressEvent))
        self.assertFalse(hasattr(KclStressEvent, "error"))
        self.assertFalse(hasattr(KclStressEvent, "timeout"))
        self.assertTrue(issubclass(KclStressEvent.start, KclStressEvent))
        self.assertTrue(issubclass(KclStressEvent.finish, KclStressEvent))

    def test_without_errors(self):
        event = KclStressEvent.failure(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertIsNone(event.errors)
        self.assertEqual(str(event), "(KclStressEvent Severity.ERROR): type=failure node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = KclStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        self.assertEqual(
            str(event),
            "(KclStressEvent Severity.ERROR): type=failure node=node1\nstress_cmd=None\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestCassandraStressLogEvent(unittest.TestCase):
    def test_known_cs_errors(self):
        self.assertTrue(issubclass(CassandraStressLogEvent.IOException, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.ConsistencyError, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.OperationOnKey, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.TooManyHintsInFlight, CassandraStressLogEvent))

    def test_cs_error_events_list(self):
        self.assertSetEqual(set(dir(CassandraStressLogEvent)) - set(dir(LogEvent)),
                            {ev.type for ev in CS_ERROR_EVENTS})

    def test_cs_hint_in_flight_error(self):
        def _get_event(line, expected_type, expected_severity):
            for pattern, event in CS_ERROR_EVENTS_PATTERNS:
                if pattern.search(line):
                    event.add_info(node='self.node', line=line, line_number=1).dont_publish()
                    assert event.type == expected_type, \
                        f'Unexpected event.type {event.type}. Expected "{expected_type}"'
                    assert event.severity == expected_severity, \
                        f'Unexpected event.severity {event.severity}. Expected "{expected_severity}"'
                    return

            raise ValueError(f"Event is not recognized in the line {line}. Expected exception type is {expected_type},"
                             f"expected severity is {expected_severity}")

        _get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                        '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many '
                        'in flight hints: 10490670',
                   expected_type='TooManyHintsInFlight',
                   expected_severity=Severity.ERROR)

        _get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                        '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many '
                        'hints in flight: 10490670',
                   expected_type='TooManyHintsInFlight',
                   expected_severity=Severity.ERROR)

        _get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                        '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) ',
                   expected_type='OperationOnKey',
                   expected_severity=Severity.CRITICAL)

        _get_event(line='java.io.IOException: Connection reset by peer',
                   expected_type='IOException',
                   expected_severity=Severity.ERROR)

        _get_event(line='03:56:37.572 [cluster1-nio-worker-4] DEBUG com.datastax.driver.core.Connection - Connection['
                        '/10.0.3.121:9042-11, inFlight=5, closed=false] Response received on stream 17856 but no '
                        'handler set anymore (either the request has timed out or it was closed due to another error). '
                        'Received message is ERROR UNAVAILABLE: Cannot achieve consistency level for cl QUORUM. '
                        'Requires 2, alive 1',
                   expected_type='ConsistencyError',
                   expected_severity=Severity.ERROR)


class TestScyllaBenchLogEvent(unittest.TestCase):
    def test_known_scylla_bench_errors(self):
        self.assertTrue(issubclass(ScyllaBenchLogEvent.ConsistencyError, ScyllaBenchLogEvent))

    def test_scylla_bench_error_events_list(self):
        self.assertSetEqual(set(dir(ScyllaBenchLogEvent)) - set(dir(LogEvent)),
                            {ev.type for ev in SCYLLA_BENCH_ERROR_EVENTS})


class TestGeminiLogEvent(unittest.TestCase):
    def test_json_line(self):
        event = GeminiLogEvent.geminievent()
        event.add_info(
            node="node1",
            line='{"L":"INFO","T":"2020-06-09T03:40:39.349Z","N":"pump","M":"Test run stopped. Exiting."}',
            line_number=1,
        )
        self.assertEqual(event.timestamp, 1591674039.349)
        self.assertEqual(event.severity, Severity.NORMAL)
        self.assertEqual(event.node, "node1")
        self.assertEqual(event.line, 'Test run stopped. Exiting. (N="pump")')
        self.assertEqual(event.line_number, 1)
        self.assertIsNone(event.backtrace)
        self.assertIsNone(event.raw_backtrace)
        self.assertTrue(event._ready_to_publish)
        self.assertEqual(
            str(event),
            '(GeminiLogEvent Severity.NORMAL): type=geminievent line_number=1 node=node1\n'
            'Test run stopped. Exiting. (N="pump")',
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_non_json_line(self):
        event = GeminiLogEvent.geminievent()
        timestamp = event.timestamp
        event.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
        self.assertEqual(event.timestamp, timestamp)
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertIsNone(event.node)
        self.assertIsNone(event.line)
        self.assertEqual(event.line_number, 0)
        self.assertIsNone(event.backtrace)
        self.assertIsNone(event.raw_backtrace)
        self.assertFalse(event._ready_to_publish)
        self.assertEqual(
            str(event),
            "(GeminiLogEvent Severity.CRITICAL): type=geminievent line_number=0 node=None\nNone",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

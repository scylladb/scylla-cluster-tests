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

from itertools import chain

from invoke.runners import Result

from sdcm.sct_events import Severity
from sdcm.sct_events.base import LogEvent
from sdcm.sct_events.loaders import \
    GeminiStressEvent, CassandraStressEvent, ScyllaBenchEvent, YcsbStressEvent, NdBenchStressEvent, CDCReaderStressEvent, \
    KclStressEvent, CassandraStressLogEvent, ScyllaBenchLogEvent, GeminiStressLogEvent, \
    CS_ERROR_EVENTS, CS_NORMAL_EVENTS, SCYLLA_BENCH_ERROR_EVENTS, CS_ERROR_EVENTS_PATTERNS, CS_NORMAL_EVENTS_PATTERNS, \
    SCYLLA_BENCH_NORMAL_EVENTS_PATTERNS, SCYLLA_BENCH_ERROR_EVENTS_PATTERNS, SCYLLA_BENCH_NORMAL_EVENTS


class TestGeminiEvent(unittest.TestCase):
    def test_continuous_event_with_error(self):
        gemini_stress_event = GeminiStressEvent(node="node", cmd="gemini_cmd", publish_event=False)
        begin_event_timestamp = 1623596860.1202102
        gemini_stress_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = gemini_stress_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         '(GeminiStressEvent Severity.NORMAL) period_type=begin '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node gemini_cmd=gemini_cmd')
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(gemini_stress_event)))

        gemini_stress_event.add_error(errors=["error1", "error2"])
        gemini_stress_event.severity = Severity.ERROR

        gemini_stress_event.end_event()
        gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623597850.6610544
        self.assertEqual(str(gemini_stress_event),
                         '(GeminiStressEvent Severity.ERROR) period_type=end '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=16m30s: node=node gemini_cmd=gemini_cmd'
                         '\nerrors=[\'error1\', \'error2\']')
        self.assertEqual(gemini_stress_event, pickle.loads(pickle.dumps(gemini_stress_event)))

    def test_continuous_event_with_result(self):
        gemini_stress_event = GeminiStressEvent(node="node", cmd="cat", publish_event=False)
        begin_event_timestamp = 1623596860.1202102
        gemini_stress_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = gemini_stress_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         '(GeminiStressEvent Severity.NORMAL) period_type=begin '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node gemini_cmd=cat')
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        gemini_stress_event.add_result(result=Result(stdout="  \n\nline1\n  line2  \nline3\n  ",
                                                     stderr="\terr\t", exited=1))

        result = "Exit code: 1\nCommand output: ['  line2  ', 'line3']\n"
        gemini_stress_event.end_event()
        gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623599860.1202102
        self.assertEqual(gemini_stress_event.result, result)
        self.assertEqual(str(gemini_stress_event),
                         '(GeminiStressEvent Severity.NORMAL) period_type=end '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=50m0s: node=node gemini_cmd=cat\n'
                         'result=Exit code: 1\nCommand output: [\'  line2  \', \'line3\']\n\n'
                         'errors=[\'Command error: \\terr\\t\\n\']')

        self.assertEqual(gemini_stress_event, pickle.loads(pickle.dumps(gemini_stress_event)))

    def test_continuous_event_without_result(self):
        gemini_stress_event = GeminiStressEvent(node="node", cmd="cat", publish_event=False)
        begin_event_timestamp = 1623596860.1202102
        gemini_stress_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = gemini_stress_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         '(GeminiStressEvent Severity.NORMAL) period_type=begin '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node gemini_cmd=cat')
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        gemini_stress_event.end_event()
        gemini_stress_event.event_timestamp = gemini_stress_event.end_timestamp = 1623696860.1202102
        self.assertEqual(gemini_stress_event.result, '')
        self.assertEqual(str(gemini_stress_event),
                         '(GeminiStressEvent Severity.NORMAL) period_type=end '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=1d3h46m40s: node=node gemini_cmd=cat')

        self.assertEqual(gemini_stress_event, pickle.loads(pickle.dumps(gemini_stress_event)))


class TestCassandraStressEvent(unittest.TestCase):
    def test_continuous_event_with_errors(self):
        begin_event_timestamp = 1623596860.1202102
        cs_event = CassandraStressEvent(node="node", stress_cmd="stress_cmd",
                                        log_file_name="log_file_name", publish_event=False)
        cs_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = cs_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         "(CassandraStressEvent Severity.NORMAL) period_type=begin "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node\nstress_cmd=stress_cmd")
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        cs_event.add_error(errors=["error1", "error2"])
        cs_event.severity = Severity.ERROR

        cs_event.end_event()
        end_event_timestamp = 1623596860.1202102
        cs_event.event_timestamp = cs_event.end_timestamp = end_event_timestamp
        self.assertEqual(str(cs_event),
                         "(CassandraStressEvent Severity.ERROR) period_type=end "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=0s: node=node\nstress_cmd=stress_cmd"
                         "\nerrors:\n\nerror1\nerror2")
        self.assertEqual(cs_event.event_timestamp, end_event_timestamp)
        self.assertEqual(cs_event.timestamp, end_event_timestamp)

        cs_event.add_error(["One more error"])
        cs_event.severity = Severity.CRITICAL
        cs_event.event_error()
        error_event_timestamp = 1623596860.1202102
        cs_event.event_timestamp = error_event_timestamp
        self.assertEqual(str(cs_event),
                         "(CassandraStressEvent Severity.CRITICAL) period_type=one-time "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=0s: node=node\n"
                         "stress_cmd=stress_cmd\nerrors:\n\nerror1\nerror2\nOne more error")
        self.assertEqual(cs_event.log_file_name, "log_file_name")
        self.assertEqual(cs_event.event_timestamp, error_event_timestamp)
        self.assertEqual(cs_event.timestamp, error_event_timestamp)
        self.assertEqual(cs_event, pickle.loads(pickle.dumps(cs_event)))

    def test_continuous_event_without_errors(self):
        begin_event_timestamp = 1623596860.1202102
        cs_event = CassandraStressEvent(node="node", stress_cmd="stress_cmd",
                                        log_file_name="log_file_name", publish_event=False)
        cs_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = cs_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         "(CassandraStressEvent Severity.NORMAL) period_type=begin "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node\nstress_cmd=stress_cmd")
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        end_event_timestamp = 1623596870.1202102
        cs_event.end_event()
        cs_event.event_timestamp = cs_event.end_timestamp = end_event_timestamp
        self.assertEqual(str(cs_event),
                         "(CassandraStressEvent Severity.NORMAL) period_type=end "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=10s: node=node\n"
                         "stress_cmd=stress_cmd")
        self.assertEqual(cs_event.event_timestamp, end_event_timestamp)
        self.assertEqual(cs_event.timestamp, end_event_timestamp)
        self.assertEqual(cs_event, pickle.loads(pickle.dumps(cs_event)))


class TestScyllaBenchEvent(unittest.TestCase):
    def test_continuous_event_with_error(self):
        begin_event_timestamp = 1623596860.1202102
        scylla_bench_event = ScyllaBenchEvent(node="node", stress_cmd="stress_cmd",
                                              log_file_name="log_file_name", publish_event=False)
        scylla_bench_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = scylla_bench_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         "(ScyllaBenchEvent Severity.NORMAL) period_type=begin "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node\nstress_cmd=stress_cmd")
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        try:
            raise ValueError('Stress command completed with bad status 1')
        except Exception as exc:  # noqa: BLE001
            scylla_bench_event.severity = Severity.ERROR
            scylla_bench_event.add_error([str(exc)])

        scylla_bench_event.end_event()
        scylla_bench_event.end_timestamp = scylla_bench_event.event_timestamp = 1623596960.1202102

        self.assertEqual(str(scylla_bench_event),
                         '(ScyllaBenchEvent Severity.ERROR) period_type=end '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=1m40s: '
                         'node=node\nstress_cmd=stress_cmd\nerrors:'
                         '\n\nStress command completed with bad status 1'
                         )
        scylla_bench_event.log_file_name = "log_file_name"
        self.assertEqual(scylla_bench_event, pickle.loads(pickle.dumps(scylla_bench_event)))

    def test_continuous_event_without_error(self):
        begin_event_timestamp = 1623596860.1202102
        scylla_bench_event = ScyllaBenchEvent(node="node", stress_cmd="stress_cmd",
                                              log_file_name="log_file_name", publish_event=False)
        scylla_bench_event.event_id = "14f35b64-2fcc-4b6e-a09d-4aeaf4faa543"
        begin_event = scylla_bench_event.begin_event()
        begin_event.event_timestamp = begin_event.begin_timestamp = begin_event_timestamp
        self.assertEqual(str(begin_event),
                         "(ScyllaBenchEvent Severity.NORMAL) period_type=begin "
                         "event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543: node=node\nstress_cmd=stress_cmd")
        self.assertEqual(begin_event.event_timestamp, begin_event_timestamp)
        self.assertEqual(begin_event.timestamp, begin_event_timestamp)
        self.assertEqual(begin_event, pickle.loads(pickle.dumps(begin_event)))

        scylla_bench_event.end_event()
        scylla_bench_event.event_timestamp = scylla_bench_event.end_timestamp = 1623596861.1202102
        self.assertEqual(str(scylla_bench_event),
                         '(ScyllaBenchEvent Severity.NORMAL) period_type=end '
                         'event_id=14f35b64-2fcc-4b6e-a09d-4aeaf4faa543 duration=1s: '
                         'node=node\nstress_cmd=stress_cmd')
        scylla_bench_event.log_file_name = "log_file_name"
        self.assertEqual(scylla_bench_event, pickle.loads(pickle.dumps(scylla_bench_event)))


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
        self.assertFalse(event.errors)
        event.event_id = "68067fe2-4c9e-421c-97b5-12db8d7ba71d"
        self.assertEqual(str(event),
                         "(YcsbStressEvent Severity.ERROR) period_type=not-set "
                         "event_id=68067fe2-4c9e-421c-97b5-12db8d7ba71d: type=error node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = YcsbStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        event.event_id = "225676a7-ddd1-4f4d-bae8-1cf5b35d0955"
        self.assertEqual(
            str(event),
            "(YcsbStressEvent Severity.CRITICAL) period_type=not-set "
            "event_id=225676a7-ddd1-4f4d-bae8-1cf5b35d0955:"
            " type=failure node=node1\nerrors:\n\ne1\ne2"
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
        self.assertFalse(event.errors)
        event.event_id = "aed3946d-33a8-4f68-b56c-1d09f71f5da9"
        self.assertEqual(str(event),
                         "(CDCReaderStressEvent Severity.NORMAL) period_type=not-set "
                         "event_id=aed3946d-33a8-4f68-b56c-1d09f71f5da9: type=start node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = CDCReaderStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        event.event_id = "3c5deb9e-7a67-49ee-9295-c7e986b015a9"
        self.assertEqual(
            str(event),
            "(CDCReaderStressEvent Severity.CRITICAL) period_type=not-set "
            "event_id=3c5deb9e-7a67-49ee-9295-c7e986b015a9: type=failure node=node1"
            "\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestNdBenchStressEvent(unittest.TestCase):
    def test_subevents(self):
        self.assertTrue(issubclass(NdBenchStressEvent.failure, NdBenchStressEvent))
        self.assertTrue(issubclass(NdBenchStressEvent.error, NdBenchStressEvent))
        self.assertFalse(hasattr(NdBenchStressEvent, "timeout"))
        self.assertTrue(issubclass(NdBenchStressEvent.start, NdBenchStressEvent))
        self.assertTrue(issubclass(NdBenchStressEvent.finish, NdBenchStressEvent))

    def test_without_errors(self):
        event = NdBenchStressEvent.error(node=[], stress_cmd="c-s", log_file_name="1.log")
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "[]")
        self.assertEqual(event.stress_cmd, "c-s")
        self.assertEqual(event.log_file_name, "1.log")
        self.assertFalse(event.errors)
        event.event_id = "a07b48fa-2706-465b-b139-698d35909cfa"
        self.assertEqual(str(event),
                         "(NdBenchStressEvent Severity.ERROR) period_type=not-set "
                         "event_id=a07b48fa-2706-465b-b139-698d35909cfa: type=error node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = NdBenchStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        event.event_id = "e45b347e-c395-4583-9f19-6e1fcdf31fab"
        self.assertEqual(
            str(event),
            "(NdBenchStressEvent Severity.CRITICAL) period_type=not-set "
            "event_id=e45b347e-c395-4583-9f19-6e1fcdf31fab: type=failure "
            "node=node1\nerrors:\n\ne1\ne2"
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
        self.assertFalse(event.errors)
        event.event_id = "1acd4202-3a38-4b0d-9464-62f4825ee148"
        self.assertEqual(str(event),
                         "(KclStressEvent Severity.ERROR) period_type=not-set "
                         "event_id=1acd4202-3a38-4b0d-9464-62f4825ee148: type=failure node=[]\nstress_cmd=c-s")
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_with_errors(self):
        event = KclStressEvent.failure(node="node1", errors=["e1", "e2"])
        self.assertEqual(event.severity, Severity.ERROR)
        self.assertEqual(event.node, "node1")
        self.assertIsNone(event.stress_cmd)
        self.assertIsNone(event.log_file_name)
        self.assertEqual(event.errors, ["e1", "e2"])
        event.event_id = "d169ca02-c119-49f2-9eb7-23f152098cb7"
        self.assertEqual(
            str(event),
            "(KclStressEvent Severity.ERROR) period_type=not-set event_id=d169ca02-c119-49f2-9eb7-23f152098cb7: "
            "type=failure node=node1\nerrors:\n\ne1\ne2"
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))


class TestCassandraStressLogEvent(unittest.TestCase):
    @staticmethod
    def get_event(line, expected_type, expected_severity):
        for pattern, event in chain(CS_NORMAL_EVENTS_PATTERNS, CS_ERROR_EVENTS_PATTERNS):
            if pattern.search(line):
                event.add_info(node='self.node', line=line, line_number=1).dont_publish()
                assert event.type == expected_type, \
                    f'Unexpected event.type {event.type}. Expected "{expected_type}"'
                assert event.severity == expected_severity, \
                    f'Unexpected event.severity {event.severity}. Expected "{expected_severity}"'
                return

        raise ValueError(f"Event is not recognized in the line {line}. Expected exception type is {expected_type},"
                         f"expected severity is {expected_severity}")

    def test_known_cs_errors(self):
        self.assertTrue(issubclass(CassandraStressLogEvent.IOException, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.ConsistencyError, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.OperationOnKey, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.TooManyHintsInFlight, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.SchemaDisagreement, CassandraStressLogEvent))

    def test_known_cs_normal(self):
        self.assertTrue(issubclass(CassandraStressLogEvent.ShardAwareDriver, CassandraStressLogEvent))
        self.assertTrue(issubclass(CassandraStressLogEvent.RackAwarePolicy, CassandraStressLogEvent))

    def test_cs_all_events_list(self):
        """ Check that all events are listed in CS_ERROR_EVENTS and CS_NORMAL_EVENTS
            since python3.14 also __annotate_func__ need to be excluded from the dir() output
        """

        assert set(dir(CassandraStressLogEvent)) - set(dir(LogEvent)) - {'__annotate_func__'} == \
            {ev.type for ev in chain(CS_NORMAL_EVENTS, CS_ERROR_EVENTS)}

    def test_cs_hint_in_flight_error(self):
        self.get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                       '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many '
                       'in flight hints: 10490670',
                       expected_type='TooManyHintsInFlight',
                       expected_severity=Severity.ERROR)

        self.get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                       '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) was overloaded: Too many '
                       'hints in flight: 10490670',
                       expected_type='TooManyHintsInFlight',
                       expected_severity=Severity.ERROR)

        self.get_event(line='java.io.IOException: Operation x10 on key(s) [334f37384f4d32303430]: Error executing: '
                       '(OverloadedException): Queried host (10.0.3.167/10.0.3.167:9042) ',
                       expected_type='OperationOnKey',
                       expected_severity=Severity.CRITICAL)

        self.get_event(line='java.io.IOException: Connection reset by peer',
                       expected_type='IOException',
                       expected_severity=Severity.ERROR)

        self.get_event(line='03:56:37.572 [cluster1-nio-worker-4] DEBUG com.datastax.driver.core.Connection - Connection['
                       '/10.0.3.121:9042-11, inFlight=5, closed=false] Response received on stream 17856 but no '
                       'handler set anymore (either the request has timed out or it was closed due to another error). '
                       'Received message is ERROR UNAVAILABLE: Cannot achieve consistency level for cl QUORUM. '
                       'Requires 2, alive 1',
                       expected_type='ConsistencyError',
                       expected_severity=Severity.ERROR)

    def test_cs_normal_shared_awarnes_event(self):
        self.get_event(line='com.datastax.driver.core.Cluster - ===== Using optimized driver!!! =====',
                       expected_type='ShardAwareDriver',
                       expected_severity=Severity.NORMAL)


class TestScyllaBenchLogEvent(unittest.TestCase):
    @staticmethod
    def get_event(line, expected_type, expected_severity):
        for pattern, event in chain(SCYLLA_BENCH_NORMAL_EVENTS_PATTERNS, SCYLLA_BENCH_ERROR_EVENTS_PATTERNS):
            if pattern.search(line):
                event.add_info(node='self.node', line=line, line_number=1).dont_publish()
                assert event.type == expected_type, \
                    f'Unexpected event.type {event.type}. Expected "{expected_type}"'
                assert event.severity == expected_severity, \
                    f'Unexpected event.severity {event.severity}. Expected "{expected_severity}"'
                return

        raise ValueError(f"Event is not recognized in the line {line}. Expected exception type is {expected_type},"
                         f"expected severity is {expected_severity}")

    def test_scylla_bench_normal_rack_awarness_event(self):
        self.get_event(line="[2025-11-01 04:15:03.653] Using provided rack name 'RACK0' for RackAwareRoundRobinPolicy",
                       expected_type='RackAwarePolicy',
                       expected_severity=Severity.NORMAL)

    def test_known_scylla_bench_errors(self):
        self.assertTrue(issubclass(ScyllaBenchLogEvent.ConsistencyError, ScyllaBenchLogEvent))
        self.assertTrue(issubclass(ScyllaBenchLogEvent.DataValidationError, ScyllaBenchLogEvent))
        self.assertTrue(issubclass(ScyllaBenchLogEvent.ParseDistributionError, ScyllaBenchLogEvent))
        self.assertTrue(issubclass(ScyllaBenchLogEvent.RackAwarePolicy, ScyllaBenchLogEvent))

    def test_scylla_bench_error_events_list(self):
        """
        Check that all events are listed in SCYLLA_BENCH_ERROR_EVENTS
        since python3.14 also __annotate_func__ need to be excluded from the dir() output
        """
        assert set(dir(ScyllaBenchLogEvent)) - set(dir(LogEvent)) - \
            {'__annotate_func__'} == {ev.type for ev in chain(SCYLLA_BENCH_NORMAL_EVENTS, SCYLLA_BENCH_ERROR_EVENTS)}


class TestGeminiLogEvent(unittest.TestCase):
    def test_json_line(self):
        event = GeminiStressLogEvent.GeminiEvent()
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
        event.event_id = "3eaf9cb2-b54b-43f5-8472-d0fbf5c25f72"
        self.assertEqual(
            str(event),
            '(GeminiStressLogEvent Severity.NORMAL) period_type=one-time event_id=3eaf9cb2-b54b-43f5-8472-d0fbf5c25f72:'
            ' type=GeminiEvent line_number=1 node=node1\nline=Test run stopped. Exiting. (N="pump")'
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

    def test_non_json_line(self):
        event = GeminiStressLogEvent.GeminiEvent()
        event_timestamp = event.event_timestamp
        event.add_info(node="node1", line="1961-04-12T06:07:00+00:00 Poyekhalee!", line_number=1)
        self.assertEqual(event.event_timestamp, event_timestamp)
        self.assertEqual(event.timestamp, event_timestamp)
        self.assertEqual(event.severity, Severity.CRITICAL)
        self.assertIsNone(event.node)
        self.assertIsNone(event.line)
        self.assertEqual(event.line_number, 0)
        self.assertIsNone(event.backtrace)
        self.assertIsNone(event.raw_backtrace)
        self.assertFalse(event._ready_to_publish)
        event.event_id = "3ce0cdeb-0866-40ce-9a20-25ea3ae08be2"
        self.assertEqual(
            str(event),
            "(GeminiStressLogEvent Severity.CRITICAL) period_type=one-time "
            "event_id=3ce0cdeb-0866-40ce-9a20-25ea3ae08be2: type=GeminiEvent line_number=0 node=None",
        )
        self.assertEqual(event, pickle.loads(pickle.dumps(event)))

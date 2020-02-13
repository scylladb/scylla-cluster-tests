from __future__ import absolute_import
from __future__ import print_function
import time
import traceback
import unittest
import tempfile
import logging
import datetime
from pathlib import Path
from multiprocessing import Event

from sdcm.tester import ClusterTester
from sdcm.utils.common import timeout
from sdcm.prometheus import start_metrics_server
from sdcm.sct_events import (start_events_device, stop_events_device, TestKiller, InfoEvent, CassandraStressEvent,
                             CoreDumpEvent, DatabaseLogEvent, DisruptionEvent, DbEventsFilter, SpotTerminationEvent,
                             KillTestEvent, Severity, ThreadFailedEvent, TestFrameworkEvent, get_logger_event_summary,
                             ScyllaBenchEvent, TestResultEvent, PrometheusAlertManagerEvent)

LOGGER = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class BaseEventsTest(unittest.TestCase):
    @classmethod
    def get_event_log_file(cls, name):
        log_file = Path(cls.temp_dir, 'events_log', name)
        data = ""
        if log_file.exists():
            with open(log_file, 'r') as file:
                data = file.read()
        return data

    @classmethod
    def get_event_logs(cls):
        return cls.get_event_log_file('events.log')

    @classmethod
    @timeout(timeout=20, sleep_time=0.05)
    def wait_for_event_log_change(cls, file_name, log_content_before):
        log_content_after = cls.get_event_log_file(file_name)
        if log_content_before == log_content_after:
            raise AssertionError("log file wasn't update with new events")
        return log_content_after

    @classmethod
    @timeout(timeout=10, sleep_time=0.05)
    def wait_for_event_summary(cls):
        return get_logger_event_summary()

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

        start_metrics_server()
        start_events_device(cls.temp_dir, timeout=5)

        cls.killed = Event()

        cls.test_killer = TestKiller(timeout_before_kill=5,
                                     test_callback=cls.killed.set)
        cls.test_killer.start()
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        for process in [cls.test_killer]:
            process.terminate()
            process.join()
        stop_events_device()


class SctEventsTests(BaseEventsTest):  # pylint: disable=too-many-public-methods

    def test_event_info(self):  # pylint: disable=no-self-use
        InfoEvent(message='jkgkjgl')

    def test_cassandra_stress(self):  # pylint: disable=no-self-use
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf"))
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf",
                                 log_file_name="/filename/"))

    def test_scylla_bench(self):  # pylint: disable=no-self-use
        str(ScyllaBenchEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf"))
        str(ScyllaBenchEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf",
                             log_file_name="/filename/"))

    def test_coredump_event(self):  # pylint: disable=no-self-use
        str(CoreDumpEvent(corefile_url='http://', backtrace="asfasdfsdf",
                          node="node xy",
                          download_instructions="gsutil cp gs://upload.scylladb.com/core.scylla-jmx.996.d173729352e34c76aaf8db3342153c3e.3968.1566979933000/core.scylla-jmx.996.d173729352e34c76aaf8db3342153c3e.3968.1566979933000000 .",
                          timestamp=time.mktime(datetime.datetime.strptime(
                              "Tue 2020-01-14 10:40:25 UTC", "%a %Y-%m-%d %H:%M:%S UTC").timetuple())
                          ))

    def test_thread_failed_event(self):  # pylint: disable=no-self-use
        try:
            1 / 0
        except ZeroDivisionError:
            _full_traceback = traceback.format_exc()

        str(ThreadFailedEvent(message='thread failed', traceback=_full_traceback))

    def test_scylla_log_event(self):  # pylint: disable=no-self-use
        str(DatabaseLogEvent(type="A", regex="B"))

        event = DatabaseLogEvent(type="A", regex="B")
        event.add_info("node", line='[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 2000 ms',
                       line_number=213)
        event.add_backtrace_info("0x002342340\n0x12423434")

        str(event)

    def test_disruption_event(self):  # pylint: disable=no-self-use
        try:
            1 / 0
        except ZeroDivisionError:
            _full_traceback = traceback.format_exc()

        str(DisruptionEvent(type='end', name="ChaosMonkeyLimited", status=False, error=str(Exception("long one")),
                            full_traceback=_full_traceback, duration=20, start=1, end=2, node='test'))

        str(DisruptionEvent(type='end', name="ChaosMonkeyLimited", status=True, duration=20, start=1, end=2,
                            node='test'))

        print(str(DisruptionEvent(type='start', name="ChaosMonkeyLimited", status=True, node='test')))

    def test_test_framework_event(self):  # pylint: disable=no-self-use
        try:
            1 / 0
        except ZeroDivisionError:
            _full_traceback = traceback.format_exc()
        event = TestFrameworkEvent(source="Tester", source_method="setUp", exception=_full_traceback)
        print(str(event))
        event.publish()

    def test_prometheus_alert_manager_event(self):  # pylint: disable=no-self-use
        alert = dict(alert_name='NoCql', start='2020-02-23T21:30:09.591Z', end='2020-02-23T21:33:09.591Z',
                     description='10.0.13.57 has denied cql connection for more than 30 seconds.', updated='2020-02-23T21:30:09.722Z',
                     state='active', fingerprint='51e06cff6ae71d83', labels={'alertname': 'NoCql',
                                                                             'cluster': 'alternator-3h-alternat-db-cluster-487c533b',
                                                                             'host': '10.0.13.57',
                                                                             'instance': '10.0.13.57',
                                                                             'job': 'scylla_manager',
                                                                             'monitor': 'scylla-monitor',
                                                                             'severity': '2',
                                                                             'sct_severity': 'CRITICAL'})
        event = PrometheusAlertManagerEvent(raw_alert=alert)
        print(event)
        assert event.severity == Severity.CRITICAL

        alert['labels']['sct_severity'] = "NOT_EXIST"
        event = PrometheusAlertManagerEvent(raw_alert=alert)
        assert event.severity == Severity.WARNING

    def test_get_logger_event_summary(self):
        log_content_before = self.get_event_log_file('events.log')
        event = TestFrameworkEvent(severity=Severity.ERROR, source="Tester", source_method="setUp")
        event.publish()
        log_content_before = self.wait_for_event_log_change('events.log', log_content_before)
        log_summary_before = self.get_event_log_file('summary.log')
        event = TestFrameworkEvent(severity=Severity.WARNING, source="Tester", source_method="setUp")
        event.publish()
        self.wait_for_event_log_change('events.log', log_content_before)
        self.wait_for_event_log_change('summary.log', log_summary_before)
        summary = self.wait_for_event_summary()
        self.assertIn('Severity.ERROR', summary)
        self.assertGreaterEqual(summary['Severity.ERROR'], 1)

    def test_filter(self):
        log_content_before = self.get_event_log_file('events.log')

        enospc_line = "[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)"
        enospc_line_2 = "2019-10-29T12:19:49+00:00  ip-172-30-0-184 !WARNING | scylla: [shard 2] storage_service - " \
                        "Commitlog error: std::filesystem::__cxx11::filesystem_error (error system:28, filesystem error: open failed: No space left on device [/var/lib/scylla/hints/2/172.30.0.116/HintsLog-1-36028797019122576.log])"
        with DbEventsFilter(type='NO_SPACE_ERROR'), \
                DbEventsFilter(type='BACKTRACE', line='No space left on device'), \
                DbEventsFilter(type='DATABASE_ERROR', line='No space left on device'), \
                DbEventsFilter(type='FILESYSTEM_ERROR', line='No space left on device'):
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line=enospc_line)
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line=enospc_line)

            DatabaseLogEvent(type="FILESYSTEM_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                      line=enospc_line_2)

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line=enospc_line)
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line=enospc_line)

        try:
            self.wait_for_event_log_change('events.log', log_content_before)
            self.fail("Log file should not be changed")  # Should not reach this point
        except AssertionError:
            pass

    def test_filter_repair(self):

        log_content_before = self.get_event_log_file('events.log')

        failed_repaired_line = '2019-07-28T10:53:29+00:00  ip-10-0-167-91 !INFO    | scylla.bin: [shard 0] repair - Got error in row level repair: std::runtime_error (repair id 1 is aborted on shard 0)'

        with DbEventsFilter(type='DATABASE_ERROR', line="repair's stream failed: streaming::stream_exception"), \
                DbEventsFilter(type='RUNTIME_ERROR', line='Can not find stream_manager'), \
                DbEventsFilter(type='RUNTIME_ERROR', line='is aborted'):
            DatabaseLogEvent(type="RUNTIME_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                   line=failed_repaired_line)
            DatabaseLogEvent(type="RUNTIME_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                   line=failed_repaired_line)
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="B", line_number=22,
                                                                                    line="not filtered")

        log_content_after = self.wait_for_event_log_change('events.log', log_content_before)
        self.assertIn("not filtered", log_content_after)
        self.assertNotIn("repair id 1", log_content_after)

    def test_filter_upgrade(self):
        log_content_before = self.get_event_log_file('events.log')

        known_failure_line = '!ERR     | scylla:  [shard 3] storage_proxy - Exception when communicating with 10.142.0.56: std::runtime_error (Failed to load schema version b40e405f-462c-38f2-a90c-6f130ddbf6f3)'
        with DbEventsFilter(type='RUNTIME_ERROR', line='Failed to load schema'):
            DatabaseLogEvent(type="RUNTIME_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                   line=known_failure_line)
            DatabaseLogEvent(type="RUNTIME_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                   line=known_failure_line)
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="B", line_number=22,
                                                                                    line="not filtered")

        log_content_after = self.wait_for_event_log_change('events.log', log_content_before)
        self.assertIn("not filtered", log_content_after)
        self.assertNotIn("Exception when communicating", log_content_after)

    def test_filter_by_node(self):
        log_content_before = self.get_event_log_file('events.log')
        with DbEventsFilter(type="NO_SPACE_ERROR", node="A"):
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="this is filtered")

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="B", line_number=22,
                                                                                    line="not filtered")

        log_content_after = self.wait_for_event_log_change('events.log', log_content_before)
        self.assertIn("not filtered", log_content_after)
        self.assertNotIn("this is filtered", log_content_after)

    def test_filter_expiration(self):
        log_content_before = self.get_event_log_file('events.log')
        line_prefix = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        with DbEventsFilter(type="NO_SPACE_ERROR", node="A"):
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="A").add_info_and_publish(node="A", line_number=22,
                                                                                    line=line_prefix + " this is filtered")

        time.sleep(5)
        DatabaseLogEvent(type="NO_SPACE_ERROR", regex="A").add_info_and_publish(node="A", line_number=22,
                                                                                line=line_prefix + " this is filtered")

        time.sleep(5)
        line_prefix = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S+00:00")
        print(line_prefix)
        DatabaseLogEvent(type="NO_SPACE_ERROR", regex="A").add_info_and_publish(node="A", line_number=22,
                                                                                line=line_prefix + " not filtered")
        log_content_after = self.wait_for_event_log_change('events.log', log_content_before)
        self.assertIn("not filtered", log_content_after)
        self.assertNotIn("this is filtered", log_content_after)

    def test_stall_severity(self):
        event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
        event.add_info_and_publish(node="A", line_number=22,
                                   line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 2000 ms")
        self.assertTrue(event.severity == Severity.NORMAL)

        event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
        event.add_info_and_publish(node="A", line_number=22,
                                   line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 5000 ms")
        self.assertTrue(event.severity == Severity.CRITICAL)

    def test_spot_termination(self):  # pylint: disable=no-self-use
        str(SpotTerminationEvent(node='test', message='{"action": "terminate", "time": "2017-09-18T08:22:00Z"}'))

    def test_default_filters(self):
        log_content_before = self.get_event_log_file('events.log')

        DatabaseLogEvent(type="BACKTRACE",
                         regex="backtrace").add_info_and_publish(node="A",
                                                                 line_number=22,
                                                                 line="Jul 01 03:37:31 ip-10-0-127-151.eu-west-1. \
                                                                       compute.internal scylla[6026]:\
                                                                       Rate-limit: supressed 4294967292 \
                                                                       backtraces on shard 5")

        DatabaseLogEvent(type="BACKTRACE",
                         regex="backtrace").add_info_and_publish(node="A",
                                                                 line_number=22,
                                                                 line="other back trace that shouldn't be filtered")

        log_content_after = self.wait_for_event_log_change('events.log', log_content_before)
        self.assertIn('other back trace', log_content_after)
        self.assertNotIn('supressed', log_content_after)

    def test_failed_stall_during_filter(self):
        with DbEventsFilter(type="NO_SPACE_ERROR"), \
                DbEventsFilter(type='BACKTRACE', line='No space left on device'):
            event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
            event.add_info_and_publish(node="A", line_number=22,
                                       line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 20")
            logging.info(event.severity)
            self.assertTrue(event.severity == Severity.CRITICAL)

    @unittest.skip("for manual use only")
    def test_measure_speed_of_events_processing(self):  # pylint: disable=no-self-use
        non_guaranteed_delivery = []
        for _ in range(5):
            start_time = time.time()
            for _ in range(1000):
                evt = DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B")
                evt.add_info(node="B", line_number=22, line="not filtered")
                evt.publish(guaranteed=False)
            non_guaranteed_delivery.append(time.time() - start_time)
        guaranteed_delivery = []
        for _ in range(5):
            start_time = time.time()
            for _ in range(1000):
                evt = DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B")
                evt.add_info(node="B", line_number=22, line="not filtered")
                evt.publish(guaranteed=True)
            guaranteed_delivery.append(time.time() - start_time)
        non_guaranteed_avg = sum(non_guaranteed_delivery) / len(non_guaranteed_delivery)
        guaranteed_avg = sum(guaranteed_delivery) / len(guaranteed_delivery)
        print(
            f"Rate of publishing 1000 events in guaranteed mode vs non guaranteed mode is {guaranteed_avg}/{non_guaranteed_avg}={(guaranteed_avg/non_guaranteed_avg) * 100}")

    @unittest.skip("this test need some more work")
    def test_kill_test_event(self):
        self.assertTrue(self.test_killer.is_alive())
        str(KillTestEvent(reason="Don't like this test"))

        LOGGER.info('sent kill')
        self.assertTrue(self.killed.wait(20), "kill wasn't sent")


class TesterFailure(BaseEventsTest):

    def test_failure_found(self):
        self.assertTrue(1 == 0)

    def tearDown(self) -> None:
        test_error, test_failure = ClusterTester.get_test_failures(self)
        tre = TestResultEvent(test_name=self.id(), error=test_error, failure=test_failure)
        assert tre.severity == Severity.CRITICAL
        tre.publish(guaranteed=True)
        print(str(tre))
        assert test_failure
        assert not test_error
        events_log = self.get_event_logs()
        assert "ERROR" not in events_log
        self._outcome.errors = []  # we don't want to fail test


class TesterError(BaseEventsTest):

    def test_error_found(self):  # pylint: disable=no-self-use
        raise Exception("error in during test")

    def tearDown(self) -> None:
        test_error, test_failure = ClusterTester.get_test_failures(self)
        tre = TestResultEvent(test_name=self.id(), error=test_error, failure=test_failure)
        assert tre.severity == Severity.CRITICAL
        tre.publish(guaranteed=True)
        print(str(tre))
        assert not test_failure
        assert test_error
        events_log = self.get_event_logs()
        assert "FAILURE" not in events_log
        self._outcome.errors = []  # we don't want to fail test


class TesterNoErrors(BaseEventsTest):

    def test_no_errors_found(self):  # pylint: disable=no-self-use
        print("happy test")

    def tearDown(self) -> None:
        test_error, test_failure = ClusterTester.get_test_failures(self)
        tre = TestResultEvent(test_name=self.id(), error=test_error, failure=test_failure)
        assert tre.severity == Severity.NORMAL
        tre.publish(guaranteed=True)
        print(str(tre))
        assert not test_failure
        assert not test_error
        events_log = self.get_event_logs()
        assert "FAILURE" not in events_log and "ERROR" not in events_log


if __name__ == "__main__":
    unittest.main(verbosity=2)

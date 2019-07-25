import os
import time
import traceback
import unittest
import tempfile
import logging

from sdcm.prometheus import start_metrics_server

from sdcm.sct_events import (start_events_device, stop_events_device, GrafanaAnnotator, Event, TestKiller, PrometheusDumper, EventsFileLogger,
                             InfoEvent, CassandraStressEvent, CoreDumpEvent, DatabaseLogEvent, DisruptionEvent, DbEventsFilter, SpotTerminationEvent,
                             KillTestEvent, Severity)

LOGGER = logging.getLogger(__name__)

logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class SctEventsTests(unittest.TestCase):

    def get_event_logs(self):
        log_file = os.path.join(self.temp_dir, 'events_log', 'events.log')

        data = ""
        if os.path.exists(log_file):
            data = open(log_file, 'r').read()
        return data

    def wait_for_event_log_change(self, log_content_before):
        for _ in range(10):
            log_content_after = self.get_event_logs()
            if log_content_before == log_content_after:
                logging.debug("logs haven't changed yet")
                time.sleep(0.05)
            else:
                break
        else:
            raise AssertionError("log file wasn't update with new events")

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

        start_metrics_server()
        start_events_device(cls.temp_dir, timeout=5)
        time.sleep(5)

        cls.killed = Event()

        def callback(x):
            cls.killed.set()

        cls.test_killer = TestKiller(timeout_before_kill=5, test_callback=callback)
        cls.test_killer.start()
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        for process in [cls.test_killer]:
            process.terminate()
            process.join()
        stop_events_device()

    def test_event_info(self):
        InfoEvent(message='jkgkjgl')

    def test_cassandra_stress(self):
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf"))
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf",
                                 log_file_name="/filename/"))

    def test_coredump_event(self):
        str(CoreDumpEvent(corefile_urls=['http://', "fsdfs", "sdsfgfg"], backtrace="asfasdfsdf",
                          download_instructions="", node="node xy"))

    def test_scylla_log_event(self):
        str(DatabaseLogEvent(type="A", regex="B"))

        e = DatabaseLogEvent(type="A", regex="B")
        e.add_info("node", line='[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 2000 ms',
                   line_number=213)
        e.add_backtrace_info("0x002342340\n0x12423434")

        str(e)

    def test_disruption_event(self):
        try:
            1 / 0
        except ZeroDivisionError:
            _full_traceback = traceback.format_exc()

        str(DisruptionEvent(type='end', name="ChaosMonkeyLimited", status=False, error=str(Exception("long one")),
                            full_traceback=_full_traceback, duration=20, start=1, end=2, node='test'))

        str(DisruptionEvent(type='end', name="ChaosMonkeyLimited", status=True, duration=20, start=1, end=2,
                            node='test'))

        print str(DisruptionEvent(type='start', name="ChaosMonkeyLimited", status=True, node='test'))

    def test_filter(self):
        with DbEventsFilter(type="NO_SPACE_ERROR"), DbEventsFilter(type='BACKTRACE', line='No space left on device'):
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")
            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  [shard 8] commitlog - Exception in segment reservation: storage_io_error (Storage I/O error: 28: No space left on device)")

    def test_filter_repair(self):

        log_content_before = self.get_event_logs()

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

        self.wait_for_event_log_change(log_content_before)
        log_content_after = self.get_event_logs()
        self.assertIn("not filtered", log_content_after)
        self.assertNotIn("repair id 1", log_content_after)

    def test_filter_by_node(self):

        log_content_before = self.get_event_logs()

        with DbEventsFilter(type="NO_SPACE_ERROR", node="A"):

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="A", line_number=22,
                                                                                    line="this is filtered")

            DatabaseLogEvent(type="NO_SPACE_ERROR", regex="B").add_info_and_publish(node="B", line_number=22,
                                                                                    line="not filtered")

        self.wait_for_event_log_change(log_content_before)

        log_content_after = self.get_event_logs()
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

    def test_spot_termination(self):
        str(SpotTerminationEvent(node='test', aws_message='{"action": "terminate", "time": "2017-09-18T08:22:00Z"}'))

    def test_default_filters(self):
        log_content_before = self.get_event_logs()

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

        self.wait_for_event_log_change(log_content_before)

        log_content_after = self.get_event_logs()
        self.assertIn('other back trace', log_content_after)
        self.assertNotIn('supressed', log_content_after)

    def test_failed_stall_during_filter(self):

        with DbEventsFilter(type="NO_SPACE_ERROR"), DbEventsFilter(type='BACKTRACE', line='No space left on device'):

            event = DatabaseLogEvent(type="REACTOR_STALLED", regex="B")
            event.add_info_and_publish(node="A", line_number=22,
                                       line="[99.80.124.204] [stdout] Mar 31 09:08:10 warning|  reactor stall 20")
            logging.info(event.severity)
            self.assertTrue(event.severity == Severity.CRITICAL)

    @unittest.skip("this test need some more work")
    def test_kill_test_event(self):
        self.assertTrue(self.test_killer.is_alive())
        str(KillTestEvent(reason="Don't like this test"))

        LOGGER.info('sent kill')
        self.assertTrue(self.killed.wait(20), "kill wasn't sent")


if __name__ == "__main__":
    unittest.main(verbosity=2)

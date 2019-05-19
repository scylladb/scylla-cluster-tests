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

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

        start_metrics_server()
        start_events_device(cls.temp_dir, timeout=5)
        time.sleep(5)

        cls.killed = Event()

        def callback(x):
            cls.killed.set()

        cls.grafana_annotator = GrafanaAnnotator()
        cls.grafana_annotator.start()
        cls.test_killer = TestKiller(timeout_before_kill=5, test_callback=callback)
        cls.test_killer.start()
        cls.prometheus_dumper = PrometheusDumper()
        cls.prometheus_dumper.start()
        cls.event_logger = EventsFileLogger()
        cls.event_logger.start(log_dir=cls.temp_dir)
        time.sleep(5)

    @classmethod
    def tearDownClass(cls):
        cls.grafana_annotator.set_grafana_url(grafana_base_url='')
        for t in [cls.event_logger, cls.prometheus_dumper, cls.grafana_annotator, cls.test_killer]:
            t.terminate()
            t.join()
        stop_events_device()

    def test_event_info(self):
        InfoEvent(message='jkgkjgl')

    def test_cassandra_stress(self):
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf"))
        str(CassandraStressEvent(type='start', node="node xy", stress_cmd="adfadsfsdfsdfsdf",
                                 log_file_name="/filename/"))

    def test_coredump_event(self):
        str(CoreDumpEvent(corefile_urls=['http://', "fsdfs", "sdsfgfg"], backtrace="asfasdfsdf",
                          download_instructions=""))

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

    @unittest.skip("this test need some more work")
    def test_kill_test_event(self):
        self.assertTrue(self.test_killer.is_alive())
        str(KillTestEvent(reason="Don't like this test"))

        LOGGER.info('sent kill')
        self.assertTrue(self.killed.wait(20), "kill wasn't sent")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)
    unittest.main(verbosity=2)

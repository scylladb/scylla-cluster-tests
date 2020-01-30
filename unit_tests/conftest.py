import logging
import tempfile
from pathlib import Path
import pytest

from sdcm.prometheus import start_metrics_server
from sdcm.ndbench_thread import RemoteDocker
from sdcm import wait
from sdcm.utils.common import timeout
from sdcm.sct_events import (start_events_device, stop_events_device)

from unit_tests.dummy_remote import LocalNode


class EventsLogUtils:
    """
    those test function are borrowed/copied from SctEventsTests,
    once SctEventsTests will changed to be pytest it would be able to use this as part of `events` fixture
    """

    def __init__(self, temp_dir):
        self.temp_dir = temp_dir

    def get_event_log_file(self, name):
        log_file = Path(self.temp_dir, 'events_log', name)
        data = ""
        if log_file.exists():
            with open(log_file, 'r') as file:
                data = file.read()
        return data

    def get_event_logs(self):
        return self.get_event_log_file('events.log')

    @timeout(timeout=20, sleep_time=0.05)
    def wait_for_event_log_change(self, file_name, log_content_before):
        log_content_after = self.get_event_log_file(file_name)
        if log_content_before == log_content_after:
            raise AssertionError("log file wasn't update with new events")
        return log_content_after


@pytest.fixture(scope='module')
def events():
    temp_dir = tempfile.mkdtemp()
    start_events_device(temp_dir)
    yield EventsLogUtils(temp_dir=temp_dir)

    stop_events_device()


@pytest.fixture(scope='session')
def prom_address():
    yield start_metrics_server()


@pytest.fixture(scope='module')
def docker_scylla():
    scylla = RemoteDocker(LocalNode(), image_name="scylladb/scylla:3.2.0",
                          command_line="--alternator-port 8000", extra_docker_opts='-p 8000 --cpus="1"')

    def db_up():
        try:
            # check that port is taken
            result_netstat = scylla.run("nodetool status | grep '^UN '",
                                        verbose=False, ignore_status=True)
            return result_netstat.exit_status == 0
        except Exception as details:  # pylint: disable=broad-except
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    wait.wait_for(func=db_up, step=5, text='Waiting for DB services to be up', timeout=30, throw_exc=True)

    return scylla

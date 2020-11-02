import os
import logging
import tempfile
from pathlib import Path

import pytest

from sdcm.prometheus import start_metrics_server
from sdcm.utils.docker_utils import RemoteDocker
from sdcm import wait
from sdcm.utils.decorators import timeout
from sdcm.sct_events import (start_events_device, stop_events_device)
from sdcm.cluster import BaseNode
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


@pytest.fixture(scope='session')
def events():
    temp_dir = tempfile.mkdtemp()
    start_events_device(temp_dir)
    yield EventsLogUtils(temp_dir=temp_dir)

    stop_events_device()


@pytest.fixture(scope='session')
def prom_address():
    yield start_metrics_server()


@pytest.fixture(scope='session')
def docker_scylla():
    # make sure the path to the file is base on the host path, and not as the docker internal path i.e. /sct/
    # since we are going to mount it in a DinD (docker-inside-docker) setup
    base_dir = os.environ.get("_SCT_BASE_DIR", None)
    entryfile_path = Path(base_dir) if base_dir else Path(__file__).parent.parent
    entryfile_path = entryfile_path.joinpath('./docker/scylla-sct/entry.sh')

    alternator_flags = "--alternator-port 8000 --alternator-write-isolation=always"
    docker_version = "scylladb/scylla-nightly:666.development-0.20201015.8068272b466"
    scylla = RemoteDocker(LocalNode(), image_name=docker_version,
                          command_line=f"--smp 1 --experimental 1 {alternator_flags}",
                          extra_docker_opts=f'-p 8000 -p 9042 --cpus="1" -v {entryfile_path}:/entry.sh --entrypoint'
                          f' /entry.sh')

    def db_up():
        try:
            return scylla.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server")
        except Exception as details:  # pylint: disable=broad-except
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    def db_alternator_up():
        try:
            return scylla.is_port_used(port=8000, service_name="scylla-server")
        except Exception as details:  # pylint: disable=broad-except
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    wait.wait_for(func=db_up, step=1, text='Waiting for DB services to be up', timeout=30, throw_exc=True)
    wait.wait_for(func=db_alternator_up, step=1, text='Waiting for DB services to be up alternator)',
                  timeout=30, throw_exc=True)

    yield scylla

    scylla.kill()

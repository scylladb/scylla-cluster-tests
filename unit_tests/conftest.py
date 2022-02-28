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

import os
import logging
import collections
from pathlib import Path

import pytest

from sdcm import wait
from sdcm.cluster import BaseNode
from sdcm.prometheus import start_metrics_server
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.utils.docker_remote import RemoteDocker


from unit_tests.dummy_remote import LocalNode, LocalScyllaClusterDummy

from unit_tests.lib.events_utils import EventsUtilsMixin
from unit_tests.lib.fake_remoter import FakeRemoter


@pytest.fixture(scope='session')
def events():
    mixing = EventsUtilsMixin()
    mixing.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
    yield mixing

    mixing.teardown_events_processes()


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
    cluster = LocalScyllaClusterDummy()
    scylla = RemoteDocker(LocalNode("scylla", cluster), image_name=docker_version,
                          command_line=f"--smp 1 --experimental 1 {alternator_flags}",
                          extra_docker_opts=f'-p 8000 -p 9042 --cpus="1" -v {entryfile_path}:/entry.sh --entrypoint'
                          f' /entry.sh')

    DummyRemoter = collections.namedtuple('DummyRemoter', ['run', 'sudo'])
    scylla.remoter = DummyRemoter(run=scylla.run, sudo=scylla.run)

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


@pytest.fixture
def fake_remoter():
    RemoteCmdRunnerBase.set_default_remoter_class(FakeRemoter)
    return FakeRemoter

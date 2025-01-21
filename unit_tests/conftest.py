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

from sdcm import wait, sct_config
from sdcm.localhost import LocalHost
from sdcm.cluster import BaseNode
from sdcm.prometheus import start_metrics_server
from sdcm.provision import provisioner_factory
from sdcm.provision.helpers.certificate import (
    create_ca, create_certificate, SCYLLA_SSL_CONF_DIR, CLIENT_FACING_CERTFILE,
    CLIENT_FACING_KEYFILE, CA_CERT_FILE, CA_KEY_FILE, CLIENT_CERT_FILE, CLIENT_KEY_FILE)
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry
from sdcm.sct_provision import region_definition_builder
from sdcm.utils.docker_remote import RemoteDocker

from unit_tests.dummy_remote import LocalNode, LocalScyllaClusterDummy

from unit_tests.lib.events_utils import EventsUtilsMixin
from unit_tests.lib.fake_provisioner import FakeProvisioner
from unit_tests.lib.fake_region_definition_builder import FakeDefinitionBuilder
from unit_tests.lib.fake_remoter import FakeRemoter
from unit_tests.lib.alternator_utils import ALTERNATOR_PORT


@pytest.fixture(scope='module')
def events():
    class LocalMixing(EventsUtilsMixin):
        pass
    mixing = LocalMixing()
    mixing.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
    yield mixing

    mixing.teardown_events_processes()


@pytest.fixture(scope='session')
def prom_address():
    yield start_metrics_server()


@pytest.fixture(name='docker_scylla', scope='function')
def fixture_docker_scylla(request: pytest.FixtureRequest, params):  # pylint: disable=too-many-locals
    docker_scylla_args = {}
    if test_marker := request.node.get_closest_marker("docker_scylla_args"):
        docker_scylla_args = test_marker.kwargs
    ssl = docker_scylla_args.get('ssl')
    docker_network = docker_scylla_args.get('docker_network')
    # make sure the path to the file is base on the host path, and not as the docker internal path i.e. /sct/
    # since we are going to mount it in a DinD (docker-inside-docker) setup
    base_dir = os.environ.get("_SCT_BASE_DIR", None)
    entryfile_path = Path(base_dir) if base_dir else Path(__file__).parent.parent
    entryfile_path = entryfile_path / 'docker' / 'scylla-sct' / ('entry_ssl.sh' if ssl else 'entry.sh')

    alternator_flags = f"--alternator-port {ALTERNATOR_PORT} --alternator-write-isolation=always"
    docker_version = docker_scylla_args.get(
        'image', "scylladb/scylla-nightly:2025.2.0-dev-0.20250128.4f5550d7f212-x86_64")
    cluster = LocalScyllaClusterDummy(params=params)

    ssl_dir = (Path(__file__).parent.parent / 'data_dir' / 'ssl_conf').absolute()
    extra_docker_opts = (f'-p {ALTERNATOR_PORT} -p {BaseNode.CQL_PORT} --cpus="1" -v {entryfile_path}:/entry.sh'
                         f' -v {ssl_dir}:{SCYLLA_SSL_CONF_DIR}'
                         ' --entrypoint /entry.sh')

    scylla = RemoteDocker(LocalNode("scylla", cluster), image_name=docker_version,
                          command_line=f"--smp 1 {alternator_flags}",
                          extra_docker_opts=extra_docker_opts, docker_network=docker_network)

    if ssl:
        curr_dir = os.getcwd()
        try:
            os.chdir(Path(__file__).parent.parent)
            localhost = LocalHost(user_prefix='unit_test_fake_user', test_id='unit_test_fake_test_id')
            create_ca(localhost)
            create_certificate(CLIENT_FACING_CERTFILE, CLIENT_FACING_KEYFILE, cname="scylladb",
                               ca_cert_file=CA_CERT_FILE, ca_key_file=CA_KEY_FILE, ip_addresses=[scylla.ip_address])
            create_certificate(CLIENT_CERT_FILE, CLIENT_KEY_FILE, cname="scylladb",
                               ca_cert_file=CA_CERT_FILE, ca_key_file=CA_KEY_FILE)
        finally:
            os.chdir(curr_dir)

    cluster.nodes = [scylla]
    DummyRemoter = collections.namedtuple('DummyRemoter', ['run', 'sudo'])
    scylla.remoter = DummyRemoter(run=scylla.run, sudo=scylla.run)

    def db_up():
        try:
            return scylla.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server")
        except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    def db_alternator_up():
        try:
            return scylla.is_port_used(port=ALTERNATOR_PORT, service_name="scylla-server")
        except Exception as details:  # pylint: disable=broad-except  # noqa: BLE001
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    wait.wait_for(func=db_up, step=1, text='Waiting for DB services to be up', timeout=120, throw_exc=True)
    wait.wait_for(func=db_alternator_up, step=1, text='Waiting for DB services to be up alternator)',
                  timeout=120, throw_exc=True)

    yield scylla

    scylla.kill()


@pytest.fixture
def fake_remoter():
    RemoteCmdRunnerBase.set_default_remoter_class(FakeRemoter)
    return FakeRemoter


@pytest.fixture(scope='session', autouse=True)
def fake_provisioner():  # pylint: disable=no-self-use
    provisioner_factory.register_provisioner(backend="fake", provisioner_class=FakeProvisioner)


@pytest.fixture(scope='session', autouse=True)
def fake_region_definition_builder():  # pylint: disable=no-self-use
    region_definition_builder.register_builder(backend="fake", builder_class=FakeDefinitionBuilder)


@pytest.fixture(scope="function", name="params")
def fixture_params(request: pytest.FixtureRequest):
    if sct_config_marker := request.node.get_closest_marker("sct_config"):
        config_files = sct_config_marker.kwargs.get('files')
        os.environ['SCT_CONFIG_FILES'] = config_files

    os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
    params = sct_config.SCTConfiguration()  # pylint: disable=attribute-defined-outside-init
    params.update(dict(
        authenticator='PasswordAuthenticator',
        authenticator_user='cassandra',
        authenticator_password='cassandra',
        authorizer='CassandraAuthorizer',
    ))
    yield params

    for k in os.environ:
        if k.startswith('SCT_'):
            del os.environ[k]


@pytest.fixture(scope='function', autouse=True)
def fixture_cleanup_continuous_events_registry():
    ContinuousEventsRegistry().cleanup_registry()


def pytest_sessionfinish():
    # pytest's capsys is enabled by default, see
    # https://docs.pytest.org/en/7.1.x/how-to/capture-stdout-stderr.html.
    # but pytest closes its internal stream for capturing the stdout and
    # stderr, so we are not able to write the logger anymore once the test
    # session finishes. see https://github.com/pytest-dev/pytest/issues/5577,
    # to silence the warnings, let's just prevent logging from raising
    # exceptions.
    logging.raiseExceptions = False

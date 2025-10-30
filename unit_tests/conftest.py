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
import shutil
import subprocess
import uuid
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from sdcm import wait, sct_config
from sdcm.localhost import LocalHost
from sdcm.cluster import BaseNode
from sdcm.cluster_docker import VectorStoreSetDocker
from sdcm.prometheus import start_metrics_server
from sdcm.provision import provisioner_factory
from sdcm.provision.helpers.certificate import (
    create_ca, create_certificate, SCYLLA_SSL_CONF_DIR, CA_CERT_FILE, CA_KEY_FILE, TLSAssets)
from sdcm.remote import RemoteCmdRunnerBase
from sdcm.sct_events.continuous_event import ContinuousEventsRegistry
from sdcm.sct_provision import region_definition_builder
from sdcm.test_config import TestConfig
from sdcm.utils.common import get_data_dir_path
from sdcm.utils.docker_remote import RemoteDocker
from sdcm.utils.subtest_utils import SUBTESTS_FAILURES

from unit_tests.dummy_remote import LocalNode, LocalScyllaClusterDummy
from unit_tests.lib.events_utils import EventsUtilsMixin
from unit_tests.lib.fake_provisioner import FakeProvisioner
from unit_tests.lib.fake_region_definition_builder import FakeDefinitionBuilder
from unit_tests.lib.fake_remoter import FakeRemoter
from unit_tests.lib.alternator_utils import ALTERNATOR_PORT


pytest_plugins = ["pytester"]


@contextmanager
def mock_remote_scylla_yaml(scylla_node):
    """A mock for remote_scylla_yaml that can actually modify scylla.yaml in running container"""

    scylla_yaml = SimpleNamespace()
    yield scylla_yaml

    changes_applied = []
    container_id = scylla_node.docker_id
    try:
        for key, value in vars(scylla_yaml).items():
            if value is not None:
                yaml_key, yaml_value = key, str(value)
                # remove existing occurrences of the key and add a new value
                remove_cmd = [
                    'docker', 'exec', container_id, 'bash', '-c',
                    f'sed -i "/^{yaml_key}:/d" /etc/scylla/scylla.yaml']
                subprocess.run(remove_cmd, capture_output=True, text=True, check=False)
                add_cmd = [
                    'docker', 'exec', container_id, 'bash', '-c',
                    f'echo "{yaml_key}: {yaml_value}" >> /etc/scylla/scylla.yaml']
                result = subprocess.run(add_cmd, capture_output=True, text=True, check=False)

                if result.returncode == 0:
                    changes_applied.append(f"{yaml_key}: {yaml_value}")
                else:
                    logging.error("Failed to apply scylla.yaml change %s: %s", yaml_key, result.stderr)
    except Exception as e:  # noqa: BLE001
        logging.error("Error in mock_remote_scylla_yaml: %s", e)


@pytest.fixture(scope='module')
def events():
    mixing = EventsUtilsMixin()
    mixing.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
    yield mixing

    mixing.teardown_events_processes()


@pytest.fixture(scope='function')
def events_function_scope():
    mixing = EventsUtilsMixin()
    mixing.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)
    yield mixing

    mixing.teardown_events_processes()


@pytest.fixture(scope='session')
def prom_address():
    yield start_metrics_server()


@contextmanager
def create_ssl_dir(test_id: str):
    ssl_dir = (Path(__file__).parent.parent / 'data_dir' / f'ssl_conf_{test_id}').absolute()
    ssl_dir.mkdir(parents=True, exist_ok=True)

    localhost = LocalHost(user_prefix='unit_test_fake_user', test_id='unit_test_fake_test_id')
    create_ca(localhost)

    for file_path in Path(get_data_dir_path('ssl_conf')).glob('*'):
        if file_path.is_file():
            shutil.copy2(file_path, ssl_dir)
    yield ssl_dir
    shutil.rmtree(ssl_dir, ignore_errors=True)


def configure_scylla_node(docker_scylla_args: dict, params, ssl_dir: Path | None = None):  # noqa: PLR0914
    ssl = docker_scylla_args.get('ssl')
    docker_network = docker_scylla_args.get('docker_network')
    # make sure the path to the file is base on the host path, and not as the docker internal path i.e. /sct/
    # since we are going to mount it in a DinD (docker-inside-docker) setup
    base_dir = os.environ.get("_SCT_BASE_DIR", None)
    entryfile_path = Path(base_dir) if base_dir else Path(__file__).parent.parent
    entryfile_path = entryfile_path / 'docker' / 'scylla-sct' / ('entry_ssl.sh' if ssl else 'entry.sh')

    alternator_flags = f"--alternator-port {ALTERNATOR_PORT} --alternator-write-isolation=always"

    default_image = "docker.io/scylladb/scylla-nightly:2025.2.0-dev-0.20250302.0343235aa269"
    docker_version = docker_scylla_args.get('scylla_docker_image') or docker_scylla_args.get('image', default_image)

    test_id = str(uuid.uuid4())[:8]
    if not params.get('user_prefix'):
        params['user_prefix'] = f'unit-test-{test_id}'

    cluster = LocalScyllaClusterDummy(params=params)

    ssl_mount = f' -v {ssl_dir}:{SCYLLA_SSL_CONF_DIR}:z' if ssl else ''

    env_vars = '-e VECTOR_SEARCH_TEST=true' if docker_scylla_args.get('scylla_docker_image') else ''
    extra_docker_opts = (f'-p {ALTERNATOR_PORT} -p {BaseNode.CQL_PORT} --cpus="1" -v {entryfile_path}:/entry.sh:z'
                         f'{ssl_mount}'
                         f' --user root {env_vars} --entrypoint /entry.sh')

    if seeds := docker_scylla_args.get("seeds"):
        seeds = f" --seeds={seeds}"
    else:
        seeds = ""

    scylla = RemoteDocker(LocalNode("scylla", cluster), image_name=docker_version,
                          command_line=f"--smp 1 {alternator_flags}{seeds}",
                          extra_docker_opts=extra_docker_opts, docker_network=docker_network)

    if ssl_dir:
        create_certificate(ssl_dir / TLSAssets.DB_CLIENT_FACING_CERT, ssl_dir / TLSAssets.DB_CLIENT_FACING_KEY,
                           cname="scylladb", ca_cert_file=CA_CERT_FILE, ca_key_file=CA_KEY_FILE,
                           ip_addresses=[scylla.internal_ip_address], dns_names=[scylla.public_dns_name])
        create_certificate(ssl_dir / TLSAssets.CLIENT_CERT, ssl_dir / TLSAssets.CLIENT_KEY,
                           cname="scylladb", ca_cert_file=CA_CERT_FILE, ca_key_file=CA_KEY_FILE)

        scylla.__class__.ssl_conf_dir = property(lambda self: ssl_dir)

    cluster.nodes = [scylla]
    DummyRemoter = collections.namedtuple('DummyRemoter', ['run', 'sudo'])
    scylla.remoter = DummyRemoter(run=scylla.run, sudo=scylla.run)

    scylla.is_running = lambda: bool(getattr(scylla, 'docker_id', None))
    scylla.remote_scylla_yaml = lambda: mock_remote_scylla_yaml(scylla)

    def db_up():
        try:
            return scylla.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server")
        except Exception as details:  # noqa: BLE001
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    def db_alternator_up():
        try:
            return scylla.is_port_used(port=ALTERNATOR_PORT, service_name="scylla-server")
        except Exception as details:  # noqa: BLE001
            logging.error("Error checking for scylla up normal: %s", details)
            return False

    wait.wait_for(func=db_up, step=1, text='Waiting for DB services to be up', timeout=120, throw_exc=True)
    wait.wait_for(func=db_alternator_up, step=1, text='Waiting for DB services to be up alternator)',
                  timeout=120, throw_exc=True)

    return scylla


@pytest.fixture(name='docker_scylla', scope='function')
def fixture_docker_scylla(request: pytest.FixtureRequest, params):  # noqa: PLR0914
    docker_scylla_args = {}
    if test_marker := request.node.get_closest_marker("docker_scylla_args"):
        docker_scylla_args = test_marker.kwargs
    ctx = create_ssl_dir(test_id=str(uuid.uuid4())[:8]) if docker_scylla_args.get('ssl') else nullcontext()
    with ctx as ssl_dir:
        scylla = configure_scylla_node(docker_scylla_args, params, ssl_dir=ssl_dir)
        yield scylla
        scylla.kill()


@pytest.fixture(name='docker_scylla_2', scope='function')
def fixture_docker_2_scylla(request: pytest.FixtureRequest, docker_scylla, params):  # noqa: PLR0914
    docker_scylla_args = {}
    if test_marker := request.node.get_closest_marker("docker_scylla_args"):
        docker_scylla_args = test_marker.kwargs
    docker_scylla_args['seeds'] = docker_scylla.ip_address
    ctx = create_ssl_dir(test_id=str(uuid.uuid4())[:8]) if docker_scylla_args.get('ssl') else nullcontext()
    with ctx as ssl_dir:
        scylla = configure_scylla_node(docker_scylla_args, params, ssl_dir=ssl_dir)
        yield scylla
        scylla.kill()


@pytest.fixture(name='docker_vector_store', scope='function')
def fixture_docker_vector_store(request: pytest.FixtureRequest, docker_scylla, params):
    docker_scylla_args = {}
    if test_marker := request.node.get_closest_marker("docker_scylla_args"):
        docker_scylla_args = test_marker.kwargs

    if not docker_scylla_args.get('vs_docker_image'):
        yield None
        return

    # restart the container to reload scylla.yaml config
    def reload_config_for_test():
        docker_scylla.node.remoter.run(f"docker restart {docker_scylla.docker_id}")
    docker_scylla.reload_config = reload_config_for_test

    cluster = docker_scylla.parent_cluster
    os.environ.setdefault('_SCT_TEST_LOGDIR', '/tmp/test_vector_search_logs')

    class MockTester:
        def __init__(self):
            self.rack_names_per_datacenter_and_rack_idx_map = {}
    TestConfig.set_tester_obj(MockTester())

    vs_docker_image_version = docker_scylla_args.get('vs_docker_image', 'scylladb/vector-store:latest')
    vs_docker_image, vs_version = (
        vs_docker_image_version.rsplit(':', 1) if ':' in vs_docker_image_version
        else (vs_docker_image_version, 'latest'))

    params.update({
        'n_vector_store_nodes': 1,
        'vector_store_port': 6080,
        'vector_store_scylla_port': 9042,
        'vector_store_threads': 2,
        'docker_network': docker_scylla_args.get('docker_network') or 'bridge',
        'user_prefix': 'test-vector',
        'vector_store_docker_image': vs_docker_image,
        'vector_store_version': vs_version})

    def destroy_vector_store_cluster(vs_cluster):
        if vs_cluster:
            try:
                vs_cluster.destroy()
            except Exception:  # noqa: BLE001
                logging.warning("Failed to destroy Vector Store cluster", exc_info=True)

    vector_store_cluster = None
    try:
        vector_store_cluster = VectorStoreSetDocker(
            params=params,
            vs_docker_image=params.get('vector_store_docker_image'),
            vs_docker_image_tag=params.get('vector_store_version'),
            cluster_prefix='test-vector-store',
            n_nodes=1)

        vector_store_cluster.configure_with_scylla_cluster(cluster)
        for node in vector_store_cluster.nodes:
            if not node.wait_for_vector_store_ready():
                raise RuntimeError(f"Vector Store service on {node.name} failed to start")

        # restart scylla container to apply vector store URI changes
        scylla_node = cluster.nodes[0]
        restart_result = subprocess.run(
            ['docker', 'restart', scylla_node.docker_id], capture_output=True, text=True, check=False)
        if restart_result.returncode != 0:
            raise RuntimeError(f"Container restart failed: {restart_result.stderr}")
        wait.wait_for(
            func=lambda: scylla_node.is_port_used(port=BaseNode.CQL_PORT, service_name="scylla-server"),
            step=2, timeout=60, text="Waiting for Scylla container to restart")

        yield vector_store_cluster

    except Exception:  # noqa: BLE001
        destroy_vector_store_cluster(vector_store_cluster)
        raise
    finally:
        destroy_vector_store_cluster(vector_store_cluster)


@pytest.fixture
def fake_remoter():
    RemoteCmdRunnerBase.set_default_remoter_class(FakeRemoter)
    return FakeRemoter


@pytest.fixture(scope='session', autouse=True)
def fake_provisioner():
    provisioner_factory.register_provisioner(backend="fake", provisioner_class=FakeProvisioner)


@pytest.fixture(scope='session', autouse=True)
def fake_region_definition_builder():
    region_definition_builder.register_builder(backend="fake", builder_class=FakeDefinitionBuilder)


@pytest.fixture(scope="function", name="params")
def fixture_params(request: pytest.FixtureRequest, monkeypatch):
    if sct_config_marker := request.node.get_closest_marker("sct_config"):
        config_files = sct_config_marker.kwargs.get('files')
        monkeypatch.setenv('SCT_CONFIG_FILES', config_files)

    monkeypatch.setenv('SCT_CLUSTER_BACKEND', "docker")
    params = sct_config.SCTConfiguration()
    params.update(dict(
        authenticator='PasswordAuthenticator',
        authenticator_user='cassandra',
        authenticator_password='cassandra',
        authorizer='CassandraAuthorizer',
    ))
    yield params


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


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_logreport(report: pytest.TestReport):
    """
    Hook to log subtest failures and their reports,
    so it can be accessed later during teardown or in fixtures.

    in this one we handle the subtests failures only, and change some of them to passed
    """
    if report.when == "call" and getattr(report, "context", None):
        if report.failed:
            SUBTESTS_FAILURES[report.nodeid].append(report)

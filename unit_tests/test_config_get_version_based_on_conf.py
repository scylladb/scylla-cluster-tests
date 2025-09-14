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
# Copyright (c) 2023 ScyllaDB

import logging
import pytest

from sdcm import sct_config

pytestmark = [
    pytest.mark.integration,
]


@pytest.fixture(scope='session', autouse=True)
def setup():
    logging.basicConfig(level=logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('anyconfig').setLevel(logging.ERROR)

    yield


@pytest.fixture(scope='function', autouse=True)
def function_setup(monkeypatch):
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')


@pytest.mark.parametrize(argnames='scylla_version, expected_docker_image, expected_outcome',
                         argvalues=[
                             pytest.param('2024.1', 'scylladb/scylla-enterprise', ('2024.1', True), id='2024.1'),
                             pytest.param('2025.1', 'scylladb/scylla-enterprise', ('2025.1', True), id='2025.1'),
                             pytest.param('latest', 'scylladb/scylla-nightly', (None, False),  id='latest'),
                             pytest.param('master:latest', 'scylladb/scylla-nightly',
                                          (None, False), id='master:latest'),
                             pytest.param('enterprise', 'scylladb/scylla-enterprise-nightly',
                                          (None, True), id='enterprise'),
                         ],
                         )
def test_docker(scylla_version, expected_docker_image, expected_outcome, monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_USE_MGMT', 'false')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', scylla_version)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert 'docker_image' in conf.dump_config()
    assert conf.get('docker_image') == expected_docker_image
    _version, _is_enterprise = conf.get_version_based_on_conf()
    if expected_outcome[0] is None:
        assert _is_enterprise == expected_outcome[1]
    else:
        assert (_version, _is_enterprise) == expected_outcome


@pytest.mark.parametrize(argnames='distro',
                         argvalues=('ubuntu-xenial', 'centos', 'debian-jessie')
                         )
@pytest.mark.parametrize(argnames='scylla_version, expected_outcome',
                         argvalues=[
                             pytest.param('2024.1', ('2024.1', True), id='2024.1'),
                             pytest.param('master:latest', (None, True), id='master'),
                             pytest.param('enterprise-2024.1:latest', (None, True), id='enterprise-2024.1'),
                             pytest.param('branch-2025.1:latest', (None, True), id='branch-2025.1'),
                         ],
                         )
def test_scylla_repo(scylla_version, expected_outcome, distro, monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv('SCT_SCYLLA_VERSION', scylla_version)
    monkeypatch.setenv(
        'SCT_GCE_IMAGE_DB', 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')
    monkeypatch.setenv('SCT_USE_PREINSTALLED_SCYLLA', 'false')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO', distro)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    _version, _is_enterprise = conf.get_version_based_on_conf()

    if expected_outcome[0] is not None:
        assert expected_outcome[0] in _version
    assert _is_enterprise == expected_outcome[1]


@pytest.mark.parametrize(
    argnames="scylla_version, expected_outcome",
    argvalues=[
        pytest.param("2024.1", ("2024.1", True), id="2024.1"),
        pytest.param("2025.1", ("2025.1", True), id="2025.1"),
        pytest.param("master:latest", (None, True), id="master"),
        pytest.param("branch-2024.1:latest", (None, True), id="branch-2024.1"),
        pytest.param("branch-2025.1:latest", (None, True), id="branch-2025.1"),
    ],
)
@pytest.mark.parametrize(argnames='backend',
                         argvalues=('aws', 'gce', 'azure')
                         )
def test_images(backend, scylla_version, expected_outcome, monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', backend)
    monkeypatch.setenv('SCT_SCYLLA_VERSION', scylla_version)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version, _is_enterprise = conf.get_version_based_on_conf()

    if expected_outcome[0] is not None:
        assert expected_outcome[0] in _version
    assert _is_enterprise == expected_outcome[1]


def test_baremetal(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'baremetal')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '6.1')
    monkeypatch.setenv('SCT_S3_BAREMETAL_CONFIG', "some_config")
    monkeypatch.setenv('SCT_DB_NODES_PRIVATE_IP', '["127.0.0.1"]')
    monkeypatch.setenv('SCT_DB_NODES_PUBLIC_IP', '["127.0.0.1"]')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert '6.1' in _version
    assert not _is_enterprise


def test_unified_package(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "gce")
    monkeypatch.setenv('SCT_UNIFIED_PACKAGE',
                       ('https://downloads.scylladb.com/unstable/scylla/master/relocatable/2023-11-13T03:04:27Z/'
                        'scylla-unified-5.5.0~dev-0.20231113.7b08886e8dd8.x86_64.tar.gz'))
    monkeypatch.setenv(
        'SCT_GCE_IMAGE_DB', 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')

    monkeypatch.setenv('SCT_USE_PREINSTALLED_SCYLLA', 'false')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    _version, _is_enterprise = conf.get_version_based_on_conf()

    assert '5.5.0' in _version
    assert not _is_enterprise

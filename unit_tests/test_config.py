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

import logging
import itertools
import unittest.mock
from collections import namedtuple

import pytest

from sdcm import sct_config
from sdcm.utils.common import get_latest_scylla_release
from sdcm.utils.aws_utils import get_ssm_ami

RPM_URL = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
          '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
          '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'


@pytest.fixture(scope="module")
def monkeymodule():
    """Fixture that provides a monkeypatching with module scope."""
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(name='conf', scope='module')
def fixture_conf(monkeymodule):
    monkeymodule.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeymodule.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeymodule.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')

    yield sct_config.SCTConfiguration()


@pytest.fixture(autouse=True)
def fixture_env(monkeypatch):

    logging.basicConfig(level=logging.ERROR)
    logging.getLogger('botocore').setLevel(logging.CRITICAL)
    logging.getLogger('boto3').setLevel(logging.CRITICAL)
    logging.getLogger('anyconfig').setLevel(logging.ERROR)

    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_USE_MGMT', 'false')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')


def test_01_dump_config(conf):
    logging.debug(conf.dump_config())


def test_02_verify_config(conf):
    conf.verify_configuration()
    conf.check_required_files()


def test_03_dump_help_config_yaml(conf):
    logging.debug(conf.dump_help_config_yaml())


def test_03_dump_help_config_markdown(conf):
    logging.debug(conf.dump_help_config_markdown())


@pytest.mark.integration
def test_04_check_env_parse(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_REGION_NAME', '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv('SCT_N_DB_NODES', '2 2')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy ami-dummy2')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.dump_config()

    assert conf.get('ami_id_db_scylla') == 'ami-dummy ami-dummy2'


def test_05_docker(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_USE_MGMT', 'false')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '3.0.3')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert 'docker_image' in conf.dump_config()
    assert conf.get('docker_image') == 'scylladb/scylla'


def test_06a_docker_latest_no_loader(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_USE_MGMT', 'false')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'latest')
    monkeypatch.setenv('SCT_N_LOADERS', "0")
    docker_tag_after_processing = "fake_specific_docker_tag"

    with unittest.mock.patch.object(
            sct_config, 'get_specific_tag_of_docker_image',
            return_value=docker_tag_after_processing):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert conf['scylla_version'] != 'latest'


def test_06b_docker_development(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_USE_MGMT', 'false')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '666.development-blah')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_07_baremetal_exception(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'baremetal')
    conf = sct_config.SCTConfiguration()

    with pytest.raises(AssertionError):
        conf.verify_configuration()


def test_08_baremetal(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'baremetal')
    monkeypatch.setenv('SCT_DB_NODES_PRIVATE_IP', '["1.2.3.4", "1.2.3.5"]')
    monkeypatch.setenv('SCT_DB_NODES_PUBLIC_IP', '["1.2.3.4", "1.2.3.5"]')
    monkeypatch.setenv('SCT_USE_PREINSTALLED_SCYLLA', 'true')
    monkeypatch.setenv('SCT_S3_BAREMETAL_CONFIG', "some_config")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert 'db_nodes_private_ip' in conf.dump_config()
    assert conf.get('db_nodes_private_ip') == ["1.2.3.4", "1.2.3.5"]


def test_09_unknown_configure(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/unknown_param_in_config.yaml')
    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError):
        conf.verify_configuration()


def test_09_unknown_env(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'docker')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/unknown_param_in_config.yaml')
    monkeypatch.setenv('SCT_WHAT_IS_THAT', 'just_made_this_up')
    monkeypatch.setenv('SCT_WHAT_IS_THAT_2', 'what is this ?')
    conf = sct_config.SCTConfiguration()

    with pytest.raises(ValueError) as context:
        conf.verify_configuration()
    msg = str(context.value)
    assert 'SCT_WHAT_IS_THAT_2=what is this ?' in msg
    assert 'SCT_WHAT_IS_THAT=just_made_this_up' in msg


@pytest.mark.integration
def test_10_longevity(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/complex_test_case_with_version.yaml')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA_DESC', 'master')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.get('user_prefix') == 'longevity-50gb-4d-not-jenkins-maste'


@pytest.mark.integration
def test_10_mananger_regression(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy ami-dummy2')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


@pytest.mark.integration
def test_12_scylla_version_ami(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    amis = conf.get('ami_id_db_scylla').split()
    assert len(amis) == 2
    assert all(ami.startswith('ami-') for ami in amis)


def test_12_scylla_version_ami_case1(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


@pytest.mark.integration
def test_12_scylla_version_ami_case2(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '99.0.3')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')

    with pytest.raises(ValueError, match="AMIs for scylla_version='99.0.3' not found in eu-west-1"):
        sct_config.SCTConfiguration()


@pytest.mark.integration
def test_12_scylla_version_repo(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


@pytest.mark.integration
def test_12_scylla_version_repo_case1(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


@pytest.mark.integration
def test_12_scylla_version_repo_case2(monkeypatch):
    monkeypatch.setenv("SCT_AMI_ID_DB_SCYLLA", "ami-dummy")
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '99.0.3')

    with pytest.raises(ValueError, match=r"repo for scylla version 99.0.3 wasn't found"):
        sct_config.SCTConfiguration()


def test_12_scylla_version_repo_ubuntu(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO', 'ubuntu-xenial')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO_LOADER', 'ubuntu-xenial')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '3.0.3')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB',
                       'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')
    expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
    with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
            unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert 'scylla_repo' in conf.dump_config()
    assert conf.get('scylla_repo') == expected_repo


def test_12_scylla_version_repo_ubuntu_loader_centos(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO', 'ubuntu-xenial')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO_LOADER', 'centos')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', '3.0.3')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB',
                       'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')
    expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
    with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
            unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert 'scylla_repo' in conf.dump_config()
    assert conf.get('scylla_repo') == expected_repo


def test_12_k8s_scylla_version_ubuntu_loader_centos(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'k8s-local-kind')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO', 'ubuntu-xenial')
    monkeypatch.setenv('SCT_SCYLLA_LINUX_DISTRO_LOADER', 'centos')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert 'scylla_repo' in conf.dump_config()
    assert not conf.get('scylla_repo')


@pytest.mark.integration
def test_13_scylla_version_ami_branch_latest(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'branch-5.2:latest')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    amis = conf.get('ami_id_db_scylla').split()
    assert len(amis) == 2
    assert all(ami.startswith('ami-') for ami in amis)


def test_14_check_rackaware_config_false_loader(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'false')
    monkeypatch.setenv('SCT_SIMULATED_RACKS', "0")
    monkeypatch.setenv('SCT_REGION_NAME', "eu-west-1")
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a,b')
    monkeypatch.setenv('SCT_N_DB_NODES', '2')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError, match="rack_aware_loader' must be set to True for rackaware validator."):
        conf.verify_configuration()


def test_14_check_rackaware_config_one_racks(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'true')
    monkeypatch.setenv('SCT_REGION_NAME', "eu-west-1")
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a')
    monkeypatch.setenv('SCT_SIMULATED_RACKS', "0")
    monkeypatch.setenv('SCT_N_DB_NODES', '2')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError, match="Rack-aware validation can only be performed in multi-availability zone or multi-"):
        conf.verify_configuration()


def test_14_check_rackaware_config_no_rack_without_loader(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'true')
    monkeypatch.setenv('SCT_REGION_NAME', "eu-west-1 eu-west-2")
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a,b')
    monkeypatch.setenv('SCT_SIMULATED_RACKS', "0")
    monkeypatch.setenv('SCT_N_DB_NODES', '2 2')
    monkeypatch.setenv('SCT_N_LOADERS', '2 2')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy ami-dummy2')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    with pytest.raises(ValueError, match="Rack-aware validation requires zones without loaders."):
        conf.verify_configuration()


def test_14_check_rackaware_config_multi_az(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'true')
    monkeypatch.setenv('SCT_REGION_NAME', "eu-west-1")
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a,b')
    monkeypatch.setenv('SCT_N_DB_NODES', '2')
    monkeypatch.setenv('SCT_N_LOADERS', '1')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_14_check_rackaware_config_multi_region(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'true')
    monkeypatch.setenv('SCT_REGION_NAME', '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv('SCT_N_DB_NODES', '2 2')
    monkeypatch.setenv('SCT_N_LOADERS', '1 0')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy ami-dummy2')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_14_check_rackaware_config_multi_az_and_region(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_RACK_AWARE_LOADER', 'true')
    monkeypatch.setenv('SCT_REGION_NAME', '["eu-west-1", "us-east-1"]')
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a,b')
    monkeypatch.setenv('SCT_N_DB_NODES', '2 2')
    monkeypatch.setenv('SCT_N_LOADERS', '1 1')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy ami-dummy2')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_TEARDOWN_VALIDATORS', """rackaware:
             enabled: true
        """)

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_conf_check_required_files(monkeypatch):  # pylint: disable=no-self-use
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    ami_id = get_ssm_ami(
        '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id', region_name='eu-west-1')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', ami_id)
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_STRESS_CMD', """cassandra-stress user profile=/tmp/cs_profile_background_reads_overload.yaml \
        ops'(insert=100)' no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore""")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.check_required_files()


def test_conf_check_required_files_negative(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/stress_cmd_with_bad_profile.yaml')
    monkeypatch.setenv('SCT_STRESS_CMD', """cassandra-stress user profile=/tmp/1232123123123123123.yaml ops'(insert=100)'\
        no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore""")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    try:
        conf.check_required_files()
    except ValueError as exc:
        assert str(
            exc) == "Stress command parameter 'stress_cmd' contains profile '/tmp/1232123123123123123.yaml' that"\
            " does not exists under data_dir/"


def test_config_dupes():
    def get_dupes(c):
        '''sort/tee/izip'''

        a, b = itertools.tee(sorted(c))
        next(b, None)
        r = None
        for k, g in zip(a, b):
            if k != g:
                continue
            if k != r:
                yield k
                r = k

    opts = [o['name'] for o in sct_config.SCTConfiguration.config_options]

    assert list(get_dupes(opts)) == []


@pytest.mark.integration
def test_13_bool(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_STORE_PERF_RESULTS', 'False')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')
    conf = sct_config.SCTConfiguration()

    assert conf['store_perf_results'] is False


@pytest.mark.integration
def test_14_aws_siren_from_env(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_DB_TYPE', 'cloud_scylla')
    monkeypatch.setenv('SCT_REGION_NAME', 'us-east-1')
    monkeypatch.setenv('SCT_N_DB_NODES', '2')
    monkeypatch.setenv('SCT_INSTANCE_TYPE_DB', 'i4i.large')
    monkeypatch.setenv('SCT_AUTHENTICATOR_USER', "user")
    monkeypatch.setenv('SCT_AUTHENTICATOR_PASSWORD', "pass")
    monkeypatch.setenv('SCT_CLOUD_CLUSTER_ID', "193712947904378")
    monkeypatch.setenv('SCT_SECURITY_GROUP_IDS', "sg-89fi3rkl")
    monkeypatch.setenv('SCT_SUBNET_ID', "sub-d32f09sdf")
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/multi_region_dc_test_case.yaml')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.get('cloud_cluster_id') == 193712947904378
    assert conf["authenticator_user"] == "user"
    assert conf["authenticator_password"] == "pass"
    assert conf["security_group_ids"] == ["sg-89fi3rkl"]
    assert conf["subnet_id"] == ["sub-d32f09sdf"]


def test_15_new_scylla_repo(monkeypatch):
    centos_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
                  '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
                  '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla' \
                  '.repo'

    monkeypatch.delenv('SCT_SCYLLA_VERSION', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_REPO', centos_repo)
    monkeypatch.setenv('SCT_NEW_SCYLLA_REPO', centos_repo)
    monkeypatch.setenv('SCT_USER_PREFIX', 'testing')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB',
                       'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')

    with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='2019.1.1', clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()
    assert conf.get('target_upgrade_version') == '2019.1.1'


def test_15a_new_scylla_repo_by_scylla_version(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'master:latest')
    monkeypatch.setenv('SCT_NEW_VERSION', 'master:latest')
    monkeypatch.setenv('SCT_USER_PREFIX', 'testing')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB',
                       'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')

    resolved_repo_link = 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/rpm\
        /centos/2021-06-09T13:12:44Z/scylla.repo'

    with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='666.development', clear=True), \
            unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=resolved_repo_link, clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()

    assert conf.get('scylla_repo') == resolved_repo_link
    target_upgrade_version = conf.get('target_upgrade_version')
    assert target_upgrade_version == '666.development' or target_upgrade_version.endswith(".dev")


def test_15b_image_id_by_scylla_version(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'master:latest')
    monkeypatch.setenv('SCT_USER_PREFIX', 'testing')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB', '')
    monkeypatch.setenv('SCT_USE_PREINSTALLED_SCYLLA', 'true')

    resolved_image_link = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/' \
                          'scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359'
    image = namedtuple("Image", "name self_link")(name='scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359',
                                                  self_link=resolved_image_link)

    with unittest.mock.patch.object(sct_config, 'get_branched_gce_images', return_value=[image], clear=True):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf._get_target_upgrade_version()

    assert conf.get('gce_image_db') == resolved_image_link


@pytest.mark.integration
def test_16_default_oracle_scylla_version_eu_west_1(monkeypatch):
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_REGION_NAME', 'eu-west-1')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_DB_TYPE', 'mixed_scylla')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert 'ami-' in conf.get('ami_id_db_oracle')


@pytest.mark.integration
def test_16_oracle_scylla_version_us_east_1(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_ORACLE_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_REGION_NAME', 'us-east-1')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_DB_TYPE', 'mixed_scylla')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.get('ami_id_db_oracle').startswith('ami-')


@pytest.mark.integration
def test_16_oracle_scylla_version_eu_west_1(monkeypatch):
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_ORACLE_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_REGION_NAME', 'eu-west-1')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')
    monkeypatch.setenv('SCT_DB_TYPE', 'mixed_scylla')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf.get('ami_id_db_oracle').startswith('ami-')


@pytest.mark.integration
def test_16_oracle_scylla_version_and_oracle_ami_together(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_REGION_NAME', 'eu-west-1')
    monkeypatch.setenv('SCT_ORACLE_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_AMI_ID_DB_ORACLE', 'ami-dummy')
    monkeypatch.setenv('SCT_DB_TYPE', 'mixed_scylla')
    monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')

    with pytest.raises(ValueError) as context:
        sct_config.SCTConfiguration()

    assert "'oracle_scylla_version' and 'ami_id_db_oracle' can't used together" in str(context.value)


def test_17_verify_scylla_bench_required_parameters_in_command(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv(
        'SCT_STRESS_CMD', "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100")
    monkeypatch.setenv('SCT_STRESS_READ_CMD',
                       "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100")

    conf = sct_config.SCTConfiguration()
    assert conf["stress_cmd"] == [
        "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"]
    assert conf["stress_read_cmd"] == [
        "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"]


def test_17_1_raise_error_if_scylla_bench_command_dont_have_workload(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv('SCT_STRESS_CMD', "scylla-bench -mode=write -replication-factor=3 -partition-count=100")
    monkeypatch.setenv('SCT_STRESS_READ_CMD',
                       "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100")

    err_msg = "Scylla-bench command scylla-bench -mode=write -replication-factor=3 -partition-count=100 doesn't have parameter -workload"

    with pytest.raises(ValueError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert err_msg in str(context.value)


def test_17_2_raise_error_if_scylla_bench_command_dont_have_mode(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv(
        'SCT_STRESS_CMD', "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100")
    monkeypatch.setenv('SCT_STRESS_READ_CMD',
                       "scylla-bench -workload=uniform -replication-factor=3 -partition-count=100")

    err_msg = "Scylla-bench command scylla-bench -workload=uniform -replication-factor=3 -partition-count=100 doesn't have parameter -mode"

    with pytest.raises(ValueError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert err_msg in str(context.value)


@pytest.mark.integration
def test_18_error_if_no_version_repo_ami_selected(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.delenv('SCT_SCYLLA_VERSION', raising=False)
    for backend in sct_config.SCTConfiguration.available_backends:
        if 'k8s' in backend:
            continue
        if 'siren' in backend:
            continue

        monkeypatch.setenv('SCT_CLUSTER_BACKEND', backend)
        monkeypatch.setenv('SCT_CONFIG_FILES', 'unit_tests/test_configs/minimal_test_case.yaml')

        conf = sct_config.SCTConfiguration()
        with pytest.raises(AssertionError, match="scylla version/repos wasn't configured correctly") as context:
            conf.verify_configuration()
        assert "scylla version/repos wasn't configured correctly" in str(context.value)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_DB_TYPE', 'cloud_scylla')

    # 1) check that SCT_CLOUD_CLUSTER_ID is used for the exception
    conf = sct_config.SCTConfiguration()

    with pytest.raises(AssertionError) as context:
        conf.verify_configuration()
    assert "scylla version/repos wasn't configured correctly" in str(context.value)
    assert "SCT_CLOUD_CLUSTER_ID" in str(context.value)

    monkeypatch.setenv('SCT_CLOUD_CLUSTER_ID', '1234')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_19_aws_region_with_no_region_data(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_REGION_NAME', 'not-exists-2')

    with pytest.raises(ValueError, match="not-exists-2 isn't supported"):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()


@pytest.mark.integration
def test_20_user_data_format_version_aws(monkeypatch):
    monkeypatch.delenv('SCT_AMI_ID_DB_SCYLLA', raising=False)
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'master:latest')
    monkeypatch.setenv('SCT_ORACLE_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    monkeypatch.setenv('SCT_DB_TYPE', 'mixed_scylla')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf['user_data_format_version'] == '3'
    assert conf['oracle_user_data_format_version'] == '3'


@pytest.mark.integration
def test_20_user_data_format_version_aws_2(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    ami_id = get_ssm_ami(
        '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id', region_name='eu-west-1')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', ami_id)  # run image which isn't scylla
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf['user_data_format_version'] == '3'
    assert 'oracle_user_data_format_version' not in conf


@pytest.mark.integration
def test_20_user_data_format_version_gce_1(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'master:latest')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf['user_data_format_version'] == '3'


@pytest.mark.integration
def test_20_user_data_format_version_gce_2(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', get_latest_scylla_release(product='scylla'))
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf['user_data_format_version'] == '3'


@pytest.mark.integration
def test_20_user_data_format_version_gce_3(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'gce')
    monkeypatch.setenv('SCT_GCE_IMAGE_DB', ('https://www.googleapis.com/compute/v1/projects/'
                                            'ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64'))
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    assert conf['user_data_format_version'] == '2'


@pytest.mark.integration
def test_20_user_data_format_version_azure(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'azure')
    monkeypatch.setenv('SCT_AZURE_REGION_NAME', 'eastus')
    monkeypatch.setenv('SCT_SCYLLA_VERSION', 'master:latest')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.verify_configuration_urls_validity()
    # since azure image listing is still buggy, can be sure which version we'll get
    # I would expect master:latest to be version 3 now, but azure.utils.get_scylla_images
    # returns something from 5 months ago.


def test_21_nested_values(monkeypatch):
    monkeypatch.setenv('SCT_CONFIG_FILES', ('["unit_tests/test_configs/minimal_test_case.yaml", '
                                            '"unit_tests/test_data/stress_image_extra_config.yaml"]'))
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-1234')
    monkeypatch.setenv('SCT_STRESS_READ_CMD.0', 'cassandra_stress')
    monkeypatch.setenv('SCT_STRESS_READ_CMD.1', 'cassandra_stress')
    monkeypatch.setenv('SCT_STRESS_IMAGE', '{"ycsb": "scylladb/something_else"}')
    monkeypatch.setenv('SCT_STRESS_IMAGE.cassandra-stress', 'scylla-bench')
    monkeypatch.setenv('SCT_STRESS_IMAGE.scylla-bench', 'scylladb/something')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    stress_image = conf.get('stress_image')
    assert stress_image['ndbench'] == 'scylladb/hydra-loaders:ndbench-jdk8-A'
    assert stress_image['nosqlbench'] == 'scylladb/hydra-loaders:nosqlbench-A'

    assert conf.get('stress_image.scylla-bench') == 'scylladb/something'
    assert conf.get('stress_image.non-exist') is None

    assert conf.get('stress_read_cmd') == ['cassandra_stress', 'cassandra_stress']


def test_22_get_none(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_CONFIG_FILES', "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-1234')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    val = conf.get(None)
    assert val is None


def test_23_1_include_nemesis_selector(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv(
        'SCT_CONFIG_FILES', '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/nemesis_selector_list.yaml"]')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf["nemesis_selector"] == "config_changes and topology_changes"


def test_23_2_nemesis_include_selector_list(monkeypatch):
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_REGION_NAME', 'eu-west-1')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', 'ami-dummy')
    monkeypatch.setenv(
        'SCT_CONFIG_FILES', '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/nemesis_selector_list_of_list.yaml"]')
    monkeypatch.setenv('SCT_NEMESIS_CLASS_NAME', 'NemesisClass:1 NemesisClass:2')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()

    assert conf["nemesis_selector"] == ["config_changes and topology_changes", "topology_changes", "disruptive"]


def test_26_run_fullscan_params_validtion_positive(monkeypatch):
    monkeypatch.setenv(
        'SCT_CONFIG_FILES', '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/positive_fullscan_param.yaml"]')
    sct_config.SCTConfiguration()


def test_27_run_fullscan_params_validtion_negative(monkeypatch):
    monkeypatch.setenv(
        'SCT_CONFIG_FILES', '["unit_tests/test_configs/minimal_test_case.yaml", "unit_tests/test_configs/negative_fullscan_param.yaml"]')
    try:
        sct_config.SCTConfiguration()
    except ValueError as exp:
        assert str(exp) == "Config params validation errors:\n\tfield 'mode' must be one of " \
            "'('random', 'table', 'partition', 'aggregate', 'table_and_aggregate')' " \
            "but got 'agggregate'\n\t" \
            "field 'ks_cf' must be an instance of <class 'str'>, but got '1'\n\t" \
            "field 'validate_data' must be an instance of <class 'bool'>, but got 'no'\n\t" \
            "field 'full_scan_aggregates_operation_limit' must be an instance of <class 'int'>, but got 'a'"


def test_28_number_of_nodes_per_az_must_be_divisable_by_number_of_az(monkeypatch):
    monkeypatch.setenv('SCT_N_DB_NODES', '3 3 2')
    monkeypatch.setenv('SCT_REGION_NAME', 'eu-west-1 eu-west-2 us-east-1')
    monkeypatch.setenv('SCT_AVAILABILITY_ZONE', 'a,b,c')
    monkeypatch.setenv('SCT_CLUSTER_BACKEND', 'aws')
    monkeypatch.setenv('SCT_AMI_ID_DB_SCYLLA', "ami-dummy ami-dummy ami-dummy")
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    assert "should be divisible by number of availability zones" in str(context.value)


def test_35_append_str_options(monkeypatch):
    monkeypatch.setenv('SCT_APPEND_SCYLLA_ARGS', '++ --overprovisioned 5')

    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.get('append_scylla_args') == (
        '--blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 '
        '--abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 '
        '--enable-sstable-key-validation 1 --overprovisioned 5'
    )


def test_35_append_list_options(monkeypatch):
    monkeypatch.setenv('SCT_SCYLLA_D_OVERRIDES_FILES', '["++", "extra_file/scylla.d/io.conf"]')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    assert conf.get('scylla_d_overrides_files') == ["extra_file/scylla.d/io.conf"]


def test_35_append_unsupported_list_options(monkeypatch):
    monkeypatch.setenv('SCT_AZURE_REGION_NAME', '["++", "euwest"]')
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert 'Option azure_region_name is not appendable' == str(context.value)


def test_35_append_unsupported_str_options(monkeypatch):
    monkeypatch.setenv('SCT_MANAGER_VERSION', '++ new version')
    with pytest.raises(AssertionError) as context:
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
    assert 'Option manager_version is not appendable' == str(context.value)


def test_36_update_config_based_on_version():
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()
    conf.scylla_version = "2025.1.0~dev"
    conf.is_enterprise = True
    conf.update_config_based_on_version()


def test_37_validates_single_thread_count_for_all_throttle_steps(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"read": 620, "write": [630], "mixed": [500]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS",
                       '{"read": ["unthrottled", "unthrottled"], "mixed": [100, "unthrottled"], "write": [200, "unthrottled"]}')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_raises_error_for_mismatched_thread_and_throttle_step_counts(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"read": [620, 630]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"read": ["100", "200", "unthrottled"]}')
    with pytest.raises(ValueError):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()


def test_37_handles_empty_performance_throughput_parameters(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", "{}")
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", "{}")
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_validates_multiple_thread_counts_per_throttle_step(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"write": [400, 420, 440, 460, 480]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"write": ["100", "200", "300", "400", "unthrottled"]}')
    conf = sct_config.SCTConfiguration()
    conf.verify_configuration()


def test_37_raises_error_for_invalid_thread_count_type(monkeypatch):
    monkeypatch.setenv("SCT_PERF_GRADUAL_THREADS", '{"mixed": ["invalid_type"]}')
    monkeypatch.setenv("SCT_PERF_GRADUAL_THROTTLE_STEPS", '{"mixed": ["unthrottled"]}')
    with pytest.raises(ValueError):
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

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
import itertools
import unittest
from collections import namedtuple
import pytest
from sdcm import sct_config
from sdcm.utils import loader_utils
from sdcm.utils.common import get_latest_scylla_release
from sdcm.utils.aws_utils import get_ssm_ami

RPM_URL = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
          '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
          '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'

# pylint: disable=no-self-use


class ConfigurationTests(unittest.TestCase):  # pylint: disable=too-many-public-methods
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('anyconfig').setLevel(logging.ERROR)
        cls.clear_sct_env_variables()
        cls.setup_default_env()

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        cls.conf = sct_config.SCTConfiguration()

        cls.clear_sct_env_variables()

        # some of the tests assume this basic case is define, to avoid putting this again and again in each test
        # and so we can run those tests specifically
        cls.setup_default_env()

    def tearDown(self):
        self.clear_sct_env_variables()
        self.setup_default_env()

    @classmethod
    def setup_default_env(cls):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_USE_MGMT'] = 'false'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

    @classmethod
    def clear_sct_env_variables(cls):
        for k in os.environ:
            if k.startswith('SCT_'):
                del os.environ[k]

    def test_01_dump_config(self):
        logging.debug(self.conf.dump_config())

    def test_02_verify_config(self):
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'

        self.conf.verify_configuration()
        self.conf.check_required_files()

    def test_03_dump_help_config_yaml(self):
        logging.debug(self.conf.dump_help_config_yaml())

    def test_03_dump_help_config_markdown(self):  # pylint: disable=invalid-name
        logging.debug(self.conf.dump_help_config_markdown())

    @pytest.mark.integration
    def test_04_check_env_parse(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = '["eu-west-1", "us-east-1"]'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i4i.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy ami-dummy2'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-dummy ami-dummy2')

    def test_05_docker(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_USE_MGMT'] = 'false'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertIn('docker_image', conf.dump_config())
        self.assertEqual(conf.get('docker_image'), 'scylladb/scylla')

    def test_06a_docker_latest_no_loader(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_USE_MGMT'] = 'false'
        os.environ['SCT_SCYLLA_VERSION'] = 'latest'
        os.environ['SCT_N_LOADERS'] = "0"
        docker_tag_after_processing = "fake_specific_docker_tag"

        with unittest.mock.patch.object(
                sct_config, 'get_specific_tag_of_docker_image',
                return_value=docker_tag_after_processing):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        assert conf['scylla_version'] != 'latest'

    def test_06b_docker_development(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_USE_MGMT'] = 'false'
        os.environ['SCT_SCYLLA_VERSION'] = '666.development-blah'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_07_baremetal_exception(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        conf = sct_config.SCTConfiguration()
        self.assertRaises(AssertionError, conf.verify_configuration)

    def test_08_baremetal(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        os.environ['SCT_DB_NODES_PRIVATE_IP'] = '["1.2.3.4", "1.2.3.5"]'
        os.environ['SCT_DB_NODES_PUBLIC_IP'] = '["1.2.3.4", "1.2.3.5"]'
        os.environ['SCT_USE_PREINSTALLED_SCYLLA'] = 'true'
        os.environ['SCT_S3_BAREMETAL_CONFIG'] = "some_config"
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('db_nodes_private_ip', conf.dump_config())
        self.assertEqual(conf.get('db_nodes_private_ip'), ["1.2.3.4", "1.2.3.5"])

    def test_09_unknown_configure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        conf = sct_config.SCTConfiguration()
        self.assertRaises(ValueError, conf.verify_configuration)

    def test_09_unknown_env(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        os.environ['SCT_WHAT_IS_THAT'] = 'just_made_this_up'
        os.environ['SCT_WHAT_IS_THAT_2'] = 'what is this ?'
        conf = sct_config.SCTConfiguration()
        with self.assertRaises(ValueError) as context:
            conf.verify_configuration()

        self.assertIn('SCT_WHAT_IS_THAT_2=what is this ?', str(context.exception))
        self.assertIn('SCT_WHAT_IS_THAT=just_made_this_up', str(context.exception))

    @pytest.mark.integration
    def test_10_longevity(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/complex_test_case_with_version.yaml'
        os.environ['SCT_AMI_ID_DB_SCYLLA_DESC'] = 'master'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('user_prefix'), 'longevity-50gb-4d-not-jenkins-maste')

    @pytest.mark.integration
    def test_10_mananger_regression(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy ami-dummy2'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_12_scylla_version_ami(self):
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        amis = conf.get('ami_id_db_scylla').split()
        assert len(amis) == 2
        assert all(ami.startswith('ami-') for ami in amis)

    def test_12_scylla_version_ami_case1(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_12_scylla_version_ami_case2(self):  # pylint: disable=invalid-name
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        self.assertRaisesRegex(
            ValueError, "AMIs for scylla_version='99.0.3' not found in eu-west-1", sct_config.SCTConfiguration)

    @pytest.mark.integration
    def test_12_scylla_version_repo(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_12_scylla_version_repo_case1(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_12_scylla_version_repo_case2(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'

        self.assertRaisesRegex(
            ValueError, r"AMIs for scylla_version='99.0.3' not found in eu-west-1 arch=x86_64", sct_config.SCTConfiguration)

    def test_12_scylla_version_repo_ubuntu(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_GCE_IMAGE_DB'] = (
            'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')

        expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         expected_repo)

    def test_12_scylla_version_repo_ubuntu_loader_centos(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'centos'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_GCE_IMAGE_DB'] = (
            'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7')

        expected_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list'
        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value="4.7.dev", clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=expected_repo, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         expected_repo)

    def test_12_k8s_scylla_version_ubuntu_loader_centos(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'k8s-local-kind'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'centos'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertFalse(conf.get('scylla_repo'))

    @pytest.mark.integration
    def test_13_scylla_version_ami_branch_latest(self):  # pylint: disable=invalid-name
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'branch-5.2:latest'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        amis = conf.get('ami_id_db_scylla').split()
        assert len(amis) == 2
        assert all(ami.startswith('ami-') for ami in amis)

    def test_conf_check_required_files(self):  # pylint: disable=no-self-use
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        ami_id = get_ssm_ami(
            '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id', region_name='eu-west-1')
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = ami_id
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ['SCT_STRESS_CMD'] = """cassandra-stress user profile=/tmp/cs_profile_background_reads_overload.yaml \
            ops'(insert=100)' no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore"""
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.check_required_files()

    def test_conf_check_required_files_negative(self):  # pylint: disable=no-self-use
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/stress_cmd_with_bad_profile.yaml'
        os.environ['SCT_STRESS_CMD'] = """cassandra-stress user profile=/tmp/1232123123123123123.yaml ops'(insert=100)'\
            no-warmup cl=ONE duration=10m -mode cql3 native -rate threads=3000 -errors ignore"""
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        try:
            conf.check_required_files()
        except ValueError as exc:
            self.assertEqual(str(
                exc), "Stress command parameter \'stress_cmd\' contains profile \'/tmp/1232123123123123123.yaml\' that"
                      " does not exists under data_dir/")

    def test_config_dupes(self):
        def get_dupes(c):
            '''sort/tee/izip'''

            # pylint: disable=invalid-name
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

        self.assertListEqual(list(get_dupes(opts)), [])

    @pytest.mark.integration
    def test_13_bool(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_STORE_PERF_RESULTS'] = 'False'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = sct_config.SCTConfiguration()

        self.assertEqual(conf['store_perf_results'], False)

    @pytest.mark.integration
    def test_14_aws_siren_from_env(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_DB_TYPE'] = 'cloud_scylla'
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_N_DB_NODES'] = '2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i4i.large'
        os.environ['SCT_AUTHENTICATOR_USER'] = "user"
        os.environ['SCT_AUTHENTICATOR_PASSWORD'] = "pass"
        os.environ['SCT_CLOUD_CLUSTER_ID'] = "193712947904378"
        os.environ['SCT_SECURITY_GROUP_IDS'] = "sg-89fi3rkl"
        os.environ['SCT_SUBNET_ID'] = "sub-d32f09sdf"

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('cloud_cluster_id'), 193712947904378)
        assert conf["authenticator_user"] == "user"
        assert conf["authenticator_password"] == "pass"
        assert conf["security_group_ids"] == ["sg-89fi3rkl"]
        assert conf["subnet_id"] == ["sub-d32f09sdf"]

    def test_15_new_scylla_repo(self):
        centos_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/' \
                      '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1' \
                      '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla' \
                      '.repo'

        os.environ.pop('SCT_SCYLLA_VERSION', None)
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_NEW_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_USER_PREFIX'] = 'testing'
        os.environ[
            'SCT_GCE_IMAGE_DB'] = 'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7'

        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='2019.1.1', clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access
        self.assertEqual(conf.get('target_upgrade_version'), '2019.1.1')

    def test_15a_new_scylla_repo_by_scylla_version(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_NEW_VERSION'] = 'master:latest'
        os.environ['SCT_USER_PREFIX'] = 'testing'
        os.environ['SCT_GCE_IMAGE_DB'] = \
            'https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/family/centos-7'

        resolved_repo_link = 'https://s3.amazonaws.com/downloads.scylladb.com/unstable/scylla/master/rpm\
            /centos/2021-06-09T13:12:44Z/scylla.repo'

        with unittest.mock.patch.object(sct_config, 'get_branch_version', return_value='666.development', clear=True), \
                unittest.mock.patch.object(sct_config, 'find_scylla_repo', return_value=resolved_repo_link, clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access

        self.assertEqual(conf.get('scylla_repo'), resolved_repo_link)
        target_upgrade_version = conf.get('target_upgrade_version')
        self.assertTrue(target_upgrade_version == '666.development' or target_upgrade_version.endswith(".dev"))

    def test_15b_image_id_by_scylla_version(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_USER_PREFIX'] = 'testing'
        os.environ['SCT_GCE_IMAGE_DB'] = ''
        os.environ['SCT_USE_PREINSTALLED_SCYLLA'] = 'true'

        resolved_image_link = 'https://www.googleapis.com/compute/v1/projects/scylla-images/global/images/' \
                              'scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359'
        image = namedtuple("Image", "name self_link")(name='scylla-4-7-dev-0-20220113-8bcd23fa0-1-build-359',
                                                      self_link=resolved_image_link)

        with unittest.mock.patch.object(sct_config, 'get_branched_gce_images', return_value=[image], clear=True):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf._get_target_upgrade_version()  # pylint: disable=protected-access

        self.assertEqual(conf.get('gce_image_db'), resolved_image_link)

    @pytest.mark.integration
    def test_16_default_oracle_scylla_version_eu_west_1(self):
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert 'ami-' in conf.get('ami_id_db_oracle')

    @pytest.mark.integration
    def test_16_oracle_scylla_version_us_east_1(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf.get('ami_id_db_oracle').startswith('ami-')

    @pytest.mark.integration
    def test_16_oracle_scylla_version_eu_west_1(self):
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf.get('ami_id_db_oracle').startswith('ami-')

    @pytest.mark.integration
    def test_16_oracle_scylla_version_and_oracle_ami_together(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        os.environ['SCT_AMI_ID_DB_ORACLE'] = 'ami-dummy'
        os.environ["SCT_DB_TYPE"] = "mixed_scylla"
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        with self.assertRaises(ValueError) as context:
            sct_config.SCTConfiguration()

        self.assertIn("'oracle_scylla_version' and 'ami_id_db_oracle' can't used together", str(context.exception))

    def test_17_verify_scylla_bench_required_parameters_in_command(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf["stress_cmd"], [os.environ['SCT_STRESS_CMD']])
        self.assertEqual(conf["stress_read_cmd"], [os.environ["SCT_STRESS_READ_CMD"]])

    @pytest.mark.integration
    def test_17_1_raise_error_if_scylla_bench_command_dont_have_workload(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -mode=read -replication-factor=3 -partition-count=100"

        err_msg = f"Scylla-bench command {os.environ['SCT_STRESS_CMD']} doesn't have parameter -workload"

        with self.assertRaises(ValueError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn(err_msg, str(context.exception))

    @pytest.mark.integration
    def test_17_2_raise_error_if_scylla_bench_command_dont_have_mode(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_STRESS_CMD'] = "scylla-bench -workload=sequential -mode=write -replication-factor=3 -partition-count=100"
        os.environ["SCT_STRESS_READ_CMD"] = "scylla-bench -workload=uniform -replication-factor=3 -partition-count=100"

        err_msg = f"Scylla-bench command {os.environ['SCT_STRESS_READ_CMD']} doesn't have parameter -mode"

        with self.assertRaises(ValueError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn(err_msg, str(context.exception))

    @pytest.mark.integration
    def test_18_error_if_no_version_repo_ami_selected(self):
        os.environ.pop('SCT_AMI_ID_DB_SCYLLA', None)
        os.environ.pop('SCT_SCYLLA_VERSION', None)
        for backend in sct_config.SCTConfiguration.available_backends:
            if 'k8s' in backend:
                continue
            if 'siren' in backend:
                continue

            os.environ['SCT_CLUSTER_BACKEND'] = backend
            os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

            conf = sct_config.SCTConfiguration()
            with self.assertRaises(AssertionError, msg=f"{backend} didn't failed") as context:
                conf.verify_configuration()
            self.assertIn("scylla version/repos wasn't configured correctly", str(context.exception), )

            self.clear_sct_env_variables()

    def test_18_error_if_no_version_repo_ami_selected_siren(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_DB_TYPE'] = 'cloud_scylla'

        # 1) check that SCT_CLOUD_CLUSTER_ID is used for the exception
        conf = sct_config.SCTConfiguration()

        with self.assertRaises(AssertionError, msg="cloud_scylla didn't failed") as context:
            conf.verify_configuration()
        self.assertIn("scylla version/repos wasn't configured correctly", str(context.exception))
        self.assertIn("SCT_CLOUD_CLUSTER_ID", str(context.exception))

        # 2) check that when SCT_CLOUD_CLUSTER_ID is defined, the verification works
        os.environ['SCT_CLOUD_CLUSTER_ID'] = '1234'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    def test_19_aws_region_with_no_region_data(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'not-exists-2'

        with self.assertRaisesRegex(ValueError, expected_regex="not-exists-2 isn't supported"):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

    @pytest.mark.integration
    def test_20_user_data_format_version_aws(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        os.environ['SCT_ORACLE_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        os.environ['SCT_DB_TYPE'] = 'mixed_scylla'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')
        self.assertEqual(conf['oracle_user_data_format_version'], '3')

    @pytest.mark.integration
    def test_20_user_data_format_version_aws_2(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'

        ami_id = get_ssm_ami(
            '/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id', region_name='eu-west-1')
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = ami_id  # run image which isn't scylla
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')
        self.assertNotIn('oracle_user_data_format_version', conf)

    @pytest.mark.integration
    def test_20_user_data_format_version_gce_1(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')

    @pytest.mark.integration
    def test_20_user_data_format_version_gce_2(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '3')

    @pytest.mark.integration
    def test_20_user_data_format_version_gce_3(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_GCE_IMAGE_DB'] = ('https://www.googleapis.com/compute/v1/projects/'
                                          'ubuntu-os-cloud/global/images/family/ubuntu-2004-lts')
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        self.assertEqual(conf['user_data_format_version'], '2')

    @pytest.mark.integration
    def test_20_user_data_format_version_azure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'azure'
        os.environ['SCT_AZURE_REGION_NAME'] = 'eastus'
        os.environ['SCT_SCYLLA_VERSION'] = 'master:latest'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.verify_configuration_urls_validity()
        # since azure image listing is still buggy, can be sure which version we'll get
        # I would expect master:latest to be version 3 now, but azure.utils.get_scylla_images
        # returns something from 5 months ago.
        self.assertIn('user_data_format_version', conf)

    def test_21_nested_values(self):
        os.environ['SCT_CONFIG_FILES'] = ('["internal_test_data/minimal_test_case.yaml", '
                                          '"unit_tests/test_data/stress_image_extra_config.yaml"]')
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-1234'
        os.environ["SCT_STRESS_READ_CMD.0"] = "cassandra_stress"
        os.environ["SCT_STRESS_READ_CMD.1"] = "cassandra_stress"

        os.environ["SCT_STRESS_IMAGE"] = '{"ycsb": "scylladb/something_else"}'
        os.environ["SCT_STRESS_IMAGE.cassandra-stress"] = "scylla-bench"
        os.environ['SCT_STRESS_IMAGE.scylla-bench'] = "scylladb/something"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        stress_image = conf.get('stress_image')
        self.assertEqual(stress_image['ndbench'], 'scylladb/hydra-loaders:ndbench-jdk8-A')
        self.assertEqual(stress_image['nosqlbench'], 'scylladb/hydra-loaders:nosqlbench-A')

        self.assertEqual(conf.get('stress_image.scylla-bench'), 'scylladb/something')
        self.assertEqual(conf.get('stress_image.non-exist'), None)

        self.assertEqual(conf.get('stress_read_cmd'), ['cassandra_stress', 'cassandra_stress'])

    def test_22_get_none(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = "internal_test_data/minimal_test_case.yaml"
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-1234'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        val = conf.get(None)
        assert val is None

    def test_23_1_include_nemesis_selector(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/minimal_test_case.yaml", \
                                             "internal_test_data/nemesis_selector_list.yaml"]'''

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf["nemesis_selector"] == "config_changes and topology_changes"

    def test_23_2_nemesis_include_selector_list(self):

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/minimal_test_case.yaml", \
                                             "internal_test_data/nemesis_selector_list_of_list.yaml"]'''
        os.environ['SCT_NEMESIS_CLASS_NAME'] = "NemesisClass:1 NemesisClass:2"
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf["nemesis_selector"] == ["config_changes and topology_changes", "topology_changes", "disruptive"]

    def test_26_run_fullscan_params_validtion_positive(self):
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/minimal_test_case.yaml", \
                                                   "internal_test_data/positive_fullscan_param.yaml"]'''
        sct_config.SCTConfiguration()

    def test_27_run_fullscan_params_validtion_negative(self):
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/minimal_test_case.yaml", \
                                                      "internal_test_data/negative_fullscan_param.yaml"]'''
        try:
            sct_config.SCTConfiguration()
        except ValueError as exp:
            assert str(exp) == "Config params validation errors:\n\tfield 'mode' must be one of " \
                "'('random', 'table', 'partition', 'aggregate', 'table_and_aggregate')' " \
                "but got 'agggregate'\n\t" \
                "field 'ks_cf' must be an instance of <class 'str'>, but got '1'\n\t" \
                "field 'validate_data' must be an instance of <class 'bool'>, but got 'no'\n\t" \
                "field 'full_scan_aggregates_operation_limit' must be an instance of <class 'int'>, but got 'a'"

    def test_28_number_of_nodes_per_az_must_be_divisable_by_number_of_az(self):
        os.environ['SCT_N_DB_NODES'] = '3 3 2'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1 eu-west-2 us-east-1'
        os.environ['SCT_AVAILABILITY_ZONE'] = 'a,b,c'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = "['ami-dummy', 'ami-dummy', 'ami-dummy']"
        with self.assertRaises(AssertionError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        self.assertIn("should be divisible by number of availability zones", str(context.exception))

    def test_29_number_of_nodes_per_az_may_not_be_divisable_by_number_of_az_on_k8s(self):
        os.environ['SCT_N_DB_NODES'] = '3'
        os.environ['SCT_REGION_NAME'] = 'eu-north-1'
        os.environ['SCT_AVAILABILITY_ZONE'] = 'b,c'
        os.environ['SCT_CLUSTER_BACKEND'] = 'k8s-eks'
        os.environ['SCT_SCYLLA_VERSION'] = "5.2.0"
        os.environ['SCT_K8S_SCYLLA_OPERATOR_HELM_REPO'] = (
            'https://storage.googleapis.com/scylla-operator-charts/latest')
        os.environ['SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION'] = 'latest'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('n_db_nodes'), 3)
        self.assertEqual(conf.get('availability_zone'), 'b,c')

    def test_30_cs_profile_parse(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = '["eu-west-1"]'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i4i.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-dummy'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/cs_user_profile.yaml'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        user_profiles, duration_per_cs_profile = \
            loader_utils.LoaderUtilsMixin.parse_cs_user_profiles_param(conf["prepare_cs_user_profiles"])
        self.assertEqual(user_profiles,
                         ['scylla-qa-internal/custom_d1/rolling_upgrade_dataset.yaml',
                          'scylla-qa-internal/custom_d1/rolling_upgrade_dataset1.yaml'])
        self.assertEqual(duration_per_cs_profile, ['30m', '20m'])

        user_profiles, duration_per_cs_profile = loader_utils.LoaderUtilsMixin.parse_cs_user_profiles_param(
            conf["cs_user_profiles"])
        self.assertEqual(user_profiles,
                         ['scylla-qa-internal/custom_d1/rolling_upgrade_dataset.yaml',
                          'scylla-qa-internal/custom_d1/rolling_upgrade_dataset1.yaml'])
        self.assertEqual(duration_per_cs_profile, ['60m', '20m'])

    @pytest.mark.integration
    def test_31_check_network_config_aws(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'eu-west-1'
        os.environ['SCT_N_DB_NODES'] = '2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i4i.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-06f919eb'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('scylla_network_config'),
                         [{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False,
                           'nic': 0},
                          {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}])

    @pytest.mark.integration
    def test_31_check_network_config_aws_interface_not_defined(self):
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/network_config_interface_not_defined.yaml"]'''

        with self.assertRaises(ValueError) as context:
            sct_config.SCTConfiguration()

        self.assertEqual("Interface address(es) were not defined: rpc_address", str(context.exception))

    @pytest.mark.integration
    def test_31_check_network_config_aws_interface_param_not_defined(self):
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/network_config_interface_param_not_defined.yaml"]'''

        with self.assertRaises(ValueError) as context:
            sct_config.SCTConfiguration()

        self.assertEqual(
            "'public' parameter value for first address is not defined. It is must parameter", str(context.exception))

    @pytest.mark.integration
    def test_31_check_network_config_aws_interface_param_public_not_primary(self):
        os.environ['SCT_CONFIG_FILES'] = '''["internal_test_data/network_config_interface_param_public_not_primary.yaml"]'''

        with self.assertRaises(ValueError) as context:
            sct_config.SCTConfiguration()

        self.assertEqual(
            "If ipv4 and public is True it has to be primary network interface, it means device index (nic) is 0", str(context.exception))

    @pytest.mark.integration
    def test_31_check_network_config_gce(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('scylla_network_config'), None)

    @pytest.mark.integration
    def test_31_check_network_config_azure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'azure'
        os.environ['SCT_AZURE_REGION_NAME'] = 'eastus'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('scylla_network_config'), None)

    @pytest.mark.integration
    def test_31_check_network_config_docker(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_USE_MGMT'] = 'false'
        os.environ['SCT_SCYLLA_VERSION'] = get_latest_scylla_release(product='scylla')

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('scylla_network_config'), None)

    @pytest.mark.integration
    def test_31_check_network_config_aws_siren(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_DB_TYPE'] = 'cloud_scylla'
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_N_DB_NODES'] = '2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i4i.large'
        os.environ['SCT_AUTHENTICATOR_USER'] = "user"
        os.environ['SCT_AUTHENTICATOR_PASSWORD'] = "pass"
        os.environ['SCT_CLOUD_CLUSTER_ID'] = "193712947904378"
        os.environ['SCT_SECURITY_GROUP_IDS'] = "sg-89fi3rkl"
        os.environ['SCT_SUBNET_ID'] = "sub-d32f09sdf"

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        # TODO: longevity with Siren backend does not work now.
        #  We will need to validate if this configuration should be supported there
        self.assertEqual(conf.get('scylla_network_config'),
                         [{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False,
                           'nic': 0},
                          {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}])

    @pytest.mark.integration
    def test_31_check_network_config_k8s(self):
        os.environ['SCT_N_DB_NODES'] = '3'
        os.environ['SCT_REGION_NAME'] = 'eu-north-1'
        os.environ['SCT_AVAILABILITY_ZONE'] = 'b,c'
        os.environ['SCT_CLUSTER_BACKEND'] = 'k8s-eks'
        os.environ['SCT_SCYLLA_VERSION'] = "5.2.0"
        os.environ['SCT_K8S_SCYLLA_OPERATOR_HELM_REPO'] = (
            'https://storage.googleapis.com/scylla-operator-charts/latest')
        os.environ['SCT_K8S_SCYLLA_OPERATOR_CHART_VERSION'] = 'latest'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('scylla_network_config'),
                         [{'address': 'listen_address', 'listen_all': False, 'ip_type': 'ipv4', 'public': False, 'use_dns': False,
                           'nic': 0},
                          {'address': 'rpc_address', 'listen_all': False, 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_rpc_address', 'ip_type': 'ipv4',
                              'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'broadcast_address', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0},
                          {'address': 'test_communication', 'ip_type': 'ipv4', 'public': False, 'use_dns': False, 'nic': 0}])

    @pytest.mark.integration
    def test_32_resolve_aws_ssm_links(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'resolve:ssm:/aws/service/debian/release/11/latest/amd64'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf["ami_id_db_scylla"].startswith('ami-')

    @pytest.mark.integration
    def test_33_resolve_aws_by_owner(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'resolve:owner:131827586825/x86_64/OL8.*'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

        assert conf["ami_id_db_scylla"].startswith('ami-')

    @pytest.mark.integration
    def test_34_nemesis_grow_shrink_instance_type_aws(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = '["eu-west-1", "us-east-1"]'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_SCYLLA_VERSION'] = "6.0.0"
        os.environ['SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE'] = 'i4i.xlarge'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_34_nemesis_grow_shrink_instance_type_gcp(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_GCE_DATACENTER'] = 'us-east1 us-west1'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_SCYLLA_VERSION'] = "6.0.0"
        os.environ['SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE'] = 'n2-highmem-32'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @pytest.mark.integration
    def test_34_nemesis_grow_shrink_instance_type_azure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'azure'
        os.environ['SCT_AZURE_REGION_NAME'] = 'eastus'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_SCYLLA_VERSION'] = "6.0.0"
        os.environ['SCT_NEMESIS_GROW_SHRINK_INSTANCE_TYPE'] = 'Standard_L8s_v3'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()

    @staticmethod
    def test_35_append_str_options():
        os.environ['SCT_APPEND_SCYLLA_ARGS'] = '++ --overprovisioned 5'
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        assert conf.get('append_scylla_args') == ('--blocked-reactor-notify-ms 25 --abort-on-lsa-bad-alloc 1 '
                                                  '--abort-on-seastar-bad-alloc --abort-on-internal-error 1 --abort-on-ebadf 1 '
                                                  '--enable-sstable-key-validation 1 --overprovisioned 5')

    @staticmethod
    def test_35_append_list_options():
        os.environ[
            'SCT_SCYLLA_D_OVERRIDES_FILES'] = '["++", "extra_file/scylla.d/io.conf"]'

        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        assert conf.get('scylla_d_overrides_files') == ["extra_file/scylla.d/io.conf"]

    @staticmethod
    def test_35_append_unsupported_list_options():
        os.environ['SCT_AZURE_REGION_NAME'] = '["++", "euwest"]'

        with pytest.raises(AssertionError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        assert 'Option azure_region_name is not appendable' == str(context.value)

    @staticmethod
    def test_35_append_unsupported_str_options():
        os.environ['SCT_MANAGER_VERSION'] = '++ new version'

        with pytest.raises(AssertionError) as context:
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()

        assert 'Option manager_version is not appendable' == str(context.value)

    @staticmethod
    def test_36_update_config_based_on_version():
        conf = sct_config.SCTConfiguration()
        conf.verify_configuration()
        conf.scylla_version = "2025.1.0~dev"
        conf.is_enterprise = True
        conf.update_config_based_on_version()


if __name__ == "__main__":
    unittest.main()

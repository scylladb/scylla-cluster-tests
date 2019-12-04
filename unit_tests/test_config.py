from __future__ import absolute_import
import os
import logging
import unittest
import itertools

from sdcm.sct_config import SCTConfiguration


class ConfigurationTests(unittest.TestCase):  # pylint: disable=too-many-public-methods
    @classmethod
    def setUpClass(cls):
        logging.basicConfig(level=logging.ERROR)
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        logging.getLogger('anyconfig').setLevel(logging.ERROR)

        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

        cls.conf = SCTConfiguration()

        for k, _ in os.environ.items():
            if k.startswith('SCT_'):
                del os.environ[k]

    def tearDown(self):
        for k, _ in os.environ.items():
            if k.startswith('SCT_'):
                del os.environ[k]
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/minimal_test_case.yaml'

    def test_01_dump_config(self):
        logging.debug(self.conf.dump_config())

    def test_02_verify_config(self):
        self.conf.verify_configuration()

    def test_03_dump_help_config_yaml(self):
        logging.debug(self.conf.dump_help_config_yaml())

    def test_03_dump_help_config_markdown(self):  # pylint: disable=invalid-name
        logging.debug(self.conf.dump_help_config_markdown())

    def test_04_check_env_parse(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = '["eu-west-1", "us-east-1"]'
        os.environ['SCT_N_DB_NODES'] = '2 2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb ami-b4f8b4cb'

        conf = SCTConfiguration()
        conf.verify_configuration()
        conf.dump_config()

        self.assertEqual(conf.get('security_group_ids'), 'sg-059a7f66a947d4b5c sg-c5e1f7a0')

    def test_05_docker(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        conf = SCTConfiguration()
        conf.verify_configuration()
        self.assertIn('docker_image', conf.dump_config())
        self.assertEqual(conf.get('docker_image'), 'scylladb/scylla')

    def test_07_baremetal_exception(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        conf = SCTConfiguration()
        self.assertRaises(AssertionError, conf.verify_configuration)

    def test_08_baremetal(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'baremetal'
        os.environ['SCT_DB_NODES_PRIVATE_IP'] = '["1.2.3.4", "1.2.3.5"]'
        os.environ['SCT_DB_NODES_PUBLIC_IP'] = '["1.2.3.4", "1.2.3.5"]'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('db_nodes_private_ip', conf.dump_config())
        self.assertEqual(conf.get('db_nodes_private_ip'), ["1.2.3.4", "1.2.3.5"])

    def test_09_unknown_configure(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        conf = SCTConfiguration()
        self.assertRaises(ValueError, conf.verify_configuration)

    def test_09_unknown_env(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/unknown_param_in_config.yaml'
        os.environ['SCT_WHAT_IS_THAT'] = 'just_made_this_up'
        os.environ['SCT_WHAT_IS_THAT_2'] = 'what is this ?'
        conf = SCTConfiguration()
        with self.assertRaises(ValueError) as context:
            conf.verify_configuration()

        self.assertIn('SCT_WHAT_IS_THAT_2=what is this ?', str(context.exception))
        self.assertIn('SCT_WHAT_IS_THAT=just_made_this_up', str(context.exception))

    def test_10_longevity(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/complex_test_case_with_version.yaml'
        os.environ['SCT_AMI_ID_DB_SCYLLA_DESC'] = 'master'
        conf = SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('user_prefix'), 'longevity-50gb-4d-not-jenkins-maste')

    @staticmethod
    def test_10_mananger_regression():
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb ami-b4f8b4cb'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_ami(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-0f1aa8afb878fed2b ami-027c1337dcb46da50')

    @staticmethod
    def test_12_scylla_version_ami_case1():  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb ami-b4f8b4cb'

        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_ami_case2(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        self.assertRaisesRegex(ValueError, r"AMI for scylla version 99.0.3 wasn't found", SCTConfiguration)

    @staticmethod
    def test_12_scylla_version_repo():
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'

        conf = SCTConfiguration()
        conf.verify_configuration()

    @staticmethod
    def test_12_scylla_version_repo_case1():  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

        conf = SCTConfiguration()
        conf.verify_configuration()

    def test_12_scylla_version_repo_case2(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_VERSION'] = '99.0.3'

        self.assertRaisesRegex(ValueError, r"repo for scylla version 99.0.3 wasn't found", SCTConfiguration)

    def test_12_scylla_version_repo_ubuntu(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list")
        self.assertEqual(conf.get('scylla_repo_loader'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list")

    def test_12_scylla_version_repo_ubuntu_loader_centos(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_SCYLLA_LINUX_DISTRO'] = 'ubuntu-xenial'
        os.environ['SCT_SCYLLA_LINUX_DISTRO_LOADER'] = 'centos'
        os.environ['SCT_SCYLLA_VERSION'] = '3.0.3'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertIn('scylla_repo', conf.dump_config())
        self.assertEqual(conf.get('scylla_repo'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/deb/ubuntu/scylla-3.0-xenial.list")
        self.assertEqual(conf.get('scylla_repo_loader'),
                         "https://s3.amazonaws.com/downloads.scylladb.com/rpm/centos/scylla-3.0.repo")

    def test_13_scylla_version_ami_branch(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'branch-3.1:9'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertEqual(conf.get('ami_id_db_scylla'), 'ami-00b5ac462978479c9 ami-0d911e53781d8a542')

    def test_13_scylla_version_ami_branch_latest(self):  # pylint: disable=invalid-name
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_SCYLLA_VERSION'] = 'branch-3.1:latest'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()
        conf.verify_configuration()

        self.assertIsNotNone(conf.get('ami_id_db_scylla'))
        self.assertEqual(len(conf.get('ami_id_db_scylla').split(' ')), 2)

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

        opts = [o['name'] for o in SCTConfiguration.config_options]

        self.assertListEqual(list(get_dupes(opts)), [])

    def test_13_bool(self):

        os.environ['SCT_CLUSTER_BACKEND'] = 'docker'
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_STORE_RESULTS_IN_ELASTICSEARCH'] = 'False'
        os.environ['SCT_CONFIG_FILES'] = 'internal_test_data/multi_region_dc_test_case.yaml'
        conf = SCTConfiguration()

        self.assertEqual(conf['store_results_in_elasticsearch'], False)

    def test_14_(self):
        os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
        os.environ['SCT_REGION_NAME'] = 'us-east-1'
        os.environ['SCT_N_DB_NODES'] = '2'
        os.environ['SCT_INSTANCE_TYPE_DB'] = 'i3.large'
        os.environ['SCT_AMI_ID_DB_SCYLLA'] = 'ami-b4f8b4cb'

        conf = SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('security_group_ids'), 'sg-c5e1f7a0')

    def test_15_new_scylla_repo(self):
        centos_repo = 'https://s3.amazonaws.com/downloads.scylladb.com/enterprise/rpm/unstable/centos/'\
                      '9f724fedb93b4734fcfaec1156806921ff46e956-2bdfa9f7ef592edaf15e028faf3b7f695f39ebc1'\
                      '-525a0255f73d454f8f97f32b8bdd71c8dec35d3d-a6b2b2355c666b1893f702a587287da978aeec22/71/scylla.repo'

        os.environ['SCT_CLUSTER_BACKEND'] = 'gce'
        os.environ['SCT_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_NEW_SCYLLA_REPO'] = centos_repo
        os.environ['SCT_USER_PREFIX'] = 'testing'

        conf = SCTConfiguration()
        conf.verify_configuration()
        self.assertEqual(conf.get('target_upgrade_version'), '2019.1.1')


if __name__ == "__main__":
    unittest.main()

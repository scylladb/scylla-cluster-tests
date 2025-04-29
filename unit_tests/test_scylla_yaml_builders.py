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
# Copyright (c) 2021 ScyllaDB

import contextlib
import json
import os
import tempfile
import unittest
from typing import List
from unittest.mock import patch

from parameterized import parameterized

from sdcm.cluster import BaseNode, BaseCluster, BaseScyllaCluster
from sdcm.provision.network_configuration import NetworkInterface, ScyllaNetworkConfiguration, ssh_connection_ip_type
from sdcm.provision.scylla_yaml import ScyllaYamlNodeAttrBuilder, ScyllaYamlClusterAttrBuilder, ScyllaYaml
from sdcm.provision.scylla_yaml.auxiliaries import ScyllaYamlAttrBuilderBase
from sdcm.sct_config import SCTConfiguration
from sdcm.test_config import TestConfig

from sdcm.utils.distro import Distro


__builtin__ = [].__class__.__module__


def is_builtin(inst):
    return inst.__module__ == __builtin__


class FakeCluster:
    seed_nodes_addresses = ['1.1.1.1']


class TestConfigWithLdap:
    LDAP_ADDRESS = '1.1.1.1', '389'
    IP_SSH_CONNECTIONS = "private"


class TestConfigWithoutLdap:
    LDAP_ADDRESS = None
    IP_SSH_CONNECTIONS = "private"


class FakeNode:
    parent_cluster = FakeCluster()
    public_ip_address = '2.2.2.2'
    private_ip_address = '1.1.1.1'
    scylla_network_configuration = None


class ScyllaYamlClusterAttrBuilderBase(unittest.TestCase):
    builder_class: ScyllaYamlAttrBuilderBase

    def _run_test(self, builder_params: dict, expected_as_dict: dict = None):
        instance = self.builder_class(**builder_params)
        resulted_as_dict = instance.dict(exclude_defaults=True, exclude_unset=True)
        if expected_as_dict:
            assert resulted_as_dict == expected_as_dict
            ScyllaYaml(**resulted_as_dict)


class ScyllaYamlClusterAttrBuilderTest(ScyllaYamlClusterAttrBuilderBase):
    builder_class = ScyllaYamlClusterAttrBuilder

    def test_aws_single_noldap(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'cluster_name': 'test-cluster',
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1',
                    'intra_node_comm_public': False,
                    'authenticator': None,
                    'authorizer': None,
                    'alternator_port': None,
                    'use_ldap_authorization': None,
                    'use_ldap': None,
                    'internode_encryption': False,
                    'client_encrypt': False,
                    'hinted_handoff': None,
                }},
            expected_as_dict={
                'cluster_name': 'test-cluster',
                'alternator_enforce_authorization': False,
                'experimental_features': [],
            }
        )

    def test_aws_multi_openldap(self):
        self._run_test(
            builder_params={
                'test_config': TestConfigWithLdap,
                'cluster_name': 'test-cluster',
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1 eu-west-2',
                    'intra_node_comm_public': False,
                    'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                    'authorizer': 'AllowAllAuthorizer',
                    'alternator_port': None,
                    'use_ldap_authorization': True,
                    'use_ldap': None,
                    'internode_encryption': False,
                    'client_encrypt': False,
                    'hinted_handoff': None,
                    'prepare_saslauthd': True,
                }
            },
            expected_as_dict={
                'cluster_name': 'test-cluster',
                'alternator_enforce_authorization': False,
                'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                'authorizer': 'AllowAllAuthorizer',
                'endpoint_snitch': 'org.apache.cassandra.locator.Ec2Snitch',
                'experimental_features': [],
                'ldap_attr_role': 'cn',
                'ldap_bind_dn': 'cn=admin,dc=scylla-qa,dc=com', 'ldap_bind_passwd': 'scylla-0',
                'ldap_url_template': 'ldap://1.1.1.1:389/dc=scylla-qa,dc=com?cn?sub?'
                                     '(uniqueMember=uid={USER},ou=Person,dc=scylla-qa,dc=com)',
                'role_manager': 'com.scylladb.auth.LDAPRoleManager',
                'saslauthd_socket_path': '/run/saslauthd/mux'
            },
        )

    def test_gce_single_openldap(self):
        self._run_test(
            builder_params={
                'test_config': TestConfigWithLdap,
                'cluster_name': 'test-cluster',
                'params': {
                    'cluster_backend': 'gce',
                    'gce_datacenter': 'eu-west-1',
                    'intra_node_comm_public': False,
                    'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                    'authorizer': 'CassandraAuthorizer',
                    'alternator_port': True,
                    'use_ldap_authorization': True,
                    'ldap_server_type': 'openldap',
                    'internode_encryption': False,
                    'client_encrypt': False,
                    'hinted_handoff': None,
                    'prepare_saslauthd': True,
                }
            },
            expected_as_dict={
                'cluster_name': 'test-cluster',
                'alternator_enforce_authorization': False,
                'alternator_port': True,
                'alternator_write_isolation': 'always_use_lwt',
                'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                'authorizer': 'CassandraAuthorizer',
                'experimental_features': [],
                'ldap_attr_role': 'cn',
                'ldap_bind_dn': 'cn=admin,dc=scylla-qa,dc=com',
                'ldap_bind_passwd': 'scylla-0',
                'ldap_url_template': 'ldap://1.1.1.1:389/dc=scylla-qa,dc=com?cn?sub?'
                                     '(uniqueMember=uid={USER},ou=Person,dc=scylla-qa,dc=com)',
                'role_manager': 'com.scylladb.auth.LDAPRoleManager',
                'saslauthd_socket_path': '/run/saslauthd/mux',
            }
        )

    def test_gce_multi_msldap(self):
        self._run_test(
            builder_params={
                'test_config': TestConfigWithLdap,
                'cluster_name': 'test-cluster',
                'msldap_server_info': {
                    'ldap_bind_dn': 'SOMEDN',
                    'admin_password': 'PASSWORD',
                    'server_address': '3.3.3.3'
                },
                'params': {
                    'cluster_backend': 'gce',
                    'gce_datacenter': 'eu-west-1 eu-west-2',
                    'intra_node_comm_public': False,
                    'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                    'authorizer': 'CassandraAuthorizer',
                    'alternator_port': False,
                    'use_ldap_authorization': True,
                    'ldap_server_type': 'ms_ad',
                    'internode_encryption': True,
                    'client_encrypt': True,
                    'hinted_handoff': False,
                    'prepare_saslauthd': True,
                }
            },
            expected_as_dict={
                'cluster_name': 'test-cluster',
                'alternator_enforce_authorization': False,
                'alternator_port': False,
                'authenticator': 'com.scylladb.auth.SaslauthdAuthenticator',
                'authorizer': 'CassandraAuthorizer',
                'endpoint_snitch': 'org.apache.cassandra.locator.GossipingPropertyFileSnitch',
                'experimental_features': [],
                'hinted_handoff_enabled': False,
                'ldap_attr_role': 'cn', 'ldap_bind_dn': 'SOMEDN', 'ldap_bind_passwd': 'PASSWORD',
                'ldap_url_template': 'ldap://3.3.3.3:389/dc=scylla-qa,dc=com?cn?sub?(member=CN={USER},dc=scylla-qa,dc=com)',
                'role_manager': 'com.scylladb.auth.LDAPRoleManager',
                'saslauthd_socket_path': '/run/saslauthd/mux',
            }
        )

    def test_validation_gce(self):
        with self.assertRaises(RuntimeError):
            self._run_test(
                builder_params={
                    'test_config': TestConfigWithLdap,
                    'cluster_name': 'test-cluster',
                    'params': {
                        'cluster_backend': 'gce',
                        'use_ldap_authorization': True,
                        'ldap_server_type': 'ms_ad',
                    }
                },
            )

    def test_validation_aws(self):
        with self.assertRaises(RuntimeError):
            self._run_test(
                builder_params={
                    'test_config': TestConfigWithoutLdap,
                    'cluster_name': 'test-cluster',
                    'params': {
                        'cluster_backend': 'aws',
                        'use_ldap_authorization': True,
                        'ip_ssh_connections': 'private',
                    }
                },
            )


class ScyllaYamlNodeAttrBuilderTest(ScyllaYamlClusterAttrBuilderBase):
    builder_class = ScyllaYamlNodeAttrBuilder

    def test_aws_single_private_noextra(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1',
                    'ip_ssh_connections': 'private',
                    'extra_network_interface': False,
                    'intra_node_comm_public': False,
                }
            },
            expected_as_dict={
                'enable_ipv6_dns_lookup': False,
                'listen_address': '1.1.1.1',
                'rpc_address': '1.1.1.1',
                'seed_provider': [
                    {
                        'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                        'parameters': [{'seeds': '1.1.1.1'}]
                    }
                ],
                'prometheus_address': '0.0.0.0',
            }
        )

    def test_aws_single_public_noextra(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1',
                    'ip_ssh_connections': 'private',
                    'extra_network_interface': False,
                    'intra_node_comm_public': True,
                }
            },
            expected_as_dict={
                'broadcast_address': '2.2.2.2',
                'broadcast_rpc_address': '2.2.2.2',
                'enable_ipv6_dns_lookup': False,
                'listen_address': '1.1.1.1',
                'rpc_address': '1.1.1.1',
                'seed_provider': [
                    {
                        'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                        'parameters': [{'seeds': '1.1.1.1'}]
                    }
                ],
                'prometheus_address': '0.0.0.0',
            }
        )

    def test_aws_single_public_extra(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1',
                    'ip_ssh_connections': 'private',
                    'extra_network_interface': True,
                    'intra_node_comm_public': True,
                }
            },
            expected_as_dict={
                'broadcast_address': '1.1.1.1',
                'broadcast_rpc_address': '1.1.1.1',
                'enable_ipv6_dns_lookup': False,
                'listen_address': '0.0.0.0',
                'rpc_address': '0.0.0.0',
                'seed_provider': [
                    {
                        'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                        'parameters': [{'seeds': '1.1.1.1'}]
                    }
                ],
                'prometheus_address': '0.0.0.0',
            }
        )

    def test_aws_multi_private_extra(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1 eu-west-2',
                    'ip_ssh_connections': 'private',
                    'extra_network_interface': True,
                    'intra_node_comm_public': False,
                }
            },
            expected_as_dict={
                'broadcast_address': '1.1.1.1',
                'broadcast_rpc_address': '1.1.1.1',
                'enable_ipv6_dns_lookup': False,
                'listen_address': '0.0.0.0',
                'rpc_address': '0.0.0.0',
                'seed_provider': [
                    {
                        'class_name': 'org.apache.cassandra.locator.SimpleSeedProvider',
                        'parameters': [{'seeds': '1.1.1.1'}]
                    }
                ],
                'prometheus_address': '0.0.0.0',
            }
        )

    def test_negative_aws_single_public_noextra(self):
        self._run_test(
            builder_params={
                'node': FakeNode(),
                'params': {
                    'cluster_backend': 'aws',
                    'region_name': 'eu-west-1',
                    'ip_ssh_connections': 'private',
                    'extra_network_interface': False,
                    'intra_node_comm_public': True,
                }
            },
        )


BASE_FOLDER = os.path.join(os.path.dirname(__file__), 'test_data/test_scylla_yaml_builders')


class FakeRemoter:
    def send_file(self, *_, **__):
        pass

    send_files = stop = start = run = sudo = send_file


class ObjectDict(dict):
    @staticmethod
    def fix_name(name):
        output = [name[0].lower()]
        for char in name[1:]:
            if char.isupper():
                output.extend(['_', char.lower()])
            else:
                output.append(char)
        return ''.join(output)

    def __getattr__(self, item):
        for key, value in self.items():
            if self.fix_name(key) == item:
                return value
        raise AttributeError(f"There no {item} attribute")


class DummyCluster(BaseScyllaCluster, BaseCluster):
    nodes: List['DummyNode']

    def __init__(self, params):
        self.nodes = []
        self.params = params
        self.name = 'dummy_cluster'
        self.node_type = "scylla-db"
        self.racks_count = 0

    @property
    def seed_nodes_addresses(self):
        return [node.ip_address for node in self.nodes]

    def init_nodes(self):
        for node in self.nodes:
            node.config_setup(append_scylla_args=self.get_scylla_args())

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        pass


class DummyNode(BaseNode):
    _system_log = None
    is_enterprise = False
    distro = Distro.CENTOS7

    def __init__(self, node_num: int, parent_cluster, base_logdir: str):
        self._private_ip_address = ['1.1.1.' + str(node_num), '1.1.2.' + str(node_num)]
        self._public_ip_address = ['2.2.2.' + str(node_num)]
        self._ipv6_ip_address = ['aaaa:db8:aaaa:aaaa:aaaa:aaaa:aaaa:aaa' + str(node_num),
                                 'aaaa:db8:aaaa:aaaa:aaaa:aaaa:aaaa:bbb' + str(node_num)]
        self._scylla_yaml = ScyllaYaml()
        super().__init__(
            base_logdir=base_logdir,
            parent_cluster=parent_cluster,
            name='node-' + str(node_num),
            ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
        )

    def init(self):
        super().init()
        if self.parent_cluster.params.environment['cluster_backend'] == 'aws':
            self.scylla_network_configuration = ScyllaNetworkConfiguration(
                network_interfaces=self.network_interfaces,
                scylla_network_config=self.parent_cluster.params["scylla_network_config"])

    @property
    def network_interfaces(self):
        return [NetworkInterface(ipv4_public_address=self._public_ip_address[0],
                                 ipv6_public_addresses=[self._ipv6_ip_address[0]],
                                 ipv4_private_addresses=[self._private_ip_address[0]],
                                 ipv6_private_address='',
                                 dns_private_name="",
                                 dns_public_name="",
                                 device_index=0
                                 ),
                NetworkInterface(ipv4_public_address=None,
                                 ipv6_public_addresses=[self._ipv6_ip_address[1]],
                                 ipv4_private_addresses=[self._private_ip_address[1]],
                                 ipv6_private_address='',
                                 dns_private_name="",
                                 dns_public_name="",
                                 device_index=1
                                 )
                ]

    def _init_remoter(self, ssh_login_info):
        self.remoter = FakeRemoter()

    @contextlib.contextmanager
    def remote_scylla_yaml(self):
        yield self._scylla_yaml

    def _get_private_ip_address(self):
        return self._private_ip_address[0]

    def _get_public_ip_address(self):
        return self._public_ip_address[0]

    def process_scylla_args(self, append_scylla_args=''):
        pass

    def fix_scylla_server_systemd_config(self):
        pass

    def _get_ipv6_ip_address(self):
        return self._ipv6_ip_address[0]

    def start_task_threads(self):
        # disable all background threads
        pass

    def wait_for_cloud_init(self):
        pass

    def set_hostname(self):
        pass

    def configure_remote_logging(self):
        pass

    def set_keep_alive(self):
        pass

    @property
    def system_log(self):
        return self._system_log

    @property
    def is_nonroot_install(self):
        return False

    @system_log.setter
    def system_log(self, log):
        self._system_log = log

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass

    def validate_scylla_yaml(self, expected_node_config, seed_node_ips):
        with self.remote_scylla_yaml() as node_yaml:
            expected_node_yaml = expected_node_config.replace(
                '__NODE_PRIVATE_ADDRESS_2__', self._private_ip_address[1])
            expected_node_yaml = expected_node_yaml.replace(
                '__NODE_PRIVATE_ADDRESS__', self.private_ip_address)
            expected_node_yaml = expected_node_yaml.replace(
                '__NODE_PUBLIC_ADDRESS__', self.public_ip_address)
            expected_node_yaml = expected_node_yaml.replace(
                '__SEED_NODE_IPS__', seed_node_ips)
            expected_node_yaml = expected_node_yaml.replace('__NODE_IPV6_ADDRESS__', self.ipv6_ip_address)
            assert json.loads(expected_node_yaml) == node_yaml.dict(
                exclude_unset=True, exclude_defaults=True, exclude_none=True)


class IntegrationTests(unittest.TestCase):
    get_scylla_ami_version_output = ObjectDict(**{
        'Architecture': 'x86_64', 'CreationDate': '2022-10-13T13:17:17.000Z',
        'ImageId': 'ami-0dfb316a2cc0ab399', 'ImageLocation': '797456418907/ScyllaDB Enterprise 2021.1.15',
        'ImageType': 'machine', 'Public': True, 'OwnerId': '797456418907',
        'PlatformDetails': 'Linux/UNIX', 'UsageOperation': 'RunInstances',
        'State': 'available', 'BlockDeviceMappings': [
            {'DeviceName': '/dev/sda1',
             'Ebs': {'DeleteOnTermination': True,
                     'SnapshotId': 'snap-0717a2bee0a38bc84',
                     'VolumeSize': 30,
                     'VolumeType': 'gp3',
                     'Encrypted': False}},
            {'DeviceName': '/dev/sdb',
             'VirtualName': 'ephemeral0'},
            {'DeviceName': '/dev/sdc',
             'VirtualName': 'ephemeral1'},
            {'DeviceName': '/dev/sdd',
             'VirtualName': 'ephemeral2'},
            {'DeviceName': '/dev/sde',
             'VirtualName': 'ephemeral3'},
            {'DeviceName': '/dev/sdf',
             'VirtualName': 'ephemeral4'},
            {'DeviceName': '/dev/sdg',
             'VirtualName': 'ephemeral5'},
            {'DeviceName': '/dev/sdh',
             'VirtualName': 'ephemeral6'},
            {'DeviceName': '/dev/sdi',
             'VirtualName': 'ephemeral7'}],
        'Description': 'ScyllaDB Enterprise 2021.1.15', 'EnaSupport': True, 'Hypervisor': 'xen',
        'Name': 'ScyllaDB Enterprise 2021.1.15', 'RootDeviceName': '/dev/sda1', 'RootDeviceType': 'ebs',
        'Tags': [
            {'Key': 'ScyllaMachineImageVersion', 'Value': '2021.1.15-20221011.82741b636ba-1'},
            {'Key': 'ScyllaPython3Version', 'Value': '2021.1.15-20221011.82741b636ba-1'},
            {'Key': 'user_data_format_version', 'Value': '2'},
            {'Key': 'ScyllaToolsVersion', 'Value': '2021.1.15-20221011.82741b636ba-1'},
            {'Key': 'ScyllaJMXVersion', 'Value': '2021.1.15-20221011.82741b636ba-1'},
            {'Key': 'branch', 'Value': 'branch-2021.1'},
            {'Key': 'scylla-git-commit', 'Value': '3d8c23d0b20af12302608c6286ace0c3dc070828'},
            {'Key': 'build-tag', 'Value': 'jenkins-enterprise-2021.1-promote-release-213'},
            {'Key': 'ScyllaVersion', 'Value': '2021.1.15-20221011.82741b636ba-1'},
            {'Key': 'build-id', 'Value': '213'}
        ], 'VirtualizationType': 'hvm'})

    @property
    def temp_dir(self):
        return tempfile.mkdtemp()

    def _run_test(self, config_path: str, expected_node_config: str, region_names: str):
        old_environ = os.environ.copy()
        old_test_config = {key: value for key, value in TestConfig().__class__.__dict__.items() if
                           is_builtin(type(value)) and not key.startswith('__')}
        while os.environ:
            os.environ.popitem()
        try:
            with patch("sdcm.sct_config.get_scylla_ami_versions", return_value=[self.get_scylla_ami_version_output]), \
                    patch("sdcm.provision.scylla_yaml.certificate_builder.install_client_certificate",
                          return_value=None):
                os.environ['SCT_CLUSTER_BACKEND'] = 'aws'
                os.environ['SCT_REGION_NAME'] = region_names
                os.environ['SCT_CONFIG_FILES'] = config_path
                os.environ['SCT_PREPARE_SASLAUTHD'] = "true"

                conf = SCTConfiguration()
                test_config = TestConfig()

                cluster_backend = conf.get('cluster_backend')
                if cluster_backend == 'aws':
                    test_config.set_multi_region(len(conf.get('region_name').split()) > 1)
                elif cluster_backend == 'gce':
                    test_config.set_multi_region(len(conf.get('gce_datacenter').split()) > 1)
                    test_config.set_intra_node_comm_public(conf.get('intra_node_comm_public'))

                test_config.set_ip_ssh_connections(ssh_connection_ip_type(conf))

                cluster = DummyCluster(params=conf)
                for node_num in range(3):
                    node = DummyNode(node_num=node_num + 1, parent_cluster=cluster, base_logdir=self.temp_dir)
                    node.init()
                    node.is_seed = False
                    cluster.nodes.append(node)
                cluster.nodes[0].is_seed = True
                cluster.init_nodes()
                seed_node_ips = ','.join(cluster.seed_nodes_addresses)
                for node in cluster.nodes:
                    node.validate_scylla_yaml(expected_node_config=expected_node_config, seed_node_ips=seed_node_ips)
        finally:
            while os.environ:
                os.environ.popitem()
            os.environ.update(old_environ)
            for key, value in old_test_config.items():
                setattr(TestConfig, key, value)

    @parameterized.expand([
        (
            os.path.join(BASE_FOLDER, config_name),
            os.path.join(BASE_FOLDER, config_name.replace('.yaml', '.result.json')),
        ) for config_name in os.listdir(BASE_FOLDER) if config_name.endswith('.yaml')
    ])
    def test_integration_node(self, config_path, result_path):
        with open(result_path, encoding="utf-8") as result_file:
            expected_node_config = result_file.read()
        self._run_test(config_path, expected_node_config=expected_node_config,
                       region_names='["eu-west-1", "us-east-1"]')

    @parameterized.expand([
        (
            os.path.join(BASE_FOLDER, "multi_network_interfaces",  config_name),
            os.path.join(BASE_FOLDER, "multi_network_interfaces", config_name.replace('.yaml', '.result.json')),
        ) for config_name in os.listdir(os.path.join(BASE_FOLDER, "multi_network_interfaces")) if config_name.endswith('.yaml')
    ])
    def test_integration_node_multi_interface(self, config_path, result_path):
        with open(result_path, encoding="utf-8") as result_file:
            expected_node_config = result_file.read()
        self._run_test(config_path, expected_node_config=expected_node_config, region_names='["eu-west-1"]')

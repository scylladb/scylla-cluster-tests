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
# Copyright (c) 2016 ScyllaDB

import glob
import logging
import os
import shutil
import time
import types
import re
import subprocess
import datetime
import platform

import boto3.session
import libvirt
import requests
from avocado import Test
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster as ClusterDriver
from cassandra.cluster import NoHostAvailable
from cassandra.policies import RetryPolicy
from cassandra.policies import WhiteListRoundRobinPolicy
from libcloud.compute.providers import get_driver
from libcloud.compute.types import Provider

from . import cluster
from . import nemesis
from .cluster import GCECredentials
from .cluster import LoaderSetGCE
from .cluster import LoaderSetLibvirt
from .cluster import LoaderSetOpenStack
from .cluster import MonitorSetGCE
from .cluster import MonitorSetLibvirt
from .cluster import MonitorSetOpenStack
from .cluster import NoMonitorSet
from .cluster import RemoteCredentials
from .cluster import ScyllaGCECluster
from .cluster import ScyllaLibvirtCluster
from .cluster import ScyllaOpenStackCluster
from .cluster import UserRemoteCredentials
from .cluster_aws import CassandraAWSCluster
from .cluster_aws import ScyllaAWSCluster
from .cluster_aws import LoaderSetAWS
from .cluster_aws import MonitorSetAWS
from .data_path import get_data_path
from . import es
from results_analyze import ResultsAnalyzer

try:
    from botocore.vendored.requests.packages.urllib3.contrib.pyopenssl import extract_from_urllib3

    # Don't use pyOpenSSL in urllib3 - it causes an ``OpenSSL.SSL.Error``
    # exception when we try an API call on an idled persistent connection.
    # See https://github.com/boto/boto3/issues/220
    extract_from_urllib3()
except ImportError:
    pass

TEST_LOG = logging.getLogger('avocado.test')


class FlakyRetryPolicy(RetryPolicy):

    """
    A retry policy that retries 5 times
    """

    def on_read_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            TEST_LOG.debug("Retrying read after timeout. Attempt #%s",
                           str(kwargs['retry_num']))
            return self.RETRY, None
        else:
            return self.RETHROW, None

    def on_write_timeout(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            TEST_LOG.debug("Retrying write after timeout. Attempt #%s",
                           str(kwargs['retry_num']))
            return self.RETRY, None
        else:
            return self.RETHROW, None

    def on_unavailable(self, *args, **kwargs):
        if kwargs['retry_num'] < 5:
            TEST_LOG.debug("Retrying request after UE. Attempt #%s",
                           str(kwargs['retry_num']))
            return self.RETRY, None
        else:
            return self.RETHROW, None


def retry_till_success(fun, *args, **kwargs):
    timeout = kwargs.pop('timeout', 60)
    bypassed_exception = kwargs.pop('bypassed_exception', Exception)

    deadline = time.time() + timeout
    while True:
        try:
            return fun(*args, **kwargs)
        except bypassed_exception:
            if time.time() > deadline:
                raise
            else:
                # brief pause before next attempt
                time.sleep(0.25)


def clean_aws_resources(method):
    """
    Ensure that AWS resources are cleaned upon unhandled exceptions.

    :param method: ScyllaClusterTester method to wrap.
    :return: Wrapped method.
    """
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception:
            TEST_LOG.exception('Exception in wrapped method %s', method.__name__)
            args[0].clean_resources()
            raise
    return wrapper


def get_stress_cmd_params(cmd):
    """
    Parsing cassandra stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd_params = {}
    cmd = cmd.strip().split('cassandra-stress')[1].strip()
    if cmd.split(' ')[0] in ['read', 'write', 'mixed', 'counter_write', 'user']:
        cmd_params['command'] = cmd.split(' ')[0]
        if 'no-warmup' in cmd:
            cmd_params['no-warmup'] = True

        match = re.search('(cl\s?=\s?\w+)', cmd)
        if match:
            cmd_params['cl'] = match.group(0).split('=')[1].strip()

        match = re.search('(duration\s?=\s?\w+)', cmd)
        if match:
            cmd_params['duration'] = match.group(0).split('=')[1].strip()

        match = re.search('( n\s?=\s?\w+)', cmd)
        if match:
            cmd_params['n'] = match.group(0).split('=')[1].strip()
        match = re.search('profile=(\S+)\s+', cmd)
        if match:
            cmd_params['profile'] = match.group(1).strip()
            match = re.search('ops(\S+)\s+', cmd)
            if match:
                cmd_params['ops'] = match.group(1).split('=')[0].strip('(')

        for temp in cmd.split(' -')[1:]:
            k = temp.split()[0]
            match = re.search('(-' + k + '\s+([^-| ]+))', cmd)
            if match:
                cmd_params[k] = match.group(2).strip()
        if 'rate' in cmd_params:
            # split rate section on separate items
            if 'threads' in cmd_params['rate']:
                cmd_params['rate threads'] = \
                    re.search('(threads\s?=\s?(\w+))', cmd_params['rate']).group(2)
            if 'throttle' in cmd_params['rate']:
                cmd_params['throttle threads'] =\
                    re.search('(throttle\s?=\s?(\w+))', cmd_params['rate']).group(2)
            if 'fixed' in cmd_params['rate']:
                cmd_params['fixed threads'] =\
                    re.search('(fixed\s?=\s?(\w+))', cmd_params['rate']).group(2)
            del cmd_params['rate']

    return cmd_params


def get_stress_bench_cmd_params(cmd):
    """
    Parsing bench stress command
    :param cmd: stress cmd
    :return: dict with params
    """
    cmd = cmd.strip().split('scylla-bench')[1].strip()
    cmd_params = {}
    for key in ['partition-count', 'clustering-row-count', 'clustering-row-size', 'mode',
                'workload', 'concurrency', 'max-rate', 'connection-count', 'replication-factor',
                'timeout', 'client-compression']:
        match = re.search('(-' + key + '\s+([^-| ]+))', cmd)
        if match:
            cmd_params[key] = match.group(2).strip()
    return cmd_params


class ClusterTester(Test):

    def __init__(self, methodName='test', name=None, params=None,
                 base_logdir=None, tag=None, job=None, runner_queue=None):
        super(ClusterTester, self).__init__(methodName=methodName, name=name,
                                            params=params,
                                            base_logdir=base_logdir, tag=tag,
                                            job=job, runner_queue=runner_queue)
        self._failure_post_behavior = self.params.get(key='failure_post_behavior',
                                                      default='destroy')
        ip_ssh_connections = self.params.get(key='ip_ssh_connections', default='public')
        self.log.debug("IP used for SSH connections is '%s'",
                       ip_ssh_connections)
        cluster.set_ip_ssh_connections(ip_ssh_connections)
        self.log.debug("Behavior on failure/post test is '%s'",
                       self._failure_post_behavior)
        cluster.register_cleanup(cleanup=self._failure_post_behavior)
        self._duration = self.params.get(key='test_duration', default=60)
        cluster.set_duration(self._duration)

        # for saving test details in DB
        self.test_index = self.__class__.__name__.lower()
        self.test_type = self.params.id.name
        self.test_id = None
        self.stats = {k: {} for k in ['test_details', 'setup_details', 'versions', 'results', 'nemesis',
                                      'errors', 'coredumps']}
        self.create_stats = True

    @clean_aws_resources
    def setUp(self):
        self.credentials = []
        self.db_cluster = None
        self.loaders = None
        self.monitors = None
        self.connections = []
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        self.init_resources()
        self.db_cluster.wait_for_init()
        db_node_address = self.db_cluster.nodes[0].private_ip_address
        self.loaders.wait_for_init(db_node_address=db_node_address)
        if len(self.db_cluster.datacenter) > 1:
            targets = [n.public_ip_address for n in self.db_cluster.nodes]
            targets += [n.public_ip_address for n in self.loaders.nodes]
        else:
            targets = [n.private_ip_address for n in self.db_cluster.nodes]
            targets += [n.private_ip_address for n in self.loaders.nodes]
        self.monitors.wait_for_init(targets=targets, scylla_version=self.db_cluster.nodes[0].scylla_version)
        if self.create_stats:
            self.create_test_stats()

    def get_nemesis_class(self):
        """
        Get a Nemesis class from parameters.

        :return: Nemesis class.
        :rtype: nemesis.Nemesis derived class
        """
        class_name = self.params.get('nemesis_class_name')
        return getattr(nemesis, class_name)

    def get_cluster_openstack(self, loader_info, db_info, monitor_info):
        if loader_info['n_nodes'] is None:
            loader_info['n_nodes'] = self.params.get('n_loaders')
        if loader_info['type'] is None:
            loader_info['type'] = self.params.get('openstack_instance_type_loader')
        if db_info['n_nodes'] is None:
            db_info['n_nodes'] = self.params.get('n_db_nodes')
        if db_info['type'] is None:
            db_info['type'] = self.params.get('openstack_instance_type_db')
        if monitor_info['n_nodes'] is None:
            monitor_info['n_nodes'] = self.params.get('n_monitor_nodes')
        if monitor_info['type'] is None:
            monitor_info['type'] = self.params.get('openstack_instance_type_monitor')
        user_prefix = self.params.get('user_prefix', None)
        user = self.params.get('openstack_user', None)
        password = self.params.get('openstack_password', None)
        tenant = self.params.get('openstack_tenant', None)
        auth_version = self.params.get('openstack_auth_version', None)
        auth_url = self.params.get('openstack_auth_url', None)
        service_type = self.params.get('openstack_service_type', None)
        service_name = self.params.get('openstack_service_name', None)
        service_region = self.params.get('openstack_service_region', None)
        service_cls = get_driver(Provider.OPENSTACK)
        service = service_cls(user, password,
                              ex_force_auth_version=auth_version,
                              ex_force_auth_url=auth_url,
                              ex_force_service_type=service_type,
                              ex_force_service_name=service_name,
                              ex_force_service_region=service_region,
                              ex_tenant_name=tenant)
        user_credentials = self.params.get('user_credentials_path', None)
        if user_credentials:
            self.credentials.append(UserRemoteCredentials(key_file=user_credentials))
        else:
            self.credentials.append(RemoteCredentials(service=service,
                                                      key_prefix='sct',
                                                      user_prefix=user_prefix))

        self.db_cluster = ScyllaOpenStackCluster(openstack_image=self.params.get('openstack_image'),
                                                 openstack_image_username=self.params.get('openstack_image_username'),
                                                 openstack_network=self.params.get('openstack_network'),
                                                 openstack_instance_type=db_info['type'],
                                                 service=service,
                                                 credentials=self.credentials,
                                                 user_prefix=user_prefix,
                                                 n_nodes=db_info['n_nodes'],
                                                 params=self.params)

        scylla_repo = get_data_path('scylla.repo')
        self.loaders = LoaderSetOpenStack(openstack_image=self.params.get('openstack_image'),
                                          openstack_image_username=self.params.get('openstack_image_username'),
                                          openstack_network=self.params.get('openstack_network'),
                                          openstack_instance_type=loader_info['type'],
                                          service=service,
                                          credentials=self.credentials,
                                          scylla_repo=scylla_repo,
                                          user_prefix=user_prefix,
                                          n_nodes=loader_info['n_nodes'],
                                          params=self.params)

        if monitor_info['n_nodes'] > 0:
            self.monitors = MonitorSetOpenStack(openstack_image=self.params.get('openstack_image'),
                                                openstack_image_username=self.params.get('openstack_image_username'),
                                                openstack_network=self.params.get('openstack_network'),
                                                openstack_instance_type=monitor_info['type'],
                                                service=service,
                                                credentials=self.credentials,
                                                scylla_repo=scylla_repo,
                                                user_prefix=user_prefix,
                                                n_nodes=monitor_info['n_nodes'],
                                                params=self.params)
        else:
            self.monitors = NoMonitorSet()

    def get_cluster_gce(self, loader_info, db_info, monitor_info):
        if loader_info['n_nodes'] is None:
            loader_info['n_nodes'] = self.params.get('n_loaders')
        if loader_info['type'] is None:
            loader_info['type'] = self.params.get('gce_instance_type_loader')
        if loader_info['disk_type'] is None:
            loader_info['disk_type'] = self.params.get('gce_root_disk_type_loader')
        if loader_info['disk_size'] is None:
            loader_info['disk_size'] = self.params.get('gce_root_disk_size_loader')
        if loader_info['n_local_ssd'] is None:
            loader_info['n_local_ssd'] = self.params.get('gce_n_local_ssd_disk_loader')
        if loader_info['pd_ssd_size'] is None:
            loader_info['pd_ssd_size'] = self.params.get('gce_pd_ssd_disk_size_loader')
        if db_info['n_nodes'] is None:
            n_db_nodes = self.params.get('n_db_nodes')
            if isinstance(n_db_nodes, int):  # legacy type
                db_info['n_nodes'] = [n_db_nodes]
            elif isinstance(n_db_nodes, str):  # latest type to support multiple datacenters
                db_info['n_nodes'] = [int(n) for n in n_db_nodes.split()]
            else:
                self.fail('Unsupported parameter type: {}'.format(type(n_db_nodes)))
        cpu = self.params.get('gce_instance_type_cpu_db')
        # unit is GB
        mem = self.params.get('gce_instance_type_mem_db')
        if cpu and mem:
            db_info['type'] = 'custom-{}-{}-ext'.format(cpu, int(mem) * 1024)
        if db_info['type'] is None:
            db_info['type'] = self.params.get('gce_instance_type_db')
        if db_info['disk_type'] is None:
            db_info['disk_type'] = self.params.get('gce_root_disk_type_db')
        if db_info['disk_size'] is None:
            db_info['disk_size'] = self.params.get('gce_root_disk_size_db')
        if db_info['n_local_ssd'] is None:
            db_info['n_local_ssd'] = self.params.get('gce_n_local_ssd_disk_db')
        if db_info['pd_ssd_size'] is None:
            db_info['pd_ssd_size'] = self.params.get('gce_pd_ssd_disk_size_db')
        if monitor_info['n_nodes'] is None:
            monitor_info['n_nodes'] = self.params.get('n_monitor_nodes')
        if monitor_info['type'] is None:
            monitor_info['type'] = self.params.get('gce_instance_type_monitor')
        if monitor_info['disk_type'] is None:
            monitor_info['disk_type'] = self.params.get('gce_root_disk_type_monitor')
        if monitor_info['disk_size'] is None:
            monitor_info['disk_size'] = self.params.get('gce_root_disk_size_monitor')
        if monitor_info['n_local_ssd'] is None:
            monitor_info['n_local_ssd'] = self.params.get('gce_n_local_ssd_disk_monitor')
        if monitor_info['pd_ssd_size'] is None:
            monitor_info['pd_ssd_size'] = self.params.get('gce_pd_ssd_disk_size_monitor')
        user_prefix = self.params.get('user_prefix', None)
        service_account_email = self.params.get('gce_service_account_email', None)
        user_credentials = self.params.get('gce_user_credentials', None)
        gce_datacenter = self.params.get('gce_datacenter', None).split()
        project = self.params.get('gce_project', None)
        service_cls = get_driver(Provider.GCE)
        services = []
        for i in gce_datacenter:
            services.append(service_cls(service_account_email, user_credentials, datacenter=i, project=project))
        if len(services) > 1:
            assert len(services) == len(db_info['n_nodes'])
        user_credentials = self.params.get('user_credentials_path', None)
        self.credentials.append(GCECredentials(key_file=user_credentials))

        gce_image_db = self.params.get('gce_image_db')
        if not gce_image_db:
            gce_image_db = self.params.get('gce_image')
        self.db_cluster = ScyllaGCECluster(gce_image=gce_image_db,
                                           gce_image_type=db_info['disk_type'],
                                           gce_image_size=db_info['disk_size'],
                                           gce_n_local_ssd=db_info['n_local_ssd'],
                                           gce_image_username=self.params.get('gce_image_username'),
                                           gce_network=self.params.get('gce_network', default='default'),
                                           gce_instance_type=db_info['type'],
                                           services=services,
                                           credentials=self.credentials,
                                           user_prefix=user_prefix,
                                           n_nodes=db_info['n_nodes'],
                                           params=self.params,
                                           gce_datacenter=gce_datacenter,
                                           gce_pd_ssd_size=db_info['pd_ssd_size'])

        scylla_repo = get_data_path('scylla.repo')
        self.loaders = LoaderSetGCE(gce_image=self.params.get('gce_image'),
                                    gce_image_type=loader_info['disk_type'],
                                    gce_image_size=loader_info['disk_size'],
                                    gce_n_local_ssd=loader_info['n_local_ssd'],
                                    gce_image_username=self.params.get('gce_image_username'),
                                    gce_network=self.params.get('gce_network', default='default'),
                                    gce_instance_type=loader_info['type'],
                                    service=services[:1],
                                    credentials=self.credentials,
                                    scylla_repo=scylla_repo,
                                    user_prefix=user_prefix,
                                    n_nodes=loader_info['n_nodes'],
                                    params=self.params,
                                    gce_pd_ssd_size=loader_info['pd_ssd_size'])

        if monitor_info['n_nodes'] > 0:
            self.monitors = MonitorSetGCE(gce_image=self.params.get('gce_image'),
                                          gce_image_type=monitor_info['disk_type'],
                                          gce_image_size=monitor_info['disk_size'],
                                          gce_n_local_ssd=monitor_info['n_local_ssd'],
                                          gce_image_username=self.params.get('gce_image_username'),
                                          gce_network=self.params.get('gce_network', default='default'),
                                          gce_instance_type=monitor_info['type'],
                                          service=services[:1],
                                          credentials=self.credentials,
                                          scylla_repo=scylla_repo,
                                          user_prefix=user_prefix,
                                          n_nodes=monitor_info['n_nodes'],
                                          params=self.params,
                                          gce_pd_ssd_size=monitor_info['pd_ssd_size'])
        else:
            self.monitors = NoMonitorSet()

    def get_cluster_aws(self, loader_info, db_info, monitor_info):
        if loader_info['n_nodes'] is None:
            loader_info['n_nodes'] = self.params.get('n_loaders')
        if loader_info['type'] is None:
            loader_info['type'] = self.params.get('instance_type_loader')
        if db_info['n_nodes'] is None:
            n_db_nodes = self.params.get('n_db_nodes')
            if type(n_db_nodes) == int:  # legacy type
                db_info['n_nodes'] = [n_db_nodes]
            elif type(n_db_nodes) == str:  # latest type to support multiple datacenters
                db_info['n_nodes'] = [int(n) for n in n_db_nodes.split()]
            else:
                self.fail('Unsupported parameter type: {}'.format(type(n_db_nodes)))
        if db_info['type'] is None:
            db_info['type'] = self.params.get('instance_type_db')
        if monitor_info['n_nodes'] is None:
            monitor_info['n_nodes'] = self.params.get('n_monitor_nodes')
        if monitor_info['type'] is None:
            monitor_info['type'] = self.params.get('instance_type_monitor')
        user_prefix = self.params.get('user_prefix', None)

        user_credentials = self.params.get('user_credentials_path', None)
        services = []
        for i in self.params.get('region_name').split():
            session = boto3.session.Session(region_name=i)
            service = session.resource('ec2')
            services.append(service)
            if user_credentials:
                self.credentials.append(UserRemoteCredentials(key_file=user_credentials))
            else:
                self.credentials.append(RemoteCredentials(service=service,
                                                          key_prefix='sct',
                                                          user_prefix=user_prefix))

        ec2_security_group_ids = []
        for i in self.params.get('security_group_ids').split():
            ec2_security_group_ids.append(i.split(','))
        ec2_subnet_id = self.params.get('subnet_id').split()

        if self.params.get('db_type') == 'scylla':
            self.db_cluster = ScyllaAWSCluster(ec2_ami_id=self.params.get('ami_id_db_scylla').split(),
                                               ec2_ami_username=self.params.get('ami_db_scylla_user'),
                                               ec2_security_group_ids=ec2_security_group_ids,
                                               ec2_subnet_id=ec2_subnet_id,
                                               ec2_instance_type=db_info['type'],
                                               services=services,
                                               credentials=self.credentials,
                                               ec2_block_device_mappings=db_info['device_mappings'],
                                               user_prefix=user_prefix,
                                               n_nodes=db_info['n_nodes'],
                                               params=self.params)
        elif self.params.get('db_type') == 'cassandra':
            self.db_cluster = CassandraAWSCluster(ec2_ami_id=self.params.get('ami_id_db_cassandra').split(),
                                                  ec2_ami_username=self.params.get('ami_db_cassandra_user'),
                                                  ec2_security_group_ids=ec2_security_group_ids,
                                                  ec2_subnet_id=ec2_subnet_id,
                                                  ec2_instance_type=db_info['type'],
                                                  service=services[:1],
                                                  ec2_block_device_mappings=db_info['device_mappings'],
                                                  credentials=self.credentials,
                                                  user_prefix=user_prefix,
                                                  n_nodes=db_info['n_nodes'],
                                                  params=self.params)
        else:
            self.error('Incorrect parameter db_type: %s' %
                       self.params.get('db_type'))

        scylla_repo = get_data_path('scylla.repo')
        self.loaders = LoaderSetAWS(ec2_ami_id=self.params.get('ami_id_loader').split(),
                                    ec2_ami_username=self.params.get('ami_loader_user'),
                                    ec2_security_group_ids=ec2_security_group_ids,
                                    ec2_subnet_id=ec2_subnet_id,
                                    ec2_instance_type=loader_info['type'],
                                    service=services[:1],
                                    ec2_block_device_mappings=loader_info['device_mappings'],
                                    credentials=self.credentials,
                                    scylla_repo=scylla_repo,
                                    user_prefix=user_prefix,
                                    n_nodes=loader_info['n_nodes'],
                                    params=self.params)

        if monitor_info['n_nodes'] > 0:
            self.monitors = MonitorSetAWS(ec2_ami_id=self.params.get('ami_id_monitor').split(),
                                          ec2_ami_username=self.params.get('ami_monitor_user'),
                                          ec2_security_group_ids=ec2_security_group_ids,
                                          ec2_subnet_id=ec2_subnet_id,
                                          ec2_instance_type=monitor_info['type'],
                                          service=services[:1],
                                          ec2_block_device_mappings=monitor_info['device_mappings'],
                                          credentials=self.credentials,
                                          scylla_repo=scylla_repo,
                                          user_prefix=user_prefix,
                                          n_nodes=monitor_info['n_nodes'],
                                          params=self.params)
        else:
            self.monitors = NoMonitorSet()

    def get_cluster_libvirt(self, loader_info, db_info, monitor_info):

        def _set_from_params(base_dict, dict_key, params_key):
            if base_dict.get(dict_key) is None:
                conf_dict = dict()
                conf_dict[dict_key] = self.params.get(params_key)
                return conf_dict
            else:
                return {}

        loader_info.update(_set_from_params(loader_info, 'n_nodes', 'n_loaders'))
        loader_info.update(_set_from_params(loader_info, 'image', 'libvirt_loader_image'))
        loader_info.update(_set_from_params(loader_info, 'user', 'libvirt_loader_image_user'))
        loader_info.update(_set_from_params(loader_info, 'password', 'libvirt_loader_image_password'))
        loader_info.update(_set_from_params(loader_info, 'os_type', 'libvirt_loader_os_type'))
        loader_info.update(_set_from_params(loader_info, 'os_variant', 'libvirt_loader_os_variant'))
        loader_info.update(_set_from_params(loader_info, 'memory', 'libvirt_loader_memory'))
        loader_info.update(_set_from_params(loader_info, 'bridge', 'libvirt_bridge'))
        loader_info.update(_set_from_params(loader_info, 'uri', 'libvirt_uri'))

        db_info.update(_set_from_params(db_info, 'n_nodes', 'n_db_nodes'))
        db_info.update(_set_from_params(db_info, 'image', 'libvirt_db_image'))
        db_info.update(_set_from_params(db_info, 'user', 'libvirt_db_image_user'))
        db_info.update(_set_from_params(db_info, 'password', 'libvirt_db_image_password'))
        db_info.update(_set_from_params(db_info, 'os_type', 'libvirt_db_os_type'))
        db_info.update(_set_from_params(db_info, 'os_variant', 'libvirt_db_os_variant'))
        db_info.update(_set_from_params(db_info, 'memory', 'libvirt_db_memory'))
        db_info.update(_set_from_params(db_info, 'bridge', 'libvirt_bridge'))
        db_info.update(_set_from_params(db_info, 'uri', 'libvirt_uri'))

        monitor_info.update(_set_from_params(monitor_info, 'n_nodes', 'n_monitor_nodes'))
        monitor_info.update(_set_from_params(monitor_info, 'image', 'libvirt_monitor_image'))
        monitor_info.update(_set_from_params(monitor_info, 'user', 'libvirt_monitor_image_user'))
        monitor_info.update(_set_from_params(monitor_info, 'password', 'libvirt_monitor_image_password'))
        monitor_info.update(_set_from_params(monitor_info, 'os_type', 'libvirt_monitor_os_type'))
        monitor_info.update(_set_from_params(monitor_info, 'os_variant', 'libvirt_monitor_os_variant'))
        monitor_info.update(_set_from_params(monitor_info, 'memory', 'libvirt_monitor_memory'))
        monitor_info.update(_set_from_params(monitor_info, 'bridge', 'libvirt_bridge'))
        monitor_info.update(_set_from_params(monitor_info, 'uri', 'libvirt_uri'))

        user_prefix = self.params.get('user_prefix', None)

        libvirt_uri = self.params.get('libvirt_uri')
        if libvirt_uri is None:
            libvirt_uri = 'qemu:///system'
        hypervisor = libvirt.open(libvirt_uri)
        cluster.set_libvirt_uri(libvirt_uri)

        if self.params.get('db_type') == 'scylla':
            self.db_cluster = ScyllaLibvirtCluster(domain_info=db_info,
                                                   hypervisor=hypervisor,
                                                   user_prefix=user_prefix,
                                                   n_nodes=db_info['n_nodes'],
                                                   params=self.params)

        elif self.params.get('db_type') == 'cassandra':
            raise NotImplementedError('No cassandra libvirt cluster '
                                      'implementation yet.')

        self.loaders = LoaderSetLibvirt(domain_info=loader_info,
                                        hypervisor=hypervisor,
                                        user_prefix=user_prefix,
                                        n_nodes=loader_info['n_nodes'],
                                        params=self.params)

        if monitor_info['n_nodes'] > 0:
            self.monitors = MonitorSetLibvirt(domain_info=monitor_info,
                                              hypervisor=hypervisor,
                                              user_prefix=user_prefix,
                                              n_nodes=monitor_info['n_nodes'],
                                              params=self.params)
        else:
            self.monitors = NoMonitorSet()

    @clean_aws_resources
    def init_resources(self, loader_info=None, db_info=None,
                       monitor_info=None):
        if loader_info is None:
            loader_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                           'device_mappings': None, 'pd_ssd_size': None}
        if db_info is None:
            db_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                       'device_mappings': None, 'pd_ssd_size': None}

        if monitor_info is None:
            monitor_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                            'device_mappings': None, 'pd_ssd_size': None}

        cluster_backend = self.params.get('cluster_backend')
        if cluster_backend is None:
            cluster_backend = 'aws'

        if cluster_backend == 'aws':
            self.get_cluster_aws(loader_info=loader_info, db_info=db_info,
                                 monitor_info=monitor_info)
        elif cluster_backend == 'libvirt':
            self.get_cluster_libvirt(loader_info=loader_info, db_info=db_info,
                                     monitor_info=monitor_info)
        elif cluster_backend == 'openstack':
            self.get_cluster_openstack(loader_info=loader_info, db_info=db_info,
                                       monitor_info=monitor_info)
        elif cluster_backend == 'gce':
            self.get_cluster_gce(loader_info=loader_info, db_info=db_info,
                                 monitor_info=monitor_info)
        seeds_num = self.params.get('seeds_num', default=1)
        for i in range(seeds_num):
            self.db_cluster.nodes[i].is_seed = True

    def _cs_add_node_flag(self, stress_cmd):
        if '-node' not in stress_cmd:
            if len(self.db_cluster.datacenter) > 1:
                ips = [ip for ip in self.db_cluster.get_node_public_ips()]
                ip = ','.join(ips)
            else:
                ip = self.db_cluster.get_node_private_ips()[0]
            stress_cmd = '%s -node %s' % (stress_cmd, ip)
        return stress_cmd

    @clean_aws_resources
    def run_stress(self, stress_cmd, duration=None):
        stress_cmd = self._cs_add_node_flag(stress_cmd)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd,
                                              duration=duration)
        self.verify_stress_thread(stress_queue)

    @clean_aws_resources
    def run_stress_thread(self, stress_cmd, duration=None, stress_num=1, keyspace_num=1, profile=None, prefix=''):
        #stress_cmd = self._cs_add_node_flag(stress_cmd)
        if duration is None:
            duration = self.params.get('test_duration')
        timeout = duration * 60 + 600
        self.update_stress_cmd_details(stress_cmd, prefix)
        return self.loaders.run_stress_thread(stress_cmd, timeout,
                                              self.outputdir,
                                              stress_num=stress_num,
                                              keyspace_num=keyspace_num,
                                              profile=profile,
                                              node_list=self.db_cluster.nodes)

    @clean_aws_resources
    def run_stress_thread_bench(self, stress_cmd, duration=None):
        if duration is None:
            duration = self.params.get('test_duration')
        timeout = duration * 60 + 600
        self.update_bench_stress_cmd_details(stress_cmd)
        return self.loaders.run_stress_thread_bench(stress_cmd, timeout,
                                              self.outputdir,
                                              node_list=self.db_cluster.nodes)


    @clean_aws_resources
    def kill_stress_thread(self):
        self.loaders.kill_stress_thread()

    @clean_aws_resources
    def verify_stress_thread(self, queue):
        errors = self.loaders.verify_stress_thread(queue, self.db_cluster)
        # Sometimes, we might have an epic error messages list
        # that will make small machines driving the avocado test
        # to run out of memory when writing the XML report. Since
        # the error message is merely informational, let's simply
        # use the last 5 lines for the final error message.
        errors = errors[-5:]
        if errors:
            self.fail("cassandra-stress errors on "
                      "nodes:\n%s" % "\n".join(errors))

    @clean_aws_resources
    def get_stress_results(self, queue, stress_num=1, keyspace_num=1):
        results = self.loaders.get_stress_results(queue, stress_num=stress_num, keyspace_num=keyspace_num)
        self.update_stress_results(results)
        return results

    @clean_aws_resources
    def get_stress_results_bench(self, queue):
        results = self.loaders.get_stress_results_bench(queue)
        self.update_stress_results(results)
        return results

    def get_auth_provider(self, user, password):
        return PlainTextAuthProvider(username=user, password=password)

    def _create_session(self, node, keyspace, user, password, compression,
                        protocol_version, load_balancing_policy=None,
                        port=None, ssl_opts=None):
        node_ips = [node.public_ip_address]
        if not port:
            port = 9042

        if protocol_version is None:
            protocol_version = 3

        authenticator = self.params.get('authenticator')
        if user is None and password is None and (authenticator and authenticator == 'PasswordAuthenticator'):
            user = 'cassandra'
            password = 'cassandra'

        if user is not None:
            auth_provider = self.get_auth_provider(user=user,
                                                   password=password)
        else:
            auth_provider = None

        cluster = ClusterDriver(node_ips, auth_provider=auth_provider,
                                compression=compression,
                                protocol_version=protocol_version,
                                load_balancing_policy=load_balancing_policy,
                                default_retry_policy=FlakyRetryPolicy(),
                                port=port, ssl_options=ssl_opts,
                                connect_timeout=100)
        session = cluster.connect()

        # temporarily increase client-side timeout to 1m to determine
        # if the cluster is simply responding slowly to requests
        session.default_timeout = 60.0

        if keyspace is not None:
            session.set_keyspace(keyspace)

        # override driver default consistency level of LOCAL_QUORUM
        session.default_consistency_level = ConsistencyLevel.ONE

        self.connections.append(session)
        return session

    def cql_connection(self, node, keyspace=None, user=None,
                       password=None, compression=True, protocol_version=None,
                       port=None, ssl_opts=None):

        wlrr = WhiteListRoundRobinPolicy(self.db_cluster.get_node_public_ips())
        return self._create_session(node, keyspace, user, password,
                                    compression, protocol_version, wlrr,
                                    port=port, ssl_opts=ssl_opts)

    def cql_connection_exclusive(self, node, keyspace=None, user=None,
                                 password=None, compression=True,
                                 protocol_version=None, port=None,
                                 ssl_opts=None):

        wlrr = WhiteListRoundRobinPolicy([node.public_ip_address])
        return self._create_session(node, keyspace, user, password,
                                    compression, protocol_version, wlrr,
                                    port=port, ssl_opts=ssl_opts)

    def cql_connection_patient(self, node, keyspace=None,
                               user=None, password=None, timeout=30,
                               compression=True, protocol_version=None,
                               port=None, ssl_opts=None):
        """
        Returns a connection after it stops throwing NoHostAvailables.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(self.cql_connection,
                                  node,
                                  keyspace=keyspace,
                                  user=user,
                                  password=password,
                                  timeout=timeout,
                                  compression=compression,
                                  protocol_version=protocol_version,
                                  port=port,
                                  ssl_opts=ssl_opts,
                                  bypassed_exception=NoHostAvailable)

    def cql_connection_patient_exclusive(self, node, keyspace=None,
                                         user=None, password=None, timeout=30,
                                         compression=True,
                                         protocol_version=None,
                                         port=None, ssl_opts=None):
        """
        Returns a connection after it stops throwing NoHostAvailables.

        If the timeout is exceeded, the exception is raised.
        """
        return retry_till_success(self.cql_connection_exclusive,
                                  node,
                                  keyspace=keyspace,
                                  user=user,
                                  password=password,
                                  timeout=timeout,
                                  compression=compression,
                                  protocol_version=protocol_version,
                                  port=port,
                                  ssl_opts=ssl_opts,
                                  bypassed_exception=NoHostAvailable)

    def create_ks(self, session, name, rf):
        query = 'CREATE KEYSPACE %s WITH replication={%s}'
        if isinstance(rf, types.IntType):
            # we assume simpleStrategy
            session.execute(query % (name,
                                     "'class':'SimpleStrategy', "
                                     "'replication_factor':%d" % rf))
        else:
            assert len(rf) != 0, "At least one datacenter/rf pair is needed"
            # we assume networkTopologyStrategy
            options = ', '.join(['\'%s\':%d' % (d, r) for
                                 d, r in rf.iteritems()])
            session.execute(query % (name,
                                     "'class':'NetworkTopologyStrategy', %s" %
                                     options))
        session.execute('USE %s' % name)

    def create_cf(self, session, name, key_type="varchar",
                  speculative_retry=None, read_repair=None, compression=None,
                  gc_grace=None, columns=None,
                  compact_storage=False):

        additional_columns = ""
        if columns is not None:
            for k, v in columns.items():
                additional_columns = "%s, %s %s" % (additional_columns, k, v)

        if additional_columns == "":
            query = ('CREATE COLUMNFAMILY %s (key %s, c varchar, v varchar, '
                     'PRIMARY KEY(key, c)) WITH comment=\'test cf\'' %
                     (name, key_type))
        else:
            query = ('CREATE COLUMNFAMILY %s (key %s PRIMARY KEY%s) '
                     'WITH comment=\'test cf\'' %
                     (name, key_type, additional_columns))

        if compression is not None:
            query = ('%s AND compression = { \'sstable_compression\': '
                     '\'%sCompressor\' }' % (query, compression))
        else:
            # if a compression option is omitted, C*
            # will default to lz4 compression
            query += ' AND compression = {}'

        if read_repair is not None:
            query = '%s AND read_repair_chance=%f' % (query, read_repair)
        if gc_grace is not None:
            query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
        if speculative_retry is not None:
            query = ('%s AND speculative_retry=\'%s\'' %
                     (query, speculative_retry))

        if compact_storage:
            query += ' AND COMPACT STORAGE'

        session.execute(query)
        time.sleep(0.2)

    @staticmethod
    def get_s3_url(file_name):
        return 'https://cloudius-jenkins-test.s3.amazonaws.com/%s/%s' % (
            os.environ.get('JOB_NAME', "local_run"), os.path.basename(
                os.path.normpath(file_name)))

    def clean_resources(self):
        self.log.debug('Cleaning up resources used in the test')
        db_cluster_errors = None
        db_cluster_coredumps = None
        if self.db_cluster is not None:
            db_cluster_errors = self.db_cluster.get_node_database_errors()
            self.db_cluster.get_backtraces()
            db_cluster_coredumps = self.db_cluster.coredumps
            for current_nemesis in self.db_cluster.nemesis:
                current_nemesis.report()
            if self._failure_post_behavior == 'destroy':
                self.db_cluster.destroy()
                self.db_cluster = None
            elif self._failure_post_behavior == 'stop':
                for node in self.db_cluster.nodes:
                    node.instance.stop()
                self.db_cluster = None
        if self.loaders is not None:
            self.loaders.get_backtraces()
            if self._failure_post_behavior == 'destroy':
                self.loaders.destroy()
                self.loaders = None
            elif self._failure_post_behavior == 'stop':
                for node in self.loaders.nodes:
                    node.instance.stop()
                self.db_cluster = None
        if self.monitors is not None:
            self.monitors.get_backtraces()
            self.monitors.get_monitor_snapshot()
            self.monitors.download_monitor_data()

            # upload prometheus data
            prometheus_folder = glob.glob(os.path.join(self.logdir, '*monitor*/*monitor*/prometheus/'))
            if prometheus_folder:
                prometheus_folder = prometheus_folder[0]
                file_path = os.path.normpath(self.job.logdir)
                shutil.make_archive(file_path, 'zip', prometheus_folder)
                result_path = '%s.zip' % file_path
                with open(result_path) as fh:
                    mydata = fh.read()
                    url_s3 = ClusterTester.get_s3_url(result_path)
                    self.log.info("uploading prometheus data on %s" % url_s3)
                    response = requests.put(url_s3, data=mydata)
                    self.log.info(response.text)

            grafana_snapshots = glob.glob(os.path.join(self.logdir, '*monitor*/*grafana-snapshot*'))
            if grafana_snapshots:
                result_path = '%s.png' % file_path
                with open(grafana_snapshots[0]) as fh:
                    mydata = fh.read()
                    url_s3 = ClusterTester.get_s3_url(result_path)
                    self.log.info("uploading grafana snapshot data on %s" % url_s3)
                    response = requests.put(url_s3, data=mydata)
                    self.log.info(response.text)

            if self._failure_post_behavior == 'destroy':
                self.monitors.destroy()
                self.monitors = None
            elif self._failure_post_behavior == 'stop':
                for node in self.monitors.nodes:
                    node.instance.stop()
                self.monitors = None
        if self.credentials is not None:
            cluster.remove_cred_from_cleanup(self.credentials)
            if self._failure_post_behavior == 'destroy':
                for cr in self.credentials:
                    cr.destroy()
                self.credentials = []

        if getattr(self, 'stats', None):
            if db_cluster_errors:
                self.stats['errors'] = db_cluster_errors
            if db_cluster_coredumps:
                self.stats['coredumps'] = db_cluster_coredumps
            self.update_test_status()

        if db_cluster_coredumps:
            self.fail('Found coredumps on DB cluster nodes: %s' %
                      db_cluster_coredumps)

        if db_cluster_errors:
            self.log.error('Errors found on DB node logs:')
            for node_errors in db_cluster_errors:
                for node_name in node_errors:
                    for (index, line) in node_errors[node_name]:
                        self.log.error('%s: L%s -> %s',
                                       node_name, index + 1, line.strip())
            self.fail('Errors found on DB node logs (see test logs)')

    def tearDown(self):
        self.clean_resources()

    def get_scylla_versions(self):
        versions = {}
        try:
            versions_output = self.db_cluster.nodes[0].remoter.run('rpm -qa | grep scylla').stdout.splitlines()
            for line in versions_output:
                for package in ['scylla-jmx', 'scylla-server', 'scylla-tools']:
                    match = re.search('(%s-(\S+)-(0.)?([0-9]{8,8}).(\w+).)' % package, line)
                    if match:
                        versions[package] = {'version': match.group(2),
                                             'date': match.group(4),
                                             'commit_id': match.group(5)}
        except Exception as ex:
            self.log.error('Failed getting scylla versions: %s', ex)

        return versions

    def get_setup_details(self):
        setup_details = {}
        is_gce = False
        for p in self.params.iteritems():
            if ('/run/backends/gce', 'cluster_backend', 'gce') == p:
                is_gce = True
        for p in self.params.iteritems():
            if p[1] in ['stress_cmd', 'stress_modes']:
                continue
            else:
                if is_gce and (p[0], p[1]) in \
                        [('/run', 'instance_type_loader'),
                         ('/run', 'instance_type_monitor'),
                         ('/run/databases/scylla', 'instance_type_db')]:
                    # exclude these params from gce run
                    continue
                elif p[1] == 'n_db_nodes' and isinstance(p[2], str) and re.search('\s', p[2]):  # multidc
                    p[2] = re.sub('\s', '_', p[2])
                setup_details[p[1]] = p[2]

        if 'es_password' in setup_details:
            # hide ES password
            setup_details['es_password'] = '******'

        new_scylla_packages = self.params.get('update_db_packages')
        setup_details['packages_updated'] = True if new_scylla_packages and os.listdir(new_scylla_packages) else False

        return setup_details

    def get_test_details(self):
        test_details = {}
        test_details['test_name'] = self.params.id.name
        test_details['sct_git_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

        test_details['job_name'] = os.environ.get('JOB_NAME', 'local_run')
        if os.environ.get('BUILD_URL'):
            test_details['job_url'] = os.environ.get('BUILD_URL', '')

        test_details['start_host'] = platform.node()
        test_details['test_duration'] = self._duration

        return test_details

    def update_test_details(self):
        self.stats['test_details']['time_completed'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        if self.monitors:
            url_s3 = ClusterTester.get_s3_url(os.path.normpath(self.job.logdir))
            self.stats['test_details']['prometheus_report'] = url_s3 + ".zip"
            self.stats['test_details']['grafana_snapshot'] = url_s3 + ".png"
        self.update_test_stats(dict(test_details=self.stats['test_details']))

    def update_stress_cmd_details(self, cmd, prefix=''):
        section = '{0}cassandra-stress'.format(prefix)
        if section not in self.stats['test_details']:
            self.stats['test_details'][section] = [] if self.create_stats else {}
        cmd_params = get_stress_cmd_params(cmd)
        if cmd_params:
            self.stats['test_details'][section].append(cmd_params) if self.create_stats else\
                self.stats['test_details'][section].update(cmd_params)
            self.update_test_stats(dict(test_details=self.stats['test_details']))

    def update_bench_stress_cmd_details(self, cmd, prefix=''):
        section = '{0}scylla-bench'.format(prefix)
        if section not in self.stats['test_details']:
            self.stats['test_details'][section] = [] if self.create_stats else {}
        cmd_params = get_stress_bench_cmd_params(cmd)
        if cmd_params:
            self.stats['test_details'][section].append(cmd_params) if self.create_stats else\
                self.stats['test_details'][section].update(cmd_params)
            self.update_test_stats(dict(test_details=self.stats['test_details']))

    def update_stress_results(self, results):
        if 'stats' not in self.stats['results']:
            self.stats['results']['stats'] = results
        else:
            self.stats['results']['stats'].extend(results)
        self.calculate_stats_average()
        self.update_test_stats(dict(results=self.stats['results']))

    def calculate_stats_average(self):
        average_stats = {}
        total_stats = {}

        for key in self.stats['results']['stats'][0].keys():
            summary = 0
            for stat in self.stats['results']['stats']:
                try:
                    summary += float(stat[key])
                except:
                    average_stats[key] = stat[key]
            if summary != summary:
                # handle non float value
                average_stats[key] = None
            if key not in average_stats:
                average_stats[key] = round(summary / len(self.stats['results']['stats']), 1)
                if key in ['op rate', 'Total errors']:
                    total_stats.update({key: summary})

        self.stats['results']['stats_average'] = average_stats
        self.stats['results']['stats_total'] = total_stats

    def update_test_status(self):
        self.stats['status'] = self.status
        self.update_test_stats(dict(status=self.stats['status']))

    def create_test_stats(self):
        self.test_id = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.stats = {k: {} for k in ['test_details', 'setup_details', 'versions', 'results', 'nemesis',
                                      'errors', 'coredumps']}
        self.stats['setup_details'] = self.get_setup_details()
        self.stats['versions'] = self.get_scylla_versions()
        self.stats['test_details'] = self.get_test_details()
        db = es.ES()
        db.create(self.test_index, self.test_type, self.test_id, self.stats)

    def update_test_stats(self, data=None):
        db = es.ES()
        if not data:
            data = self.stats
        if self.test_id:
            try:
                self.log.info('Update info for test %s: %s', self.test_id, data)
                db.update(self.test_index, self.test_type, self.test_id, data)
            except Exception as ex:
                self.log.error('Failed to update test stats: test_id: %s, error: %s', self.test_id, ex)

    def check_regression(self):
        ra = ResultsAnalyzer(index=self.test_index)
        is_gce = True if self.params.get('cluster_backend') == 'gce' else False
        try:
            ra.check_regression(self.test_id, is_gce)
        except Exception as ex:
            self.log.exception('Failed to check regression: %s', ex)

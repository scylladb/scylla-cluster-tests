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

import re
import os
import logging
import time
import types
from functools import wraps
import boto3.session
import libvirt
from avocado import Test
from avocado.utils.process import CmdError
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
from .cluster_libvirt import LoaderSetLibvirt
from .cluster_openstack import LoaderSetOpenStack
from .cluster_libvirt import MonitorSetLibvirt
from .cluster_openstack import MonitorSetOpenStack
from .cluster import NoMonitorSet, SCYLLA_DIR
from .cluster import RemoteCredentials
from .cluster_libvirt import ScyllaLibvirtCluster
from .cluster_openstack import ScyllaOpenStackCluster
from .cluster import UserRemoteCredentials
from .cluster_gce import ScyllaGCECluster
from .cluster_gce import LoaderSetGCE
from .cluster_gce import MonitorSetGCE
from .cluster_aws import CassandraAWSCluster
from .cluster_aws import ScyllaAWSCluster
from .cluster_aws import LoaderSetAWS
from .cluster_aws import MonitorSetAWS
from .utils import get_data_dir_path, log_run_info, retrying
from . import docker
from . import cluster_baremetal
from . import db_stats
from sdcm.db_stats import PrometheusDBStats
from results_analyze import PerformanceResultsAnalyzer


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


def clean_resources_on_exception(method):
    """
    Ensure that resources used in test are cleaned upon unhandled exceptions.

    :param method: ScyllaClusterTester method to wrap.
    :return: Wrapped method.
    """
    @wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception:
            TEST_LOG.exception("Exception in %s. Will clean resources", method.__name__)
            args[0].clean_resources()
            raise
    return wrapper


class ClusterTester(db_stats.TestStatsMixin, Test):

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

        cluster.Setup.reuse_cluster(self.params.get('reuse_cluster', default=False))

        # for saving test details in DB
        self.create_stats = self.params.get(key='store_results_in_elasticsearch',default=True)
        self.scylla_dir = SCYLLA_DIR
        self.scylla_hints_dir = os.path.join(self.scylla_dir, "hints")


    @clean_resources_on_exception
    def setUp(self):
        self.credentials = []
        self.db_cluster = None
        self.cs_db_cluster = None
        self.loaders = None
        self.monitors = None
        self.connections = []
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        if self.create_stats:
            self.create_test_stats()
        self.init_resources()
        if self.params.get('seeds_first', default='false') == 'true':
            seeds_num = self.params.get('seeds_num', default=1)
            self.db_cluster.wait_for_init(node_list=self.db_cluster.nodes[:seeds_num])
            self.db_cluster.wait_for_init(node_list=self.db_cluster.nodes[seeds_num:])
        else:
            self.db_cluster.wait_for_init()
        if self.cs_db_cluster:
            self.cs_db_cluster.wait_for_init()
        db_node_address = self.db_cluster.nodes[0].private_ip_address
        self.loaders.wait_for_init(db_node_address=db_node_address)
        self.monitors.wait_for_init()

        # cancel reuse cluster - for new nodes added during the test
        cluster.Setup.reuse_cluster(False)
        self.prometheusDB = PrometheusDBStats(host=self.monitors.nodes[0].public_ip_address)

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

        scylla_repo = get_data_dir_path('scylla.repo')
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
                                                params=self.params,
                                                targets=dict(db_cluster=self.db_cluster,
                                                             loaders=self.loaders)
                                                )
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
        self.credentials.append(UserRemoteCredentials(key_file=user_credentials))

        gce_image_db = self.params.get('gce_image_db')
        if not gce_image_db:
            gce_image_db = self.params.get('gce_image')
        gce_image_monitor = self.params.get('gce_image_monitor')
        if not gce_image_monitor:
            gce_image_monitor = self.params.get('gce_image')
        cluster_additional_disks = {'pd-ssd': self.params.get('gce_pd_ssd_disk_size_db', default=0),
                                    'pd-standard': self.params.get('gce_pd_standard_disk_size_db', default=0)}
        common_params = dict(gce_image_username=self.params.get('gce_image_username'),
                             gce_network=self.params.get('gce_network', default='default'),
                             credentials=self.credentials,
                             user_prefix=user_prefix,
                             params=self.params,
                             )
        self.db_cluster = ScyllaGCECluster(gce_image=gce_image_db,
                                           gce_image_type=db_info['disk_type'],
                                           gce_image_size=db_info['disk_size'],
                                           gce_n_local_ssd=db_info['n_local_ssd'],
                                           gce_instance_type=db_info['type'],
                                           services=services,
                                           n_nodes=db_info['n_nodes'],
                                           add_disks=cluster_additional_disks,
                                           gce_datacenter=gce_datacenter,
                                           **common_params)

        loader_additional_disks = {'pd-ssd': self.params.get('gce_pd_ssd_disk_size_loader', default=0)}
        self.loaders = LoaderSetGCE(gce_image=self.params.get('gce_image'),
                                    gce_image_type=loader_info['disk_type'],
                                    gce_image_size=loader_info['disk_size'],
                                    gce_n_local_ssd=loader_info['n_local_ssd'],
                                    gce_instance_type=loader_info['type'],
                                    service=services[:1],
                                    n_nodes=loader_info['n_nodes'],
                                    add_disks=loader_additional_disks,
                                    **common_params)

        if monitor_info['n_nodes'] > 0:
            monitor_additional_disks = {'pd-ssd': self.params.get('gce_pd_ssd_disk_size_monitor', default=0)}
            self.monitors = MonitorSetGCE(gce_image=gce_image_monitor,
                                          gce_image_type=monitor_info['disk_type'],
                                          gce_image_size=monitor_info['disk_size'],
                                          gce_n_local_ssd=monitor_info['n_local_ssd'],
                                          gce_instance_type=monitor_info['type'],
                                          service=services[:1],
                                          n_nodes=monitor_info['n_nodes'],
                                          add_disks=monitor_additional_disks,
                                          targets=dict(db_cluster=self.db_cluster,
                                                       loaders=self.loaders),
                                          **common_params)
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
        if monitor_info['disk_size'] is None:
            monitor_info['disk_size'] = self.params.get('aws_root_disk_size_monitor', default=10)
        if monitor_info['device_mappings'] is None:
            monitor_info['device_mappings'] = [{
                "DeviceName": "/dev/sda1",  # Root device
                "Ebs": {
                    "VolumeSize": monitor_info['disk_size'],
                    "VolumeType": "gp2"
                }
            }]
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

        common_params = dict(ec2_security_group_ids=ec2_security_group_ids,
                             ec2_subnet_id=ec2_subnet_id,
                             services=services,
                             credentials=self.credentials,
                             user_prefix=user_prefix,
                             params=self.params
                             )

        def create_cluster(db_type='scylla'):
            cl_params = dict(
                ec2_instance_type=db_info['type'],
                ec2_block_device_mappings=db_info['device_mappings'],
                n_nodes=db_info['n_nodes']
            )
            cl_params.update(common_params)
            if db_type == 'scylla':
                return ScyllaAWSCluster(
                    ec2_ami_id=self.params.get('ami_id_db_scylla').split(),
                    ec2_ami_username=self.params.get('ami_db_scylla_user'),
                    **cl_params)
            elif db_type == 'cassandra':
                return CassandraAWSCluster(
                    ec2_ami_id=self.params.get('ami_id_db_cassandra').split(),
                    ec2_ami_username=self.params.get('ami_db_cassandra_user'),
                    **cl_params)

        db_type = self.params.get('db_type')
        if db_type in ('scylla', 'cassandra'):
            self.db_cluster = create_cluster(db_type)
        elif db_type == 'mixed':
            self.db_cluster = create_cluster('scylla')
            self.cs_db_cluster = create_cluster('cassandra')
        else:
            self.error('Incorrect parameter db_type: %s' %
                       self.params.get('db_type'))

        self.loaders = LoaderSetAWS(
            ec2_ami_id=self.params.get('ami_id_loader').split(),
            ec2_ami_username=self.params.get('ami_loader_user'),
            ec2_instance_type=loader_info['type'],
            ec2_block_device_mappings=loader_info['device_mappings'],
            n_nodes=loader_info['n_nodes'],
            **common_params)

        if monitor_info['n_nodes'] > 0:
            self.monitors = MonitorSetAWS(
                ec2_ami_id=self.params.get('ami_id_monitor').split(),
                ec2_ami_username=self.params.get('ami_monitor_user'),
                ec2_instance_type=monitor_info['type'],
                ec2_block_device_mappings=monitor_info['device_mappings'],
                n_nodes=monitor_info['n_nodes'],
                targets=dict(db_cluster=self.db_cluster,
                             loaders=self.loaders),
                **common_params)
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
                                              params=self.params,
                                              targets=dict(db_cluster=self.db_cluster,
                                                           loaders=self.loaders)
                                              )
        else:
            self.monitors = NoMonitorSet()

    def get_cluster_docker(self):
        user_credentials = self.params.get('user_credentials_path', None)
        self.credentials.append(UserRemoteCredentials(key_file=user_credentials))
        params = dict(
            docker_image=self.params.get('docker_image', None),
            n_nodes=[self.params.get('n_db_nodes')],
            user_prefix=self.params.get('user_prefix', None),
            credentials=self.credentials,
            params=self.params
        )
        self.db_cluster = docker.ScyllaDockerCluster(**params)

        params['n_nodes'] = self.params.get('n_loaders')
        self.loaders = docker.LoaderSetDocker(**params)

        params['n_nodes'] = int(self.params.get('n_monitor_nodes', default=0))
        self.log.warning("Scylla monitoring is currently not supported on Docker")
        self.monitors = NoMonitorSet()

    def get_cluster_baremetal(self):
        user_credentials = self.params.get('user_credentials_path', None)
        self.credentials.append(UserRemoteCredentials(key_file=user_credentials))
        params = dict(
            n_nodes=[self.params.get('n_db_nodes')],
            public_ips=self.params.get('db_nodes_public_ip', None),
            private_ips=self.params.get('db_nodes_private_ip', None),
            user_prefix=self.params.get('user_prefix', None),
            credentials=self.credentials,
            params=self.params,
            targets=dict(db_cluster=self.db_cluster, loaders=self.loaders),
        )
        self.db_cluster = cluster_baremetal.ScyllaPhysicalCluster(**params)

        params['n_nodes'] = self.params.get('n_loaders')
        params['public_ips'] = self.params.get('loaders_public_ip')
        params['private_ips'] = self.params.get('loaders_private_ip')
        self.loaders = cluster_baremetal.LoaderSetPhysical(**params)

        params['n_nodes'] = self.params.get('n_monitor_nodes')
        params['public_ips'] = self.params.get('monitor_nodes_public_ip')
        params['private_ips'] = self.params.get('monitor_nodes_private_ip')
        self.monitors = cluster_baremetal.MonitorSetPhysical(**params)

    @clean_resources_on_exception
    def init_resources(self, loader_info=None, db_info=None,
                       monitor_info=None):
        if loader_info is None:
            loader_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                           'device_mappings': None}
        if db_info is None:
            db_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                       'device_mappings': None}

        if monitor_info is None:
            monitor_info = {'n_nodes': None, 'type': None, 'disk_size': None, 'disk_type': None, 'n_local_ssd': None,
                            'device_mappings': None}

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
        elif cluster_backend == 'docker':
            self.get_cluster_docker()
        elif cluster_backend == 'baremetal':
            self.get_cluster_baremetal()

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

    @clean_resources_on_exception
    def run_stress(self, stress_cmd, duration=None):
        stress_cmd = self._cs_add_node_flag(stress_cmd)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd,
                                              duration=duration)
        self.verify_stress_thread(stress_queue)

    @clean_resources_on_exception
    def run_stress_thread(self, stress_cmd, duration=None, stress_num=1, keyspace_num=1, profile=None, prefix='',
                          keyspace_name='', round_robin=False, stats_aggregate_cmds=True):
        # stress_cmd = self._cs_add_node_flag(stress_cmd)
        if duration is None:
            duration = self.params.get('test_duration')
        timeout = duration * 60 + 600
        if self.create_stats:
            self.update_stress_cmd_details(stress_cmd, prefix, stresser="cassandra-stress", aggregate=stats_aggregate_cmds)
        return self.loaders.run_stress_thread(stress_cmd, timeout,
                                              self.outputdir,
                                              stress_num=stress_num,
                                              keyspace_num=keyspace_num,
                                              keyspace_name=keyspace_name,
                                              profile=profile,
                                              node_list=self.db_cluster.nodes,
                                              round_robin=round_robin)

    @clean_resources_on_exception
    def run_stress_thread_bench(self, stress_cmd, duration=None, stats_aggregate_cmds=True):
        if duration is None:
            duration = self.params.get('test_duration')
        timeout = duration * 60 + 600
        if self.create_stats:
            self.update_stress_cmd_details(stress_cmd, stresser="scylla-bench", aggregate=stats_aggregate_cmds)
        return self.loaders.run_stress_thread_bench(stress_cmd, timeout,
                                              self.outputdir,
                                              node_list=self.db_cluster.nodes)

    def kill_stress_thread(self):
        if self.loaders:  # the test can fail on provision step and loaders are still not provisioned
            if self.params.get('bench_run', default=False):
                self.loaders.kill_stress_thread_bench()
            else:
                self.loaders.kill_stress_thread()

    def verify_stress_thread(self, queue):
        results, errors = self.loaders.verify_stress_thread(queue, self.db_cluster)
        # Sometimes, we might have an epic error messages list
        # that will make small machines driving the avocado test
        # to run out of memory when writing the XML report. Since
        # the error message is merely informational, let's simply
        # use the last 5 lines for the final error message.
        if results and self.create_stats:
            self.update_stress_results(results)
        else:
            self.log.warning('There is no stress results, probably stress thread has failed.')
        errors = errors[-5:]
        if errors:
            self.fail("cassandra-stress errors on "
                      "nodes:\n%s" % "\n".join(errors))

    @clean_resources_on_exception
    def get_stress_results(self, queue, store_results=True):
        results = self.loaders.get_stress_results(queue)
        if store_results and self.create_stats:
            self.update_stress_results(results)
        return results

    @clean_resources_on_exception
    def get_stress_results_bench(self, queue):
        results = self.loaders.get_stress_results_bench(queue)
        if self.create_stats:
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
                  compact_storage=False, in_memory=False):

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
        if in_memory:
            query += " AND in_memory=true AND compaction={'class': 'InMemoryCompactionStrategy'}"
        if compact_storage:
            query += ' AND COMPACT STORAGE'

        session.execute(query)
        time.sleep(0.2)

    def truncate_cf(self, ks_name, table_name, session):
        try:
            session.execute('TRUNCATE TABLE {0}.{1}'.format(ks_name, table_name))
        except Exception as e:
            self.log.debug('Failed to truncate base table {0}.{1}. Error: {2}'.format(ks_name, table_name, e.message))

    def create_materialized_view(self, ks_name, base_table_name, mv_name, mv_partition_key, mv_clustering_key, session,
                                 mv_columns='*', speculative_retry=None, read_repair=None, compression=None,
                                 gc_grace=None, columns=None, compact_storage=False):
        mv_columns_str = mv_columns
        if isinstance(mv_columns, list):
            mv_columns_str = ', '.join(c for c in mv_columns)

        where_clause = []
        mv_partition_key = mv_partition_key if isinstance(mv_partition_key, list) else list(mv_partition_key)
        mv_clustering_key = mv_clustering_key if isinstance(mv_clustering_key, list) else list(mv_clustering_key)

        for kc in mv_partition_key + mv_clustering_key:
            where_clause.append('{} is not null'.format(kc))

        pk_clause = ', '.join(pk for pk in mv_partition_key)
        cl_clause = ', '.join(cl for cl in mv_clustering_key)

        query = 'CREATE MATERIALIZED VIEW {ks}.{mv_name} AS SELECT {mv_columns} FROM {ks}.{table_name} ' \
                    'WHERE {where_clause} PRIMARY KEY ({pk}, {cl}) WITH comment=\'test MV\''.format(ks=ks_name, mv_name=mv_name, mv_columns=mv_columns_str,
                                                    table_name=base_table_name, where_clause=' and '.join(wc for wc in where_clause),
                                                    pk=pk_clause, cl=cl_clause)
        if compression is not None:
            query = ('%s AND compression = { \'sstable_compression\': '
                     '\'%sCompressor\' }' % (query, compression))

        if read_repair is not None:
            query = '%s AND read_repair_chance=%f' % (query, read_repair)
        if gc_grace is not None:
            query = '%s AND gc_grace_seconds=%d' % (query, gc_grace)
        if speculative_retry is not None:
            query = ('%s AND speculative_retry=\'%s\'' %
                     (query, speculative_retry))

        if compact_storage:
            query += ' AND COMPACT STORAGE'

        self.log.debug('MV create statement: {}'.format(query))
        session.execute(query)

    def _wait_for_view(self, cluster, session, ks, view):
        self.log.debug("Waiting for view {}.{} to finish building...".format(ks, view))

        def _view_build_finished(live_nodes_amount):
            result = self.rows_to_list(session.execute("SELECT status FROM system_distributed.view_build_status WHERE keyspace_name='{0}' "
                                                       "AND view_name='{1}'".format(ks, view)))
            self.log.debug('View build status result: {}'.format(result))
            return len([status for status in result  if status[0] == 'SUCCESS']) >= live_nodes_amount

        attempts = 20
        nodes_status = cluster.get_nodetool_status()
        live_nodes_amount = 0
        for dc in nodes_status.itervalues():
            for ip in dc.itervalues():
                if ip['state'] == 'UN':
                    live_nodes_amount += 1

        while attempts > 0:
            if _view_build_finished(live_nodes_amount):
                return
            time.sleep(3)
            attempts -= 1

        raise Exception("View {}.{} not built".format(ks, view))

    def _wait_for_view_build_start(self, session, ks, view, seconds_to_wait = 20):

        def _check_build_started():
            result = self.rows_to_list(session.execute("SELECT last_token FROM system.views_builds_in_progress "
                                                  "WHERE keyspace_name='{0}' AND view_name='{1}'".format(ks, view)))
            self.log.debug('View build in progress: {}'.format(result))
            return result != []

        self.log.debug("Ensure view building started.")
        start = time.time()
        while not _check_build_started():
            if time.time() - start > seconds_to_wait:
                raise Exception("View building didn't start in {} seconds".format(seconds_to_wait))

    @staticmethod
    def rows_to_list(rows):
        return [list(row) for row in rows]

    def clean_resources(self):
        self.log.debug('Cleaning up resources used in the test')
        self.kill_stress_thread()
        db_cluster_errors = None
        db_cluster_coredumps = None
        if self.db_cluster is not None:
            db_cluster_errors = self.db_cluster.get_node_database_errors()
            self.db_cluster.get_backtraces()
            db_cluster_coredumps = self.db_cluster.coredumps
            for current_nemesis in self.db_cluster.nemesis:
                current_nemesis.report()
            # Stopping nemesis, using timeout of 30 minutes, since replace/decommission node can take time
            self.db_cluster.stop_nemesis(timeout=1800)
            # TODO: this should be run in parallel
            for node in self.db_cluster.nodes:
                node.stop_task_threads(timeout=60)

            if self._failure_post_behavior == 'destroy':
                self.db_cluster.destroy()
                self.db_cluster = None
                if self.cs_db_cluster:
                    self.cs_db_cluster.destroy()
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

        self.update_test_details(db_cluster_errors, db_cluster_coredumps)

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

    def populate_data_parallel(self, size_in_gb, blocking=True, read=False):
        base_cmd = "cassandra-stress write cl=QUORUM "
        if read:
            base_cmd = "cassandra-stress read cl=ONE "
        stress_fixed_params = " -schema 'replication(factor=3) compaction(strategy=LeveledCompactionStrategy)' " \
                              "-port jmx=6868 -mode cql3 native -rate threads=200 -col 'size=FIXED(1024) n=FIXED(1)' "
        stress_keys = "n="
        population = " -pop seq="

        total_keys = size_in_gb * 1024 * 1024
        n_loaders = self.params.get('n_loaders')
        keys_per_node = total_keys / n_loaders

        write_queue = list()
        start = 1
        for i in range(1, n_loaders + 1):
            stress_cmd = base_cmd + stress_keys + str(keys_per_node) + population + str(start) + ".." + \
                         str(keys_per_node * i) + stress_fixed_params
            start = keys_per_node * i + 1

            write_queue.append(self.run_stress_thread(stress_cmd=stress_cmd, round_robin=True))
            time.sleep(3)

        if blocking:
            for stress in write_queue:
                self.verify_stress_thread(queue=stress)

        return write_queue

    @log_run_info
    def alter_table_to_in_memory(self, key_space_name="keyspace1", table_name="standard1", node=None):
        if not node:
            node = self.db_cluster.nodes[0]
        compaction_strategy = "%s" % {"class": "InMemoryCompactionStrategy"}
        cql_cmd = "ALTER table {key_space_name}.{table_name} " \
                  "WITH in_memory=true AND compaction={compaction_strategy}".format(**locals())
        node.remoter.run('cqlsh -e "{}" {}'.format(cql_cmd, node.private_ip_address), verbose=True)

    def get_num_of_hint_files(self, node):
        result = node.remoter.run("sudo find {0.scylla_hints_dir} -name *.log -type f| wc -l".format(self),
                                  verbose=True)
        total_hint_files = int(result.stdout.strip())
        self.log.debug("Number of hint files on '%s': %s." % (node.name, total_hint_files))
        return total_hint_files

    def get_num_shards(self, node):
        result = node.remoter.run("sudo ls -1 {0.scylla_hints_dir}| wc -l".format(self), verbose=True)
        return int(result.stdout.strip())

    @retrying(n=3, sleep_time=15, allowed_exceptions=(AssertionError,))
    def hints_sending_in_progress(self):
        q = "sum(rate(scylla_hints_manager_sent{}[15s]))"
        now = time.time()
        # check status of sending hints during last minute range
        results = self.prometheusDB.query(query=q, start=now - 60, end=now)
        self.log.debug("scylla_hints_manager_sent: %s" % results)
        assert results, "No results from Prometheus"
        # if all are zeros the result will be False, otherwise we are still sending
        return any([float(v[1]) for v in results[0]["values"]])

    @retrying(n=30, sleep_time=60, allowed_exceptions=(AssertionError, CmdError))
    def wait_for_hints_to_be_sent(self, node, num_dest_nodes):
        num_shards = self.get_num_shards(node)
        hints_after_send_completed = num_shards * num_dest_nodes
        # after hints were sent to all nodes, the number of files should be 1 per shard per destination
        assert self.get_num_of_hint_files(node) <= hints_after_send_completed, "Waiting until the number of hint files " \
                                                                        "will be %s." % hints_after_send_completed
        assert self.hints_sending_in_progress() is False, "Waiting until Prometheus hints counter will not change"

    def verify_no_drops_and_errors(self, starting_from):
        q_dropped = "sum(rate(scylla_hints_manager_dropped{}[15s]))"
        q_errors = "sum(rate(scylla_hints_manager_errors{}[15s]))"
        queries_to_check = [q_dropped, q_errors]
        for q in queries_to_check:
            results = self.prometheusDB.query(query=q, start=starting_from, end=time.time())
            err_msg = "There were hint manager %s detected during the test!" % "drops" if "dropped" in q else "errors"
            assert any([float(v[1]) for v in results[0]["values"]]) is False, err_msg

    def get_data_set_size(self, cs_cmd):
        """:returns value of n in stress comand, that is approximation and currently doesn't take in consideration
            column size definitions if they present in the command
        """
        try:
            return int(re.search("n=(\d+) ", cs_cmd).group(1))
        except Exception:
            self.fail("Unable to get data set size from cassandra-stress command: %s" % cs_cmd)

    @retrying(n=60, sleep_time=60, allowed_exceptions=(AssertionError,))
    def wait_data_dir_reaching(self, size, node):
        q = '(sum(node_filesystem_size{{mountpoint="{0.scylla_dir}", ' \
            'instance=~"{1.private_ip_address}"}})-sum(node_filesystem_avail{{mountpoint="{0.scylla_dir}", ' \
            'instance=~"{1.private_ip_address}"}}))'.format(self, node)
        res = self.prometheusDB.query(query=q, start=time.time(), end=time.time())
        assert res, "No results from Prometheus"
        used = int(res[0]["values"][0][1]) / (2 ** 10)
        assert used >= size, "Waiting for Scylla data dir to reach '{size}', " \
                             "current size is: '{used}'".format(**locals())

    def check_regression(self):
        ra = PerformanceResultsAnalyzer(es_index=self._test_index, es_doc_type=self._es_doc_type,
                             send_email=self.params.get('send_email', default=True),
                             email_recipients=self.params.get('email_recipients', default=None))
        is_gce = True if self.params.get('cluster_backend') == 'gce' else False
        try:
            ra.check_regression(self._test_id, is_gce)
        except Exception as ex:
            self.log.exception('Failed to check regression: %s', ex)

    # Wait for up to 40 mins that there are no running compactions
    @retrying(n=40, sleep_time=60, allowed_exceptions=(AssertionError,))
    def wait_no_compactions_running(self):
        q = "sum(scylla_compaction_manager_compactions{})"
        now = time.time()
        results = self.prometheusDB.query(query=q, start=now - 60, end=now)
        self.log.debug("scylla_hints_manager_sent: %s" % results)
        assert results, "No results from Prometheus"
        # if all are zeros the result will be False, otherwise there are still compactions
        assert any([float(v[1]) for v in results[0]["values"]]) is False, \
            "Waiting until all compactions settle down"
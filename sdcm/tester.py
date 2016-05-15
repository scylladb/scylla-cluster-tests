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

import logging
import time
import types

import boto3.session

from avocado import Test

from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster as ClusterDriver
from cassandra.cluster import NoHostAvailable
from cassandra.policies import RetryPolicy
from cassandra.policies import WhiteListRoundRobinPolicy

from . import cluster
from . import nemesis
from .cluster import CassandraCluster
from .cluster import LoaderSet
from .cluster import RemoteCredentials
from .cluster import ScyllaCluster
from .data_path import get_data_path

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
            args[0].clean_resources()
            raise
    return wrapper


class ClusterTester(Test):

    def __init__(self, methodName='test', name=None, params=None,
                 base_logdir=None, tag=None, job=None, runner_queue=None):
        super(ClusterTester, self).__init__(methodName=methodName, name=name, params=params,
                                            base_logdir=base_logdir, tag=tag, job=job,
                                            runner_queue=runner_queue)
        self._failure_post_behavior = self.params.get(key='failure_post_behavior', default='destroy')
        self.log.debug("Behavior on failure/post test is '%s'", self._failure_post_behavior)
        cluster.register_cleanup(cleanup=self._failure_post_behavior)

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        self.connections = []
        logging.getLogger('botocore').setLevel(logging.CRITICAL)
        logging.getLogger('boto3').setLevel(logging.CRITICAL)
        self.init_resources()
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()

    def get_nemesis_class(self):
        """
        Get a Nemesis class from parameters.

        :return: Nemesis class.
        :rtype: nemesis.Nemesis derived class
        """
        class_name = self.params.get('nemesis_class_name')
        return getattr(nemesis, class_name)

    @clean_aws_resources
    def init_resources(self, n_db_nodes=None, n_loader_nodes=None, dbs_block_device_mappings=None, loaders_block_device_mappings=None, loaders_type=None, dbs_type=None):
        if n_db_nodes is None:
            n_db_nodes = self.params.get('n_db_nodes')
        if n_loader_nodes is None:
            n_loader_nodes = self.params.get('n_loaders')
        if loaders_type is None:
            loaders_type = self.params.get('instance_type_loader')
        if dbs_type is None:
            dbs_type = self.params.get('instance_type_db')
        user_prefix = self.params.get('user_prefix', None)
        session = boto3.session.Session(region_name=self.params.get('region_name'))
        service = session.resource('ec2')
        self.credentials = RemoteCredentials(service=service,
                                             key_prefix='longevity-test',
                                             user_prefix=user_prefix)

        if self.params.get('db_type') == 'scylla':
            self.db_cluster = ScyllaCluster(ec2_ami_id=self.params.get('ami_id_db_scylla'),
                                            ec2_ami_username=self.params.get('ami_db_scylla_user'),
                                            ec2_security_group_ids=[self.params.get('security_group_ids')],
                                            ec2_subnet_id=self.params.get('subnet_id'),
                                            ec2_instance_type=dbs_type,
                                            service=service,
                                            credentials=self.credentials,
                                            ec2_block_device_mappings=dbs_block_device_mappings,
                                            user_prefix=user_prefix,
                                            n_nodes=n_db_nodes,
                                            params=self.params)
        elif self.params.get('db_type') == 'cassandra':
            self.db_cluster = CassandraCluster(ec2_ami_id=self.params.get('ami_id_db_cassandra'),
                                               ec2_ami_username=self.params.get('ami_db_cassandra_user'),
                                               ec2_security_group_ids=[self.params.get('security_group_ids')],
                                               ec2_subnet_id=self.params.get('subnet_id'),
                                               ec2_instance_type=dbs_type,
                                               service=service,
                                               ec2_block_device_mappings=dbs_block_device_mappings,
                                               credentials=self.credentials,
                                               user_prefix=user_prefix,
                                               n_nodes=n_db_nodes,
                                               params=self.params)
        else:
            self.error('Incorrect parameter db_type: %s' %
                       self.params.get('db_type'))

        scylla_repo = get_data_path('scylla.repo')
        self.loaders = LoaderSet(ec2_ami_id=self.params.get('ami_id_loader'),
                                 ec2_ami_username=self.params.get('ami_loader_user'),
                                 ec2_security_group_ids=[self.params.get('security_group_ids')],
                                 ec2_subnet_id=self.params.get('subnet_id'),
                                 ec2_instance_type=loaders_type,
                                 service=service,
                                 ec2_block_device_mappings=loaders_block_device_mappings,
                                 credentials=self.credentials,
                                 scylla_repo=scylla_repo,
                                 user_prefix=user_prefix,
                                 n_nodes=n_loader_nodes,
                                 params=self.params)

    def get_stress_cmd(self, duration=None, threads=None):
        """
        Get a cassandra stress cmd string.

        The default for this class is RF=3 and CL=QUORUM.
        Other tests might want to override this method to use something
        that suits them better.

        :param duration: Duration of stress (minutes).
        :param threads: Number of threads used by cassandra stress.
        :return: Cassandra stress string
        :rtype: basestring
        """
        ip = self.db_cluster.get_node_private_ips()[0]
        if duration is None:
            duration = self.params.get('cassandra_stress_duration')
        if threads is None:
            threads = self.params.get('cassandra_stress_threads')
        return ("cassandra-stress write cl=QUORUM duration=%sm "
                "-schema 'replication(factor=3)' -port jmx=6868 "
                "-mode cql3 native -rate threads=%s "
                "-pop seq=1..1000000 -node %s" % (duration, threads, ip))

    @clean_aws_resources
    def run_stress(self, stress_cmd=None, duration=None):
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd,
                                              duration=duration)
        self.verify_stress_thread(stress_queue)

    @clean_aws_resources
    def run_stress_thread(self, stress_cmd=None, duration=None):
        if stress_cmd is None:
            stress_cmd = self.get_stress_cmd(duration=duration)
        if duration is None:
            duration = self.params.get('cassandra_stress_duration')
        timeout = duration * 60 + 600
        return self.loaders.run_stress_thread(stress_cmd, timeout,
                                              self.outputdir)

    @clean_aws_resources
    def kill_stress_thread(self):
        self.loaders.kill_stress_thread()

    @clean_aws_resources
    def verify_stress_thread(self, queue):
        errors = self.loaders.verify_stress_thread(queue)
        # Sometimes, we might have an epic error messages list
        # that will make small machines driving the avocado test
        # to run out of memory when writing the XML report. Since
        # the error message is merely informational, let's simply
        # use the last 5 lines for the final error message.
        errors = errors[-5:]
        if errors:
            self.fail("cassandra-stress errors on "
                      "nodes:\n%s" % "\n".join(errors))

    def get_auth_provider(self, user, password):
        return PlainTextAuthProvider(username=user, password=password)

    def _create_session(self, node, keyspace, user, password, compression,
                        protocol_version, load_balancing_policy=None,
                        port=None, ssl_opts=None):
        node_ips = [node.instance.public_ip_address]
        if not port:
            port = 9042

        if protocol_version is None:
            protocol_version = 3

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

        wlrr = WhiteListRoundRobinPolicy([node.instance.public_ip_address])
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

    def clean_resources(self):
        self.log.debug('Cleaning up resources used in the test')
        if self.db_cluster is not None:
            self.db_cluster.get_backtraces()
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
        if self.credentials is not None:
            cluster.remove_cred_from_cleanup(self.credentials)
            if self._failure_post_behavior == 'destroy':
                self.credentials.destroy()
                self.credentials = None

    def tearDown(self):
        self.clean_resources()

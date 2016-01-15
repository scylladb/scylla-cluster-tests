import boto3.session

from avocado import Test

from .cluster import LoaderSet
from .cluster import RemoteCredentials
from .cluster import ScyllaCluster
from .cluster import CassandraCluster
from .data_path import get_data_path

import cluster


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

    @clean_aws_resources
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        self.init_resources()
        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()

    def init_resources(self):
        session = boto3.session.Session(region_name=self.params.get('region_name'))
        service = session.resource('ec2')
        self.credentials = RemoteCredentials(service=service,
                                             key_prefix='longevity-test')

        if self.params.get('db_type') == 'scylla':
            self.db_cluster = ScyllaCluster(ec2_ami_id=self.params.get('ami_id_db_scylla'),
                                            ec2_security_group_ids=[self.params.get('security_group_ids')],
                                            ec2_subnet_id=self.params.get('subnet_id'),
                                            ec2_instance_type=self.params.get('instance_type_db'),
                                            service=service,
                                            credentials=self.credentials,
                                            n_nodes=self.params.get('n_db_nodes'))
        elif self.params.get('db_type') == 'cassandra':
            self.db_cluster = CassandraCluster(ec2_ami_id=self.params.get('ami_id_db_cassandra'),
                                               ec2_security_group_ids=[self.params.get('security_group_ids')],
                                               ec2_subnet_id=self.params.get('subnet_id'),
                                               ec2_instance_type=self.params.get('instance_type_db'),
                                               service=service,
                                               credentials=self.credentials,
                                               n_nodes=self.params.get('n_db_nodes'))
        else:
            self.error('Incorrect parameter db_type: {}'.format(self.params.get('db_type')))

        scylla_repo = get_data_path('scylla.repo')
        self.loaders = LoaderSet(ec2_ami_id=self.params.get('ami_id_loader'),
                                 ec2_security_group_ids=[self.params.get('security_group_ids')],
                                 ec2_subnet_id=self.params.get('subnet_id'),
                                 ec2_instance_type=self.params.get('instance_type_loader'),
                                 service=service,
                                 credentials=self.credentials,
                                 scylla_repo=scylla_repo,
                                 n_nodes=self.params.get('n_loaders'))

    @clean_aws_resources
    def run_stress(self, duration=None):
        # pickup the first node
        # cassandra-stress driver is topology aware
        # and will contact the others nodes
        ip = self.db_cluster.get_node_private_ips()[0]
        # Use replication factor = 3 (-schema 3)
        stress_cmd = ("cassandra-stress write cl=QUORUM duration={}m -schema 'replication(factor=3)' -port jmx=6868 "
                      "-mode cql3 native -rate threads={} "
                      "-node {}".format(self.params.get('duration'),
                                        self.params.get('threads'),
                                        ip))

        if duration is not None:
            timeout = duration * 60
        else:
            timeout = int(int(self.params.get('duration')) * 60)
        timeout += 180
        errors = self.loaders.run_stress(stress_cmd, timeout,
                                         self.outputdir)
        if errors:
            self.fail("cassandra-stress errors on nodes:\n{}".format("\n".join(errors)))

    def clean_resources(self):
        print('Cleaning up resources used in the test')
        if self.db_cluster is not None:
            self.db_cluster.destroy()
            self.db_cluster = None
        if self.loaders is not None:
            self.loaders.destroy()
            self.loaders = None
        if self.credentials is not None:
            cluster.remove_cred_from_cleanup(self.credentials)
            self.credentials.destroy()
            self.credentials = None

    def tearDown(self):
        self.clean_resources()

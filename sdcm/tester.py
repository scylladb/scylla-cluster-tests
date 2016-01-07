import boto3.session

from avocado import Test

from .cluster import LoaderSet
from .cluster import RemoteCredentials
from .cluster import ScyllaCluster


class ScyllaClusterTester(Test):

    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        try:
            self.init_resources()
            self.loaders.wait_for_init()
            self.db_cluster.wait_for_init()
        except Exception:
            self.clean_resources()
            raise

    def init_resources(self):
        session = boto3.session.Session(region_name=self.params.get('region_name'))
        service = session.resource('ec2')
        self.credentials = RemoteCredentials(service=service,
                                             key_prefix='longevity-test')

        self.db_cluster = ScyllaCluster(ec2_ami_id=self.params.get('ami_id_db'),
                                        ec2_security_group_ids=[self.params.get('security_group_ids')],
                                        ec2_subnet_id=self.params.get('subnet_id'),
                                        ec2_instance_type=self.params.get('instance_type_db'),
                                        service=service,
                                        credentials=self.credentials,
                                        n_nodes=self.params.get('n_db_nodes'))

        scylla_repo = self.get_data_path('scylla.repo')
        self.loaders = LoaderSet(ec2_ami_id=self.params.get('ami_id_loader'),
                                 ec2_security_group_ids=[self.params.get('security_group_ids')],
                                 ec2_subnet_id=self.params.get('subnet_id'),
                                 ec2_instance_type=self.params.get('instance_type_loader'),
                                 service=service,
                                 credentials=self.credentials,
                                 scylla_repo=scylla_repo,
                                 n_nodes=self.params.get('n_loaders'))

    def run_stress(self, duration=None):
        try:
            scylla_node_ips = ",".join(self.db_cluster.get_node_private_ips())
            # Use replication factor = 3 (-schema 3)
            stress_cmd = ("cassandra-stress write cl=QUORUM duration={}m -schema 'replication(factor=3)' "
                          "-mode cql3 native -rate threads={} "
                          "-node {}".format(self.params.get('duration'),
                                            self.params.get('threads'),
                                            scylla_node_ips))

            if duration is not None:
                timeout = duration * 60
            else:
                timeout = int(int(self.params.get('duration')) * 60)
            timeout += 180
            errors = self.loaders.run_stress(stress_cmd, timeout,
                                             self.outputdir)
            if errors:
                self.fail("cassandra-stress errors on nodes:\n{}".format("\n".join(errors)))
        except:
            self.clean_resources()
            raise

    def clean_resources(self):
        print('Cleaning up resources used in the test')
        if self.db_cluster is not None:
            self.db_cluster.destroy()
            self.db_cluster = None
        if self.loaders is not None:
            self.loaders.destroy()
            self.loaders = None
        if self.credentials is not None:
            self.credentials.destroy()
            self.credentials = None

    def tearDown(self):
        self.clean_resources()

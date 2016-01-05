#!/usr/bin/env python

from avocado import main
from avocado import Test

import boto3.session

from cluster import ScyllaCluster
from cluster import LoaderSet
from cluster import RemoteCredentials

stress_params = {'duration': 10,
                 'threads': 4}

node_amounts = {'n_db_nodes': 4,
                'n_loaders': 2}

us_west_2 = {'region_name': 'us-west-2',
             'ami_id_db': 'ami-20776841',
             'instance_type_db': 'c4.xlarge',
             'ami_id_loader': 'ami-05f4ed35',
             'instance_type_loader': 'c4.xlarge',
             'security_group_ids': ['sg-81703ae4'],
             'subnet_id': 'subnet-5207ee37'}

us_east_1 = {'region_name': 'us-east-1',
             'ami_id_db': 'ami-972863fd',
             'instance_type_db': 'c4.xlarge',
             'ami_id_loader': 'ami-a51564c0',
             'instance_type_loader': 'c4.xlarge',
             'security_group_ids': ['sg-c5e1f7a0'],
             'subnet_id': 'subnet-ec4a72c4'}

us_east_1.update(stress_params)
us_east_1.update(node_amounts)
us_west_2.update(stress_params)
us_west_2.update(node_amounts)


class DistributedClusterTester(Test):
    def setUp(self):
        args = us_west_2
        self.args = args
        print('Starting test. Args: {}'.format(args))
        session = boto3.session.Session(region_name=args['region_name'])
        service = session.resource('ec2')
        self.credentials = RemoteCredentials(service=service,
                                             key_prefix='longevity-test')

        self.db_cluster = ScyllaCluster(ec2_ami_id=args['ami_id_db'],
                                        ec2_security_group_ids=args['security_group_ids'],
                                        ec2_subnet_id=args['subnet_id'],
                                        ec2_instance_type=args['instance_type_db'],
                                        service=service,
                                        credentials=self.credentials,
                                        n_nodes=args['n_db_nodes'])

        self.loaders = LoaderSet(ec2_ami_id=args['ami_id_loader'],
                                 ec2_security_group_ids=args['security_group_ids'],
                                 ec2_subnet_id=args['subnet_id'],
                                 ec2_instance_type=args['instance_type_loader'],
                                 service=service,
                                 credentials=self.credentials,
                                 n_nodes=args['n_loaders'])

        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()

    def run_stress(self, duration=None):
        try:
            scylla_node_ips = ",".join(self.db_cluster.get_node_private_ips())
            stress_cmd = ('cassandra-stress write duration={}m '
                          '-mode cql3 native -rate threads={} '
                          '-node {}'.format(self.args['duration'],
                                            self.args['threads'],
                                            scylla_node_ips))

            if duration is not None:
                timeout = duration * 60
            else:
                timeout = int(self.args['duration'] * 60)
            timeout += 180

            self.loaders.run_stress(stress_cmd, timeout)
        except:
            self.clean_resources()
            raise

    def clean_resources(self):
        print('Cleaning up resources used in the test')
        self.db_cluster.destroy()
        self.loaders.destroy()
        self.credentials.destroy()

    def tearDown(self):
        self.clean_resources()


class LongevityTest(DistributedClusterTester):

    def test_10min(self):
        """
        avocado: enable
        """
        self.run_stress(duration=10)


if __name__ == '__main__':
    main()

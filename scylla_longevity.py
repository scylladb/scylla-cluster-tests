#!/usr/bin/env python

import os

import boto3.session

from avocado import Test
from avocado import main

from sdcm.cluster import LoaderSet
from sdcm.cluster import RemoteCredentials
from sdcm.cluster import ScyllaCluster
from sdcm.nemesis import ChaosMonkey

stress_params = {'duration': 10,
                 'threads': 4}

node_amounts = {'n_db_nodes': 4,
                'n_loaders': 1}

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


class ScyllaClusterTester(Test):
    def setUp(self):
        self.credentials = None
        self.db_cluster = None
        self.loaders = None
        args = us_west_2
        self.args = args
        print('Starting test. Args: {}'.format(args))
        try:
            self.init_resources(args)
        except:
            self.clean_resources()

        self.loaders.wait_for_init()
        self.db_cluster.wait_for_init()

    def init_resources(self, args):
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

        scylla_repo = os.path.join('scylla_longevity.py.data', 'scylla.repo')
        self.loaders = LoaderSet(ec2_ami_id=args['ami_id_loader'],
                                 ec2_security_group_ids=args['security_group_ids'],
                                 ec2_subnet_id=args['subnet_id'],
                                 ec2_instance_type=args['instance_type_loader'],
                                 service=service,
                                 credentials=self.credentials,
                                 scylla_repo=scylla_repo,
                                 n_nodes=args['n_loaders'])

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


class LongevityTest(ScyllaClusterTester):

    """
    Test a Scylla cluster stability over a time period.

    :avocado: enable
    """
    def test_twenty_minutes(self):
        """
        Run a very short test, as a config/code sanity check.
        """
        self.db_cluster.add_nemesis(ChaosMonkey)
        self.db_cluster.start_nemesis(interval=5)
        self.run_stress(duration=20)


if __name__ == '__main__':
    main()

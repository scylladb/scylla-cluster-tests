#!/usr/bin/env python

import boto3.session
from cluster import ScyllaCluster
from cluster import LoaderSet
from cluster import RemoteCredentials


def longevity_test(args):
    print('Starting test. Args: {}'.format(args))
    session = boto3.session.Session(region_name=args['region_name'])
    service = session.resource('ec2')
    credentials = RemoteCredentials(service=service,
                                    key_prefix='longevity-test')

    scylla_cluster = ScyllaCluster(ec2_ami_id=args['ami_id_db'],
                                   ec2_security_group_ids=args['security_group_ids'],
                                   ec2_subnet_id=args['subnet_id'],
                                   ec2_instance_type=args['instance_type_db'],
                                   service=service,
                                   credentials=credentials,
                                   n_nodes=args['n_db_nodes'])

    loaders = LoaderSet(ec2_ami_id=args['ami_id_loader'],
                        ec2_security_group_ids=args['security_group_ids'],
                        ec2_subnet_id=args['subnet_id'],
                        ec2_instance_type=args['instance_type_loader'],
                        service=service,
                        credentials=credentials,
                        n_nodes=args['n_loaders'])

    try:
        loaders.wait_for_init()
        scylla_cluster.wait_for_init()
        scylla_node_ips = ",".join(scylla_cluster.get_node_internal_ips())
        stress_cmd = ('cassandra-stress write duration={}m '
                      '-mode cql3 native -rate threads={} '
                      '-node {}'.format(args['duration'], args['threads'],
                                        scylla_node_ips))

        timeout = int(args['duration'] * 60) + 180
        loaders.run_stress(stress_cmd, timeout)

    finally:
        print('Cleaning up resources used in the test')
        scylla_cluster.destroy()
        loaders.destroy()
        credentials.destroy()


if __name__ == '__main__':
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

    longevity_test(args=us_east_1)

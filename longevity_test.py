import boto3.session
from cluster import ScyllaCluster
from cluster import LoaderSet
from cluster import RemoteCredentials


def main(args):
    print('Starting test. Args: {}'.format(args))
    session = boto3.session.Session(region_name=args['region_name'])
    service = session.resource('ec2')
    credentials = RemoteCredentials(service=service,
                                    key_prefix='longevity-test')

    scylla_cluster = ScyllaCluster(ec2_ami_id=args['ami_id_db'],
                                   ec2_security_group_ids=args['security_group_ids'],
                                   ec2_subnet_id=args['subnet_id'],
                                   service=service,
                                   credentials=credentials,
                                   n_nodes=3)

    loaders = LoaderSet(ec2_ami_id=args['ami_id_loader'],
                        ec2_security_group_ids=args['security_group_ids'],
                        ec2_subnet_id=args['subnet_id'],
                        service=service,
                        credentials=credentials,
                        n_nodes=1)

    try:
        loaders.wait_for_init()
        scylla_cluster.wait_for_init()
        scylla_node_ips = ",".join(scylla_cluster.get_node_internal_ips())
        stress_cmd = ('cassandra-stress write duration=30m '
                      '-mode cql3 native -rate threads=4 '
                      '-node {}'.format(scylla_node_ips))
        results = [result for result in
                   loaders.run_all_nodes(stress_cmd, timeout=33*60)]
        print(results)
    finally:
        scylla_cluster.destroy()
        loaders.destroy()
        credentials.destroy()


if __name__ == '__main__':
    us_west_2 = {'region_name': 'us-west-2',
                 'ami_id_db': 'ami-20776841',
                 'ami_id_loader': 'ami-05f4ed35',
                 'security_group_ids': ['sg-81703ae4'],
                 'subnet_id': 'subnet-5207ee37'}
    us_east_1 = {'region_name': 'us-east-1',
                 'ami_id_db': 'ami-972863fd',
                 'ami_id_loader': 'ami-a51564c0',
                 'security_group_ids': ['sg-c5e1f7a0'],
                 'subnet_id': 'subnet-ec4a72c4'}
    main(args=us_east_1)

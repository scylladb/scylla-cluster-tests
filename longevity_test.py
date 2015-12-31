import boto3.session
import time
from cluster import ScyllaCluster
from cluster import LoaderCluster
from cluster import RemoteCredentials


def main():
    session = boto3.session.Session(region_name='us-west-2')
    service = session.resource('ec2')
    ami_id = 'ami-20776841'
    security_group_ids = ['sg-81703ae4']
    subnet_id = 'subnet-5207ee37'
    key_name = 'keypair-{}-{}'.format('longevity_tests',
                                      time.strftime('%Y-%m-%d-%H.%M.%S'))
    credentials = RemoteCredentials(service=service, name=key_name)

    scylla_cluster = ScyllaCluster(ec2_ami_id=ami_id,
                                   ec2_security_group_ids=security_group_ids,
                                   ec2_subnet_id=subnet_id,
                                   service=service,
                                   credentials=credentials,
                                   n_nodes=4)

    loaders = LoaderCluster(ec2_ami_id=ami_id,
                            ec2_security_group_ids=security_group_ids,
                            ec2_subnet_id=subnet_id,
                            service=service,
                            credentials=credentials,
                            n_nodes=1)

    try:
        scylla_cluster.wait_for_init()
        scylla_node_ips = ",".join(scylla_cluster.get_node_internal_ips())
        stress_cmd = ('cassandra-stress write duration=30m '
                      '-mode cql3 native -rate threads=4 '
                      '-node {}'.format(scylla_node_ips))
        results = [result for result in loaders.run_all_nodes(stress_cmd)]
        print(results)
    finally:
        for node in scylla_cluster.nodes + loaders.nodes:
            node.destroy()
        credentials.destroy()


if __name__ == '__main__':
    main()

import boto3
import time
from cluster import ScyllaCluster
from cluster import LoaderCluster
from cluster import RemoteCredentials


def main():
    service = boto3.resource('ec2')
    ami_id = 'ami-e08ccb8a'
    security_group_ids = ['sg-c5e1f7a0']
    subnet_id = 'subnet-ec4a72c4'
    key_name = 'keypair-{}-{}'.format('longevity_tests',
                                      time.strftime('%Y-%m-%d-%H.%M.%S'))
    credentials = RemoteCredentials(service=service, name=key_name)

    scylla_cluster = ScyllaCluster(ec2_ami_id=ami_id,
                                   ec2_security_group_ids=security_group_ids,
                                   ec2_subnet_id=subnet_id,
                                   service=service,
                                   credentials=credentials,
                                   n_nodes=2)

    loaders = LoaderCluster(ec2_ami_id=ami_id,
                            ec2_security_group_ids=security_group_ids,
                            ec2_subnet_id=subnet_id,
                            service=service,
                            credentials=credentials,
                            n_nodes=1)

    for node in scylla_cluster.nodes + loaders.nodes:
        node.remoter.uptime()

    time.sleep(180)

    for loader_node in loaders.nodes:
        loader_node.remoter.run('systemctl stop scylla-server.service',
                                ignore_status=True)
        loader_node.remoter.run('systemctl stop scylla-jmx.service',
                                ignore_status=True)

    scylla_node_ips = ",".join(scylla_cluster.get_node_internal_ips())
    stress_cmd = ('cassandra-stress write n=1000 -mode cql3 native -rate '
                  'threads=4 -node {}'.format(scylla_node_ips))

    for loader_node in loaders.nodes:
        loader_node.remoter.run(stress_cmd)


if __name__ == '__main__':
    main()

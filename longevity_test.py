import boto3.session
import time
from cluster import ScyllaCluster
from cluster import LoaderCluster
from cluster import RemoteCredentials
from remote import CmdError


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
                                   n_nodes=2)

    loaders = LoaderCluster(ec2_ami_id=ami_id,
                            ec2_security_group_ids=security_group_ids,
                            ec2_subnet_id=subnet_id,
                            service=service,
                            credentials=credentials,
                            n_nodes=1)

    all_nodes = scylla_cluster.nodes + loaders.nodes
    node_initialized_map = {node: False for node in scylla_cluster.nodes}
    all_nodes_initialized = [True for _ in scylla_cluster.nodes]
    verify_pause = 30

    try:
        print("Waiting until all DB nodes are functional")
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                try:
                    node.remoter.run('netstat -a | grep :9042', timeout=120)
                    node_initialized_map[node] = True
                except CmdError:
                    pass
            print("Nodes functional map: {}".format(node_initialized_map))
            print("Waiting {} s before checking again".format(verify_pause))
            time.sleep(verify_pause)

        scylla_node_ips = ",".join(scylla_cluster.get_node_internal_ips())
        stress_cmd = ('cassandra-stress write duration=60m n=1000 '
                      '-mode cql3 native -rate threads=4 '
                      '-node {}'.format(scylla_node_ips))

        for loader_node in loaders.nodes:
            loader_node.remoter.run(stress_cmd)
    finally:
        for node in all_nodes:
            node.destroy()
        credentials.destroy()


if __name__ == '__main__':
    main()

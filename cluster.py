import base64
import os
import tempfile
import time

from remote import Remote


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, name):
        self.key_pair = service.create_key_pair(KeyName=name)
        self.key_file = os.path.join(tempfile.gettempdir(),
                                     '{}.pem'.format(name))
        self.write_key_file()
        print("Created Key Pair {} -> {}".format(name, self.key_file))

    def write_key_file(self):
        with open(self.key_file, 'w') as key_file_obj:
            key_file_obj.write(self.key_pair.key_material)
        os.chmod(self.key_file, 0o400)

    def destroy(self):
        self.key_pair.delete()
        try:
            os.remove(self.key_file)
        except OSError:
            pass


class Node(object):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials,
                 node_prefix='node', ami_username='root'):
        self.instance = ec2_instance
        self.name = '{}-{}'.format(node_prefix,
                                   time.strftime('%Y-%m-%d-%H.%M.%S'))
        print('Creating Node {}'.format(self.name))
        self.ec2 = ec2_service
        self.ec2.create_tags(Resources=[self.instance.id],
                             Tags=[{'Key': 'Name', 'Value': self.name}])
        self.instance.wait_until_running()
        self.wait_public_ip()
        self.remoter = Remote(hostname=self.instance.public_ip_address,
                              username=ami_username,
                              key_filename=credentials.key_file,
                              timeout=120, attempts=10, quiet=False)

    def wait_public_ip(self):
        print('Waiting for public IP (Node {})'.format(self.name))
        while self.instance.public_ip_address is None:
            time.sleep(1)
            self.instance.reload()
        print("Instance {} IP : {}".format(self.name,
                                           self.instance.public_ip_address))

    def destroy(self):
        pass


class Cluster(object):

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='root',
                 ec2_user_data='', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10):
        self.ec2 = service
        self.name = '{}-{}'.format(cluster_prefix,
                                   time.strftime('%Y-%m-%d-%H.%M.%S'))
        self.credentials = credentials
        print('Using AMI ID: {}'.format(ec2_ami_id))
        instances = self.ec2.create_instances(ImageId=ec2_ami_id,
                                              UserData=ec2_user_data,
                                              MinCount=n_nodes,
                                              MaxCount=n_nodes,
                                              KeyName=self.credentials.key_pair.name,
                                              SecurityGroupIds=ec2_security_group_ids,
                                              SubnetId=ec2_subnet_id,
                                              InstanceType=ec2_instance_type)
        self.nodes = [self.create_node(instance, ec2_ami_username, node_prefix)
                      for instance in instances]

    def create_node(self, instance, ami_username, node_prefix):
        return Node(ec2_instance=instance, ec2_service=self.ec2,
                    credentials=self.credentials, ami_username=ami_username,
                    node_prefix=node_prefix)

    def get_node_internal_ips(self):
        return [node.instance.private_ip_address for node in self.nodes]

    def destroy(self):
        for node in self.nodes:
            node.destroy()


class ScyllaCluster(Cluster):
    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='fedora', n_nodes=10):
        user_data = '--clustername scylla --totalnodes {}'.format(n_nodes)
        user_data = base64.b64encode(user_data)
        super(ScyllaCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                            ec2_subnet_id=ec2_subnet_id,
                                            ec2_security_group_ids=ec2_security_group_ids,
                                            ec2_instance_type=ec2_instance_type,
                                            ec2_ami_username=ec2_ami_username,
                                            ec2_user_data=user_data,
                                            service=service,
                                            credentials=credentials,
                                            cluster_prefix='scylla-cluster',
                                            node_prefix='scylla-node',
                                            n_nodes=n_nodes)


class LoaderCluster(Cluster):
    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='fedora', n_nodes=10):
        super(LoaderCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                            ec2_subnet_id=ec2_subnet_id,
                                            ec2_security_group_ids=ec2_security_group_ids,
                                            ec2_instance_type=ec2_instance_type,
                                            ec2_ami_username=ec2_ami_username,
                                            service=service,
                                            credentials=credentials,
                                            cluster_prefix='scylla-loader',
                                            node_prefix='scylla-loader-node',
                                            n_nodes=n_nodes)

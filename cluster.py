import base64
import os
import tempfile
import time
import uuid

from remote import Remote
from remote import CmdError


class NodeInitError(Exception):

    def __init__(self, node, result):
        self.node = node
        self.result = result

    def __str__(self):
        return "Node {} init fail:\n{}".format(str(self.node),
                                               self.result.stdout)


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, key_prefix='keypair'):
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        self.name = '{}-{}'.format(key_prefix, self.shortid)
        self.key_pair = service.create_key_pair(KeyName=self.name)
        self.key_file = os.path.join(tempfile.gettempdir(),
                                     '{}.pem'.format(self.name))
        self.write_key_file()
        print("{}: Created".format(str(self)))

    def __str__(self):
        return "Key Pair {} -> {}".format(self.name, self.key_file)

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
        print("{}: Destroyed".format(str(self)))


class Node(object):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials,
                 node_prefix='node', node_index=1, ami_username='root'):
        self.instance = ec2_instance
        self.name = '{}-{}'.format(node_prefix, node_index)
        self.ec2 = ec2_service
        self.instance.wait_until_running()
        self.wait_public_ip()
        self.ec2.create_tags(Resources=[self.instance.id],
                             Tags=[{'Key': 'Name', 'Value': self.name}])
        print('{}: Started'.format(self))
        self.remoter = Remote(hostname=self.instance.public_ip_address,
                              username=ami_username,
                              key_filename=credentials.key_file,
                              timeout=120, attempts=10, quiet=False)

    def __str__(self):
        return 'Node {} [{} | {}]'.format(self.name,
                                          self.instance.public_ip_address,
                                          self.instance.private_ip_address)

    def wait_public_ip(self):
        while self.instance.public_ip_address is None:
            time.sleep(1)
            self.instance.reload()

    def destroy(self):
        terminate_msg = '{}: Destroyed'.format(self)
        self.instance.terminate()
        print(terminate_msg)


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
        self.ec2_ami_id = ec2_ami_id
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        self.name = '{}-{}'.format(cluster_prefix, self.shortid)
        self.credentials = credentials
        print('{}: Init nodes '.format(str(self)))
        instances = self.ec2.create_instances(ImageId=ec2_ami_id,
                                              UserData=ec2_user_data,
                                              MinCount=n_nodes,
                                              MaxCount=n_nodes,
                                              KeyName=self.credentials.key_pair.name,
                                              SecurityGroupIds=ec2_security_group_ids,
                                              SubnetId=ec2_subnet_id,
                                              InstanceType=ec2_instance_type)
        self.nodes = [self.create_node(instance, ec2_ami_username,
                                       node_prefix, node_index)
                      for node_index, instance in
                      enumerate(instances, start=1)]

    def __str__(self):
        return 'Cluster {} (AMI ID {})'.format(self.name, self.ec2_ami_id)

    def create_node(self, instance, ami_username, node_prefix, node_index):
        node_prefix = '{}-{}'.format(node_prefix, self.shortid)
        return Node(ec2_instance=instance, ec2_service=self.ec2,
                    credentials=self.credentials, ami_username=ami_username,
                    node_prefix=node_prefix, node_index=node_index)

    def get_node_internal_ips(self):
        return [node.instance.private_ip_address for node in self.nodes]

    def run_all_nodes(self, cmd, ignore_status=False, timeout=60):
        """
        Run cmd in all nodes (parallel execution)

        :param cmd: Shell Command to run.
        :param ignore_status: Whether to ignore errors on the execution
        :param timeout: Time to wait for command to finish
        :return: Iterator with parallel execution results
        """
        for node in self.nodes:
            yield node.remoter.run_parallel(cmd, ignore_status=ignore_status,
                                            timeout=timeout)

    def destroy(self):
        print('{}: Destroy nodes '.format(str(self)))
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
                                            cluster_prefix='scylla-db-cluster',
                                            node_prefix='scylla-db-node',
                                            n_nodes=n_nodes)

    def wait_for_init(self):
        node_initialized_map = {node: False for node in self.nodes}
        all_nodes_initialized = [True for _ in self.nodes]
        verify_pause = 60
        print("Waiting until all DB nodes are functional. "
              "Polling interval: {} s".format(verify_pause))
        start_time = time.time()
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                try:
                    node.remoter.run_quiet('netstat -a | grep :9042',
                                           timeout=120)
                    node_initialized_map[node] = True
                except CmdError:
                    try:
                        node.remoter.run_quiet("grep 'Aborting the "
                                               "clustering of this "
                                               "reservation' "
                                               "/home/fedora/ami.log",
                                               timeout=120)
                        result = node.remoter.run_quiet("tail -5 "
                                                        "/home/fedora/ami.log",
                                                        timeout=120)
                        raise NodeInitError(node=node, result=result)
                    except CmdError:
                        pass
            initialized_nodes = len([node for node in node_initialized_map if
                                    node_initialized_map[node]])
            total_nodes = len(node_initialized_map.keys())
            time_elapsed = time.time() - start_time
            print("({}/{}) DB nodes ready. "
                  "Time elapsed: {:d} s".format(initialized_nodes,
                                                total_nodes,
                                                int(time_elapsed)))
            time.sleep(verify_pause)


class LoaderSet(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='fedora', n_nodes=10):
        super(LoaderSet, self).__init__(ec2_ami_id=ec2_ami_id,
                                        ec2_subnet_id=ec2_subnet_id,
                                        ec2_security_group_ids=ec2_security_group_ids,
                                        ec2_instance_type=ec2_instance_type,
                                        ec2_ami_username=ec2_ami_username,
                                        service=service,
                                        credentials=credentials,
                                        cluster_prefix='scylla-loader-set',
                                        node_prefix='scylla-loader-node',
                                        n_nodes=n_nodes)

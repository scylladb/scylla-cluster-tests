import atexit
import os
import tempfile
import threading
import time
import uuid
import yaml

from avocado.utils import path

import fabric.api
import fabric.network
import fabric.operations
import fabric.tasks

from remote import CmdError
from remote import Remote
from remote import CmdResult

SCYLLA_CLUSTER_DEVICE_MAPPINGS = [{"DeviceName": "/dev/xvdb",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}},
                                  {"DeviceName": "/dev/xvdc",
                                   "Ebs": {"VolumeSize": 40,
                                           "DeleteOnTermination": True,
                                           "Encrypted": False}}]

CREDENTIALS = []
EC2_INSTANCES = []


def cleanup_instances():
    global EC2_INSTANCES
    global CREDENTIALS

    for instance in EC2_INSTANCES:
        instance.terminate()

    for cred in CREDENTIALS:
        cred.destroy()


def remove_cred_from_cleanup(cred):
    global CREDENTIALS
    CREDENTIALS.remove(cred)


def register_cleanup():
    atexit.register(cleanup_instances)

register_cleanup()


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
                 service, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10):
        global CREDENTIALS
        CREDENTIALS.append(credentials)

        self.ec2_ami_id = ec2_ami_id
        self.ec2_subnet_id = ec2_subnet_id
        self.ec2_security_group_ids = ec2_security_group_ids
        self.ec2 = service
        self.credentials = credentials
        self.ec2_instance_type = ec2_instance_type
        self.ec2_ami_username = ec2_ami_username
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        self.ec2_block_device_mappings = ec2_block_device_mappings
        self.ec2_user_data = ec2_user_data
        self.node_prefix = node_prefix
        self.ec2_ami_id = ec2_ami_id
        if cluster_uuid is None:
            self.uuid = uuid.uuid4()
        else:
            self.uuid = cluster_uuid
        self.shortid = str(self.uuid)[:8]
        self.name = '{}-{}'.format(cluster_prefix, self.shortid)
        print('{}: Init nodes '.format(str(self)))
        self.nodes = []
        self.add_nodes(n_nodes)

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            ec2_user_data = self.ec2_user_data
        global EC2_INSTANCES
        print("{}: Passing user_data '{}' to "
              "create_instances".format(str(self), ec2_user_data))
        instances = self.ec2.create_instances(ImageId=self.ec2_ami_id,
                                              UserData=ec2_user_data,
                                              MinCount=count,
                                              MaxCount=count,
                                              KeyName=self.credentials.key_pair.name,
                                              SecurityGroupIds=self.ec2_security_group_ids,
                                              BlockDeviceMappings=self.ec2_block_device_mappings,
                                              SubnetId=self.ec2_subnet_id,
                                              InstanceType=self.ec2_instance_type)
        EC2_INSTANCES += instances
        added_nodes = [self._create_node(instance, self.ec2_ami_username,
                                         self.node_prefix, node_index)
                       for node_index, instance in
                       enumerate(instances, start=len(self.nodes) + 1)]
        self.nodes += added_nodes
        return added_nodes

    def __str__(self):
        return 'Cluster {} (AMI ID {})'.format(self.name, self.ec2_ami_id)

    def _create_node(self, instance, ami_username, node_prefix, node_index):
        node_prefix = '{}-{}'.format(node_prefix, self.shortid)
        return Node(ec2_instance=instance, ec2_service=self.ec2,
                    credentials=self.credentials, ami_username=ami_username,
                    node_prefix=node_prefix, node_index=node_index)

    def get_node_private_ips(self):
        return [node.instance.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.instance.public_ip_address for node in self.nodes]

    @fabric.api.parallel
    def run_all_nodes(self, cmd, ignore_status=False, timeout=60):
        def _run(command, ignore_status=False, timeout=60):
            result = CmdResult()
            start_time = time.time()
            end_time = time.time() + (timeout or 0)   # Support timeout=None
            # Fabric sometimes returns NetworkError even when timeout not
            # reached
            fabric_result = None
            fabric_exception = None
            while True:
                try:
                    fabric_result = fabric.operations.run(command=command,
                                                          quiet=False,
                                                          warn_only=True,
                                                          timeout=timeout)
                    break
                except fabric.network.NetworkError, details:
                    fabric_exception = details
                    timeout = end_time - time.time()
                if time.time() < end_time:
                    break
            if fabric_result is None:
                if fabric_exception is not None:
                    raise fabric_exception  # pylint: disable=E0702
                else:
                    f_msg = ("Remote execution of '{}' failed without any "
                             "exception. This should not "
                             "happen".format(command))
                    raise fabric.network.NetworkError(f_msg)
            end_time = time.time()
            duration = end_time - start_time
            result.command = command
            result.stdout = str(fabric_result)
            result.stderr = fabric_result.stderr
            result.duration = duration
            result.exit_status = fabric_result.return_code
            result.failed = fabric_result.failed
            result.succeeded = fabric_result.succeeded
            if not ignore_status:
                if result.failed:
                    raise CmdError(command=command, result=result)
            return result

        fabric.api.env.update(hosts=self.get_node_public_ips(),
                              user=self.ec2_ami_username)
        return fabric.tasks.execute(_run, cmd, ignore_status, timeout)

    def destroy(self):
        print('{}: Destroy nodes '.format(str(self)))
        global EC2_INSTANCES
        for node in self.nodes:
            EC2_INSTANCES.remove(node.instance)
            node.destroy()


class ScyllaCluster(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='fedora',
                 ec2_block_device_mappings=SCYLLA_CLUSTER_DEVICE_MAPPINGS,
                 n_nodes=10):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = 'scylla-db-cluster'
        shortid = str(cluster_uuid)[:8]
        name = '{}-{}'.format(cluster_prefix, shortid)
        user_data = ('--clustername {} '
                     '--totalnodes {}'.format(name, n_nodes))
        super(ScyllaCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                            ec2_subnet_id=ec2_subnet_id,
                                            ec2_security_group_ids=ec2_security_group_ids,
                                            ec2_instance_type=ec2_instance_type,
                                            ec2_ami_username=ec2_ami_username,
                                            ec2_user_data=user_data,
                                            ec2_block_device_mappings=ec2_block_device_mappings,
                                            cluster_uuid=cluster_uuid,
                                            service=service,
                                            credentials=credentials,
                                            cluster_prefix=cluster_prefix,
                                            node_prefix='scylla-db-node',
                                            n_nodes=n_nodes)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()

    def get_seed_nodes(self):
        node = self.nodes[0]
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'), 'scylla.yaml')
        node.remoter.receive_files(yaml_dst_path, '/etc/scylla/scylla.yaml')
        with open(yaml_dst_path, 'r') as yaml_stream:
            conf_dict = yaml.load(yaml_stream)
            try:
                return conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
            except:
                raise ValueError('Unexpected scylla.yaml '
                                 'contents:\n{}'.format(yaml_stream.read()))

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            if self.nodes:
                seeds = ",".join(self.get_seed_nodes())
                ec2_user_data = ('--clustername {} --bootstrap true '
                                 '--totalnodes {} --seeds {}'.format(self.name,
                                                                     count,
                                                                     seeds))
        added_nodes = super(ScyllaCluster, self).add_nodes(count=count,
                                                           ec2_user_data=ec2_user_data)
        return added_nodes

    def wait_for_init(self, node_list=None, verbose=False):
        if node_list is None:
            node_list = self.nodes
        node_initialized_map = {node: False for node in node_list}
        all_nodes_initialized = [True for _ in node_list]
        verify_pause = 60
        print("{}: Waiting scylla services to start. "
              "Polling interval: {} s".format(str(self), verify_pause))
        start_time = time.time()
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                if verbose:
                    run_cmd = node.remoter.run
                else:
                    run_cmd = node.remoter.run_quiet
                try:
                    run_cmd('netstat -a | grep :9042', timeout=120)
                    node_initialized_map[node] = True
                except CmdError:
                    try:
                        run_cmd("grep 'Aborting the clustering of this "
                                "reservation' /home/fedora/ami.log",
                                timeout=120)
                        result = run_cmd("tail -5 /home/fedora/ami.log",
                                         timeout=120)
                        raise NodeInitError(node=node, result=result)
                    except CmdError:
                        run_cmd("sudo systemctl start scylla-server.service",
                                timeout=120)
            initialized_nodes = len([node for node in node_initialized_map if
                                    node_initialized_map[node]])
            total_nodes = len(node_initialized_map.keys())
            time_elapsed = time.time() - start_time
            print("({}/{}) DB nodes ready. "
                  "Time elapsed: {:d} s".format(initialized_nodes,
                                                total_nodes,
                                                int(time_elapsed)))
            time.sleep(verify_pause)

    def add_nemesis(self, nemesis):
        self.nemesis.append(nemesis(cluster=self,
                                    termination_event=self.termination_event))

    def start_nemesis(self, interval=30):
        for nemesis in self.nemesis:
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval,), verbose=True)
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)

    def stop_nemesis(self):
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(10)

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaCluster, self).destroy()


class CassandraCluster(ScyllaCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='ubuntu',
                 ec2_block_device_mappings=None,
                 n_nodes=10):
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = 'cassandra-db-cluster'
        shortid = str(cluster_uuid)[:8]
        name = '{}-{}'.format(cluster_prefix, shortid)
        user_data = ('--clustername {} '
                     '--totalnodes {} --version community '
                     '--release 2.1.8'.format(name, n_nodes))
        super(ScyllaCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                            ec2_subnet_id=ec2_subnet_id,
                                            ec2_security_group_ids=ec2_security_group_ids,
                                            ec2_instance_type=ec2_instance_type,
                                            ec2_ami_username=ec2_ami_username,
                                            ec2_user_data=user_data,
                                            ec2_block_device_mappings=ec2_block_device_mappings,
                                            cluster_uuid=cluster_uuid,
                                            service=service,
                                            credentials=credentials,
                                            cluster_prefix=cluster_prefix,
                                            node_prefix='cassandra-db-node',
                                            n_nodes=n_nodes)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()

    def wait_for_init(self, node_list=None, verbose=False):
        if node_list is None:
            node_list = self.nodes
        node_initialized_map = {node: False for node in node_list}
        all_nodes_initialized = [True for _ in node_list]
        verify_pause = 60
        print("{}: Waiting cassandra services to start. "
              "Polling interval: {} s".format(str(self), verify_pause))
        start_time = time.time()
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                if verbose:
                    run_cmd = node.remoter.run
                else:
                    run_cmd = node.remoter.run_quiet
                try:
                    run_cmd('netstat -a | grep :9042', timeout=60)
                    node_initialized_map[node] = True
                except Exception:
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
                 ec2_ami_username='fedora', scylla_repo=None, n_nodes=10):
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
        self.scylla_repo = scylla_repo

    def wait_for_init(self, verbose=False):
        print("{}: Setting all DB loader nodes".format(str(self)))
        for loader in self.nodes:
            if verbose:
                run_cmd = loader.remoter.run
            else:
                run_cmd = loader.remoter.run_quiet
            loader.remoter.send_files(self.scylla_repo,
                                      '/home/fedora/scylla.repo')
            run_cmd('sudo mv /home/fedora/scylla.repo '
                    '/etc/yum.repos.d/scylla.repo')
            run_cmd('sudo dnf install -y scylla-tools', timeout=300)

    def run_stress(self, stress_cmd, timeout, output_dir):
        def check_output(result_obj, node):
            output = result_obj.stdout + result_obj.stderr
            lines = output.splitlines()
            for line in lines:
                if 'java.io.IOException' in line:
                    return ['{}:{}'.format(node, line.strip())]
            return []

        print("{}: Running {} in all loaders, timeout {} s".format(str(self),
                                                                   stress_cmd,
                                                                   timeout))
        logdir = path.init_dir(output_dir, self.name)
        result_dict = self.run_all_nodes(stress_cmd, timeout=timeout)
        errors = []
        for node in self.nodes:
            result = result_dict[node.instance.public_ip_address]
            log_file_name = os.path.join(logdir,
                                         '{}.log'.format(node.name))
            print("{}: Writing log file {}".format(str(self), log_file_name))
            with open(log_file_name, 'w') as log_file:
                log_file.write(result.stdout)
            errors += check_output(result, node)
        return errors

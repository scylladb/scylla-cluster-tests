# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB

import Queue
import atexit
import logging
import os
import tempfile
import threading
import time
import uuid
import yaml

from avocado.utils import path
from avocado.utils import process

from .log import SDCMAdapter
from .remote import Remote
from . import wait

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
    if cred in CREDENTIALS:
        CREDENTIALS.remove(cred)


def register_cleanup():
    atexit.register(cleanup_instances)

register_cleanup()


class NodeInitError(Exception):

    def __init__(self, node, result):
        self.node = node
        self.result = result

    def __str__(self):
        return "Node %s init fail:\n%s" % (str(self.node), self.result.stdout)


class LoaderSetInitError(Exception):
    pass


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, key_prefix='keypair'):
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        self.name = '%s-%s' % (key_prefix, self.shortid)
        self.key_pair = service.create_key_pair(KeyName=self.name)
        self.key_file = os.path.join(tempfile.gettempdir(),
                                     '%s.pem' % self.name)
        self.write_key_file()
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Created')

    def __str__(self):
        return "Key Pair %s -> %s" % (self.name, self.key_file)

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
        self.log.info('Destroyed')


class Node(object):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials,
                 node_prefix='node', node_index=1, ami_username='root'):
        self.instance = ec2_instance
        self.name = '%s-%s' % (node_prefix, node_index)
        self.ec2 = ec2_service
        self.instance.wait_until_running()
        self.wait_public_ip()
        self.is_seed = None
        self.ec2.create_tags(Resources=[self.instance.id],
                             Tags=[{'Key': 'Name', 'Value': self.name}])
        # Make the instance created to be immune to Tzach's killer script
        self.ec2.create_tags(Resources=[self.instance.id],
                             Tags=[{'Key': 'keep', 'Value': 'alive'}])
        self.remoter = Remote(hostname=self.instance.public_ip_address,
                              user=ami_username,
                              key_file=credentials.key_file)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.debug("SSH access -> 'ssh -i %s %s@%s'",
                       credentials.key_file, ami_username,
                       self.instance.public_ip_address)

    def __str__(self):
        return 'Node %s [%s | %s] (seed: %s)' % (self.name,
                                                 self.instance.public_ip_address,
                                                 self.instance.private_ip_address,
                                                 self.is_seed)

    def wait_public_ip(self):
        while self.instance.public_ip_address is None:
            time.sleep(1)
            self.instance.reload()

    def restart(self):
        self.instance.stop()
        self.instance.wait_until_stopped()
        self.instance.start()
        self.instance.wait_until_running()
        self.wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self.instance.public_ip_address)
        self.remoter.hostname = self.instance.public_ip_address
        self.wait_db_up()

    def destroy(self):
        self.instance.terminate()
        global EC2_INSTANCES
        EC2_INSTANCES.remove(self.instance)
        self.log.info('Destroyed')

    def wait_ssh_up(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for SSH to be up' % self
        wait.wait_for(func=self.remoter.is_up, step=10,
                      text=text)

    def db_up(self):
        try:
            result = self.remoter.run('netstat -a | grep :9042',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception, details:
            self.log.error('Error checking for DB up: %s', details)
            return False

    def cs_installed(self, cassandra_stress_bin=None):
        if cassandra_stress_bin is None:
            cassandra_stress_bin = '/usr/bin/cassandra-stress'
        try:
            result = self.remoter.run('test -x %s' % cassandra_stress_bin,
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception, details:
            self.log.error('Error checking for cassandra-stress: %s', details)
            return False

    def wait_db_up(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60,
                      text=text)

    def wait_db_down(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=lambda: not self.db_up, step=60,
                      text=text)

    def wait_cs_installed(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for cassandra-stress' % self
        wait.wait_for(func=self.cs_installed, step=60,
                      text=text)


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
        self.name = '%s-%s' % (cluster_prefix, self.shortid)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Init nodes')
        self.nodes = []
        self.add_nodes(n_nodes)

    def send_file(self, src, dst, verbose=False):
        for loader in self.nodes:
            loader.remoter.send_files(src=src, dst=dst, verbose=verbose)

    def run(self, cmd, verbose=False):
        for loader in self.nodes:
            loader.remoter.run(cmd=cmd, verbose=verbose)

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            ec2_user_data = self.ec2_user_data
        global EC2_INSTANCES
        self.log.debug("Passing user_data '%s' to create_instances",
                       ec2_user_data)
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
        return 'Cluster %s (AMI: %s Type: %s)' % (self.name,
                                                  self.ec2_ami_id,
                                                  self.ec2_instance_type)

    def _create_node(self, instance, ami_username, node_prefix, node_index):
        node_prefix = '%s-%s' % (node_prefix, self.shortid)
        return Node(ec2_instance=instance, ec2_service=self.ec2,
                    credentials=self.credentials, ami_username=ami_username,
                    node_prefix=node_prefix, node_index=node_index)

    def get_node_private_ips(self):
        return [node.instance.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.instance.public_ip_address for node in self.nodes]

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()


class ScyllaCluster(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 n_nodes=10):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = 'scylla-db-cluster'
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s' % (name, n_nodes))
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
        self.seed_nodes_private_ips = None

    def get_seed_nodes_private_ips(self):
        if self.seed_nodes_private_ips is None:
            node = self.nodes[0]
            yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'), 'scylla.yaml')
            node.remoter.receive_files(src='/etc/scylla/scylla.yaml',
                                       dst=yaml_dst_path)
            with open(yaml_dst_path, 'r') as yaml_stream:
                conf_dict = yaml.load(yaml_stream)
                try:
                    self.seed_nodes_private_ips = conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
                except:
                    raise ValueError('Unexpected scylla.yaml '
                                     'contents:\n%s' % yaml_stream.read())
        return self.seed_nodes_private_ips

    def get_seed_nodes(self):
        seed_nodes_private_ips = self.get_seed_nodes_private_ips()
        seed_nodes = []
        for node in self.nodes:
            if node.instance.private_ip_address in seed_nodes_private_ips:
                node.is_seed = True
                seed_nodes.append(node)
            else:
                node.is_seed = False
        return seed_nodes

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            if self.nodes:
                node_private_ips = [node.instance.private_ip_address for node
                                    in self.nodes if node.is_seed]
                seeds = ",".join(node_private_ips)
                ec2_user_data = ('--clustername %s --bootstrap true '
                                 '--totalnodes %s --seeds %s' % (self.name,
                                                                 count,
                                                                 seeds))
        added_nodes = super(ScyllaCluster, self).add_nodes(count=count,
                                                           ec2_user_data=ec2_user_data)
        return added_nodes

    def get_node_info_list(self, verification_node):
        assert verification_node in self.nodes
        cmd_result = verification_node.remoter.run('nodetool status')
        node_info_list = []
        for line in cmd_result.stdout.splitlines():
            line = line.strip()
            if line.startswith('UN'):
                try:
                    status, ip, load, _, tokens, owns, host_id, rack = line.split()
                    node_info = {'status': status,
                                 'ip': ip,
                                 'load': load,
                                 'tokens': tokens,
                                 'owns': owns,
                                 'host_id': host_id,
                                 'rack': rack}
                    # Cassandra banners have nodetool status output as well.
                    # Need to guarantee unique set of results.
                    node_ips = [node_info['ip'] for node_info in node_info_list]
                    if node_info['ip'] not in node_ips:
                        node_info_list.append(node_info)
                except ValueError:
                    pass
        return node_info_list

    def wait_for_init(self, node_list=None, verbose=False):
        if node_list is None:
            node_list = self.nodes
        node_initialized_map = {node: False for node in node_list}
        all_nodes_initialized = [True for _ in node_list]
        verify_pause = 60
        self.log.info('Waiting scylla services to start. '
                      'Polling interval: %s s', verify_pause)
        start_time = time.time()
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                node.wait_ssh_up(verbose=verbose)
                try:
                    node.remoter.run('netstat -a | grep :9042',
                                     verbose=verbose)
                    node_initialized_map[node] = True
                except process.CmdError:
                    try:
                        node.remoter.run("grep 'Aborting the clustering of "
                                         "this reservation' "
                                         "/home/centos/ami.log",
                                         verbose=verbose)
                        result = node.remoter.run("tail -5 "
                                                  "/home/centos/ami.log")
                        raise NodeInitError(node=node, result=result)
                    except process.CmdError:
                        pass
            initialized_nodes = len([node for node in node_initialized_map if
                                    node_initialized_map[node]])
            total_nodes = len(node_initialized_map.keys())
            time_elapsed = time.time() - start_time
            self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                          initialized_nodes, total_nodes, int(time_elapsed))
            time.sleep(verify_pause)
        # Mark all seed nodes
        self.get_seed_nodes()

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
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s --version community '
                     '--release 2.1.8' % (name, n_nodes))
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

    def get_seed_nodes(self):
        node = self.nodes[0]
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='cassandra-longevity'), 'cassandra.yaml')
        node.remoter.receive_files(src='/etc/cassandra/cassandra.yaml',
                                   dst=yaml_dst_path)
        with open(yaml_dst_path, 'r') as yaml_stream:
            conf_dict = yaml.load(yaml_stream)
            try:
                return conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
            except:
                raise ValueError('Unexpected cassandra.yaml '
                                 'contents:\n%s' % yaml_stream.read())

    def add_nodes(self, count, ec2_user_data=''):
        if not ec2_user_data:
            if self.nodes:
                seeds = ",".join(self.get_seed_nodes())
                ec2_user_data = ('--clustername %s --bootstrap true '
                                 '--totalnodes %s --seeds %s '
                                 '--version community '
                                 '--release 2.1.8' % (self.name,
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
        self.log.info('Waiting cassandra services to start. '
                      'Polling interval: %s s', verify_pause)
        start_time = time.time()
        while node_initialized_map.values() != all_nodes_initialized:
            for node in node_initialized_map.keys():
                node.wait_ssh_up(verbose=verbose)
                try:
                    node.remoter.run('netstat -a | grep :9042',
                                     verbose=verbose)
                    node_initialized_map[node] = True
                except Exception:
                    pass
            initialized_nodes = len([node for node in node_initialized_map if
                                    node_initialized_map[node]])
            total_nodes = len(node_initialized_map.keys())
            time_elapsed = time.time() - start_time
            self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                          initialized_nodes, total_nodes, int(time_elapsed))
            time.sleep(verify_pause)


class LoaderSet(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos', scylla_repo=None, n_nodes=10):
        super(LoaderSet, self).__init__(ec2_ami_id=ec2_ami_id,
                                        ec2_subnet_id=ec2_subnet_id,
                                        ec2_security_group_ids=ec2_security_group_ids,
                                        ec2_instance_type=ec2_instance_type,
                                        ec2_ami_username=ec2_ami_username,
                                        service=service,
                                        ec2_block_device_mappings=ec2_block_device_mappings,
                                        credentials=credentials,
                                        cluster_prefix='scylla-loader-set',
                                        node_prefix='scylla-loader-node',
                                        n_nodes=n_nodes)
        self.scylla_repo = scylla_repo

    def wait_for_init(self, verbose=False):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup')
            node.wait_ssh_up(verbose=verbose)
            # The init scripts should install/update c-s, so
            # let's try to guarantee it will be there before
            # proceeding
            node.wait_cs_installed(verbose=verbose)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))

    def run_stress_thread(self, stress_cmd, timeout, output_dir):
        queue = Queue.Queue()

        def node_run_stress(node):
            try:
                logdir = path.init_dir(output_dir, self.name)
            except OSError:
                logdir = os.path.join(output_dir, self.name)
            result = node.remoter.run(cmd=stress_cmd, timeout=timeout,
                                      ignore_status=True)
            log_file_name = os.path.join(logdir, '%s.log' % node.name)
            self.log.debug('Writing log file %s', log_file_name)
            with open(log_file_name, 'w') as log_file:
                log_file.write(str(result))
            queue.put((node, result))
            queue.task_done()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_run_stress,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()

        return queue

    def verify_stress_thread(self, queue):
        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass

        errors = []
        for node, result in results:
            output = result.stdout + result.stderr
            lines = output.splitlines()
            for line in lines:
                if 'java.io.IOException' in line:
                    errors += ['%s: %s' % (node, line.strip())]

        return errors

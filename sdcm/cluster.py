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
import getpass
import logging
import os
import tempfile
import threading
import time
import uuid
import yaml
import matplotlib
# Force matplotlib to not use any Xwindows backend.
matplotlib.use('Agg')
import matplotlib.pyplot as pl

from avocado.utils import path
from avocado.utils import script
from botocore.exceptions import WaiterError

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
DEFAULT_USER_PREFIX = getpass.getuser()


def cleanup_instances(behavior='destroy'):
    global EC2_INSTANCES
    global CREDENTIALS

    for instance in EC2_INSTANCES:
        if behavior == 'destroy':
            instance.terminate()
        elif behavior == 'stop':
            instance.stop()

    for cred in CREDENTIALS:
        if behavior == 'destroy':
            cred.destroy()


def destroy_instances():
    cleanup_instances(behavior='destroy')


def stop_instances():
    cleanup_instances(behavior='stop')


def remove_cred_from_cleanup(cred):
    global CREDENTIALS
    if cred in CREDENTIALS:
        CREDENTIALS.remove(cred)


def register_cleanup(cleanup='destroy'):
    if cleanup == 'destroy':
        atexit.register(destroy_instances)
    elif cleanup == 'stop':
        atexit.register(stop_instances)


class NodeError(Exception):

    def __init__(self, msg=None):
        self.msg = msg

    def __str__(self):
        if self.msg is not None:
            return self.msg


class LoaderSetInitError(Exception):
    pass


def _prepend_user_prefix(user_prefix, base_name):
    if user_prefix is None:
        user_prefix = DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class RemoteCredentials(object):

    """
    Wraps EC2.KeyPair, so that we can save keypair info into .pem files.
    """

    def __init__(self, service, key_prefix='keypair', user_prefix=None):
        self.uuid = uuid.uuid4()
        self.shortid = str(self.uuid)[:8]
        key_prefix = _prepend_user_prefix(user_prefix, key_prefix)
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
                 node_prefix='node', node_index=1, ami_username='root',
                 base_logdir=None):
        self.instance = ec2_instance
        self.name = '%s-%s' % (node_prefix, node_index)
        try:
            self.logdir = path.init_dir(base_logdir, self.name)
        except OSError:
            self.logdir = os.path.join(base_logdir, self.name)
        self.ec2 = ec2_service
        self._instance_wait_safe(self.instance.wait_until_running)
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
        self._journal_thread = None
        self.start_journal_thread()
        # We'll assume 0 coredump for starters, if by a chance there
        # are already coredumps in there the coredump backtrace
        # code will report all of them.
        self._n_coredumps = 0
        self._backtrace_thread = None
        self.start_backtrace_thread()

    def _instance_wait_safe(self, instance_method):
        """
        Wrapper around AWS instance waiters that is safer to use.

        Since AWS adopts an eventual consistency model, sometimes the method
        wait_until_running will raise a botocore.exceptions.WaiterError saying
        the instance does not exist. AWS API guide [1] recommends that the
        procedure is retried using an exponencial backoff algorithm [2].

        :see: [1] http://docs.aws.amazon.com/AWSEC2/latest/APIReference/query-api-troubleshooting.html#eventual-consistency
        :see: [2] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        treshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                instance_method()
                ok = True
            except WaiterError:
                time.sleep(max((2 ** retries) * 2, treshold))
                retries += 1

        if not ok:
            raise NodeError('AWS instance %s waiter error after '
                            'exponencial backoff wait' % self.instance.id)

    def retrieve_journal(self):
        try:
            log_file = os.path.join(self.logdir, 'db_services.log')
            self.remoter.run('sudo journalctl -f '
                             '-u scylla-io-setup.service '
                             '-u scylla-server.service '
                             '-u scylla-jmx.service',
                             verbose=True, ignore_status=True,
                             log_file=log_file)
        except Exception, details:
            self.log.error('Error retrieving remote node journal: %s', details)

    def journal_thread(self):
        while True:
            self.wait_ssh_up(verbose=False)
            self.retrieve_journal()

    def start_journal_thread(self):
        self._journal_thread = threading.Thread(target=self.journal_thread)
        self._journal_thread.start()

    def _get_coredump_backtraces(self, last=True):
        """
        Get coredump backtraces.

        :param last: Whether to only show the last backtrace.
        :return: process.CmdResult output
        """
        try:
            backtrace_cmd = 'sudo coredumpctl info'
            if last:
                backtrace_cmd += ' -1'
            return self.remoter.run(backtrace_cmd,
                                    verbose=False, ignore_status=True)
        except Exception, details:
            self.log.error('Error retrieving core dump backtraces : %s',
                           details)

    def _notify_backtrace(self, last):
        """
        Notify coredump backtraces to test log and coredump.log file.

        :param last: Whether to show only the last backtrace.
        """
        result = self._get_coredump_backtraces(last=last)
        log_file = os.path.join(self.logdir, 'coredump.log')
        output = result.stdout + result.stderr
        base_upload_url = 'scylladb-users-upload.s3.amazonaws.com/%s/%s'
        for line in output.splitlines():
            line = line.strip()
            if line.startswith('Coredump:'):
                coredump = line.split()[-1]
                coredump_id = os.path.basename(coredump)[:-3]
                upload_url = base_upload_url % (coredump_id, os.path.basename(coredump))
                self.log.info('Uploading coredump %s to %s' % (coredump, upload_url))
                self.remoter.run("sudo curl --request PUT --upload-file '%s' '%s'" %
                                 (coredump, upload_url))
                self.log.info("To download it, you may use 'curl --user-agent [user-agent] %s > %s'", upload_url,
                              os.path.basename(coredump))
        with open(log_file, 'a') as log_file_obj:
            log_file_obj.write(output)
        for line in output.splitlines():
            self.log.error(line)

    def _get_n_coredumps(self):
        """
        Get the number of coredumps stored on this Node.

        :return: Number of coredumps
        :rtype: int
        """
        try:
            n_backtraces_cmd = 'sudo coredumpctl --no-legend 2>/dev/null'
            result = self.remoter.run(n_backtraces_cmd,
                                      verbose=False, ignore_status=True)
            return len(result.stdout.splitlines())
        except Exception, details:
            self.log.error('Error retrieving number of core dumps : %s',
                           details)
            return None

    def get_backtraces(self):
        """
        Verify the number of backtraces stored, report if new ones were found.
        """
        self.wait_ssh_up(verbose=False)
        new_n_coredumps = self._get_n_coredumps()
        if new_n_coredumps is not None:
            if (new_n_coredumps - self._n_coredumps) == 1:
                self._notify_backtrace(last=True)
            elif (new_n_coredumps - self._n_coredumps) > 1:
                self._notify_backtrace(last=False)
            self._n_coredumps = new_n_coredumps

    def backtrace_thread(self):
        """
        Keep reporting new coredumps found, every 30 seconds.
        """
        while True:
            self.get_backtraces()
            time.sleep(30)

    def start_backtrace_thread(self):
        self._backtrace_thread = threading.Thread(target=self.backtrace_thread)
        self._backtrace_thread.start()

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
        self._instance_wait_safe(self.instance.wait_until_stopped)
        self.instance.start()
        self._instance_wait_safe(self.instance.wait_until_running)
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
            self.log.error('Error checking for DB status: %s', details)
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

    @staticmethod
    def _parse_cfstats(cfstats_output):
        stat_dict = {}
        for line in cfstats_output.splitlines()[1:]:
            stat_line = [element for element in line.strip().split(':') if element]
            if stat_line:
                try:
                    try:
                        if '.' in stat_line[1].split()[0]:
                            stat_dict[stat_line[0]] = float(stat_line[1].split()[0])
                        else:
                            stat_dict[stat_line[0]] = int(stat_line[1].split()[0])
                    except IndexError:
                        continue
                except ValueError:
                    stat_dict[stat_line[0]] = stat_line[1].split()[0]
        return stat_dict

    def get_cfstats(self):
        def keyspace1_available():
            res = self.remoter.run('nodetool cfstats keyspace1', ignore_status=True)
            return res.exit_status == 0
        wait.wait_for(keyspace1_available, step=60, text='Waiting until keyspace1 is available')
        result = self.remoter.run('nodetool cfstats keyspace1')
        return self._parse_cfstats(result.stdout)

    def wait_db_up(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be up' % self
        wait.wait_for(func=self.db_up, step=60,
                      text=text)

    def wait_db_down(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for DB services to be down' % self
        wait.wait_for(func=lambda: not self.db_up(), step=60,
                      text=text)

    def wait_cs_installed(self, verbose=True):
        text = None
        if verbose:
            text = '%s: Waiting for cassandra-stress' % self
        wait.wait_for(func=self.cs_installed, step=60,
                      text=text)

    def update_db_binary(self, new_scylla_bin=None):
        if new_scylla_bin:
            self.log.info('Upgrading a DB binary for Node started')

            self.remoter.send_files(new_scylla_bin, '/tmp/scylla', verbose=True)

            # TODO: We should wait for scylla-server.service and scylla-ami-setup.service
            # has finished instead of sleep here.
            # As a workaround, sleep 3 minutes hoping they will finish
            # With --stop-services option in ami, /usr/lib/scylla/scylla_prepare
            # will fail the scylla-server.service
            time.sleep(180)

            # stop scylla-server before replace the binary
            self.remoter.run('sudo systemctl stop scylla-server.service')

            # replace the binary
            self.remoter.run('sudo cp -f /usr/bin/scylla /usr/bin/scylla.origin')
            self.remoter.run('sudo cp -f /tmp/scylla /usr/bin/scylla')
            self.remoter.run('sudo chown root.root /usr/bin/scylla')
            self.remoter.run('sudo chmod +x  /usr/bin/scylla')

            self.remoter.run('sudo systemctl restart scylla-server.service')
            self.remoter.run('sudo systemctl restart scylla-jmx.service')

            self.log.info('Upgrading a DB binary for Node done')


class Cluster(object):

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
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
        # I wanted to avoid some parameter passing
        # from the tester class to the cluster test.
        assert 'AVOCADO_TEST_LOGDIR' in os.environ
        try:
            self.logdir = path.init_dir(os.environ['AVOCADO_TEST_LOGDIR'],
                                        self.name)
        except OSError:
            self.logdir = os.path.join(os.environ['AVOCADO_TEST_LOGDIR'],
                                       self.name)
        logger = logging.getLogger('avocado.test')
        self.log = SDCMAdapter(logger, extra={'prefix': str(self)})
        self.log.info('Init nodes')
        self.nodes = []
        self.params = params
        self.add_nodes(n_nodes)

    def send_file(self, src, dst, verbose=False):
        for loader in self.nodes:
            loader.remoter.send_files(src=src, dst=dst, verbose=verbose)

    def run(self, cmd, verbose=False):
        for loader in self.nodes:
            loader.remoter.run(cmd=cmd, verbose=verbose)

    def get_backtraces(self):
        for node in self.nodes:
            node.get_backtraces()

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
                                         self.node_prefix, node_index,
                                         self.logdir)
                       for node_index, instance in
                       enumerate(instances, start=len(self.nodes) + 1)]
        self.nodes += added_nodes
        return added_nodes

    def __str__(self):
        return 'Cluster %s (AMI: %s Type: %s)' % (self.name,
                                                  self.ec2_ami_id,
                                                  self.ec2_instance_type)

    def _create_node(self, instance, ami_username, node_prefix, node_index,
                     base_logdir):
        node_prefix = '%s-%s' % (node_prefix, self.shortid)
        return Node(ec2_instance=instance, ec2_service=self.ec2,
                    credentials=self.credentials, ami_username=ami_username,
                    node_prefix=node_prefix, node_index=node_index,
                    base_logdir=base_logdir)

    def get_node_private_ips(self):
        return [node.instance.private_ip_address for node in self.nodes]

    def get_node_public_ips(self):
        return [node.instance.public_ip_address for node in self.nodes]

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()

    def cfstat_reached_treshold(self, key, treshold):
        """
        Find whether a certain cfstat key in all nodes reached a certain treshold value.

        :param key: cfstat key, example, 'Space used (total)'.
        :param treshold: Treshold value for cfstats key. Example, 2432043080.
        :return: Whether all nodes reached that treshold or not.
        """
        cfstats = [node.get_cfstats()[key] for node in self.nodes]
        reached_treshold = True
        for value in cfstats:
            if value < treshold:
                reached_treshold = False
        if reached_treshold:
            self.log.debug("Done waiting on cfstats: %s" % cfstats)
        return reached_treshold

    def wait_cfstat_reached_treshold(self, key, treshold):
        text = "Waiting until cfstat '%s' reaches value '%s'" % (key, treshold)
        wait.wait_for(func=self.cfstat_reached_treshold, step=10,
                      text=text, key=key, treshold=treshold)

    def wait_total_space_used_per_node(self, size=None):
        if size is None:
            size = int(self.params.get('space_node_treshold'))
        self.wait_cfstat_reached_treshold('Space used (total)', size)


class ScyllaCluster(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=10,
                 params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-db-node')
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)

        stop_services = ' '
        if params:
            new_scylla_bin = params.get('update_db_binary')
            if new_scylla_bin:
                stop_services = '--stop-services '

        user_data = ('--clustername %s %s'
                     '--totalnodes %s' % (name, stop_services, n_nodes))

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
                                            node_prefix=node_prefix,
                                            n_nodes=n_nodes,
                                            params=params)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()
        self.seed_nodes_private_ips = None
        self.version = '2.1'

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
                new_scylla_bin = self.params.get('update_db_binary')
                stop_services = ' '
                if new_scylla_bin:
                    stop_services = '--stop-services '
                ec2_user_data = ('--clustername %s --bootstrap true %s'
                                 '--totalnodes %s --seeds %s' % (self.name,
                                                                 stop_services,
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

        queue = Queue.Queue()

        def node_setup(node, new_scylla_bin):
            node.wait_ssh_up(verbose=verbose)
            # update db binary
            node.update_db_binary(new_scylla_bin)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y scylla-gdb',
                             verbose=verbose, ignore_status=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        new_scylla_bin = self.params.get('update_db_binary')
        for node in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node, new_scylla_bin))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
                time_elapsed = time.time() - start_time
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list),
                              int(time_elapsed))
            except Queue.Empty:
                pass

        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))

    def add_nemesis(self, nemesis):
        self.nemesis.append(nemesis(cluster=self,
                                    termination_event=self.termination_event))

    def start_nemesis(self, interval=30):
        self.log.debug('Start nemesis begin')
        self.termination_event = threading.Event()
        for nemesis in self.nemesis:
            nemesis.set_termination_event(self.termination_event)
            nemesis.set_target_node()
            nemesis_thread = threading.Thread(target=nemesis.run,
                                              args=(interval,), verbose=True)
            nemesis_thread.start()
            self.nemesis_threads.append(nemesis_thread)
        self.log.debug('Start nemesis end')

    def stop_nemesis(self, timeout=10):
        self.log.debug('Stop nemesis begin')
        self.termination_event.set()
        for nemesis_thread in self.nemesis_threads:
            nemesis_thread.join(timeout)
        self.nemesis_threads = []
        self.log.debug('Stop nemesis end')

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaCluster, self).destroy()

class CassandraCluster(ScyllaCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='ubuntu',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=10,
                 params=None):
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix,
                                              'cassandra-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'cassandra-db-node')
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
                                            node_prefix=node_prefix,
                                            n_nodes=n_nodes,
                                            params=params)
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

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            node.wait_db_up(verbose=verbose)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in node_list:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
                time_elapsed = time.time() - start_time
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list),
                              int(time_elapsed))
            except Queue.Empty:
                pass

        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))


class LoaderSet(Cluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 service, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'scylla-loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'scylla-loader-set')
        super(LoaderSet, self).__init__(ec2_ami_id=ec2_ami_id,
                                        ec2_subnet_id=ec2_subnet_id,
                                        ec2_security_group_ids=ec2_security_group_ids,
                                        ec2_instance_type=ec2_instance_type,
                                        ec2_ami_username=ec2_ami_username,
                                        service=service,
                                        ec2_block_device_mappings=ec2_block_device_mappings,
                                        credentials=credentials,
                                        cluster_prefix=cluster_prefix,
                                        node_prefix=node_prefix,
                                        n_nodes=n_nodes,
                                        params=params)
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
            log_file_name = os.path.join(logdir, 'cassandra-stress-%s.log' % uuid.uuid4())
            self.log.debug('Writing cassandra-stress log %s', log_file_name)
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

    def kill_stress_thread(self):
        kill_script_contents = 'PIDS=$(pgrep -f cassandra-stress) && pkill -TERM -P $PIDS'
        kill_script = script.TemporaryScript(name='kill_cassandra_stress.sh',
                                             content=kill_script_contents)
        kill_script.save()
        kill_script_dir = os.path.dirname(kill_script.path)
        for loader in self.nodes:
            loader.remoter.run(cmd='mkdir -p %s' % kill_script_dir)
            loader.remoter.send_files(kill_script.path, kill_script_dir)
            loader.remoter.run(cmd='chmod +x %s' % kill_script.path)
            result = loader.remoter.run(kill_script.path)
            self.log.debug('Terminate cassandra-stress process on loader %s: %s', str(loader), str(result))
            loader.remoter.run(cmd='rm -rf %s' % kill_script_dir)
        kill_script.remove()

    def do_plot(self, lines, plotfile='plot'):
        time_plot = []
        ops_plot = []
        latmax_plot = []
        lat999_plot = []
        lat99_plot = []
        lat95_plot = []
        for line in lines:
            line.strip()
            if line.startswith('total,'):
                items = line.split(',')
                totalops = items[1]
                ops = items[2]
                lat95 = items[7]
                lat99 = items[8]
                lat999 = items[9]
                latmax = items[10]
                time = items[11]
                time_plot.append(time)
                ops_plot.append(ops)
                lat95_plot.append(lat95)
                lat99_plot.append(lat99)
                lat999_plot.append(lat999)
                latmax_plot.append(latmax)
        # ops
        pl.plot(time_plot, ops_plot, label='ops', color='green')
        pl.title('Plot of time v.s. ops')
        pl.xlabel('time')
        pl.ylabel('ops')
        pl.legend()
        pl.savefig(plotfile + '-ops.svg')
        pl.close()

        # lat
        pl.plot(time_plot, lat95_plot, label='lat95', color='blue')
        pl.plot(time_plot, lat99_plot, label='lat99', color='green')
        pl.plot(time_plot, lat999_plot, label='lat999', color='black')
        pl.plot(time_plot, latmax_plot, label='latmax', color='red')

        pl.title('Plot of time v.s. latency')
        pl.xlabel('time')
        pl.ylabel('latency')
        pl.legend()
        pl.grid()
        pl.savefig(plotfile + '-lat.svg')
        pl.close()

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
            plotfile = os.path.join(self.logdir, str(node))
            self.do_plot(lines, plotfile)

        return errors

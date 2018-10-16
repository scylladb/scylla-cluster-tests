import logging
import time
import uuid
import os
import tempfile
import yaml

from botocore.exceptions import WaiterError, ClientError
import boto3.session
from avocado.utils import runtime as avocado_runtime

import cluster
import ec2_client

logger = logging.getLogger(__name__)

INSTANCE_PROVISION_ON_DEMAND = 'on_demand'
INSTANCE_PROVISION_SPOT_FLEET = 'spot_fleet'
INSTANCE_PROVISION_SPOT_LOW_PRICE = 'spot_low_price'
INSTANCE_PROVISION_SPOT_DURATION = 'spot_duration'
SPOT_CNT_LIMIT = 20
SPOT_FLEET_LIMIT = 50


def _prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = cluster.DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


def clean_aws_instances(region_name, instance_ids):
    try:
        session = boto3.session.Session(region_name=region_name)
        service = session.resource('ec2')
        service.instances.filter(InstanceIds=instance_ids).terminate()
    except Exception as details:
        logger.exception(str(details))


def clean_aws_credential(region_name, credential_key_name, credential_key_file):
    try:
        session = boto3.session.Session(region_name=region_name)
        service = session.resource('ec2')
        key_pair_info = service.KeyPair(credential_key_name)
        key_pair_info.delete()
        cluster.remove_if_exists(credential_key_file)
    except Exception as details:
        logger.exception(str(details))


class AWSCluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        region_names = params.get('region_name').split()
        if len(credentials) > 1 or len(region_names) > 1:
            assert len(credentials) == len(region_names)
        for idx, region_name in enumerate(region_names):
            credential = credentials[idx]
            if credential.type == 'generated':
                credential_key_name = credential.key_pair_name
                credential_key_file = credential.key_file
                if params.get('failure_post_behavior') == 'destroy':
                    avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_aws_credential,
                                                                   'args': (region_name,
                                                                            credential_key_name,
                                                                            credential_key_file),
                                                                   'once': True})
            cluster.CREDENTIALS.append(credential)

        self._ec2_ami_id = ec2_ami_id
        self._ec2_subnet_id = ec2_subnet_id
        self._ec2_security_group_ids = ec2_security_group_ids
        self._ec2_services = services
        self._credentials = credentials
        self._reuse_credentials = None
        self._ec2_instance_type = ec2_instance_type
        self._ec2_ami_username = ec2_ami_username
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        self._ec2_block_device_mappings = ec2_block_device_mappings
        self._ec2_user_data = ec2_user_data
        self._ec2_ami_id = ec2_ami_id
        self.region_names = region_names
        self.instance_provision = params.get('instance_provision', default=INSTANCE_PROVISION_ON_DEMAND)
        super(AWSCluster, self).__init__(cluster_uuid=cluster_uuid,
                                         cluster_prefix=cluster_prefix,
                                         node_prefix=node_prefix,
                                         n_nodes=n_nodes,
                                         params=params,
                                         region_names=self.region_names)

    def __str__(self):
        return 'Cluster %s (AMI: %s Type: %s)' % (self.name,
                                                  self._ec2_ami_id,
                                                  self._ec2_instance_type)

    def write_node_public_ip_file(self):
        public_ip_file_path = os.path.join(self.logdir, 'public_ips')
        with open(public_ip_file_path, 'w') as public_ip_file:
            public_ip_file.write("%s" % "\n".join(self.get_node_public_ips()))
            public_ip_file.write("\n")

    def write_node_private_ip_file(self):
        private_ip_file_path = os.path.join(self.logdir, 'private_ips')
        with open(private_ip_file_path, 'w') as private_ip_file:
            private_ip_file.write("%s" % "\n".join(self.get_node_private_ips()))
            private_ip_file.write("\n")

    def _create_on_demand_instances(self, count, interfaces, ec2_user_data, dc_idx=0):
        instances = self._ec2_services[dc_idx].create_instances(ImageId=self._ec2_ami_id[dc_idx],
                                                                UserData=ec2_user_data,
                                                                MinCount=count,
                                                                MaxCount=count,
                                                                KeyName=self._credentials[dc_idx].key_pair_name,
                                                                BlockDeviceMappings=self._ec2_block_device_mappings,
                                                                NetworkInterfaces=interfaces,
                                                                InstanceType=self._ec2_instance_type)
        return instances

    def _create_spot_instances(self, count,  interfaces, ec2_user_data='', dc_idx=0):
        ec2 = ec2_client.EC2Client(region_name=self.region_names[0])
        subnet_info = ec2.get_subnet_info(self._ec2_subnet_id[dc_idx])
        spot_params = dict(instance_type=self._ec2_instance_type,
                           image_id=self._ec2_ami_id[dc_idx],
                           region_name=subnet_info['AvailabilityZone'],
                           network_if=interfaces,
                           key_pair=self._credentials[dc_idx].key_pair_name,
                           user_data=ec2_user_data,
                           count=count)

        if self.instance_provision == INSTANCE_PROVISION_SPOT_DURATION:
            # duration value must be a multiple of 60
            spot_params.update({'duration': cluster.TEST_DURATION / 60 * 60 + 60})

        limit = SPOT_FLEET_LIMIT if self.instance_provision == INSTANCE_PROVISION_SPOT_FLEET else SPOT_CNT_LIMIT
        request_cnt = 1
        tail_cnt = 0
        if count > limit:
            # pass common reservationid
            spot_params['user_data'] += (' --customreservation %s' % str(uuid.uuid4())[:18])
            self.log.debug("User_data to spot instances: '%s'", spot_params['user_data'])
            request_cnt = count / limit
            spot_params['count'] = limit
            tail_cnt = count % limit
            if tail_cnt:
                request_cnt += 1
        instances = []
        for i in range(request_cnt):
            if tail_cnt and i == request_cnt - 1:
                spot_params['count'] = tail_cnt
            try:
                if self.instance_provision == INSTANCE_PROVISION_SPOT_FLEET and count > 1:
                    instances_i = ec2.create_spot_fleet(**spot_params)
                else:
                    instances_i = ec2.create_spot_instances(**spot_params)
                instances.extend(instances_i)
            except ClientError as cl_ex:
                if ec2_client.MAX_SPOT_EXCEEDED_ERROR in cl_ex.message:
                    self.log.debug('Cannot create spot instance(-s): %s.'
                                   'Creating on demand instance(-s) instead.', cl_ex)
                    instances_i = self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx)
                    instances.extend(instances_i)
                else:
                    raise

        return instances

    def _create_instances(self, count, ec2_user_data='', dc_idx=0):
        if not ec2_user_data:
            ec2_user_data = self._ec2_user_data
        self.log.debug("Passing user_data '%s' to create_instances", ec2_user_data)
        interfaces = [{'DeviceIndex': 0,
                       'SubnetId': self._ec2_subnet_id[dc_idx],
                       'AssociatePublicIpAddress': True,
                       'Groups': self._ec2_security_group_ids[dc_idx]}]

        if self.instance_provision == INSTANCE_PROVISION_ON_DEMAND:
            instances = self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx)
        else:
            instances = self._create_spot_instances(count, interfaces, ec2_user_data, dc_idx)

        return instances

    def _get_instances(self):
        ec2 = ec2_client.EC2Client(region_name=self.region_names[0])
        return [ec2.get_instance_by_private_ip(ip) for ip in self._node_private_ips]

    def update_bootstrap(self, ec2_user_data, enable_auto_bootstrap):
        """
        Update --bootstrap argument inside ec2_user_data string.
        """
        if enable_auto_bootstrap:
            if '--bootstrap ' in ec2_user_data:
                ec2_user_data.replace('--bootstrap false', '--bootstrap true')
            else:
                ec2_user_data += ' --bootstrap true '
        else:
            if '--bootstrap ' in ec2_user_data:
                ec2_user_data.replace('--bootstrap true', '--bootstrap false')
            else:
                ec2_user_data += ' --bootstrap false '
        return ec2_user_data

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if cluster.Setup.REUSE_CLUSTER:
            instances = self._get_instances()
        else:
            instances = self._create_instances(count, ec2_user_data, dc_idx)

        instance_ids = [i.id for i in instances]
        region_name = self.params.get('region_name')
        if self.params.get('failure_post_behavior') == 'destroy':
            avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_aws_instances,
                                                           'args': (region_name,
                                                                    instance_ids),
                                                           'once': True})
        cluster.EC2_INSTANCES += instances
        added_nodes = [self._create_node(instance, self._ec2_ami_username,
                                         self.node_prefix, node_index,
                                         self.logdir, dc_idx=dc_idx)
                       for node_index, instance in
                       enumerate(instances, start=self._node_index + 1)]
        for node in added_nodes:
            node.enable_auto_bootstrap = enable_auto_bootstrap
        self._node_index += len(added_nodes)
        self.nodes += added_nodes
        self.write_node_public_ip_file()
        self.write_node_private_ip_file()
        return added_nodes

    def _create_node(self, instance, ami_username, node_prefix, node_index,
                     base_logdir, dc_idx):
        return AWSNode(ec2_instance=instance, ec2_service=self._ec2_services[dc_idx],
                       credentials=self._credentials[dc_idx], ami_username=ami_username,
                       node_prefix=node_prefix, node_index=node_index,
                       base_logdir=base_logdir, dc_idx=dc_idx)


class AWSNode(cluster.BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials,
                 node_prefix='node', node_index=1, ami_username='root',
                 base_logdir=None, dc_idx=0):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = ec2_instance
        self._ec2_service = ec2_service
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        ssh_login_info = {'hostname': None,
                          'user': ami_username,
                          'key_file': credentials.key_file}
        super(AWSNode, self).__init__(name=name,
                                      ssh_login_info=ssh_login_info,
                                      base_logdir=base_logdir,
                                      node_prefix=node_prefix,
                                      dc_idx=dc_idx)

        if not cluster.Setup.REUSE_CLUSTER:
            tags_list = [{'Key': 'Name', 'Value': name},
                         {'Key': 'workspace', 'Value': cluster.WORKSPACE},
                         {'Key': 'uname', 'Value': ' | '.join(os.uname())}]
            if cluster.TEST_DURATION >= 24 * 60:
                self.log.info('Test duration set to %s. '
                              'Tagging node with {"keep": "alive"}',
                              cluster.TEST_DURATION)
                tags_list.append({'Key': 'keep', 'Value': 'alive'})
            self._ec2_service.create_tags(Resources=[self._instance.id],
                                          Tags=tags_list)

    @property
    def public_ip_address(self):
        return self._instance.public_ip_address

    @property
    def private_ip_address(self):
        return self._instance.private_ip_address

    def _instance_wait_safe(self, instance_method, *args, **kwargs):
        """
        Wrapper around AWS instance waiters that is safer to use.

        Since AWS adopts an eventual consistency model, sometimes the method
        wait_until_running will raise a botocore.exceptions.WaiterError saying
        the instance does not exist. AWS API guide [1] recommends that the
        procedure is retried using an exponencial backoff algorithm [2].

        :see: [1] http://docs.aws.amazon.com/AWSEC2/latest/APIReference/query-api-troubleshooting.html#eventual-consistency
        :see: [2] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        threshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                instance_method(*args, **kwargs)
                ok = True
            except WaiterError:
                time.sleep(min((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise cluster.NodeError('AWS instance %s waiter error after '
                                    'exponencial backoff wait' % self._instance.id)

    def _wait_public_ip(self):
        while self._instance.public_ip_address is None:
            time.sleep(1)
            self._instance.reload()

    def restart(self):
        # We differenciate between "Restart" and "Reboot".
        # Restart in AWS will be a Stop and Start of an instance.
        # When using storage optimized instances like i2 or i3, the data on disk is deleted upon STOP. therefore, we
        # need to setup the instance and treat it as a new instance.
        if ('i2' or 'i3') in self._instance.instance_type:
            self.remoter.run('sudo rm -f /etc/scylla/ami_configured')
        self._instance.stop()
        self._instance_wait_safe(self._instance.wait_until_stopped)
        self._instance.start()
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self._instance.public_ip_address)
        self.remoter.hostname = self._instance.public_ip_address

    def reboot(self, hard=True):
        if hard:
            self.log.debug('Hardly rebooting node')
            self._instance_wait_safe(self._instance.reboot)
        else:
            self.log.debug('Softly rebooting node')
            self.remoter.run('sudo reboot')

    def destroy(self):
        self.stop_task_threads()
        self._instance.terminate()
        cluster.EC2_INSTANCES.remove(self._instance)
        self.log.info('Destroyed')


class ScyllaAWSCluster(cluster.BaseScyllaCluster, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=[10],
                 params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'db-node')
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s' % (name, sum(n_nodes)))
        if params.get('stop_service', default='true') == 'true':
            user_data += ' --stop-services'
        super(ScyllaAWSCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                               ec2_subnet_id=ec2_subnet_id,
                                               ec2_security_group_ids=ec2_security_group_ids,
                                               ec2_instance_type=ec2_instance_type,
                                               ec2_ami_username=ec2_ami_username,
                                               ec2_user_data=user_data,
                                               ec2_block_device_mappings=ec2_block_device_mappings,
                                               cluster_uuid=cluster_uuid,
                                               services=services,
                                               credentials=credentials,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)
        self.seed_nodes_private_ips = None
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if not ec2_user_data:
            if self._ec2_user_data:
                ec2_user_data = self._ec2_user_data
            else:
                ec2_user_data = ('--clustername %s --totalnodes %s ' % (self.name, count))
        if self.nodes:
            if dc_idx > 0:
                node_public_ips = [node.public_ip_address for node
                                   in self.nodes if node.is_seed]
                seeds = ",".join(node_public_ips)
                if not seeds:
                    seeds = self.nodes[0].public_ip_address
            else:
                node_private_ips = [node.private_ip_address for node
                                    in self.nodes if node.is_seed]
                seeds = ",".join(node_private_ips)
                if not seeds:
                    seeds = self.nodes[0].private_ip_address
            ec2_user_data += ' --seeds %s ' % seeds

        ec2_user_data = self.update_bootstrap(ec2_user_data, enable_auto_bootstrap)
        added_nodes = super(ScyllaAWSCluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data,
                                                              dc_idx=dc_idx,
                                                              enable_auto_bootstrap=enable_auto_bootstrap)
        return added_nodes

    def node_config_setup(self, node, seed_address, endpoint_snitch):
        if len(self.datacenter) > 1:
            node.config_setup(seed_address=seed_address,
                              enable_exp=self._param_enabled('experimental'),
                              endpoint_snitch=endpoint_snitch,
                              broadcast=node.public_ip_address,
                              authenticator=self.params.get('authenticator'),
                              server_encrypt=self._param_enabled('server_encrypt'),
                              client_encrypt=self._param_enabled('client_encrypt'),
                              append_scylla_args=self.params.get('append_scylla_args'))
        else:
            node.config_setup(enable_exp=self._param_enabled('experimental'),
                              endpoint_snitch=endpoint_snitch,
                              authenticator=self.params.get('authenticator'),
                              server_encrypt=self._param_enabled('server_encrypt'),
                              client_encrypt=self._param_enabled('client_encrypt'),
                              append_scylla_args=self.params.get('append_scylla_args'))

    def node_setup(self, node, verbose=False, timeout=3600):
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = self.get_seed_nodes_by_flag(private_ip=False)

        if not cluster.Setup.REUSE_CLUSTER:

            node.wait_ssh_up(verbose=verbose)
            if len(self.datacenter) > 1:
                if not endpoint_snitch:
                    endpoint_snitch = "Ec2MultiRegionSnitch"
                node.datacenter_setup(self.datacenter)
            self.node_config_setup(node, seed_address, endpoint_snitch)

            node.stop_scylla_server(verify_down=False)
            node.start_scylla_server(verify_up=False)

        node.wait_db_up(verbose=verbose, timeout=timeout)
        node.remoter.run('nodetool status', verbose=True, retry=5)


    def destroy(self):
        self.stop_nemesis()
        super(ScyllaAWSCluster, self).destroy()


class CassandraAWSCluster(ScyllaAWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='ubuntu',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=[10],
                 params=None):
        if ec2_block_device_mappings is None:
            ec2_block_device_mappings = []
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = uuid.uuid4()
        cluster_prefix = _prepend_user_prefix(user_prefix,
                                              'cs-db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'cs-db-node')
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)
        user_data = ('--clustername %s '
                     '--totalnodes %s --version community '
                     '--release 2.1.15' % (name, sum(n_nodes)))

        super(ScyllaAWSCluster, self).__init__(ec2_ami_id=ec2_ami_id,
                                               ec2_subnet_id=ec2_subnet_id,
                                               ec2_security_group_ids=ec2_security_group_ids,
                                               ec2_instance_type=ec2_instance_type,
                                               ec2_ami_username=ec2_ami_username,
                                               ec2_user_data=user_data,
                                               ec2_block_device_mappings=ec2_block_device_mappings,
                                               cluster_uuid=cluster_uuid,
                                               services=services,
                                               credentials=credentials,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)

    def get_seed_nodes(self):
        node = self.nodes[0]
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct-cassandra'), 'cassandra.yaml')
        node.remoter.receive_files(src='/etc/cassandra/cassandra.yaml',
                                   dst=yaml_dst_path)
        with open(yaml_dst_path, 'r') as yaml_stream:
            conf_dict = yaml.load(yaml_stream)
            try:
                return conf_dict['seed_provider'][0]['parameters'][0]['seeds'].split(',')
            except:
                raise ValueError('Unexpected cassandra.yaml '
                                 'contents:\n%s' % yaml_stream.read())

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if not ec2_user_data:
            if self.nodes:
                seeds = ",".join(self.get_seed_nodes())
                ec2_user_data = ('--clustername %s '
                                 '--totalnodes %s --seeds %s '
                                 '--version community '
                                 '--release 2.1.15' % (self.name,
                                                       count,
                                                       seeds))
        ec2_user_data = self.update_bootstrap(ec2_user_data, enable_auto_bootstrap)
        added_nodes = super(ScyllaAWSCluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data,
                                                              dc_idx=dc_idx)
        return added_nodes

    def node_setup(self, node, verbose=False):
        node.wait_ssh_up(verbose=verbose)
        node.wait_db_up(verbose=verbose)

        if cluster.Setup.REUSE_CLUSTER:
            return

        node.wait_apt_not_running()
        node.remoter.run('sudo apt-get update')
        node.remoter.run('sudo apt-get install -y collectd collectd-utils')
        node.remoter.run('sudo apt-get install -y openjdk-6-jdk')

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False):
        self.get_seed_nodes()


class LoaderSetAWS(cluster.BaseLoaderSet, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos',
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'loader-set')
        user_data = ('--clustername %s --totalnodes %s --bootstrap false --stop-services' %
                     (cluster_prefix, n_nodes))
        cluster.BaseLoaderSet.__init__(self,
                                       params=params)
        AWSCluster.__init__(self,
                            ec2_ami_id=ec2_ami_id,
                            ec2_subnet_id=ec2_subnet_id,
                            ec2_security_group_ids=ec2_security_group_ids,
                            ec2_instance_type=ec2_instance_type,
                            ec2_ami_username=ec2_ami_username,
                            ec2_user_data=user_data,
                            services=services,
                            ec2_block_device_mappings=ec2_block_device_mappings,
                            credentials=credentials,
                            cluster_prefix=cluster_prefix,
                            node_prefix=node_prefix,
                            n_nodes=n_nodes,
                            params=params)



class MonitorSetAWS(cluster.BaseMonitorSet, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos',
                 user_prefix=None, n_nodes=10, targets=None, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'monitor-set')
        user_data = ('--clustername %s --totalnodes %s --bootstrap false --stop-services' %
                     (cluster_prefix, n_nodes))
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        AWSCluster.__init__(self,
                            ec2_ami_id=ec2_ami_id,
                            ec2_subnet_id=ec2_subnet_id,
                            ec2_security_group_ids=ec2_security_group_ids,
                            ec2_instance_type=ec2_instance_type,
                            ec2_ami_username=ec2_ami_username,
                            ec2_user_data=user_data,
                            services=services,
                            ec2_block_device_mappings=ec2_block_device_mappings,
                            credentials=credentials,
                            cluster_prefix=cluster_prefix,
                            node_prefix=node_prefix,
                            n_nodes=n_nodes,
                            params=params)

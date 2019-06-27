import logging
import time
import uuid
import os
import tempfile
import yaml
import json
import base64

from botocore.exceptions import WaiterError, ClientError
from textwrap import dedent
from threading import Thread
from datetime import datetime

import cluster
import ec2_client
from sdcm.utils.common import retrying, list_instances_aws
from sdcm.sct_events import SpotTerminationEvent, DbEventsFilter
from . import wait

logger = logging.getLogger(__name__)

INSTANCE_PROVISION_ON_DEMAND = 'on_demand'
INSTANCE_PROVISION_SPOT_FLEET = 'spot_fleet'
INSTANCE_PROVISION_SPOT_LOW_PRICE = 'spot_low_price'
INSTANCE_PROVISION_SPOT_DURATION = 'spot_duration'
SPOT_CNT_LIMIT = 20
SPOT_FLEET_LIMIT = 50


def create_tags_list():
    tags_list = [{'Key': k, 'Value': v} for k, v in cluster.create_common_tags().items()]
    if cluster.TEST_DURATION >= 24 * 60 or cluster.Setup.KEEP_ALIVE:
        tags_list.append({'Key': 'keep', 'Value': 'alive'})

    return tags_list


class PublicIpNotReady(Exception):
    pass


class AWSCluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None, node_type=None):
        region_names = params.get('region_name').split()
        if len(credentials) > 1 or len(region_names) > 1:
            assert len(credentials) == len(region_names)
        for idx, region_name in enumerate(region_names):
            credential = credentials[idx]
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
        self.region_names = region_names
        self.instance_provision = params.get('instance_provision', default=INSTANCE_PROVISION_ON_DEMAND)
        self.params = params
        self.node_type = node_type
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

    def _create_on_demand_instances(self, count, interfaces, ec2_user_data, dc_idx=0, tags_list=[]):
        ami_id = self._ec2_ami_id[dc_idx]
        self.log.debug("Creating {count} on-demand instances using AMI id '{ami_id}'... ".format(**locals()))
        instances = self._ec2_services[dc_idx].create_instances(ImageId=ami_id,
                                                                UserData=ec2_user_data,
                                                                MinCount=count,
                                                                MaxCount=count,
                                                                KeyName=self._credentials[dc_idx].key_pair_name,
                                                                BlockDeviceMappings=self._ec2_block_device_mappings,
                                                                NetworkInterfaces=interfaces,
                                                                InstanceType=self._ec2_instance_type,
                                                                TagSpecifications=[
                                                                    {
                                                                        'ResourceType': 'instance',
                                                                        'Tags': tags_list
                                                                    },
                                                                ],
                                                                )
        self.log.debug("Created instances: %s." % instances)
        return instances

    def _create_spot_instances(self, count, interfaces, ec2_user_data='', dc_idx=0, tags_list=[]):
        ec2 = ec2_client.EC2Client(region_name=self.region_names[dc_idx],
                                   spot_max_price_percentage=self.params.get('spot_max_price', default=0.60))
        subnet_info = ec2.get_subnet_info(self._ec2_subnet_id[dc_idx])
        spot_params = dict(instance_type=self._ec2_instance_type,
                           image_id=self._ec2_ami_id[dc_idx],
                           region_name=subnet_info['AvailabilityZone'],
                           network_if=interfaces,
                           key_pair=self._credentials[dc_idx].key_pair_name,
                           user_data=ec2_user_data,
                           count=count,
                           block_device_mappings=self._ec2_block_device_mappings,
                           tags_list=tags_list)
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
                    instances_i = self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
                    instances.extend(instances_i)
                else:
                    raise

        return instances

    def _create_instances(self, count, ec2_user_data='', dc_idx=0):

        tags_list = create_tags_list()
        tags_list.append({'Key': 'NodeType', 'Value': self.node_type})

        if not ec2_user_data:
            ec2_user_data = self._ec2_user_data
        self.log.debug("Passing user_data '%s' to create_instances", ec2_user_data)
        interfaces = [{'DeviceIndex': 0,
                       'SubnetId': self._ec2_subnet_id[dc_idx],
                       'AssociatePublicIpAddress': True,
                       'Groups': self._ec2_security_group_ids[dc_idx]}]
        if self.instance_provision == 'mixed':
            instances = self._create_mixed_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        elif self.instance_provision == INSTANCE_PROVISION_ON_DEMAND:
            instances = self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        else:
            instances = self._create_spot_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)

        return instances

    def _create_mixed_instances(self, count, interfaces, ec2_user_data, dc_idx, tags_list=[]):
        instances = []
        MAX_NUM_ON_DEMAND = 2
        if isinstance(self, ScyllaAWSCluster) or isinstance(self, CassandraAWSCluster):
            if count > 2:
                count_on_demand = MAX_NUM_ON_DEMAND
            elif count == 2:
                count_on_demand = 1
            else:
                count_on_demand = 0

            if self.nodes:
                num_of_on_demand = len([node for node in self.nodes if not node.is_spot])
                if num_of_on_demand < MAX_NUM_ON_DEMAND:
                    count_on_demand = MAX_NUM_ON_DEMAND - num_of_on_demand
                else:
                    count_on_demand = 0

            count_spot = count - count_on_demand

            if count_spot > 0:
                self.instance_provision = INSTANCE_PROVISION_SPOT_LOW_PRICE
                instances.extend(self._create_spot_instances(count_spot, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
            if count_on_demand > 0:
                self.instance_provision = INSTANCE_PROVISION_ON_DEMAND
                instances.extend(self._create_on_demand_instances(count_on_demand, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
            self.instance_provision = 'mixed'
        elif isinstance(self, LoaderSetAWS):
            self.instance_provision = INSTANCE_PROVISION_SPOT_LOW_PRICE
            instances = self._create_spot_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        elif isinstance(self, MonitorSetAWS):
            self.instance_provision = INSTANCE_PROVISION_ON_DEMAND
            instances.extend(self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
        else:
            raise Exception('Unsuported type of cluster type %s' % self)
        return instances

    def _get_instances(self, dc_idx):

        test_id = cluster.Setup.test_id()
        if not test_id:
            raise ValueError("test_id should be configured for using reuse_cluster")

        ec2 = ec2_client.EC2Client(region_name=self.region_names[dc_idx],
                                   spot_max_price_percentage=self.params.get('spot_max_price', default=0.60))
        instances = list_instances_aws(tags_dict={'TestId': test_id, 'NodeType': self.node_type}, region_name=self.region_names[dc_idx])

        def sort_by_index(item):
            for tag in item['Tags']:
                if tag['Key'] == 'NodeIndex':
                    return tag['Value']
            else:
                return '0'
        instances = sorted(instances, key=sort_by_index)
        return [ec2.get_instance(instance['InstanceId']) for instance in instances]

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

        post_boot_script = cluster.Setup.get_startup_script()

        if 'clustername' in ec2_user_data:
            ec2_user_data += " --base64postscript={0}".format(base64.b64encode(post_boot_script))
        else:
            ec2_user_data = post_boot_script

        if cluster.Setup.REUSE_CLUSTER:
            instances = self._get_instances(dc_idx)
        else:
            instances = self._create_instances(count, ec2_user_data, dc_idx)

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
                       credentials=self._credentials[dc_idx], parent_cluster=self, ami_username=ami_username,
                       node_prefix=node_prefix, node_index=node_index,
                       base_logdir=base_logdir, dc_idx=dc_idx, node_type=self.node_type)


class AWSNode(cluster.BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials, parent_cluster,
                 node_prefix='node', node_index=1, ami_username='root',
                 base_logdir=None, dc_idx=0, node_type=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = ec2_instance
        self._ec2_service = ec2_service
        logger.debug("Waiting until instance {0._instance} starts running...".format(self))
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        ssh_login_info = {'hostname': None,
                          'user': ami_username,
                          'key_file': credentials.key_file}
        self._spot_aws_termination_task = None
        super(AWSNode, self).__init__(name=name,
                                      parent_cluster=parent_cluster,
                                      ssh_login_info=ssh_login_info,
                                      base_logdir=base_logdir,
                                      node_prefix=node_prefix,
                                      dc_idx=dc_idx)
        if not cluster.Setup.REUSE_CLUSTER:
            tags_list = create_tags_list()
            tags_list.append({'Key': 'Name', 'Value': name})
            tags_list.append({'Key': 'NodeIndex', 'Value': str(node_index)})
            tags_list.append({'Key': 'NodeType', 'Value': node_type})
            if cluster.TEST_DURATION >= 24 * 60 or cluster.Setup.KEEP_ALIVE:
                self.log.info('Test duration set to %s. '
                              'Keep cluster on failure %s. '
                              'Tagging node with {"keep": "alive"}',
                              cluster.TEST_DURATION, cluster.Setup.KEEP_ALIVE)

            self._ec2_service.create_tags(Resources=[self._instance.id],
                                          Tags=tags_list)

    @property
    def is_spot(self):
        if (self._instance.instance_lifecycle and
                'spot' in self._instance.instance_lifecycle.lower()):
            return True
        else:
            return False

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
            try:
                self._instance.reload()
            except Exception as e:
                logger.exception("Error while reloading instance metadata: %s", e)
            finally:
                method_name = instance_method.__name__
                instance_id = self._instance.id
                logger.debug(self._instance.meta.data)
                msg = "Timeout while running '{method_name}' method on AWS instance '{instance_id}'".format(**locals())
                raise cluster.NodeError(msg)

    @retrying(n=7, sleep_time=10, allowed_exceptions=(PublicIpNotReady,),
              message="Waiting for instance to get public ip")
    def _wait_public_ip(self):
        self._instance.reload()
        if self._instance.public_ip_address is None:
            raise PublicIpNotReady(self._instance)
        logger.debug("[{0._instance}] Got public ip: {0._instance.public_ip_address}".format(self))

    def restart(self):
        # We differenciate between "Restart" and "Reboot".
        # Restart in AWS will be a Stop and Start of an instance.
        # When using storage optimized instances like i2 or i3, the data on disk is deleted upon STOP. therefore, we
        # need to setup the instance and treat it as a new instance.
        if self._instance.spot_instance_request_id:
            logger.debug("target node is spot instance, impossible to stop this instance, skipping the restart")
            return

        event_filters = ()
        if any(ss in self._instance.instance_type for ss in ['i3', 'i2']):
            # since there's no disk yet in those type, lots of the errors here are acceptable, and we'll ignore them
            event_filters = DbEventsFilter(type="DATABASE_ERROR"), DbEventsFilter(type="SCHEMA_FAILURE"), \
                DbEventsFilter(type="NO_SPACE_ERROR"), DbEventsFilter(type="FILESYSTEM_ERROR")

            clean_script = dedent("""
                sudo sed -e '/.*scylla/s/^/#/g' -i /etc/fstab
                sudo sed -e '/auto_bootstrap:.*/s/False/True/g' -i /etc/scylla/scylla.yaml
            """)
            self.remoter.run("sudo bash -cxe '%s'" % clean_script)
            output = self.remoter.run('sudo grep replace_address: /etc/scylla/scylla.yaml', ignore_status=True)
            if 'replace_address_first_boot:' not in output.stdout:
                self.remoter.run('sudo echo replace_address_first_boot: %s >> /etc/scylla/scylla.yaml' %
                                 self._instance.private_ip_address)
        self._instance.stop()
        self._instance_wait_safe(self._instance.wait_until_stopped)
        self._instance.start()
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self._instance.public_ip_address)
        self.remoter.hostname = self._instance.public_ip_address
        self.wait_ssh_up()

        if any(ss in self._instance.instance_type for ss in ['i3', 'i2']):
            try:
                self.remoter.run('sudo /usr/lib/scylla/scylla-ami/scylla_create_devices')
                self.stop_scylla_server(verify_down=False)
                self.start_scylla_server(verify_up=False)
            finally:
                if event_filters:
                    for event_filter in event_filters:
                        event_filter.cancel_filter()

    def hard_reboot(self):
        self._instance_wait_safe(self._instance.reboot)

    def destroy(self):
        self.stop_task_threads()
        self._instance.terminate()
        self.log.info('Destroyed')

    def start_task_threads(self):
        if self._instance.spot_instance_request_id and 'spot' in self._instance.instance_lifecycle.lower():
            self.start_aws_termination_monitoring()
        super(AWSNode, self).start_task_threads()

    def stop_task_threads(self, timeout=10):
        if self._spot_aws_termination_task and not self.termination_event.isSet():
            self.termination_event.set()
            self._spot_aws_termination_task.join(timeout)
        super(AWSNode, self).stop_task_threads(timeout)

    def get_aws_termination_notification(self):
        try:
            result = self.remoter.run('curl http://169.254.169.254/latest/meta-data/spot/instance-action', verbose=False)
            status = result.stdout.strip()
            if '404 - Not Found' not in status:
                return status
        except Exception as details:
            self.log.warning('Error during getting aws termination notification %s' % details)
        return None

    def monitor_aws_termination_thread(self):
        while True:
            duration = 5
            if self.termination_event.isSet():
                break
            try:
                self.wait_ssh_up(verbose=False)
            except Exception as ex:
                logger.warning("Unable to connect to '%s'. Probably the node was terminated or is still booting. "
                               "Error details: '%s'", self.name, ex)
                continue
            aws_message = self.get_aws_termination_notification()
            if aws_message:
                self.log.warning('Got spot termination notification from AWS %s' % aws_message)
                terminate_action = json.loads(aws_message)
                terminate_action_timestamp = time.mktime(datetime.strptime(terminate_action['time'], "%Y-%m-%dT%H:%M:%SZ").timetuple())
                duration = terminate_action_timestamp - time.time() - 15
                if duration <= 0:
                    duration = 5
                terminate_action['time-left'] = terminate_action_timestamp - time.time()
                SpotTerminationEvent(node=self, aws_message=terminate_action)
            time.sleep(duration)

    def start_aws_termination_monitoring(self):
        self._spot_aws_termination_task = Thread(target=self.monitor_aws_termination_thread)
        self._spot_aws_termination_task.daemon = True
        self._spot_aws_termination_task.start()

    def get_console_output(self):
        """Get instance console Output

        Get console output of instance which is printed during initiating and loading
        Get only last 64KB of output data.
        """
        result = self._ec2_service.meta.client.get_console_output(
            InstanceId=self._instance.id,
        )
        console_output = result.get('Output', '')

        if not console_output:
            self.log.warning('Some error during getting console output')
        return console_output

    def get_console_screenshot(self):
        result = self._ec2_service.meta.client.get_console_screenshot(
            InstanceId=self._instance.id
        )
        imagedata = result.get('ImageData', '')

        if not imagedata:
            self.log.warning('Some error during getting console screenshot')
        return imagedata


class ScyllaAWSCluster(cluster.BaseScyllaCluster, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=[10],
                 params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = cluster.Setup.test_id()
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        node_type = 'scylla-db'
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
                                               params=params,
                                               node_type=node_type)
        self.seed_nodes_ips = None
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if not ec2_user_data:
            if self._ec2_user_data:
                ec2_user_data = self._ec2_user_data
            else:
                ec2_user_data = ('--clustername %s --totalnodes %s ' % (self.name, count))
        if self.nodes:
            node_ips = [node.ip_address for node in self.nodes if node.is_seed]
            seeds = ",".join(node_ips)

            if not seeds:
                seeds = self.nodes[0].ip_address

            ec2_user_data += ' --seeds %s ' % seeds

        ec2_user_data = self.update_bootstrap(ec2_user_data, enable_auto_bootstrap)
        added_nodes = super(ScyllaAWSCluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data,
                                                              dc_idx=dc_idx,
                                                              enable_auto_bootstrap=enable_auto_bootstrap)
        return added_nodes

    def node_config_setup(self, node, seed_address, endpoint_snitch):
        setup_params = dict(
            enable_exp=self._param_enabled('experimental'),
            endpoint_snitch=endpoint_snitch,
            authenticator=self.params.get('authenticator'),
            server_encrypt=self._param_enabled('server_encrypt'),
            client_encrypt=self._param_enabled('client_encrypt'),
            append_scylla_args=self.get_scylla_args(),
            authorizer=self.params.get('authorizer'),
            hinted_handoff=self.params.get('hinted_handoff'),
        )
        if cluster.Setup.INTRA_NODE_COMM_PUBLIC:
            setup_params.update(dict(
                seed_address=seed_address,
                broadcast=node.public_ip_address,
            ))

        node.config_setup(**setup_params)

    def node_setup(self, node, verbose=False, timeout=3600):
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = self.get_seed_nodes_by_flag()

        def scylla_ami_setup_done():
            """
            Scylla-ami-setup will update config files and trigger to start the scylla-server service.
            `--stop-services` parameter in ec2 user-data, not really stop running scylla-server
            service, but deleting a flag file (/etc/scylla/ami_disabled) in first start of scylla-server
            (by scylla_prepare), and fail the first start.

            We use this function to make sure scylla-ami-setup finishes, and first start is
            done (fail as expected, /etc/scylla/ami_disabled is deleted). Then it won't effect
            reconfig in SCT.

            The fllowing two examples are different opportunity to help understand.

            # opportunity 1: scylla-ami-setup finishes:
              result = node.remoter.run('systemctl status scylla-ami-setup', ignore_status=True)
              return 'Started Scylla AMI Setup' in result.stdout

            # opportunity 2: flag file is deleted in scylla_prepare:
              result = node.remoter.run('test -e /etc/scylla/ami_disabled', ignore_status=True)
              return result.exit_status != 0
            """

            # make sure scylla-ami-setup finishes, flag file is deleted, and first start fails as expected.
            result = node.remoter.run('systemctl status scylla-server', ignore_status=True)
            return 'Failed to start Scylla Server.' in result.stdout

        if not cluster.Setup.REUSE_CLUSTER:

            node.wait_ssh_up(verbose=verbose)
            wait.wait_for(scylla_ami_setup_done, step=10, timeout=300)
            node.install_scylla_debuginfo()

            if cluster.Setup.MULTI_REGION:
                if not endpoint_snitch:
                    endpoint_snitch = "Ec2MultiRegionSnitch"
                node.datacenter_setup(self.datacenter)
            self.node_config_setup(node, seed_address, endpoint_snitch)

            node.stop_scylla_server(verify_down=False)
            node.start_scylla_server(verify_up=False)
        else:
            # for reconfigure rsyslog
            node.run_startup_script()

        node.wait_db_up(verbose=verbose, timeout=timeout)
        node.check_nodes_status()

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
        cluster_prefix = cluster.prepend_user_prefix(user_prefix,
                                                     'cs-db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'cs-db-node')
        node_type = 'cs-db'
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
                                               params=params,
                                               node_type=node_type)

    def get_seed_nodes(self):
        node = self.nodes[0]
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='sct-cassandra'), 'cassandra.yaml')
        node.remoter.receive_files(src='/etc/cassandra/cassandra.yaml',
                                   dst=yaml_dst_path)
        with open(yaml_dst_path, 'r') as yaml_stream:
            conf_dict = yaml.load(yaml_stream, Loader=yaml.SafeLoader)
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
            # for reconfigure rsyslog
            node.run_startup_script()
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
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        node_type = 'loader'
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')
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
                            params=params,
                            node_type=node_type)


class MonitorSetAWS(cluster.BaseMonitorSet, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos',
                 user_prefix=None, n_nodes=10, targets=None, params=None):
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        node_type = 'monitor'
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)

        AWSCluster.__init__(self,
                            ec2_ami_id=ec2_ami_id,
                            ec2_subnet_id=ec2_subnet_id,
                            ec2_security_group_ids=ec2_security_group_ids,
                            ec2_instance_type=ec2_instance_type,
                            ec2_ami_username=ec2_ami_username,
                            services=services,
                            ec2_block_device_mappings=ec2_block_device_mappings,
                            credentials=credentials,
                            cluster_prefix=cluster_prefix,
                            node_prefix=node_prefix,
                            n_nodes=n_nodes,
                            params=params,
                            node_type=node_type)

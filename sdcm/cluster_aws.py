# pylint: disable=too-many-lines, too-many-public-methods

import re
import logging
import time
import uuid
import os
import tempfile
import json
import base64
from textwrap import dedent
from datetime import datetime
from distutils.version import LooseVersion  # pylint: disable=no-name-in-module,import-error

import yaml
from botocore.exceptions import WaiterError, ClientError
import boto3

from sdcm import cluster
from sdcm import ec2_client
from sdcm.cluster import INSTANCE_PROVISION_ON_DEMAND
from sdcm.utils.common import retrying, list_instances_aws, get_ami_tags
from sdcm.sct_events import SpotTerminationEvent, DbEventsFilter
from sdcm import wait
from sdcm.remote import LocalCmdRunner, NETWORK_EXCEPTIONS

LOGGER = logging.getLogger(__name__)

INSTANCE_PROVISION_SPOT_FLEET = 'spot_fleet'
INSTANCE_PROVISION_SPOT_LOW_PRICE = 'spot_low_price'
INSTANCE_PROVISION_SPOT_DURATION = 'spot_duration'
SPOT_CNT_LIMIT = 20
SPOT_FLEET_LIMIT = 50
SPOT_TERMINATION_CHECK_OVERHEAD = 15
LOCAL_CMD_RUNNER = LocalCmdRunner()

# pylint: disable=too-many-lines


def create_tags_list():
    tags_list = [{'Key': k, 'Value': v} for k, v in cluster.create_common_tags().items()]
    if cluster.TEST_DURATION >= 24 * 60:
        tags_list.append({'Key': 'keep', 'Value': 'alive'})

    return tags_list


class PublicIpNotReady(Exception):
    pass


class AWSCluster(cluster.BaseCluster):  # pylint: disable=too-many-instance-attributes,abstract-method,

    """
    Cluster of Node objects, started on Amazon EC2.
    """

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,  # pylint: disable=too-many-arguments
                 services, credentials, cluster_uuid=None,
                 ec2_instance_type='c4.xlarge', ec2_ami_username='root',
                 ec2_user_data='', ec2_block_device_mappings=None,
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None, node_type=None, extra_network_interface=False):
        # pylint: disable=too-many-locals
        region_names = params.get('region_name').split()
        if len(credentials) > 1 or len(region_names) > 1:
            assert len(credentials) == len(region_names)
        for idx, _ in enumerate(region_names):
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
        self.params = params

        super(AWSCluster, self).__init__(cluster_uuid=cluster_uuid,
                                         cluster_prefix=cluster_prefix,
                                         node_prefix=node_prefix,
                                         n_nodes=n_nodes,
                                         params=params,
                                         region_names=self.region_names,
                                         node_type=node_type,
                                         extra_network_interface=extra_network_interface
                                         )

    def __str__(self):
        return 'Cluster %s (AMI: %s Type: %s)' % (self.name,
                                                  self._ec2_ami_id,
                                                  self._ec2_instance_type)

    def _create_on_demand_instances(self, count, interfaces, ec2_user_data, dc_idx=0, tags_list=None):  # pylint: disable=too-many-arguments
        ami_id = self._ec2_ami_id[dc_idx]
        self.log.debug(f"Creating {count} on-demand instances using AMI id '{ami_id}'... ")
        params = dict(ImageId=ami_id,
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
                              'Tags': tags_list if tags_list else []
                          },
                      ]
                      )
        instance_profile = self.params.get('aws_instance_profile_name')
        if instance_profile:
            params['IamInstanceProfile'] = {'Name': instance_profile}
        instances = self._ec2_services[dc_idx].create_instances(**params)
        self.log.debug("Created instances: %s." % instances)
        return instances

    def _create_spot_instances(self, count, interfaces, ec2_user_data='', dc_idx=0, tags_list=None):  # pylint: disable=too-many-arguments
        # pylint: disable=too-many-locals
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
                           aws_instance_profile=self.params.get('aws_instance_profile_name'),
                           tags_list=tags_list if tags_list else [])
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
            request_cnt = count // limit
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
                if ec2_client.MAX_SPOT_EXCEEDED_ERROR in str(cl_ex):
                    self.log.debug('Cannot create spot instance(-s): %s.'
                                   'Creating on demand instance(-s) instead.', cl_ex)
                    instances_i = self._create_on_demand_instances(
                        count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
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
        if self.extra_network_interface:
            interfaces = [{'DeviceIndex': 0,
                           'SubnetId': self._ec2_subnet_id[dc_idx],
                           'Groups': self._ec2_security_group_ids[dc_idx]},
                          {'DeviceIndex': 1,
                           'SubnetId': self._ec2_subnet_id[dc_idx],
                           'Groups': self._ec2_security_group_ids[dc_idx]}]

        if self.instance_provision == 'mixed':
            instances = self._create_mixed_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        elif self.instance_provision == INSTANCE_PROVISION_ON_DEMAND:
            instances = self._create_on_demand_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        else:
            instances = self._create_spot_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)

        return instances

    def _create_mixed_instances(self, count, interfaces, ec2_user_data, dc_idx, tags_list=None):  # pylint: disable=too-many-arguments
        tags_list = tags_list if tags_list else []
        instances = []
        max_num_on_demand = 2
        if isinstance(self, (ScyllaAWSCluster, CassandraAWSCluster)):
            if count > 2:
                count_on_demand = max_num_on_demand
            elif count == 2:
                count_on_demand = 1
            else:
                count_on_demand = 0

            if self.nodes:
                num_of_on_demand = len([node for node in self.nodes if not node.is_spot])
                if num_of_on_demand < max_num_on_demand:
                    count_on_demand = max_num_on_demand - num_of_on_demand
                else:
                    count_on_demand = 0

            count_spot = count - count_on_demand

            if count_spot > 0:
                self.instance_provision = INSTANCE_PROVISION_SPOT_LOW_PRICE
                instances.extend(self._create_spot_instances(
                    count_spot, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
            if count_on_demand > 0:
                self.instance_provision = INSTANCE_PROVISION_ON_DEMAND
                instances.extend(self._create_on_demand_instances(
                    count_on_demand, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
            self.instance_provision = 'mixed'
        elif isinstance(self, LoaderSetAWS):
            self.instance_provision = INSTANCE_PROVISION_SPOT_LOW_PRICE
            instances = self._create_spot_instances(count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list)
        elif isinstance(self, MonitorSetAWS):
            self.instance_provision = INSTANCE_PROVISION_ON_DEMAND
            instances.extend(self._create_on_demand_instances(
                count, interfaces, ec2_user_data, dc_idx, tags_list=tags_list))
        else:
            raise Exception('Unsuported type of cluster type %s' % self)
        return instances

    def _get_instances(self, dc_idx):

        test_id = cluster.Setup.test_id()
        if not test_id:
            raise ValueError("test_id should be configured for using reuse_cluster")

        ec2 = ec2_client.EC2Client(region_name=self.region_names[dc_idx],
                                   spot_max_price_percentage=self.params.get('spot_max_price', default=0.60))
        results = list_instances_aws(tags_dict={'TestId': test_id, 'NodeType': self.node_type},
                                     region_name=self.region_names[dc_idx], group_as_region=True)
        instances = results[self.region_names[dc_idx]]

        def sort_by_index(item):
            for tag in item['Tags']:
                if tag['Key'] == 'NodeIndex':
                    return tag['Value']
            return '0'
        instances = sorted(instances, key=sort_by_index)
        return [ec2.get_instance(instance['InstanceId']) for instance in instances]

    @staticmethod
    def update_bootstrap(ec2_user_data, enable_auto_bootstrap):
        """
        Update --bootstrap argument inside ec2_user_data string.
        """
        if isinstance(ec2_user_data, dict):
            ec2_user_data['scylla_yaml']['auto_bootstrap'] = enable_auto_bootstrap
            return ec2_user_data

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

    # This is workaround for issue #5179: IPv6 - routing in scylla AMI isn't configured properly
    @staticmethod
    def network_config_ipv6_workaround_script():  # pylint: disable=invalid-name
        return dedent("""
            BASE_EC2_NETWORK_URL=http://169.254.169.254/latest/meta-data/network/interfaces/macs/
            MAC=`curl -s ${BASE_EC2_NETWORK_URL}`
            IPv6_CIDR=`curl -s ${BASE_EC2_NETWORK_URL}${MAC}/subnet-ipv6-cidr-blocks`

            sudo ip route add $IPv6_CIDR dev eth0
        """)

    @staticmethod
    def configure_eth1_script():
        return dedent("""
            BASE_EC2_NETWORK_URL=http://169.254.169.254/latest/meta-data/network/interfaces/macs/
            NUMBER_OF_ENI=`curl -s ${BASE_EC2_NETWORK_URL} | wc -w`
            for mac in `curl -s ${BASE_EC2_NETWORK_URL}`
            do
                DEVICE_NUMBER=`curl -s ${BASE_EC2_NETWORK_URL}${mac}/device-number`
                if [[ "$DEVICE_NUMBER" == "1" ]]; then
                   ETH1_MAC=${mac}
                fi
            done

            if [[ ! "${DEVICE_NUMBER}x" == "x" ]]; then
               ETH1_IP_ADDRESS=`curl -s ${BASE_EC2_NETWORK_URL}${ETH1_MAC}/local-ipv4s`
               ETH1_CIDR_BLOCK=`curl -s ${BASE_EC2_NETWORK_URL}${ETH1_MAC}/subnet-ipv4-cidr-block`
            fi

            sudo bash -c "echo 'GATEWAYDEV=eth0' >> /etc/sysconfig/network"

            echo "
            DEVICE="eth1"
            BOOTPROTO="dhcp"
            ONBOOT="yes"
            TYPE="Ethernet"
            USERCTL="yes"
            PEERDNS="yes"
            IPV6INIT="no"
            PERSISTENT_DHCLIENT="1"
            " > /etc/sysconfig/network-scripts/ifcfg-eth1

            echo "
            default via 10.0.0.1 dev eth1 table 2
            ${ETH1_CIDR_BLOCK} dev eth1 src ${ETH1_IP_ADDRESS} table 2
            " > /etc/sysconfig/network-scripts/route-eth1

            echo "
            from ${ETH1_IP_ADDRESS}/32 table 2
            " > /etc/sysconfig/network-scripts/rule-eth1

            sudo systemctl restart network
        """)

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):

        post_boot_script = cluster.Setup.get_startup_script()
        if self.extra_network_interface:
            post_boot_script += self.configure_eth1_script()

        if self.params.get('ip_ssh_connections') == 'ipv6':
            post_boot_script += self.network_config_ipv6_workaround_script()

        if isinstance(ec2_user_data, dict):
            ec2_user_data['post_configuration_script'] = base64.b64encode(
                post_boot_script.encode('utf-8')).decode('ascii')
            ec2_user_data = json.dumps(ec2_user_data)
        else:
            if 'clustername' in ec2_user_data:
                ec2_user_data += " --base64postscript={0}".format(
                    base64.b64encode(post_boot_script.encode('utf-8')).decode('ascii'))
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
            if self.params.get('ip_ssh_connections') == 'ipv6':
                node.config_ipv6_as_persistent()
        self._node_index += len(added_nodes)
        self.nodes += added_nodes
        self.write_node_public_ip_file()
        self.write_node_private_ip_file()
        return added_nodes

    def _create_node(self, instance, ami_username, node_prefix, node_index,  # pylint: disable=too-many-arguments
                     base_logdir, dc_idx):
        return AWSNode(ec2_instance=instance, ec2_service=self._ec2_services[dc_idx],
                       credentials=self._credentials[dc_idx], parent_cluster=self, ami_username=ami_username,
                       node_prefix=node_prefix, node_index=node_index,
                       base_logdir=base_logdir, dc_idx=dc_idx, node_type=self.node_type)


class AWSNode(cluster.BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, ec2_instance, ec2_service, credentials, parent_cluster,  # pylint: disable=too-many-arguments
                 node_prefix='node', node_index=1, ami_username='root',
                 base_logdir=None, dc_idx=0, node_type=None):

        name = '%s-%s' % (node_prefix, node_index)
        self._instance = ec2_instance
        self._ec2_service = ec2_service
        LOGGER.debug("Waiting until instance {0._instance} starts running...".format(self))
        self._instance_wait_safe(self._instance.wait_until_running)
        self._eth1_private_ip_address = None
        self.eip_allocation_id = None
        ssh_login_info = {'hostname': None,
                          'user': ami_username,
                          'key_file': credentials.key_file}
        if len(self._instance.network_interfaces) == 2:
            # first we need to configure the both networks so we'll have public ip
            self.allocate_and_attach_elastic_ip(parent_cluster, dc_idx)

        self._wait_public_ip()
        super(AWSNode, self).__init__(name=name,
                                      parent_cluster=parent_cluster,
                                      ssh_login_info=ssh_login_info,
                                      base_logdir=base_logdir,
                                      node_prefix=node_prefix,
                                      dc_idx=dc_idx)
        if not cluster.Setup.REUSE_CLUSTER:

            self.set_node_tags(name, node_index, node_type)
            self.set_keep_tag()

    def set_node_tags(self, name=None, node_index=None, node_type=None):
        tags_list = create_tags_list()
        tags_list.append({'Key': 'Name', 'Value': name})
        tags_list.append({'Key': 'NodeIndex', 'Value': str(node_index)})
        tags_list.append({'Key': 'NodeType', 'Value': node_type})

        self._ec2_service.create_tags(Resources=[self._instance.id],
                                      Tags=tags_list)

    def set_keep_tag(self):
        tags_list = []
        if "db" in self.name and cluster.Setup.KEEP_ALIVE_DB_NODES:
            self.log.info('Keep db cluster %s', cluster.Setup.KEEP_ALIVE_DB_NODES)
            tags_list.append({'Key': 'keep', 'Value': 'alive'})
        if "loader" in self.name and cluster.Setup.KEEP_ALIVE_LOADER_NODES:
            self.log.info('Keep loader cluster %s', cluster.Setup.KEEP_ALIVE_LOADER_NODES)
            tags_list.append({'Key': 'keep', 'Value': 'alive'})
        if "monitor" in self.name and cluster.Setup.KEEP_ALIVE_MONITOR_NODES:
            self.log.info('Keep monitor cluster %s', cluster.Setup.KEEP_ALIVE_MONITOR_NODES)
            tags_list.append({'Key': 'keep', 'Value': 'alive'})

        if tags_list:
            self._ec2_service.create_tags(Resources=[self._instance.id],
                                          Tags=tags_list)

    @retrying(n=10, sleep_time=5, allowed_exceptions=NETWORK_EXCEPTIONS, message="Retrying set_hostname")
    def set_hostname(self):
        self.log.info('Changing hostname to %s', self.name)
        # Using https://aws.amazon.com/premiumsupport/knowledge-center/linux-static-hostname-rhel7-centos7/
        # FIXME: workaround to avoid host rename generating errors on other commands
        if self.is_debian():
            return
        result = self.remoter.run(f"sudo hostnamectl set-hostname --static {self.name}", ignore_status=True)
        if result.ok:
            self.log.debug('Hostname has been changed succesfully. Apply')
            apply_hostname_change_script = dedent(f"""
                if ! grep "\\$LocalHostname {self.name}" /etc/rsyslog.conf; then
                    echo "" >> /etc/rsyslog.conf
                    echo "\\$LocalHostname {self.name}" >> /etc/rsyslog.conf
                fi
                grep -P "127.0.0.1[^\\\\n]+{self.name}" /etc/hosts || sed -ri "s/(127.0.0.1[ \\t]+localhost[^\\n]*)$/\\1\\t{self.name}/" /etc/hosts
                grep "preserve_hostname: true" /etc/cloud/cloud.cfg 1>/dev/null 2>&1 || echo "preserve_hostname: true" >> /etc/cloud/cloud.cfg
                systemctl restart rsyslog
            """)
            self.remoter.run(f"sudo bash -cxe '{apply_hostname_change_script}'")
            self.log.debug('Continue node %s set up', self.name)
        else:
            self.log.warning('Hostname has not been changed. Error: %s.\n Continue with old name', result.stderr)

    @property
    def is_spot(self):
        return bool(self._instance.instance_lifecycle and 'spot' in self._instance.instance_lifecycle.lower())

    def check_spot_termination(self):
        try:
            result = self.remoter.run(
                'curl http://169.254.169.254/latest/meta-data/spot/instance-action', verbose=False)
            status = result.stdout.strip()
        except Exception as details:  # pylint: disable=broad-except
            self.log.warning('Error during getting spot termination notification %s', details)
            return 0

        if '404 - Not Found' in status:
            return 0

        self.log.warning('Got spot termination notification from AWS %s', status)
        terminate_action = json.loads(status)
        terminate_action_timestamp = time.mktime(datetime.strptime(
            terminate_action['time'], "%Y-%m-%dT%H:%M:%SZ").timetuple())
        next_check_delay = terminate_action['time-left'] = terminate_action_timestamp - time.time()
        SpotTerminationEvent(node=self, message=terminate_action)

        return max(next_check_delay - SPOT_TERMINATION_CHECK_OVERHEAD, 0)

    @property
    def external_address(self):
        """
        the communication address for usage between the test and the nodes
        :return:
        """
        if self.parent_cluster.params.get("ip_ssh_connections") == "ipv6":
            return self.ipv6_ip_address
        elif cluster.IP_SSH_CONNECTIONS == 'public' or cluster.Setup.INTRA_NODE_COMM_PUBLIC:
            return self.public_ip_address
        else:
            return self._instance.private_ip_address

    @property
    def public_ip_address(self):
        return self._instance.public_ip_address

    @property
    def private_ip_address(self):
        if self._eth1_private_ip_address:
            return self._eth1_private_ip_address

        return self._instance.private_ip_address

    @property
    def ipv6_ip_address(self):
        return self._instance.network_interfaces[0].ipv6_addresses[0]["Ipv6Address"]

    def _refresh_instance_state(self):
        raise NotImplementedError()

    def allocate_and_attach_elastic_ip(self, parent_cluster, dc_idx):
        primary_interface = [
            interface for interface in self._instance.network_interfaces if interface.attachment['DeviceIndex'] == 0][0]
        if primary_interface.association_attribute is None:
            # create and attach EIP
            client = boto3.client('ec2', region_name=parent_cluster.region_names[dc_idx])
            response = client.allocate_address(Domain='vpc')

            self.eip_allocation_id = response['AllocationId']

            client.create_tags(
                Resources=[
                    self.eip_allocation_id
                ],
                Tags=create_tags_list()
            )
            client.associate_address(
                AllocationId=self.eip_allocation_id,
                NetworkInterfaceId=primary_interface.id,
            )
        self._eth1_private_ip_address = [interface for interface in self._instance.network_interfaces if
                                         interface.attachment['DeviceIndex'] == 1][0].private_ip_address

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
            except Exception as ex:  # pylint: disable=broad-except
                LOGGER.exception("Error while reloading instance metadata: %s", ex)
            finally:
                method_name = instance_method.__name__
                instance_id = self._instance.id
                LOGGER.debug(self._instance.meta.data)
                msg = "Timeout while running '{method_name}' method on AWS instance '{instance_id}'".format(
                    method_name=method_name, instance_id=instance_id)
                raise cluster.NodeError(msg)

    @retrying(n=7, sleep_time=10, allowed_exceptions=(PublicIpNotReady,),
              message="Waiting for instance to get public ip")
    def _wait_public_ip(self):
        self._instance.reload()
        if self._instance.public_ip_address is None:
            raise PublicIpNotReady(self._instance)
        LOGGER.debug("[{0._instance}] Got public ip: {0._instance.public_ip_address}".format(self))

    def config_ipv6_as_persistent(self):
        cidr = dedent("""
                        BASE_EC2_NETWORK_URL=http://169.254.169.254/latest/meta-data/network/interfaces/macs/
                        MAC=`curl -s ${BASE_EC2_NETWORK_URL}`
                        curl -s ${BASE_EC2_NETWORK_URL}${MAC}/subnet-ipv6-cidr-blocks
                    """)
        output = self.remoter.run(f"sudo bash -cxe '{cidr}'")
        ipv6_cidr = output.stdout.strip()
        self.remoter.run(
            f"sudo sh -c  \"echo 'sudo ip route add {ipv6_cidr} dev eth0' >> /etc/sysconfig/network-scripts/init.ipv6-global\"")

        res = self.remoter.run(f"grep '{ipv6_cidr}' /etc/sysconfig/network-scripts/init.ipv6-global")
        LOGGER.debug('init.ipv6-global was {}updated'.format('' if res.stdout.strip else 'NOT '))

    def restart(self):
        # We differenciate between "Restart" and "Reboot".
        # Restart in AWS will be a Stop and Start of an instance.
        # When using storage optimized instances like i2 or i3, the data on disk is deleted upon STOP. therefore, we
        # need to setup the instance and treat it as a new instance.
        if self._instance.spot_instance_request_id:
            LOGGER.debug("target node is spot instance, impossible to stop this instance, skipping the restart")
            return

        event_filters = ()
        if any(ss in self._instance.instance_type for ss in ['i3', 'i2']):
            # since there's no disk yet in those type, lots of the errors here are acceptable, and we'll ignore them
            event_filters = DbEventsFilter(type="DATABASE_ERROR", node=self), \
                DbEventsFilter(type="SCHEMA_FAILURE", node=self), \
                DbEventsFilter(type="NO_SPACE_ERROR", node=self), \
                DbEventsFilter(type="FILESYSTEM_ERROR", node=self), \
                DbEventsFilter(type="RUNTIME_ERROR", node=self)

            clean_script = dedent("""
                sudo sed -e '/.*scylla/s/^/#/g' -i /etc/fstab
                sudo sed -e '/auto_bootstrap:.*/s/False/True/g' -i /etc/scylla/scylla.yaml
            """)
            self.remoter.run("sudo bash -cxe '%s'" % clean_script)
            output = self.remoter.run('sudo grep replace_address: /etc/scylla/scylla.yaml', ignore_status=True)
            if 'replace_address_first_boot:' not in output.stdout:
                self.remoter.run('echo replace_address_first_boot: %s |sudo tee --append /etc/scylla/scylla.yaml' %
                                 self._instance.private_ip_address)
        self._instance.stop()
        self._instance_wait_safe(self._instance.wait_until_stopped)
        self._instance.start()
        self._instance_wait_safe(self._instance.wait_until_running)
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self._instance.public_ip_address)
        self.remoter.hostname = self.external_address
        self.wait_ssh_up()

        if any(ss in self._instance.instance_type for ss in ['i3', 'i2']):
            try:
                self.stop_scylla_server(verify_down=False)

                # the scylla_create_devices has been moved to the '/opt/scylladb' folder in the master branch
                for create_devices_file in ['/usr/lib/scylla/scylla-ami/scylla_create_devices',
                                            '/opt/scylladb/scylla-ami/scylla_create_devices',
                                            '/opt/scylladb/scylla-cloud-image/scylla_create_devices']:
                    result = self.remoter.run('sudo test -e %s' % create_devices_file, ignore_status=True)
                    if result.exit_status == 0:
                        self.remoter.run('sudo %s' % create_devices_file)
                        break
                else:
                    raise IOError('scylla_create_devices file isn\'t found')

                self.start_scylla_server(verify_up=False)
                self.remoter.run(
                    'sudo sed -i -e "s/^replace_address_first_boot:/# replace_address_first_boot:/g" /etc/scylla/scylla.yaml')
                self.remoter.run("sudo sed -e '/auto_bootstrap:.*/s/True/False/g' -i /etc/scylla/scylla.yaml")
            finally:
                if event_filters:
                    for event_filter in event_filters:
                        event_filter.cancel_filter()

    def hard_reboot(self):
        self._instance_wait_safe(self._instance.reboot)

    def destroy(self):
        self.stop_task_threads()
        self._instance.terminate()
        if self.eip_allocation_id:
            client = boto3.client('ec2', region_name=self.parent_cluster.region_names[self.dc_idx])
            response = client.release_address(AllocationId=self.eip_allocation_id)
            self.log.debug("release elastic ip . Result: %s\n", response)
        self.log.info('Destroyed')

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
        return imagedata.encode('ascii')

    def traffic_control(self, tcconfig_params=None):
        """
        run tcconfig locally to create tc commands, and run them on the node
        :param tcconfig_params: commandline arguments for tcset, if None will call tcdel
        :return: None
        """

        self.remoter.run("sudo modprobe sch_netem")

        if tcconfig_params is None:
            tc_command = LOCAL_CMD_RUNNER.run("tcdel eth1 --tc-command", ignore_status=True).stdout
            self.remoter.run('sudo bash -cxe "%s"' % tc_command, ignore_status=True)
        else:
            tc_command = LOCAL_CMD_RUNNER.run("tcset eth1 {} --tc-command".format(tcconfig_params)).stdout
            self.remoter.run('sudo bash -cxe "%s"' % tc_command)


class ScyllaAWSCluster(cluster.BaseScyllaCluster, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,  # pylint: disable=too-many-arguments
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='centos',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=3,
                 params=None):
        # pylint: disable=too-many-locals
        # We have to pass the cluster name in advance in user_data
        cluster_uuid = cluster.Setup.test_id()
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        node_type = 'scylla-db'
        shortid = str(cluster_uuid)[:8]
        name = '%s-%s' % (cluster_prefix, shortid)

        scylla_cloud_image_version = get_ami_tags(ec2_ami_id[0], region_name=params.get(
            'region_name').split()[0]).get('sci_version', '1')
        if LooseVersion(scylla_cloud_image_version) >= LooseVersion('2'):
            user_data = dict(scylla_yaml=dict(cluster_name=name), start_scylla_on_first_boot=False)
        else:
            user_data = ('--clustername %s '
                         '--totalnodes %s' % (name, sum(n_nodes)))
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
                                               node_type=node_type,
                                               extra_network_interface=params.get('extra_network_interface'))
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if not ec2_user_data:
            if self._ec2_user_data and isinstance(self._ec2_user_data, str):
                ec2_user_data = re.sub(r'(--totalnodes\s)(\d*)(\s)',
                                       r'\g<1>{}\g<3>'.format(len(self.nodes) + count), self._ec2_user_data)
            elif self._ec2_user_data and isinstance(self._ec2_user_data, dict):
                ec2_user_data = self._ec2_user_data
            else:
                ec2_user_data = ('--clustername %s --totalnodes %s ' % (self.name, count))
        if self.nodes and isinstance(ec2_user_data, str):
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

    def node_config_setup(self, node, seed_address=None, endpoint_snitch=None, murmur3_partitioner_ignore_msb_bits=None, client_encrypt=None):  # pylint: disable=too-many-arguments
        setup_params = dict(
            enable_exp=self.params.get('experimental'),
            endpoint_snitch=endpoint_snitch,
            authenticator=self.params.get('authenticator'),
            server_encrypt=self.params.get('server_encrypt'),
            client_encrypt=client_encrypt if client_encrypt is not None else self.params.get('client_encrypt'),
            append_scylla_args=self.get_scylla_args(),
            authorizer=self.params.get('authorizer'),
            hinted_handoff=self.params.get('hinted_handoff'),
            alternator_port=self.params.get('alternator_port'),
            seed_address=seed_address,
            append_scylla_yaml=self.params.get('append_scylla_yaml'),
            murmur3_partitioner_ignore_msb_bits=murmur3_partitioner_ignore_msb_bits,
            ip_ssh_connections=self.params.get('ip_ssh_connections'),
        )
        if cluster.Setup.INTRA_NODE_COMM_PUBLIC:
            setup_params.update(dict(
                broadcast=node.public_ip_address,
            ))

        if self.extra_network_interface:
            setup_params.update(dict(
                seed_address=seed_address,
                broadcast=node.private_ip_address,
                listen_on_all_interfaces=True,
            ))

        node.config_setup(**setup_params)

    def node_setup(self, node, verbose=False, timeout=3600):
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = ','.join(self.seed_nodes_ips)

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

            if self.params.get('ip_ssh_connections') == 'ipv6':
                node.set_web_listen_address()

            node.stop_scylla_server(verify_down=False)
            node.start_scylla_server(verify_up=False)
        else:
            # for reconfigure rsyslog
            node.run_startup_script()

        node.wait_db_up(verbose=verbose, timeout=timeout)
        node.check_nodes_status()
        self.clean_replacement_node_ip(node)

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaAWSCluster, self).destroy()


class CassandraAWSCluster(ScyllaAWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,  # pylint: disable=too-many-arguments
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_ami_username='ubuntu',
                 ec2_block_device_mappings=None,
                 user_prefix=None,
                 n_nodes=3,
                 params=None):
        # pylint: disable=too-many-locals
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

        super(CassandraAWSCluster, self).__init__(ec2_ami_id=ec2_ami_id,
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
        added_nodes = super(CassandraAWSCluster, self).add_nodes(count=count,
                                                                 ec2_user_data=ec2_user_data,
                                                                 dc_idx=dc_idx)
        return added_nodes

    def node_setup(self, node, verbose=False, timeout=3600):
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
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):
        self.get_seed_nodes()


class LoaderSetAWS(cluster.BaseLoaderSet, AWSCluster):

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,  # pylint: disable=too-many-arguments
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos',
                 user_prefix=None, n_nodes=10, params=None):
        # pylint: disable=too-many-locals
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

    def __init__(self, ec2_ami_id, ec2_subnet_id, ec2_security_group_ids,  # pylint: disable=too-many-arguments
                 services, credentials, ec2_instance_type='c4.xlarge',
                 ec2_block_device_mappings=None,
                 ec2_ami_username='centos',
                 user_prefix=None, n_nodes=10, targets=None, params=None):
        # pylint: disable=too-many-locals
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

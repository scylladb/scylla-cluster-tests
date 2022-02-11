import logging
import time
from functools import cached_property
from typing import Dict, Optional

from sdcm import cluster
from sdcm.keystore import KeyStore
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, VmInstance

LOGGER = logging.getLogger(__name__)


class CreateAzureNodeError(Exception):
    pass


class AzureNode(cluster.BaseNode):

    """
    Wraps Azure instances, so that we can also control the instance through SSH.
    """

    log = LOGGER

    def __init__(self, azure_instance: VmInstance, vm_provisioner, credentials, parent_cluster,  # pylint: disable=too-many-arguments
                 node_prefix='node', node_index=1, azure_image_username='root',
                 base_logdir=None, dc_idx=0):
        region = parent_cluster.params.get('azure_region_name').split()[dc_idx]
        name = f"{node_prefix}-{region}-{node_index}".lower()
        self.node_index = node_index
        self._instance: VmInstance = azure_instance
        self._vm_provisioner = vm_provisioner
        ssh_login_info = {'hostname': None,
                          'user': azure_image_username,
                          'key_file': credentials.key_file,
                          'extra_ssh_options': '-tt'}
        super().__init__(name=name,
                         parent_cluster=parent_cluster,
                         ssh_login_info=ssh_login_info,
                         base_logdir=base_logdir,
                         node_prefix=node_prefix,
                         dc_idx=dc_idx)

    @cached_property
    def tags(self) -> Dict[str, str]:
        return {**super().tags,
                "NodeIndex": str(self.node_index), }

    def _set_keep_alive(self) -> bool:
        # todo lukasz: no setting keep alive for now. To be added later.
        return False

    def _instance_wait_safe(self, instance_method, *args, **kwargs):
        """
        Wrapper around Azure instance methods that is safer to use.

        Let's try a method, and if it fails, let's retry using an exponential
        backoff algorithm, similar to what Amazon recommends for it's own
        service [1].

        :see: [1] http://docs.aws.amazon.com/general/latest/gr/api-retries.html
        """
        threshold = 300
        ok = False
        retries = 0
        max_retries = 9
        while not ok and retries <= max_retries:
            try:
                return instance_method(*args, **kwargs)
            except Exception as details:  # pylint: disable=broad-except
                self.log.error('Call to method %s (retries: %s) failed: %s',
                               instance_method, retries, details)
                time.sleep(min((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise cluster.NodeError('Azure instance %s method call error after '
                                    'exponential backoff wait' % self._instance.id)

    def _refresh_instance_state(self):
        ip_tuple = (self._instance.public_ip_address, self._instance.private_ip_address)
        return ip_tuple

    @property
    def region(self):
        return self._instance.region

    def set_hostname(self):
        self.log.debug("Hostname for node %s left as is", self.name)

    @property
    def is_spot(self):
        return self._instance.pricing_model.is_spot()

    def check_spot_termination(self):
        """Check if a spot instance termination was initiated by the cloud.
        """
        # todo: fix it
        return False

    def restart(self):
        # When using local_ssd disks in Azure, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance_wait_safe(self._instance.reboot)

    def hard_reboot(self):
        self._instance_wait_safe(self._instance.reboot)

    def _safe_destroy(self):
        raise NotImplementedError()

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance_wait_safe(self._safe_destroy)
        super().destroy()

    def get_console_output(self):
        # TODO adding console output from instance on Azure
        self.log.warning('Method is not implemented for AzureNode')
        return ''

    def get_console_screenshot(self):
        # TODO adding console output from instance on Azure
        self.log.warning('Method is not implemented for AzureNode')
        return b''

    def _get_ipv6_ip_address(self):
        # todo: fix it
        return ""

    @property
    def image(self):
        return self._instance.image

    def _get_public_ip_address(self) -> Optional[str]:
        return self._instance.public_ip_address

    def _get_private_ip_address(self) -> Optional[str]:
        return self._instance.private_ip_address


class AzureCluster(cluster.BaseCluster):   # pylint: disable=too-many-instance-attributes
    def __init__(self, image_id, azure_image_type, azure_image_size, azure_network, provisioner: AzureProvisioner, credentials,  # pylint: disable=too-many-arguments, too-many-locals
                 cluster_uuid=None, azure_instance_type='Standard_L8s_v2', azure_region_names=None,
                 azure_n_local_ssd=1, azure_image_username='root', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=3, add_disks=None, params=None, node_type=None, service_accounts=None):
        self.provisioner: AzureProvisioner = provisioner
        self._image_id = image_id
        self._azure_image_type = azure_image_type
        self._azure_image_size = azure_image_size
        self._azure_network = azure_network
        self._vm_provisioner = provisioner
        self._credentials = credentials
        self._azure_instance_type = azure_instance_type
        self._azure_image_username = azure_image_username
        self._azure_region_names = azure_region_names
        self._azure_n_local_ssd = int(azure_n_local_ssd) if azure_n_local_ssd else 0
        self._add_disks = add_disks
        self._service_accounts = service_accounts
        # the full node prefix will contain unique uuid, so use this for search of existing nodes
        self._node_prefix = node_prefix
        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=azure_region_names,
                         node_type=node_type)
        self.log.debug("AzureCluster constructor")

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False):  # pylint: disable=too-many-arguments
        self.log.info("Adding nodes to cluster")
        nodes = []
        region = self.params.get('azure_region_name').split()[dc_idx]
        instances = self._create_instances(count, region)

        self.log.debug('instances: %s', instances)
        for node_index, instance in enumerate(instances, start=self._node_index + 1):
            node = self._create_node(instance, node_index, dc_idx)
            nodes.append(node)
            self.nodes.append(node)
            self.log.info("Added node: %s", node.name)
            node.enable_auto_bootstrap = enable_auto_bootstrap

        self._node_index += count
        self.log.info('added nodes: %s', nodes)
        return nodes

    def _create_node(self, instance, node_index, dc_idx):
        # try:
        node = AzureNode(azure_instance=instance,
                         vm_provisioner=self.provisioner,
                         credentials=self._credentials[0],
                         parent_cluster=self,
                         azure_image_username=self._azure_image_username,
                         node_prefix=self.node_prefix,
                         node_index=node_index,
                         base_logdir=self.logdir,
                         dc_idx=dc_idx)
        node.init()
        return node
        # except Exception as ex:
        #     raise CreateAzureNodeError('Failed to create node: %s' % ex) from ex

    def _create_instances(self, count, region):
        assert region, "no region provided, please add `azure_region_name` param"
        pricing_model = PricingModel.SPOT if 'spot' in self.instance_provision else PricingModel.ON_DEMAND
        instance_definition = InstanceDefinition(
            name="to_be_replaced",
            image_id=self._image_id,
            type=self._azure_instance_type,
            user_name=self._azure_image_username,
            tags=self.tags,
            ssh_public_key=KeyStore().get_ec2_ssh_key_pair().public_key.decode()
        )
        instances = []

        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            instance_definition.name = f"{self._node_prefix}-{region}-{node_index}".lower()
            instances.append(self.provisioner.create_virtual_machine(region=region, definition=instance_definition,
                                                                     pricing_model=pricing_model))
        return instances

    def get_node_ips_param(self, public_ip=True):
        # todo lukasz: why gce cluster didn't implement this?
        raise NotImplementedError("get_node_ips_param should not run")

    def node_setup(self, node, verbose=False, timeout=3600):
        # todo lukasz: why gce cluster didn't implement this?
        raise NotImplementedError("node_setup should not run")

    def wait_for_init(self):
        # todo lukasz: why gce cluster didn't implement this?
        raise NotImplementedError("wait_for_init should not run")


class ScyllaAzureCluster(cluster.BaseScyllaCluster, AzureCluster):

    def __init__(self, image_id, azure_image_type, azure_image_size, azure_network, provisioner: AzureProvisioner, credentials,  # pylint: disable=too-many-arguments
                 azure_instance_type='Standard_L8s_v2', azure_n_local_ssd=1,
                 azure_image_username='ubuntu',
                 user_prefix=None, n_nodes=3, add_disks=None, params=None, azure_datacenter=None, service_accounts=None):
        # pylint: disable=too-many-locals
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        super().__init__(
            image_id=image_id,
            azure_image_type=azure_image_type,
            azure_image_size=azure_image_size,
            azure_n_local_ssd=azure_n_local_ssd,
            azure_network=azure_network,
            azure_instance_type=azure_instance_type,
            azure_image_username=azure_image_username,
            provisioner=provisioner,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            add_disks=add_disks,
            params=params,
            azure_region_names=azure_datacenter,
            node_type='scylla-db',
            service_accounts=service_accounts,
        )
        self.version = '2.1'

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()

import logging
from functools import cached_property
from typing import Dict, Optional, List

from sdcm import cluster
from sdcm.keystore import KeyStore
from sdcm.provision.azure.provisioner import AzureProvisioner
from sdcm.provision.provisioner import InstanceDefinition, PricingModel, VmInstance
from sdcm.utils.decorators import retrying

LOGGER = logging.getLogger(__name__)


class CreateAzureNodeError(Exception):
    pass


class AzureNode(cluster.BaseNode):

    """
    Wraps Azure instances, so that we can also control the instance through SSH.
    """

    log = LOGGER

    def __init__(self, azure_instance: VmInstance,  # pylint: disable=too-many-arguments
                 credentials, parent_cluster,
                 node_prefix='node', node_index=1, user_name='root',
                 base_logdir=None, dc_idx=0):
        region = parent_cluster.params.get('azure_region_name').split()[dc_idx]
        name = f"{node_prefix}-{region}-{node_index}".lower()
        self.node_index = node_index
        self._instance: VmInstance = azure_instance
        ssh_login_info = {'hostname': None,
                          'user': user_name,
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
        # When using NVMe disks in Azure, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance.reboot(wait=True)

    def hard_reboot(self):
        self._instance.reboot(wait=True)

    def destroy(self):
        self.stop_task_threads()
        self.wait_till_tasks_threads_are_stopped()
        self._instance.terminate(wait=True)
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
    def __init__(self, image_id, root_disk_size,  # pylint: disable=too-many-arguments, too-many-locals
                 provisioners: List[AzureProvisioner], credentials,
                 cluster_uuid=None, instance_type='Standard_L8s_v2', region_names=None,
                 user_name='root', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=3, params=None, node_type=None):
        self.provisioners: List[AzureProvisioner] = provisioners
        self._image_id = image_id
        self._root_disk_size = root_disk_size
        self._credentials = credentials
        self._instance_type = instance_type
        self._user_name = user_name
        self._azure_region_names = region_names
        self._node_prefix = node_prefix
        super().__init__(cluster_uuid=cluster_uuid,
                         cluster_prefix=cluster_prefix,
                         node_prefix=node_prefix,
                         n_nodes=n_nodes,
                         params=params,
                         region_names=region_names,
                         node_type=node_type)
        self.log.debug("AzureCluster constructor")

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False):  # pylint: disable=too-many-arguments
        self.log.info("Adding nodes to cluster")
        nodes = []

        instances = self._create_instances(count, dc_idx)

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
        try:
            node = AzureNode(azure_instance=instance,
                             credentials=self._credentials[0],
                             parent_cluster=self,
                             user_name=self._user_name,
                             node_prefix=self.node_prefix,
                             node_index=node_index,
                             base_logdir=self.logdir,
                             dc_idx=dc_idx)
            node.init()
            return node
        except Exception as ex:
            raise CreateAzureNodeError('Failed to create node: %s' % ex) from ex

    def _create_instances(self, count, dc_idx=0):
        region = self.params.get('azure_region_name').split()[dc_idx]
        assert region, "no region provided, please add `azure_region_name` param"
        pricing_model = PricingModel.SPOT if 'spot' in self.instance_provision else PricingModel.ON_DEMAND
        instances = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            instance_definition = InstanceDefinition(
                name=f"{self._node_prefix}-{region}-{node_index}".lower(),
                image_id=self._image_id,
                type=self._instance_type,
                user_name=self._user_name,
                tags=self.tags,
                ssh_public_key=KeyStore().get_ec2_ssh_key_pair().public_key.decode()
            )
            instances.append(self._provision_instance(instance_definition=instance_definition,
                                                      pricing_model=pricing_model, dc_idx=dc_idx))
        return instances

    @retrying(n=3, sleep_time=1)
    def _provision_instance(self, instance_definition: InstanceDefinition, pricing_model: PricingModel, dc_idx: int):
        return self.provisioners[dc_idx].get_or_create_instance(definition=instance_definition,
                                                                pricing_model=pricing_model)

    def get_node_ips_param(self, public_ip=True):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("get_node_ips_param should not run")

    def node_setup(self, node, verbose=False, timeout=3600):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("node_setup should not run")

    def wait_for_init(self):
        # todo lukasz: why gce cluster didn't have to implement this?
        raise NotImplementedError("wait_for_init should not run")


class ScyllaAzureCluster(cluster.BaseScyllaCluster, AzureCluster):

    def __init__(self, image_id, root_disk_size,  # pylint: disable=too-many-arguments
                 provisioners: List[AzureProvisioner], credentials,
                 instance_type='Standard_L8s_v2',
                 user_name='ubuntu',
                 user_prefix=None, n_nodes=3, params=None, region_names=None):
        # pylint: disable=too-many-locals
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        super().__init__(
            image_id=image_id,
            root_disk_size=root_disk_size,
            instance_type=instance_type,
            user_name=user_name,
            provisioners=provisioners,
            credentials=credentials,
            cluster_prefix=cluster_prefix,
            node_prefix=node_prefix,
            n_nodes=n_nodes,
            params=params,
            region_names=region_names,
            node_type='scylla-db'
        )
        self.version = '2.1'

    @staticmethod
    def _wait_for_preinstalled_scylla(node):
        node.wait_for_machine_image_configured()


class LoaderSetAzure(cluster.BaseLoaderSet, AzureCluster):

    def __init__(self, image_id, root_disk_size, provisioners, credentials,  # pylint: disable=too-many-arguments
                 instance_type='Standard_D2s_v3',
                 user_name='centos',
                 user_prefix=None, n_nodes=1, params=None, region_names=None):
        # pylint: disable=too-many-locals
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')
        cluster.BaseLoaderSet.__init__(self, params=params)
        AzureCluster.__init__(self,
                              image_id=image_id,
                              root_disk_size=root_disk_size,
                              instance_type=instance_type,
                              user_name=user_name,
                              provisioners=provisioners,
                              credentials=credentials,
                              cluster_prefix=cluster_prefix,
                              node_prefix=node_prefix,
                              n_nodes=n_nodes,
                              params=params,
                              node_type='loader',
                              region_names=region_names
                              )


class MonitorSetAzure(cluster.BaseMonitorSet, AzureCluster):

    def __init__(self, image_id, root_disk_size, provisioners, credentials,  # pylint: disable=too-many-arguments
                 instance_type='Standard_D2s_v3',
                 user_name='centos', user_prefix=None, n_nodes=1,
                 targets=None, params=None, region_names=None):
        # pylint: disable=too-many-locals
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')

        targets = targets if targets else {}
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        AzureCluster.__init__(self,
                              image_id=image_id,
                              root_disk_size=root_disk_size,
                              instance_type=instance_type,
                              user_name=user_name,
                              provisioners=provisioners,
                              credentials=credentials,
                              cluster_prefix=cluster_prefix,
                              node_prefix=node_prefix,
                              n_nodes=n_nodes,
                              params=params,
                              node_type='monitor',
                              region_names=region_names
                              )

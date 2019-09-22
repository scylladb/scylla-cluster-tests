import logging
import atexit

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

from sdcm import cluster

LOGGER = logging.getLogger(__name__)


def get_openstack_service(user, password, auth_version, auth_url, service_type, service_name, service_region, tenant):  # pylint: disable=too-many-arguments
    service_cls = get_driver(Provider.OPENSTACK)
    service = service_cls(user, password,
                          ex_force_auth_version=auth_version,
                          ex_force_auth_url=auth_url,
                          ex_force_service_type=service_type,
                          ex_force_service_name=service_name,
                          ex_force_service_region=service_region,
                          ex_tenant_name=tenant)
    return service


def clean_openstack_instance(user, password, auth_version, auth_url, service_type, service_name, service_region,  # pylint: disable=too-many-arguments
                             tenant, instance_name):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        instance = [n for n in service.list_nodes() if n.name == instance_name][0]
        service.destroy_node(instance)
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.error(str(details))


def clean_openstack_credential(user, password, auth_version, auth_url, service_type, service_name, service_region,  # pylint: disable=too-many-arguments
                               tenant, credential_key_name, credential_key_file):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        key_pair = service.get_key_pair(credential_key_name)
        service.delete_key_pair(key_pair)
        cluster.remove_if_exists(credential_key_file)
    except Exception as details:  # pylint: disable=broad-except
        LOGGER.exception(str(details))


class OpenStackNode(cluster.BaseNode):  # pylint: disable=abstract-method

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, openstack_instance, openstack_service, parent_cluster, credentials,  # pylint: disable=too-many-arguments
                 node_prefix='node', node_index=1, openstack_image_username='root',
                 base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = openstack_instance
        self._openstack_service = openstack_service
        self._wait_private_ip()
        ssh_login_info = {'hostname': None,
                          'user': openstack_image_username,
                          'key_file': credentials.key_file,
                          'extra_ssh_options': '-tt'}
        super(OpenStackNode, self).__init__(name=name,
                                            parent_cluster=parent_cluster,
                                            ssh_login_info=ssh_login_info,
                                            base_logdir=base_logdir,
                                            node_prefix=node_prefix)

    def _refresh_instance_state(self):
        node_name = self._instance.name
        instance = [n for n in self._openstack_service.list_nodes() if n.name == node_name][0]
        self._instance = instance
        ip_tuple = (instance.public_ips, instance.private_ips)
        return ip_tuple

    def restart(self):
        self._instance.reboot()

    def destroy(self):
        self.stop_task_threads()
        self._instance.destroy()
        self.log.info('Destroyed')


class OpenStackCluster(cluster.BaseCluster):  # pylint: disable=abstract-method,too-many-instance-attributes

    """
    Cluster of Node objects, started on OpenStack.
    """

    def __init__(self, openstack_image, openstack_network, service, credentials, cluster_uuid=None,  # pylint: disable=too-many-arguments
                 openstack_instance_type='m1.small', openstack_image_username='root',
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        # pylint: disable=too-many-locals
        if credentials.type == 'generated':
            credential_key_name = credentials.key_pair_name
            credential_key_file = credentials.key_file
            user = params.get('openstack_user', None)
            password = params.get('openstack_password', None)
            tenant = params.get('openstack_tenant', None)
            auth_version = params.get('openstack_auth_version', None)
            auth_url = params.get('openstack_auth_url', None)
            service_type = params.get('openstack_service_type', None)
            service_name = params.get('openstack_service_name', None)
            service_region = params.get('openstack_service_region', None)
            if cluster.OPENSTACK_SERVICE is None:
                cluster.OPENSTACK_SERVICE = service
            if params.get('failure_post_behavior') == 'destroy':
                atexit.register(clean_openstack_credential, user,
                                password,
                                tenant,
                                auth_version,
                                auth_url,
                                service_type,
                                service_name,
                                service_region,
                                credential_key_name,
                                credential_key_file)
        cluster.CREDENTIALS.append(credentials)

        self._openstack_image = openstack_image
        self._openstack_network = openstack_network
        self._openstack_service = service
        self._credentials = credentials
        self._openstack_instance_type = openstack_instance_type
        self._openstack_image_username = openstack_image_username
        super(OpenStackCluster, self).__init__(cluster_uuid=cluster_uuid,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)

    def __str__(self):
        return 'Cluster %s (Image: %s Type: %s)' % (self.name,
                                                    self._openstack_image,
                                                    self._openstack_instance_type)

    def add_nodes(self, count, ec2_user_data=''):  # pylint: disable=arguments-differ
        nodes = []
        size = [d for d in self._openstack_service.list_sizes() if d.name == self._openstack_instance_type][0]
        image = self._openstack_service.get_image(self._openstack_image)
        networks = [n for n in self._openstack_service.ex_list_networks() if n.name == self._openstack_network]
        for node_index in range(self._node_index + 1, count + 1):
            name = '%s-%s' % (self.node_prefix, node_index)
            instance = self._openstack_service.create_node(name=name, image=image, size=size, networks=networks,
                                                           ex_keyname=self._credentials.name)
            cluster.OPENSTACK_INSTANCES.append(instance)
            nodes.append(OpenStackNode(openstack_instance=instance, openstack_service=self._openstack_service,
                                       credentials=self._credentials, parent_cluster=self,
                                       openstack_image_username=self._openstack_image_username,
                                       node_prefix=self.node_prefix, node_index=node_index,
                                       base_logdir=self.logdir))

        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes

        return nodes


class ScyllaOpenStackCluster(OpenStackCluster, cluster.BaseScyllaCluster):  # pylint: disable=abstract-method
    def __init__(self, openstack_image, openstack_network, service, credentials,  # pylint: disable=too-many-arguments,unused-argument
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos',
                 user_prefix=None, n_nodes=10, params=None):
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')
        super(ScyllaOpenStackCluster, self).__init__(openstack_image=openstack_image,
                                                     openstack_network=openstack_network,
                                                     openstack_instance_type=openstack_instance_type,
                                                     openstack_image_username=openstack_image_username,
                                                     services=service,
                                                     credentials=credentials,
                                                     cluster_prefix=cluster_prefix,
                                                     node_prefix=node_prefix,
                                                     n_nodes=n_nodes,
                                                     params=params)
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data=''):
        added_nodes = super(ScyllaOpenStackCluster, self).add_nodes(count=count,
                                                                    ec2_user_data=ec2_user_data)
        return added_nodes


class LoaderSetOpenStack(OpenStackCluster, cluster.BaseLoaderSet):  # pylint: disable=abstract-method

    def __init__(self, openstack_image, openstack_network, service, credentials,  # pylint: disable=too-many-arguments
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None):
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')
        super(LoaderSetOpenStack, self).__init__(openstack_image=openstack_image,
                                                 openstack_network=openstack_network,
                                                 openstack_instance_type=openstack_instance_type,
                                                 openstack_image_username=openstack_image_username,
                                                 services=service,
                                                 credentials=credentials,
                                                 cluster_prefix=cluster_prefix,
                                                 node_prefix=node_prefix,
                                                 n_nodes=n_nodes,
                                                 params=params)
        self.scylla_repo = scylla_repo


class MonitorSetOpenStack(cluster.BaseMonitorSet, OpenStackCluster):  # pylint: disable=abstract-method

    def __init__(self, openstack_image, openstack_network, service, credentials,  # pylint: disable=too-many-arguments
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, targets=None, params=None):
        targets = targets if targets else {}
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        OpenStackCluster.__init__(self,
                                  openstack_image=openstack_image,
                                  openstack_network=openstack_network,
                                  openstack_instance_type=openstack_instance_type,
                                  openstack_image_username=openstack_image_username,
                                  service=service,
                                  credentials=credentials,
                                  cluster_prefix=cluster_prefix,
                                  node_prefix=node_prefix,
                                  n_nodes=n_nodes,
                                  params=params)
        self.scylla_repo = scylla_repo

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()

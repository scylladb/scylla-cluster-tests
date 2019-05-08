import re
import logging

import cluster

logger = logging.getLogger(__name__)

BASE_NAME = 'db-node'
LOADER_NAME = 'loader-node'
MONITOR_NAME = 'monitor-node'


class NodeIpsNotConfiguredError(Exception):
    pass


class PhysicalMachineNode(cluster.BaseNode):

    def __init__(self, name, public_ip, private_ip, credentials, base_logdir=None, node_prefix=None):
        ssh_login_info = {'hostname': None,
                          'user': credentials.name,
                          'key_file': credentials.key_file}
        self._public_ip = public_ip
        self._private_ip = private_ip
        super(PhysicalMachineNode, self).__init__(name=name,
                                                  base_logdir=base_logdir,
                                                  ssh_login_info=ssh_login_info,
                                                  node_prefix=node_prefix)
        self.set_hostname()

    @property
    def public_ip_address(self):
        return self._public_ip

    @property
    def private_ip_address(self):
        return self._private_ip

    def set_hostname(self):
        # disabling for now, since doesn't working with Fabric from within docker, and not needed for syclla-cloud,
        # since not using hostname anywhere
        # self.remoter.run('sudo hostnamectl set-hostname {}'.format(self.name))
        pass

    def restart(self):
        self.remoter.run('sudo shutdown -r now', ignore_status=True)

    def reboot(self, hard=True, verify_ssh=True):
        if hard:
            self.remoter.run('sudo shutdown -f', ignore_status=True)
        else:
            self.remoter.run('sudo shutdown -r now', ignore_status=True)
        if verify_ssh:
            self.wait_ssh_up()

    def destroy(self):
        self.stop_task_threads()  # For future implementation of destroy


class PhysicalMachineCluster(cluster.BaseCluster):

    def __init__(self, **kwargs):
        self.nodes = []
        self.credentials = kwargs.get('credentials')
        params = kwargs.get('params')
        n_nodes = kwargs.get('n_nodes')
        self._node_public_ips = kwargs.get('public_ips', None) or []
        self._node_private_ips = kwargs.get('private_ips', None) or []
        node_cnt = n_nodes[0] if isinstance(n_nodes, list) else n_nodes
        if len(self._node_public_ips) < node_cnt or len(self._node_private_ips) < node_cnt:
            raise NodeIpsNotConfiguredError('Physical hosts IPs are not configured!')
        super(PhysicalMachineCluster, self).__init__(node_prefix=kwargs.get('node_prefix'),
                                                     cluster_prefix=kwargs.get('cluster_prefix'),
                                                     n_nodes=n_nodes,
                                                     params=params)

    def _create_node(self, name, public_ip, private_ip):
        return PhysicalMachineNode(name,
                                   public_ip,
                                   private_ip,
                                   credentials=self.credentials[0],
                                   base_logdir=self.logdir,
                                   node_prefix=self.node_prefix)

    def add_nodes(self, count, dc_idx=0):
        for node_index in xrange(count):
            node_name = '%s-%s' % (self.node_prefix, node_index)
            self.nodes.append(self._create_node(node_name,
                                                self._node_public_ips[node_index],
                                                self._node_private_ips[node_index]))

    def destroy(self):
        logger.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()


class ScyllaPhysicalCluster(cluster.BaseScyllaCluster, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix', None)
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        super(ScyllaPhysicalCluster, self).__init__(cluster_prefix=cluster_prefix, node_prefix=node_prefix,
                                                    **kwargs)


class ScyllaCloudPhysicalCluster(ScyllaPhysicalCluster):

    def node_setup(self, node, verbose=False, timeout=3600):
        """
        Configure scylla.yaml on cluster nodes.
        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.
        """
        pass  # self._node_setup(node, verbose)


class LoaderSetPhysical(cluster.BaseLoaderSet, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix', None)
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')

        cluster.BaseLoaderSet.__init__(self, params=kwargs.get('params', None))

        PhysicalMachineCluster.__init__(self, cluster_prefix=cluster_prefix, node_prefix=node_prefix, **kwargs)

    @classmethod
    def _get_node_ips_param(cls, ip_type='public'):
        return cluster.BaseLoaderSet.get_node_ips_param(ip_type)


class MonitorSetPhysical(cluster.BaseMonitorSet, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix', None)
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')

        cluster.BaseMonitorSet.__init__(self,
                                        targets=kwargs["targets"],
                                        params=kwargs["params"])
        PhysicalMachineCluster.__init__(self, cluster_prefix=cluster_prefix, node_prefix=node_prefix, **kwargs)

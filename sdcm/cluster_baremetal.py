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
        self.remoter.run('sudo hostnamectl set-hostname {}'.format(self.name))

    def detect_disks(self):
        """
        Detect local disks
        """
        min_size = 50 * 1024 * 1024 * 1024  # 50gb
        result = self.remoter.run('lsblk -nbo KNAME,SIZE,MOUNTPOINT -s -d')
        lines = [line.split() for line in result.stdout.splitlines()]
        disks = ['/dev/{}'.format(l[0]) for l in lines if l and int(l[1]) > min_size and len(l) == 2]
        assert disks, 'Failed to find disks!'
        return disks

    def restart(self):
        self.remoter.run('sudo reboot -h now', ignore_status=True)

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
            node.destroy(force=True)


class ScyllaPhysicalCluster(PhysicalMachineCluster, cluster.BaseScyllaCluster):

    def __init__(self, **kwargs):
        self._user_prefix = kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX)
        self._node_prefix = '%s-%s' % (self._user_prefix, BASE_NAME)
        super(ScyllaPhysicalCluster, self).__init__(node_prefix=kwargs.get('node_prefix', self._node_prefix),
                                                    **kwargs)

    def node_setup(self, node, verbose=False):
        """
        Configure scylla.yaml on cluster nodes.
        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.
        """
        self._node_setup(node, verbose)


class LoaderSetPhysical(PhysicalMachineCluster, cluster.BaseLoaderSet):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), LOADER_NAME)
        super(LoaderSetPhysical, self).__init__(node_prefix=self._node_prefix, **kwargs)

    @classmethod
    def _get_node_ips_param(cls, ip_type='public'):
        return cluster.BaseLoaderSet.get_node_ips_param(ip_type)


class MonitorSetPhysical(cluster.BaseMonitorSet, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), MONITOR_NAME)
        cluster.BaseMonitorSet.__init__(self,
                                        targets=kwargs["targets"],
                                        params=kwargs["params"])
        PhysicalMachineCluster.__init__(self, node_prefix=self._node_prefix, **kwargs)

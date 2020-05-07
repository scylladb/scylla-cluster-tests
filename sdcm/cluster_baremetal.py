import logging

from sdcm import cluster

LOGGER = logging.getLogger(__name__)

BASE_NAME = 'db-node'
LOADER_NAME = 'loader-node'
MONITOR_NAME = 'monitor-node'


class NodeIpsNotConfiguredError(Exception):
    pass


class PhysicalMachineNode(cluster.BaseNode):
    log = LOGGER

    def __init__(self, name, parent_cluster, public_ip, private_ip, credentials, base_logdir=None, node_prefix=None):  # pylint: disable=too-many-arguments
        ssh_login_info = {'hostname': None,
                          'user': credentials.name,
                          'key_file': credentials.key_file}
        self._public_ip = public_ip
        self._private_ip = private_ip
        super(PhysicalMachineNode, self).__init__(name=name,
                                                  parent_cluster=parent_cluster,
                                                  base_logdir=base_logdir,
                                                  ssh_login_info=ssh_login_info,
                                                  node_prefix=node_prefix)

    def init(self):
        super().init()
        self.set_hostname()
        self.run_startup_script()

    @property
    def public_ip_address(self):
        return self._public_ip

    @property
    def private_ip_address(self):
        return self._private_ip

    def set_hostname(self):
        # disabling for now, since doesn't working with Fabric from within docker, and not needed for scylla-cloud,
        # since not using hostname anywhere
        # self.remoter.run('sudo hostnamectl set-hostname {}'.format(self.name))
        pass

    def detect_disks(self, nvme=True):  # pylint: disable=unused-argument
        """
        Detect local disks
        """
        min_size = 50 * 1024 * 1024 * 1024  # 50gb
        result = self.remoter.run('lsblk -nbo KNAME,SIZE,MOUNTPOINT -s -d')
        lines = [line.split() for line in result.stdout.splitlines()]
        disks = ['/dev/{}'.format(l[0]) for l in lines if l and int(l[1]) > min_size and len(l) == 2]
        assert disks, 'Failed to find disks!'
        return disks

    def reboot(self, hard=True, verify_ssh=True):
        raise NotImplementedError("reboot not implemented")

    def restart(self):
        self.remoter.run('sudo reboot -h now', ignore_status=True)

    def destroy(self):
        self.stop_task_threads()  # For future implementation of destroy
        super().destroy()


class PhysicalMachineCluster(cluster.BaseCluster):  # pylint: disable=abstract-method
    def __init__(self, **kwargs):
        self.nodes = []
        self.credentials = kwargs.pop('credentials')
        n_nodes = kwargs.get('n_nodes')
        self._node_public_ips = kwargs.get('public_ips', None) or []
        self._node_private_ips = kwargs.get('private_ips', None) or []
        node_cnt = n_nodes[0] if isinstance(n_nodes, list) else n_nodes
        if len(self._node_public_ips) < node_cnt or len(self._node_private_ips) < node_cnt:
            raise NodeIpsNotConfiguredError('Physical hosts IPs are not configured!')
        super(PhysicalMachineCluster, self).__init__(**kwargs)

    def _create_node(self, name, public_ip, private_ip):
        node = PhysicalMachineNode(name,
                                   parent_cluster=self,
                                   public_ip=public_ip,
                                   private_ip=private_ip,
                                   credentials=self.credentials[0],
                                   base_logdir=self.logdir,
                                   node_prefix=self.node_prefix)
        node.init()
        return node

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):  # pylint: disable=unused-argument
        for node_index in range(count):
            node_name = '%s-%s' % (self.node_prefix, node_index)
            self.nodes.append(self._create_node(node_name,
                                                self._node_public_ips[node_index],
                                                self._node_private_ips[node_index]))


class ScyllaPhysicalCluster(cluster.BaseScyllaCluster, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.pop('user_prefix')
        kwargs.update(dict(
            cluster_prefix=cluster.prepend_user_prefix(user_prefix, 'db-cluster'),
            node_prefix=cluster.prepend_user_prefix(user_prefix, 'db-node'),
            node_type='scylla-db'
        ))
        super(ScyllaPhysicalCluster, self).__init__(**kwargs)

    def node_setup(self, node, verbose=False, timeout=3600):
        """
        Configure scylla.yaml on cluster nodes.
        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.
        """
        # self._node_setup(node, verbose)


class LoaderSetPhysical(PhysicalMachineCluster, cluster.BaseLoaderSet):  # pylint: disable=abstract-method

    def __init__(self, **kwargs):
        user_prefix = kwargs.pop('user_prefix')
        kwargs.update(dict(
            cluster_prefix=cluster.prepend_user_prefix(user_prefix, 'loader-set'),
            node_prefix=cluster.prepend_user_prefix(user_prefix, 'loader-node'),
            node_type='loader'
        ))
        super(LoaderSetPhysical, self).__init__(**kwargs)

    @classmethod
    def _get_node_ips_param(cls, ip_type='public'):
        return cluster.BaseLoaderSet.get_node_ips_param(ip_type)


class MonitorSetPhysical(cluster.BaseMonitorSet, PhysicalMachineCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.pop('user_prefix')
        kwargs.update(dict(
            cluster_prefix=cluster.prepend_user_prefix(user_prefix, 'monitor-set'),
            node_prefix=cluster.prepend_user_prefix(user_prefix, 'monitor-node'),
            node_type='monitor'
        ))
        cluster.BaseMonitorSet.__init__(self,
                                        targets=kwargs["targets"],
                                        params=kwargs["params"])
        PhysicalMachineCluster.__init__(self, **kwargs)

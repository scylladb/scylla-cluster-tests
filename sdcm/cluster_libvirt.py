import subprocess
import time
import os
import shutil
import uuid
import xml.etree.cElementTree as etree
import atexit

from sdcm import wait
from sdcm.remote import LocalCmdRunner
from sdcm import cluster


LOCALRUNNER = LocalCmdRunner()


class LibvirtNode(cluster.BaseNode):  # pylint: disable=abstract-method

    """
    Wraps a domain object, so that we can also control the it through SSH.
    """

    def __init__(self, domain, hypervisor, parent_cluster, node_prefix='node', node_index=1,  # pylint: disable=too-many-arguments
                 domain_username='root', domain_password='', base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._backing_image = None
        self._domain = domain
        self._hypervisor = hypervisor
        wait.wait_for(self._domain.isActive)
        self._wait_public_ip()
        ssh_login_info = {'hostname': None,
                          'user': domain_username,
                          'password': domain_password}
        super(LibvirtNode, self).__init__(name=name,
                                          parent_cluster=parent_cluster,
                                          ssh_login_info=ssh_login_info,
                                          base_logdir=base_logdir,
                                          node_prefix=node_prefix)

    def _get_public_ip_address(self):
        desc = etree.fromstring(self._domain.XMLDesc(0))
        mac_path = "devices/interface[@type='bridge']/mac"
        node = desc.find(mac_path)
        if node is None:
            return None

        mac = node.get("address")
        if mac is None:
            return None

        mac = mac.lower().strip()
        output = subprocess.Popen(["arp", "-n"],
                                  stdout=subprocess.PIPE).communicate()[0]
        lines = [line.split() for line in output.split("\n")[1:]]
        addresses = [line[0] for line in lines if (line and (line[2] == mac))]
        if addresses:
            # Just return the first address, this is a best effort attempt
            return addresses[0]

        return None

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_public_ip_address()

    def _wait_public_ip(self):
        while self._get_public_ip_address() is None:
            time.sleep(1)

    # Remove after node setup is finished
    def cs_installed(self, cassandra_stress_bin=None):
        if cassandra_stress_bin is None:
            cassandra_stress_bin = '/usr/bin/cassandra-stress'
        return self.file_exists(cassandra_stress_bin)

    def restart(self):
        self._domain.reboot()
        self._wait_public_ip()
        self.log.debug('Got new public IP %s',
                       self.public_ip_address)
        self.remoter.hostname = self.public_ip_address
        self.wait_db_up()

    def destroy(self):
        self.stop_task_threads()
        self._domain.destroy()
        self._domain.undefine()
        cluster.remove_if_exists(self._backing_image)
        self.log.info('Destroyed')


class LibvirtCluster(cluster.BaseCluster):  # pylint: disable=abstract-method

    """
    Cluster of Node objects, started on Libvirt.
    """

    def __init__(self, domain_info, hypervisor, cluster_uuid=None,  # pylint: disable=too-many-arguments
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
        self._domain_info = domain_info
        self._hypervisor = hypervisor
        super(LibvirtCluster, self).__init__(cluster_uuid=cluster_uuid,
                                             cluster_prefix=cluster_prefix,
                                             node_prefix=node_prefix,
                                             n_nodes=n_nodes,
                                             params=params)

    def __str__(self):
        return 'LibvirtCluster %s (Image: %s)' % (self.name,
                                                  os.path.basename(self._domain_info['image']))

    def _set_post_behavior(self, action, obj):
        if "Scylla" in self.__class__.__name__ and self.params.get('post_behavior_db_nodes') == 'destroy':
            atexit.register(action, obj)
        elif "Loader" in self.__class__.__name__ and self.params.get('post_behavior_loader_nodes') == 'destroy':
            atexit.register(action, obj)
        elif "Monitor" in self.__class__.__name__ and self.params.get('post_behavior_monitor_nodes') == 'destroy':
            atexit.register(action, obj)
        else:
            self.log.debug("Post behavior is not set")

    def add_nodes(self, count, **kwargs):  # pylint: disable=unused-argument, arguments-differ
        # pylint: disable=too-many-locals
        nodes = []
        os_type = self._domain_info['os_type']
        os_variant = self._domain_info['os_variant']
        memory = self._domain_info['memory']
        bridge = self._domain_info['bridge']
        uri = self._domain_info['uri']
        cluster.LIBVIRT_URI = uri
        image_parent_dir = os.path.dirname(self._domain_info['image'])
        for index in range(self._node_index, self._node_index + count):
            index += 1
            name = '%s-%s' % (self.node_prefix, index)
            dst_image_basename = '%s.qcow2' % name
            dst_image_path = os.path.join(image_parent_dir, dst_image_basename)
            self._set_post_behavior(cluster.remove_if_exists, dst_image_path)
            self.log.info('Copying %s -> %s',
                          self._domain_info['image'], dst_image_path)
            cluster.LIBVIRT_IMAGES.append(dst_image_path)
            shutil.copyfile(self._domain_info['image'], dst_image_path)
            self._set_post_behavior(cluster.clean_domain, name)
            virt_install_cmd = ('virt-install --connect %s --name %s '
                                '--memory %s --os-type=%s '
                                '--os-variant=%s '
                                '--disk %s,device=disk,bus=virtio '
                                '--network bridge=%s,model=virtio '
                                '--vnc --noautoconsole --import' %
                                (uri, name, memory, os_type, os_variant,
                                 dst_image_path, bridge))
            LOCALRUNNER.run(virt_install_cmd)
            cluster.LIBVIRT_DOMAINS.append(name)
            for domain in self._hypervisor.listAllDomains():
                if domain.name() == name:
                    node = LibvirtNode(hypervisor=self._hypervisor,
                                       domain=domain,
                                       parent_cluster=self,
                                       node_prefix=self.node_prefix,
                                       node_index=index,
                                       domain_username=self._domain_info[
                                           'user'],
                                       domain_password=self._domain_info[
                                           'password'],
                                       base_logdir=self.logdir)
                    node._backing_image = dst_image_path  # pylint: disable=protected-access
                    nodes.append(node)
        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes
        self.write_node_public_ip_file()
        self.write_node_private_ip_file()
        return nodes


class ScyllaLibvirtCluster(LibvirtCluster, cluster.BaseScyllaCluster):  # pylint: disable=abstract-method

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,  # pylint: disable=too-many-arguments
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        super(ScyllaLibvirtCluster, self).__init__(domain_info=domain_info,
                                                   hypervisor=hypervisor,
                                                   cluster_uuid=cluster_uuid,
                                                   cluster_prefix=cluster_prefix,
                                                   node_prefix=node_prefix,
                                                   n_nodes=n_nodes,
                                                   params=params)


class LoaderSetLibvirt(LibvirtCluster, cluster.BaseLoaderSet):  # pylint: disable=abstract-method

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,  # pylint: disable=too-many-arguments
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = cluster.prepend_user_prefix(
            user_prefix, 'loader-node')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')

        super(LoaderSetLibvirt, self).__init__(domain_info=domain_info,
                                               hypervisor=hypervisor,
                                               cluster_uuid=cluster_uuid,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params)


class MonitorSetLibvirt(cluster.BaseMonitorSet, LibvirtCluster):  # pylint: disable=abstract-method

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=1,  # pylint: disable=too-many-arguments
                 params=None, targets=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        LibvirtCluster.__init__(self,
                                domain_info=domain_info,
                                hypervisor=hypervisor,
                                cluster_uuid=cluster_uuid,
                                cluster_prefix=cluster_prefix,
                                node_prefix=node_prefix,
                                n_nodes=n_nodes,
                                params=params)

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()

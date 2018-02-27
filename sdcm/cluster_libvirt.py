import subprocess
import time
import os
import shutil
import threading
import uuid
import Queue
import xml.etree.cElementTree as etree

from avocado.utils import runtime as avocado_runtime
from avocado.utils import process

from . import wait
from .loader import CassandraStressExporterSetup
import cluster


class LibvirtNode(cluster.BaseNode):

    """
    Wraps a domain object, so that we can also control the it through SSH.
    """

    def __init__(self, domain, hypervisor, node_prefix='node', node_index=1,
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
    def db_up(self):
        try:
            result = self.remoter.run('netstat -l | grep :9042',
                                      verbose=False, ignore_status=True)
            return result.exit_status == 0
        except Exception as details:
            self.log.error('Error checking for DB status: %s', details)
            return False

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
        self._domain.destroy()
        self._domain.undefine()
        cluster.remove_if_exists(self._backing_image)
        self.stop_task_threads()
        self.log.info('Destroyed')


class LibvirtCluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on Libvirt.
    """

    def __init__(self, domain_info, hypervisor, cluster_uuid=None,
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

    def add_nodes(self, count, user_data=None):
        del user_data
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
            if self.params.get('failure_post_behavior') == 'destroy':
                avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': cluster.remove_if_exists,
                                                               'args': (dst_image_path,),
                                                               'once': True})
            self.log.info('Copying %s -> %s',
                          self._domain_info['image'], dst_image_path)
            cluster.LIBVIRT_IMAGES.append(dst_image_path)
            shutil.copyfile(self._domain_info['image'], dst_image_path)
            if self.params.get('failure_post_behavior') == 'destroy':
                avocado_runtime.CURRENT_TEST.runner_queue.put({'func_at_exit': clean_domain,
                                                               'args': (name,),
                                                               'once': True})
            virt_install_cmd = ('virt-install --connect %s --name %s '
                                '--memory %s --os-type=%s '
                                '--os-variant=%s '
                                '--disk %s,device=disk,bus=virtio '
                                '--network bridge=%s,model=virtio '
                                '--vnc --noautoconsole --import' %
                                (uri, name, memory, os_type, os_variant,
                                 dst_image_path, bridge))
            process.run(virt_install_cmd)
            cluster.LIBVIRT_DOMAINS.append(name)
            for domain in self._hypervisor.listAllDomains():
                if domain.name() == name:
                    node = LibvirtNode(hypervisor=self._hypervisor,
                                       domain=domain,
                                       node_prefix=self.node_prefix,
                                       node_index=index,
                                       domain_username=self._domain_info[
                                           'user'],
                                       domain_password=self._domain_info[
                                           'password'],
                                       base_logdir=self.logdir)
                    node._backing_image = dst_image_path
                    nodes.append(node)
        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes
        self.write_node_public_ip_file()
        self.write_node_private_ip_file()
        return nodes


class ScyllaLibvirtCluster(LibvirtCluster, cluster.BaseScyllaCluster):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
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
        self.seed_nodes_private_ips = None

    def _node_setup(self, node):
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y "scylla*"')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update -y --skip-broken')
        node.remoter.run('sudo yum install -y rsync tcpdump screen')
        yum_config_path = '/etc/yum.repos.d/scylla.repo'
        node.remoter.run('sudo curl %s -o %s -L' %
                         (self.params.get('scylla_repo'), yum_config_path))
        node.remoter.run('sudo yum install -y {}'.format(node.scylla_pkg()))
        node.config_setup(seed_address=self.get_seed_nodes_by_flag(),
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          append_conf=self.params.get('append_conf'))

        node.remoter.run(
            'sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.remoter.run('sudo systemctl start scylla-server.service')
        node.remoter.run('sudo systemctl start scylla-jmx.service')
        node.remoter.run('sudo iptables -F')

    def wait_for_init(self, node_list=None, verbose=False):
        """
        Configure scylla.yaml on all cluster nodes.

        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.

        :param node_list: List of nodes to watch for init.
        :param verbose: Whether to print extra info while watching for init.
        :return:
        """
        if node_list is None:
            node_list = self.nodes

        queue = Queue.Queue()

        def node_setup(node):
            node.wait_ssh_up(verbose=verbose)
            self._node_setup(node=node)
            node.wait_db_up(verbose=verbose)
            node.remoter.run('sudo yum install -y {}-gdb'.format(node.scylla_pkg()),
                             verbose=verbose, ignore_status=True)
            queue.put(node)
            queue.task_done()

        start_time = time.time()

        # avoid using node.remoter in thread
        for node in node_list:
            node.wait_ssh_up(verbose=verbose)

        # If we setup all nodes in paralel, we might have troubles
        # with nodes not able to contact the seed node.
        # Let's setup the seed node first, then set up the others
        seed_address = self.get_seed_nodes_by_flag()
        seed_address_list = seed_address.split(',')
        for i in seed_address_list:
            node_setup(i)
        for node in node_list:
            if node in seed_address_list:
                continue
            setup_thread = threading.Thread(target=node_setup,
                                            args=(node,))
            setup_thread.daemon = True
            setup_thread.start()

        results = []
        while len(results) != len(node_list):
            try:
                results.append(queue.get(block=True, timeout=5))
                time_elapsed = time.time() - start_time
                self.log.info("(%d/%d) DB nodes ready. Time elapsed: %d s",
                              len(results), len(node_list),
                              int(time_elapsed))
            except Queue.Empty:
                pass

        self.update_db_binary(node_list)
        self.get_seed_nodes()
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))
        if not node_list[0].scylla_version:
            result = node_list[0].remoter.run("scylla --version")
            for node in node_list:
                node.scylla_version = result.stdout

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaLibvirtCluster, self).destroy()


class LoaderSetLibvirt(LibvirtCluster, cluster.BaseLoaderSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=10,
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

    def wait_for_init(self, verbose=False, db_node_address=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in LoaderSetLibvirt')
            node.wait_ssh_up(verbose=verbose)
            yum_config_path = '/etc/yum.repos.d/scylla.repo'
            node.remoter.run('sudo curl %s -o %s -L' %
                             (self.params.get('scylla_repo'), yum_config_path))
            node.remoter.run('sudo yum install -y {}-tools'.format(node.scylla_pkg()))
            node.wait_cs_installed(verbose=verbose)
            node.remoter.run('sudo yum install -y screen')
            if db_node_address is not None:
                node.remoter.run("echo 'export DB_ADDRESS=%s' >> $HOME/.bashrc" %
                                 db_node_address)

            cs_exporter_setup = CassandraStressExporterSetup()
            cs_exporter_setup.install(node)

            queue.put(node)
            queue.task_done()

        start_time = time.time()

        for loader in self.nodes:
            setup_thread = threading.Thread(target=node_setup,
                                            args=(loader,))
            setup_thread.daemon = True
            setup_thread.start()
            time.sleep(30)

        results = []
        while len(results) != len(self.nodes):
            try:
                results.append(queue.get(block=True, timeout=5))
            except Queue.Empty:
                pass
        time_elapsed = time.time() - start_time
        self.log.debug('Setup duration -> %s s', int(time_elapsed))


class MonitorSetLibvirt(LibvirtCluster, cluster.BaseMonitorSet):

    def __init__(self, domain_info, hypervisor, user_prefix, n_nodes=1,
                 params=None):
        cluster_uuid = uuid.uuid4()
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')

        super(MonitorSetLibvirt, self).__init__(domain_info=domain_info,
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


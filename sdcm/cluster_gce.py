import os
import re
import time
import threading
import tempfile
import Queue

import cluster
from . import wait
from .loader import CassandraStressExporterSetup


def _prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = cluster.DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class GCENode(cluster.BaseNode):

    """
    Wraps GCE instances, so that we can also control the instance through SSH.
    """

    def __init__(self, gce_instance, gce_service, credentials,
                 node_prefix='node', node_index=1, gce_image_username='root',
                 base_logdir=None, dc_idx=0):
        name = '%s-%s-%s' % (node_prefix, dc_idx, node_index)
        self._instance = gce_instance
        self._gce_service = gce_service
        self._wait_public_ip()
        ssh_login_info = {'hostname': None,
                          'user': gce_image_username,
                          'key_file': credentials.key_file,
                          'extra_ssh_options': '-tt'}
        super(GCENode, self).__init__(name=name,
                                      ssh_login_info=ssh_login_info,
                                      base_logdir=base_logdir,
                                      node_prefix=node_prefix,
                                      dc_idx=dc_idx)
        if cluster.TEST_DURATION >= 24 * 60:
            self.log.info('Test duration set to %s. '
                          'Tagging node with "keep-alive"',
                          cluster.TEST_DURATION)
            self._instance_wait_safe(self._gce_service.ex_set_node_tags,
                                     self._instance, ['keep-alive'])
        self._instance_wait_safe(self._gce_service.ex_set_node_metadata,
                                 self._instance, {'workspace': cluster.WORKSPACE, 'uname': ' | '.join(os.uname())})

    def _instance_wait_safe(self, instance_method, *args, **kwargs):
        """
        Wrapper around GCE instance methods that is safer to use.

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
            except Exception, details:
                self.log.error('Call to method %s (retries: %s) failed: %s',
                               instance_method, retries, details)
                time.sleep(min((2 ** retries) * 2, threshold))
                retries += 1

        if not ok:
            raise cluster.NodeError('GCE instance %s method call error after '
                                    'exponential backoff wait' % self._instance.id)

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_private_ip_address()

    def _get_public_ip_address(self):
        public_ips, _ = self._refresh_instance_state()
        if public_ips:
            return public_ips[0]
        else:
            return None

    def _get_private_ip_address(self):
        _, private_ips = self._refresh_instance_state()
        if private_ips:
            return private_ips[0]
        else:
            return None

    def _wait_public_ip(self):
        public_ips, _ = self._refresh_instance_state()
        while not public_ips:
            time.sleep(1)
            public_ips, _ = self._refresh_instance_state()

    def _refresh_instance_state(self):
        node_name = self._instance.name
        instance = self._instance_wait_safe(self._gce_service.ex_get_node, node_name)
        self._instance = instance
        ip_tuple = (instance.public_ips, instance.private_ips)
        return ip_tuple

    def restart(self):
        self._instance_wait_safe(self._instance.reboot)

    def destroy(self):
        self._instance_wait_safe(self._instance.destroy)
        self.stop_task_threads()
        self.log.info('Destroyed')


class GCECluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on GCE (Google Compute Engine).
    """

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,
                 cluster_uuid=None, gce_instance_type='n1-standard-1', gce_region_names=['us-east1-b'],
                 gce_n_local_ssd=1, gce_image_username='root', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=[10], params=None, gce_pd_ssd_size=0):

        self._gce_image = gce_image
        self._gce_image_type = gce_image_type
        self._gce_image_size = gce_image_size
        self._gce_network = gce_network
        self._gce_services = services
        self._credentials = credentials
        self._gce_instance_type = gce_instance_type
        self._gce_image_username = gce_image_username
        self._gce_region_names = gce_region_names
        self._gce_n_local_ssd = int(gce_n_local_ssd) if gce_n_local_ssd else 0
        self._gce_pd_ssd_size = int(gce_pd_ssd_size) if gce_pd_ssd_size else 0
        super(GCECluster, self).__init__(cluster_uuid=cluster_uuid,
                                         cluster_prefix=cluster_prefix,
                                         node_prefix=node_prefix,
                                         n_nodes=n_nodes,
                                         params=params,
                                         # services=services,
                                         region_names=gce_region_names)

    def __str__(self):
        identifier = 'GCE Cluster %s | ' % self.name
        identifier += 'Image: %s | ' % os.path.basename(self._gce_image)
        identifier += 'Root Disk: %s %s GB | ' % (self._gce_image_type, self._gce_image_size)
        if self._gce_n_local_ssd:
            identifier += 'Local SSD: %s | ' % self._gce_n_local_ssd
        if self._gce_pd_ssd_size:
            identifier += 'PD SSD: %s | ' % self._gce_pd_ssd_size
        identifier += 'Type: %s' % self._gce_instance_type
        return identifier

    def _get_disk_url(self, disk_type='pd-standard', dc_idx=0):
        project = self._gce_services[dc_idx].ex_get_project()
        return "projects/%s/zones/%s/diskTypes/%s" % (project.name, self._gce_region_names[dc_idx], disk_type)

    def _get_root_disk_struct(self, name, disk_type='pd-standard', dc_idx=0):
        device_name = '%s-root-%s' % (name, disk_type)
        return {"type": "PERSISTENT",
                "deviceName": device_name,
                "initializeParams": {
                    # diskName parameter has a limit of 62 chars, comment it to use system allocated name
                    # "diskName": device_name,
                    "diskType": self._get_disk_url(disk_type, dc_idx=dc_idx),
                    "diskSizeGb": self._gce_image_size,
                    "sourceImage": self._gce_image
                },
                "boot": True,
                "autoDelete": True}

    def _get_scratch_disk_struct(self, name, index, dc_idx=0):
        device_name = '%s-data-local-ssd-%s' % (name, index)
        return {"type": "SCRATCH",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url('local-ssd', dc_idx=dc_idx),
                },
                "interface": 'NVME',
                "autoDelete": True}

    def _get_pdssd_disk_struct(self, name, disk_type='pd-ssd', dc_idx=0):
        device_name = '%s-data-%s' % (name, disk_type)
        return {"type": "SCRATCH",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url(disk_type, dc_idx=dc_idx),
                    "diskSizeGb": self._gce_pd_ssd_size,
                    "sourceImage": self._gce_image
                },
                "autoDelete": True}

    def add_nodes(self, count, ec2_user_data='', dc_idx=0):
        nodes = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            name = "%s-%s-%s" % (self.node_prefix, dc_idx, node_index)
            gce_disk_struct = list()
            gce_disk_struct.append(self._get_root_disk_struct(name=name,
                                                              disk_type=self._gce_image_type,
                                                              dc_idx=dc_idx))
            for i in range(self._gce_n_local_ssd):
                gce_disk_struct.append(self._get_scratch_disk_struct(name=name, index=i, dc_idx=dc_idx))
            if self._gce_pd_ssd_size:
                gce_disk_struct.append(self._get_pdssd_disk_struct(name=name, dc_idx=dc_idx))
            self.log.info(gce_disk_struct)
            # Name must start with a lowercase letter followed by up to 63
            # lowercase letters, numbers, or hyphens, and cannot end with a hyphen
            assert len(name) <= 63, "Max length of instance name is 63"
            instance = self._gce_services[dc_idx].create_node(name=name,
                                                              size=self._gce_instance_type,
                                                              image=self._gce_image,
                                                              ex_network=self._gce_network,
                                                              ex_disks_gce_struct=gce_disk_struct)
            self.log.info('Created instance %s', instance)
            cluster.GCE_INSTANCES.append(instance)
            try:
                n = GCENode(gce_instance=instance,
                            gce_service=self._gce_services[dc_idx],
                            credentials=self._credentials[0],
                            gce_image_username=self._gce_image_username,
                            node_prefix=self.node_prefix,
                            node_index=node_index,
                            base_logdir=self.logdir,
                            dc_idx=dc_idx)
                nodes.append(n)
            except Exception as ex:
                self.log.exception('Failed to create node: %s', ex)
            else:
                self._node_index += 1
                self.nodes += [n]

                local_nodes = [n for n in self.nodes if n.dc_idx == dc_idx]
                if len(local_nodes) > len(nodes):
                    n.is_addition = True

        assert len(nodes) == count, 'Fail to create {} instances'.format(count)
        self.log.info('added nodes: %s', nodes)

        return nodes


class ScyllaGCECluster(GCECluster, cluster.BaseScyllaCluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=[10], params=None, gce_datacenter=None, gce_pd_ssd_size=0):
        # We have to pass the cluster name in advance in user_data
        cluster_prefix = _prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = _prepend_user_prefix(user_prefix, 'db-node')
        super(ScyllaGCECluster, self).__init__(gce_image=gce_image,
                                               gce_image_type=gce_image_type,
                                               gce_image_size=gce_image_size,
                                               gce_n_local_ssd=gce_n_local_ssd,
                                               gce_network=gce_network,
                                               gce_instance_type=gce_instance_type,
                                               gce_image_username=gce_image_username,
                                               services=services,
                                               credentials=credentials,
                                               cluster_prefix=cluster_prefix,
                                               node_prefix=node_prefix,
                                               n_nodes=n_nodes,
                                               params=params,
                                               gce_region_names=gce_datacenter,
                                               gce_pd_ssd_size=gce_pd_ssd_size)
        self.nemesis = []
        self.nemesis_threads = []
        self.termination_event = threading.Event()
        self.seed_nodes_private_ips = None
        self.version = '2.1'
        self._seed_node_rebooted = False

    def add_nodes(self, count, ec2_user_data='', dc_idx=0):
        added_nodes = super(ScyllaGCECluster, self).add_nodes(count=count,
                                                              ec2_user_data=ec2_user_data,
                                                              dc_idx=dc_idx)
        return added_nodes

    def _node_setup(self, node):
        node.remoter.run('sudo systemctl stop scylla-server.service', ignore_status=True)
        yaml_dst_path = os.path.join(tempfile.mkdtemp(prefix='scylla-longevity'),
                                     'scylla.yaml')
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y "scylla*"')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        result = node.remoter.run('ls /etc/yum.repos.d/epel.repo', ignore_status=True)
        if result.exit_status == 0:
            node.remoter.run('sudo yum update -y --skip-broken --disablerepo=epel')
        else:
            node.remoter.run('sudo yum update -y --skip-broken')
        node.remoter.run('sudo yum install -y rsync tcpdump screen wget net-tools')
        yum_config_path = '/etc/yum.repos.d/scylla.repo'
        node.remoter.run('sudo curl %s -o %s' %
                         (self.params.get('scylla_repo'), yum_config_path))
        node.remoter.run('sudo yum install -y {}'.format(node.scylla_pkg()))

        endpoint_snitch = ''
        if len(self.datacenter) > 1:
            endpoint_snitch = "GossipingPropertyFileSnitch"
            node.datacenter_setup(self.datacenter)
        authenticator = self.params.get('authenticator')
        seed_address = self.get_seed_nodes_by_flag()
        seed_address_list = seed_address.split(',')
        node.config_setup(seed_address=seed_address,
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          endpoint_snitch=endpoint_snitch,
                          authenticator=authenticator,
                          server_encrypt=self._param_enabled('server_encrypt'),
                          client_encrypt=self._param_enabled('client_encrypt'),
                          append_conf=self.params.get('append_conf'))

        if self._gce_n_local_ssd:
            # detect local-ssd disks
            result = node.remoter.run('ls /dev/nvme0n*')
            disks_str = ",".join(re.findall('/dev/nvme0n\w+', result.stdout))
        if self._gce_pd_ssd_size:
            # detect pd-ssd disks
            result = node.remoter.run('ls /dev/sd[b-z]')
            disks_str = ",".join(re.findall('/dev/sd\w+', result.stdout))
        assert disks_str != ""
        node.remoter.run('sudo /usr/lib/scylla/scylla_setup --nic eth0 --disks {}'.format(disks_str))
        node.remoter.run('sudo sync')
        self.log.info('io.conf right after setup')
        node.remoter.run('sudo cat /etc/scylla.d/io.conf')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.remoter.run('sudo sync')
        node.remoter.run('sudo rm -rf /var/lib/scylla/commitlog/*')
        node.remoter.run('sudo rm -rf /var/lib/scylla/data/*')

        if node.private_ip_address not in seed_address_list:
            wait.wait_for(func=lambda: self._seed_node_rebooted is True,
                          step=30,
                          text='Wait for seed node to be up after reboot')
        node.restart()
        node.wait_ssh_up()
        if node.private_ip_address in seed_address_list:
            self.log.info('Seed node is up after reboot')
            self._seed_node_rebooted = True

        self.log.info('io.conf right after reboot')
        node.remoter.run('sudo cat /etc/scylla.d/io.conf')
        node.wait_db_up()
        node.wait_jmx_up()

    def wait_for_init(self, node_list=None, verbose=False, timeout=None):
        """
        Configure scylla.yaml on all cluster nodes.

        We have to modify scylla.yaml on our own because we are not on AWS,
        where there are auto config scripts in place.

        :param node_list: List of nodes to watch for init.
        :param verbose: Whether to print extra info while watching for init.
        :param timeout: timeout in minutes to wait for init to be finished
        :return:
        """
        if node_list is None:
            node_list = self.nodes
        res = self._wait_for_node_setup(node_list, verbose, timeout)
        assert res is True, 'Node setup has failed!'

        if not node_list[0].scylla_version:
            result = node_list[0].remoter.run("rpm -q {}".format(node_list[0].scylla_pkg()))
            match = re.findall("scylla-(.*).el7.centos", result.stdout)
            node_list[0].scylla_version = match[0] if match else ''
            for node in node_list:
                node.scylla_version = result.stdout

        for node in node_list:
            dst_nodes = [n for n in node_list if n.dc_idx != node.dc_idx]
            local_nodes = [n for n in node_list if n.dc_idx == node.dc_idx and n != node]
            if self._param_enabled('enable_tc'):
                self.set_tc(node, dst_nodes, local_nodes)

    def destroy(self):
        self.stop_nemesis()
        super(ScyllaGCECluster, self).destroy()


class LoaderSetGCE(GCECluster, cluster.BaseLoaderSet):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None, gce_pd_ssd_size=0):
        node_prefix = _prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'loader-set')
        super(LoaderSetGCE, self).__init__(gce_image=gce_image,
                                           gce_network=gce_network,
                                           gce_image_type=gce_image_type,
                                           gce_image_size=gce_image_size,
                                           gce_n_local_ssd=gce_n_local_ssd,
                                           gce_instance_type=gce_instance_type,
                                           gce_image_username=gce_image_username,
                                           services=service,
                                           credentials=credentials,
                                           cluster_prefix=cluster_prefix,
                                           node_prefix=node_prefix,
                                           n_nodes=n_nodes,
                                           params=params,
                                           gce_pd_ssd_size=gce_pd_ssd_size)
        self.scylla_repo = scylla_repo

    def wait_for_init(self, verbose=False, db_node_address=None):
        queue = Queue.Queue()

        def node_setup(node):
            self.log.info('Setup in LoaderSetGCE')
            node.wait_ssh_up(verbose=verbose)
            yum_config_path = '/etc/yum.repos.d/scylla.repo'
            node.remoter.run('sudo curl %s -o %s' %
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


class MonitorSetGCE(GCECluster, cluster.BaseMonitorSet):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, params=None, gce_pd_ssd_size=0):
        node_prefix = _prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'monitor-set')
        super(MonitorSetGCE, self).__init__(gce_image=gce_image,
                                            gce_image_type=gce_image_type,
                                            gce_image_size=gce_image_size,
                                            gce_n_local_ssd=gce_n_local_ssd,
                                            gce_network=gce_network,
                                            gce_instance_type=gce_instance_type,
                                            gce_image_username=gce_image_username,
                                            services=service,
                                            credentials=credentials,
                                            cluster_prefix=cluster_prefix,
                                            node_prefix=node_prefix,
                                            n_nodes=n_nodes,
                                            params=params,
                                            gce_pd_ssd_size=gce_pd_ssd_size)
        self.scylla_repo = scylla_repo

    def destroy(self):
        self.log.info('Destroy nodes')
        for node in self.nodes:
            node.destroy()

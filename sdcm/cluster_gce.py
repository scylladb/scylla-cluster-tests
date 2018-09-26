import os
import time
import cluster


def _prepend_user_prefix(user_prefix, base_name):
    if not user_prefix:
        user_prefix = cluster.DEFAULT_USER_PREFIX
    return '%s-%s' % (user_prefix, base_name)


class CreateGCENodeError(Exception):
    pass


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
        # When using local_ssd disks in GCE, there is no option to Stop and Start an instance.
        # So, for now we will keep restart the same as hard reboot.
        self._instance_wait_safe(self._instance.reboot)

    def reboot(self, hard=True):
        if hard:
            self.log.debug('Hardly rebooting node')
            self._instance_wait_safe(self._instance.reboot)
        else:
            self.log.debug('Softly rebooting node')
            self.remoter.run('sudo reboot')

    def destroy(self):
        self.stop_task_threads()
        self._instance_wait_safe(self._instance.destroy)
        self.log.info('Destroyed')


class GCECluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on GCE (Google Compute Engine).
    """

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,
                 cluster_uuid=None, gce_instance_type='n1-standard-1', gce_region_names=['us-east1-b'],
                 gce_n_local_ssd=1, gce_image_username='root', cluster_prefix='cluster',
                 node_prefix='node', n_nodes=[10], add_disks=None, params=None):

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
        self._add_disks = add_disks
        # the full node prefix will contain unique uuid, so use this for search of existing nodes
        self._node_prefix = node_prefix
        super(GCECluster, self).__init__(cluster_uuid=cluster_uuid,
                                         cluster_prefix=cluster_prefix,
                                         node_prefix=node_prefix,
                                         n_nodes=n_nodes,
                                         params=params,
                                         # services=services,
                                         region_names=gce_region_names)
        self.log.debug("GCECluster constructor")

    def __str__(self):
        identifier = 'GCE Cluster %s | ' % self.name
        identifier += 'Image: %s | ' % os.path.basename(self._gce_image)
        identifier += 'Root Disk: %s %s GB | ' % (self._gce_image_type, self._gce_image_size)
        if self._gce_n_local_ssd:
            identifier += 'Local SSD: %s | ' % self._gce_n_local_ssd
        if self._add_disks:
            for disk_type, disk_size in self._add_disks.iteritems():
                if int(disk_size):
                    identifier += '%s: %s | ' % (disk_type, disk_size)
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

    def _get_local_ssd_disk_struct(self, name, index, interface='NVME', dc_idx=0):
        device_name = '%s-data-local-ssd-%s' % (name, index)
        return {"type": "SCRATCH",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url('local-ssd', dc_idx=dc_idx),
                },
                "interface": interface,
                "autoDelete": True}

    def _get_persistent_disk_struct(self, name, disk_size, disk_type='pd-ssd', dc_idx=0):
        device_name = '%s-data-%s' % (name, disk_type)
        return {"type": "SCRATCH",
                "deviceName": device_name,
                "initializeParams": {
                    "diskType": self._get_disk_url(disk_type, dc_idx=dc_idx),
                    "diskSizeGb": disk_size,
                    "sourceImage": self._gce_image
                },
                "autoDelete": True}

    def _create_instance(self, node_index, dc_idx):
        name = "%s-%s-%s" % (self.node_prefix, dc_idx, node_index)
        gce_disk_struct = list()
        gce_disk_struct.append(self._get_root_disk_struct(name=name,
                                                          disk_type=self._gce_image_type,
                                                          dc_idx=dc_idx))
        for i in range(self._gce_n_local_ssd):
            gce_disk_struct.append(self._get_local_ssd_disk_struct(name=name, index=i, dc_idx=dc_idx))
        if self._add_disks:
            for disk_type, disk_size in self._add_disks.iteritems():
                disk_size = int(disk_size)
                if disk_size:
                    gce_disk_struct.append(self._get_persistent_disk_struct(name=name, disk_size=disk_size,
                                                                            disk_type=disk_type, dc_idx=dc_idx))
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
        return instance

    def _create_instances(self, count, dc_idx=0):
        instances = []
        for node_index in range(self._node_index + 1, self._node_index + count + 1):
            instances.append(self._create_instance(node_index, dc_idx))
        return instances

    def _get_instances_by_prefix(self, dc_idx=0):
        instances_by_zone = self._gce_services[dc_idx].list_nodes(ex_zone=self._gce_region_names[dc_idx])
        return [node for node in instances_by_zone if node.name.startswith(self._node_prefix)]

    def _get_instances(self):
        instances = self._get_instances_by_prefix()
        ips = self._node_public_ips or self._node_private_ips
        attr_name = 'public_ips' if self._node_public_ips else 'private_ips'
        return [node for node in instances if getattr(node, attr_name)[0] in ips]

    def _create_node(self, instance, node_index, dc_idx):
        try:
            return GCENode(gce_instance=instance,
                           gce_service=self._gce_services[dc_idx],
                           credentials=self._credentials[0],
                           gce_image_username=self._gce_image_username,
                           node_prefix=self.node_prefix,
                           node_index=node_index,
                           base_logdir=self.logdir,
                           dc_idx=dc_idx)
        except Exception as ex:
            raise CreateGCENodeError('Failed to create node: %s', ex)

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        self.log.info("Adding nodes to cluster")
        nodes = []
        if cluster.Setup.REUSE_CLUSTER:
            instances = self._get_instances()
        else:
            instances = self._create_instances(count, dc_idx)

        self.log.debug('instances: %s', instances)
        self.log.debug('GCE instance extra info: %s', instances[0].extra)
        for ind, instance in enumerate(instances):
            node_index = ind + self._node_index + 1
            node = self._create_node(instance, node_index, dc_idx)
            nodes.append(node)
            self.nodes.append(node)
            self.log.info("Added node: %s", node.name)
            node.enable_auto_bootstrap = enable_auto_bootstrap

        self._node_index += count
        self.log.info('added nodes: %s', nodes)
        return nodes


class ScyllaGCECluster(cluster.BaseScyllaCluster, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, services, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos',
                 user_prefix=None, n_nodes=[10], add_disks=None, params=None, gce_datacenter=None):
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
                                               add_disks=add_disks,
                                               params=params,
                                               gce_region_names=gce_datacenter)
        self.version = '2.1'


class LoaderSetGCE(cluster.BaseLoaderSet, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos',
                 user_prefix=None, n_nodes=10, add_disks=None, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'loader-set')
        cluster.BaseLoaderSet.__init__(self,
                                       params=params)
        GCECluster.__init__(self,
                            gce_image=gce_image,
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
                            add_disks=add_disks,
                            params=params)


class MonitorSetGCE(cluster.BaseMonitorSet, GCECluster):

    def __init__(self, gce_image, gce_image_type, gce_image_size, gce_network, service, credentials,
                 gce_instance_type='n1-standard-1', gce_n_local_ssd=1,
                 gce_image_username='centos', user_prefix=None, n_nodes=[1],
                 targets={}, add_disks=None, params=None):
        node_prefix = _prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = _prepend_user_prefix(user_prefix, 'monitor-set')
        cluster.BaseMonitorSet.__init__(self,
                                        targets=targets,
                                        params=params)
        GCECluster.__init__(self,
                            gce_image=gce_image,
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
                            add_disks=add_disks,
                            params=params)

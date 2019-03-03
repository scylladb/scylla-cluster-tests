import time
import logging
import threading
import Queue
import atexit

from libcloud.compute.types import Provider
from libcloud.compute.providers import get_driver

import cluster

loggger = logging.getLogger(__name__)


def get_openstack_service(user, password, auth_version, auth_url, service_type, service_name, service_region, tenant):
    service_cls = get_driver(Provider.OPENSTACK)
    service = service_cls(user, password,
                          ex_force_auth_version=auth_version,
                          ex_force_auth_url=auth_url,
                          ex_force_service_type=service_type,
                          ex_force_service_name=service_name,
                          ex_force_service_region=service_region,
                          ex_tenant_name=tenant)
    return service


def clean_openstack_instance(user, password, auth_version, auth_url, service_type, service_name, service_region,
                             tenant, instance_name):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        instance = [n for n in service.list_nodes() if n.name == instance_name][0]
        service.destroy_node(instance)
    except Exception as details:
        loggger.error(str(details))


def clean_openstack_credential(user, password, auth_version, auth_url, service_type, service_name, service_region,
                               tenant, credential_key_name, credential_key_file):
    try:
        service = get_openstack_service(user, password, auth_version, auth_url, service_type, service_name,
                                        service_region, tenant)
        key_pair = service.get_key_pair(credential_key_name)
        service.delete_key_pair(key_pair)
        cluster.remove_if_exists(credential_key_file)
    except Exception as details:
        loggger.error(str(details))


class OpenStackNode(cluster.BaseNode):

    """
    Wraps EC2.Instance, so that we can also control the instance through SSH.
    """

    def __init__(self, openstack_instance, openstack_service, credentials,
                 node_prefix='node', node_index=1, openstack_image_username='root',
                 base_logdir=None):
        name = '%s-%s' % (node_prefix, node_index)
        self._instance = openstack_instance
        self._openstack_service = openstack_service
        self._wait_private_ip()
        ssh_login_info = {'hostname': None,
                          'user': openstack_image_username,
                          'key_file': credentials.key_file,
                          'wait_key_installed': 30,
                          'extra_ssh_options': '-tt'}
        super(OpenStackNode, self).__init__(name=name,
                                            ssh_login_info=ssh_login_info,
                                            base_logdir=base_logdir,
                                            node_prefix=node_prefix)

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

    def _wait_private_ip(self):
        _, private_ips = self._refresh_instance_state()
        while not private_ips:
            time.sleep(1)
            _, private_ips = self._refresh_instance_state()

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


class OpenStackCluster(cluster.BaseCluster):

    """
    Cluster of Node objects, started on OpenStack.
    """

    def __init__(self, openstack_image, openstack_network, service, credentials, cluster_uuid=None,
                 openstack_instance_type='m1.small', openstack_image_username='root',
                 cluster_prefix='cluster',
                 node_prefix='node', n_nodes=10, params=None):
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

    def add_nodes(self, count, ec2_user_data=''):
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
                                       credentials=self._credentials,
                                       openstack_image_username=self._openstack_image_username,
                                       node_prefix=self.node_prefix, node_index=node_index,
                                       base_logdir=self.logdir))

        self.log.info('added nodes: %s', nodes)
        self._node_index += len(nodes)
        self.nodes += nodes

        return nodes


class ScyllaOpenStackCluster(OpenStackCluster, cluster.BaseScyllaCluster):
    def __init__(self, openstack_image, openstack_network, service, credentials,
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
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
        self.seed_nodes_ips = None
        self.version = '2.1'

    def add_nodes(self, count, ec2_user_data=''):
        added_nodes = super(ScyllaOpenStackCluster, self).add_nodes(count=count,
                                                                    ec2_user_data=ec2_user_data)
        return added_nodes

    def _node_setup(self, node):
        # Sometimes people might set up base images with
        # previous versions of scylla installed (they shouldn't).
        # But anyway, let's cover our bases as much as possible.
        node.remoter.run('sudo yum remove -y "scylla*"')
        node.remoter.run('sudo yum remove -y abrt')
        # Let's re-create the yum database upon update
        node.remoter.run('sudo yum clean all')
        node.remoter.run('sudo yum update -y --skip-broken')
        node.remoter.run('sudo yum install -y rsync tcpdump screen wget')
        node.download_scylla_repo(self.params.get('scylla_repo'))
        node.remoter.run('sudo yum install -y {}'.format(node.scylla_pkg()))
        node.config_setup(seed_address=self.get_seed_nodes_by_flag(),
                          cluster_name=self.name,
                          enable_exp=self._param_enabled('experimental'),
                          append_conf=self.params.get('append_conf'),
                          hinted_handoff_disabled=self._param_enabled('hinted_handoff_disabled'))

        node.remoter.run('sudo /usr/lib/scylla/scylla_setup --nic eth0 --no-raid-setup')
        # Work around a systemd bug in RHEL 7.3 -> https://github.com/scylladb/scylla/issues/1846
        node.remoter.run('sudo sh -c "sed -i s/OnBootSec=0/OnBootSec=3/g /usr/lib/systemd/system/scylla-housekeeping.timer"')
        node.remoter.run('sudo cat /usr/lib/systemd/system/scylla-housekeeping.timer')
        node.remoter.run('sudo systemctl daemon-reload')
        node.remoter.run('sudo systemctl enable scylla-server.service')
        node.remoter.run('sudo systemctl enable scylla-jmx.service')
        node.restart()
        node.wait_ssh_up()
        node.wait_db_up()
        node.wait_jmx_up()

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
        super(ScyllaOpenStackCluster, self).destroy()


class LoaderSetOpenStack(OpenStackCluster, cluster.BaseLoaderSet):

    def __init__(self, openstack_image, openstack_network, service, credentials,
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


class MonitorSetOpenStack(cluster.BaseMonitorSet, OpenStackCluster):

    def __init__(self, openstack_image, openstack_network, service, credentials,
                 openstack_instance_type='m1.small',
                 openstack_image_username='centos', scylla_repo=None,
                 user_prefix=None, n_nodes=10, targets={}, params=None):
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

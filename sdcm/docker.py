import re
import json
import time
import logging
import os
import types

from sdcm import cluster
from sdcm.utils.common import retrying
from sdcm.remote import LocalCmdRunner
from sdcm.log import SDCMAdapter
from sdcm.utils.common import makedirs

LOGGER = logging.getLogger(__name__)
LOCALRUNNER = LocalCmdRunner()


class DockerCommandError(Exception):
    pass


class DockerContainerNotExists(Exception):
    pass


class DockerContainerNotRunning(Exception):
    pass


class CannotFindContainers(Exception):
    pass


def _cmd(cmd, timeout=10):
    res = LOCALRUNNER.run('docker {}'.format(cmd), ignore_status=True, timeout=timeout)
    if res.exit_status:
        if 'No such container:' in res.stderr:
            raise DockerContainerNotExists(res.stderr)
        raise DockerCommandError('command: {}, error: {}, output: {}'.format(cmd, res.stderr, res.stdout))
    return res


class DockerNode(cluster.BaseNode):  # pylint: disable=abstract-method

    def __init__(self, name, credentials, parent_cluster, base_logdir=None, node_prefix=None):  # pylint: disable=too-many-arguments
        ssh_login_info = {'hostname': None,
                          'user': 'scylla-test',
                          'key_file': credentials.key_file}
        self._public_ip_address = None
        super(DockerNode, self).__init__(name=name,
                                         parent_cluster=parent_cluster,
                                         base_logdir=base_logdir,
                                         ssh_login_info=ssh_login_info,
                                         node_prefix=node_prefix)
        self.wait_for_status_running()
        self.wait_public_ip()

    def _get_public_ip_address(self):
        if not self._public_ip_address:
            out = _cmd("inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {}".format(self.name)).stdout
            self._public_ip_address = out.strip()
        return self._public_ip_address

    def is_running(self):
        out = _cmd("inspect --format='{{{{json .State.Running}}}}' {}".format(self.name)).stdout
        return json.loads(out)

    @retrying(n=10, sleep_time=2, allowed_exceptions=(DockerContainerNotRunning,))
    def wait_for_status_running(self):
        if not self.is_running():
            raise DockerContainerNotRunning(self.name)

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_public_ip_address()

    def wait_public_ip(self):
        while not self._public_ip_address:
            self._get_public_ip_address()
            time.sleep(1)

    def start(self, timeout=30):
        _cmd('start {}'.format(self.name), timeout=timeout)

    def restart(self, timeout=30):  # pylint: disable=arguments-differ
        _cmd('restart {}'.format(self.name), timeout=timeout)

    def stop(self, timeout=30):
        _cmd('stop {}'.format(self.name), timeout=timeout)

    def destroy(self, force=True):  # pylint: disable=arguments-differ
        force_param = '-f' if force else ''
        _cmd('rm {} -v {}'.format(force_param, self.name))

    def start_scylla_server(self, verify_up=True, verify_down=False, timeout=300):
        if verify_down:
            self.wait_db_down(timeout=timeout)
        self.remoter.run('supervisorctl start scylla', timeout=timeout)
        if verify_up:
            self.wait_db_up(timeout=timeout)

    @cluster.log_run_info
    def start_scylla(self, verify_up=True, verify_down=False, timeout=300):
        self.start_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        if verify_up:
            self.wait_jmx_up(timeout=timeout)

    def stop_scylla_server(self, verify_up=False, verify_down=True, timeout=300, ignore_status=False):
        if verify_up:
            self.wait_db_up(timeout=timeout)
        self.remoter.run('supervisorctl stop scylla', timeout=timeout)
        if verify_down:
            self.wait_db_down(timeout=timeout)

    @cluster.log_run_info
    def stop_scylla(self, verify_up=False, verify_down=True, timeout=300):
        self.stop_scylla_server(verify_up=verify_up, verify_down=verify_down, timeout=timeout)
        if verify_down:
            self.wait_jmx_down(timeout=timeout)


class DockerCluster(cluster.BaseCluster):  # pylint: disable=abstract-method

    def __init__(self, **kwargs):
        self._image = kwargs.get('docker_image', 'scylladb/scylla-nightly')
        self._version_tag = kwargs.get('docker_image_tag', 'latest')
        self.nodes = []
        self.credentials = kwargs.get('credentials')
        self._node_prefix = kwargs.get('node_prefix')
        self._node_img_tag = 'scylla-sct-img'
        self._context_path = os.path.join(os.path.dirname(__file__), '../docker/scylla-sct')
        self._create_node_image()
        super(DockerCluster, self).__init__(node_prefix=self._node_prefix,
                                            n_nodes=kwargs.get('n_nodes'),
                                            params=kwargs.get('params'),
                                            region_names=["localhost-dc"])  # no multi dc currently supported

    def _create_node_image(self):
        self._update_image()
        _cmd(f'build --build-arg SOURCE_IMAGE={self._image}:{self._version_tag} -t {self._node_img_tag} {self._context_path}',
             timeout=300)

    @staticmethod
    def _clean_old_images():
        images = " ".join(_cmd('images -f "dangling=true" -q').stdout.split('\n'))
        if images:
            _cmd(f'rmi {images}', timeout=90)

    def _update_image(self):
        LOGGER.debug('update scylla image')
        _cmd(f'pull {self._image}:{self._version_tag}', timeout=300)
        self._clean_old_images()

    def _create_container(self, node_name, is_seed=False, seed_ip=None):
        labels = f"--label 'test_id={cluster.Setup.test_id()}'"
        cmd = f'run --cpus="1" --name {node_name} {labels} -d {self._node_img_tag}'
        if not is_seed and seed_ip:
            cmd = f'{cmd} --seeds="{seed_ip}"'
        _cmd(cmd, timeout=30)

        # remove the message of the day
        _cmd(f"""exec {node_name} bash -c "sed  '/\\/dev\\/stderr/d' /etc/bashrc -i" """)

    def _get_containers_by_prefix(self):
        c_ids = _cmd('container ls -a -q --filter name={}'.format(self._node_prefix)).stdout
        if not c_ids:
            raise CannotFindContainers('name prefix: %s' % self._node_prefix)
        return [_ for _ in c_ids.split('\n') if _]

    @staticmethod
    def _get_connainer_name_by_id(c_id):
        return json.loads(_cmd("inspect --format='{{{{json .Name}}}}' {}".format(c_id)).stdout).lstrip('/')

    def _create_node(self, node_name):
        return DockerNode(node_name,
                          credentials=self.credentials[0],
                          parent_cluster=self,
                          base_logdir=self.logdir,
                          node_prefix=self.node_prefix)

    def _get_node_name_and_index(self):
        """Is important when node is added to replace some dead node"""
        node_names = [node.name for node in self.nodes]
        node_index = 0
        while True:
            node_name = '%s-%s' % (self.node_prefix, node_index)
            if node_name not in node_names:
                return node_name, node_index
            node_index += 1

    def _create_nodes(self, count, dc_idx=0, enable_auto_bootstrap=False):  # pylint: disable=unused-argument
        """
        Create nodes from docker containers
        :param count: count of nodes to create
        :param dc_idx: datacenter index
        :return: list of DockerNode objects
        """
        new_nodes = []
        for _ in range(count):
            node_name, node_index = self._get_node_name_and_index()
            is_seed = (node_index == 0)
            seed_ip = self.nodes[0].public_ip_address if not is_seed else None
            self._create_container(node_name, is_seed, seed_ip)
            new_node = self._create_node(node_name)
            new_node.enable_auto_bootstrap = enable_auto_bootstrap
            self.nodes.append(new_node)
            new_nodes.append(new_node)
        return new_nodes

    def _get_nodes(self):
        """
        Find the existing containers by node name prefix
        and create nodes from it.
        :return: list of DockerNode objects
        """
        c_ids = self._get_containers_by_prefix()
        for c_id in c_ids:
            node_name = self._get_connainer_name_by_id(c_id)
            LOGGER.debug('Node name: %s', node_name)
            new_node = self._create_node(node_name)
            if not new_node.is_running():
                new_node.start()
                new_node.wait_for_status_running()
            self.nodes.append(new_node)
        return self.nodes

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        if cluster.Setup.REUSE_CLUSTER:
            return self._get_nodes()
        else:
            return self._create_nodes(count, dc_idx, enable_auto_bootstrap)

    def destroy(self):
        LOGGER.info('Destroy nodes')
        for node in self.nodes:
            node.destroy(force=True)


class ScyllaDockerCluster(cluster.BaseScyllaCluster, DockerCluster):  # pylint: disable=abstract-method

    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'db-cluster')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'db-node')

        self.aws_extra_network_interface = False  # TODO: move to Base
        super(ScyllaDockerCluster, self).__init__(node_prefix=node_prefix,
                                                  cluster_prefix=cluster_prefix,
                                                  **kwargs)

    def node_setup(self, node, verbose=False, timeout=3600):
        endpoint_snitch = self.params.get('endpoint_snitch')
        seed_address = ','.join(self.seed_nodes_ips)

        node.wait_ssh_up(verbose=verbose)
        if cluster.Setup.BACKTRACE_DECODING:
            node.install_scylla_debuginfo()

        self.node_config_setup(node, seed_address, endpoint_snitch)

        node.stop_scylla_server(verify_down=False)
        # clear data folder to drop wrong cluster name data
        node.remoter.run('sudo rm -Rf /var/lib/scylla/data/*')
        node.start_scylla_server(verify_up=False)

        node.wait_db_up(verbose=verbose, timeout=timeout)
        node.check_nodes_status()
        self.clean_replacement_node_ip(node)

    @cluster.wait_for_init_wrap
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):   # pylint: disable=unused-argument,arguments-differ
        node_list = node_list if node_list else self.nodes
        for node in node_list:
            node.wait_for_status_running()
        self.wait_for_nodes_up_and_normal(node_list)

    def get_scylla_args(self):
        # pylint: disable=no-member
        append_scylla_args = self.params.get('append_scylla_args_oracle') if self.name.find('oracle') > 0 else \
            self.params.get('append_scylla_args')
        return re.sub(r'--blocked-reactor-notify-ms\s.*\s', '', append_scylla_args)


class LoaderSetDocker(cluster.BaseLoaderSet, DockerCluster):

    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'loader-set')

        cluster.BaseLoaderSet.__init__(self,
                                       params=kwargs.get("params"))
        DockerCluster.__init__(self,
                               node_prefix=node_prefix,
                               cluster_prefix=cluster_prefix, **kwargs)

    def node_setup(self, node, verbose=False, db_node_address=None, **kwargs):
        self.install_gemini(node=node)
        if self.params.get('client_encrypt'):
            node.config_client_encrypt()


def send_receive_files(self, src, dst, delete_dst=False, preserve_perm=True, preserve_symlinks=False):  # pylint: disable=too-many-arguments,unused-argument
    if src != dst:
        self.remoter.run(f'cp {src} {dst}')


class DummyMonitoringNode(cluster.BaseNode):  # pylint: disable=abstract-method,too-many-instance-attributes
    def __init__(self, name, node_prefix=None, parent_cluster=None, base_logdir=None):  # pylint: disable=too-many-arguments,super-init-not-called
        self.name = name
        self.node_prefix = node_prefix
        self.remoter = LOCALRUNNER
        self.remoter.receive_files = types.MethodType(send_receive_files, self)
        self.remoter.send_files = types.MethodType(send_receive_files, self)
        self.parent_cluster = parent_cluster
        self.is_seed = False
        self._distro = None

        self.logdir = os.path.join(base_logdir, self.name)
        makedirs(self.logdir)
        self.log = SDCMAdapter(LOGGER, extra={'prefix': str(self)})

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass

    def update_repo_cache(self):
        pass

    def stop_task_threads(self, timeout=None):
        pass

    def _refresh_instance_state(self):
        return ["127.0.0.1"], ["127.0.0.1"]


class MonitorSetDocker(cluster.BaseMonitorSet, DockerCluster):  # pylint: disable=abstract-method
    def __init__(self, **kwargs):
        user_prefix = kwargs.get('user_prefix')
        node_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-node')
        cluster_prefix = cluster.prepend_user_prefix(user_prefix, 'monitor-set')

        cluster.BaseMonitorSet.__init__(self,
                                        targets=kwargs.get('targets'),
                                        params=kwargs.get('params'))
        DockerCluster.__init__(self,
                               node_prefix=node_prefix,
                               cluster_prefix=cluster_prefix,
                               **kwargs)

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, enable_auto_bootstrap=False):
        return self._create_nodes(count, dc_idx, enable_auto_bootstrap)

    def _create_nodes(self, count, dc_idx=0, enable_auto_bootstrap=False):  # pylint: disable=unused-argument
        """
        Create nodes from docker containers
        :param count: count of nodes to create
        :param dc_idx: datacenter index
        :return: list of DockerNode objects
        """
        new_nodes = []
        for _ in range(count):
            node_name, _ = self._get_node_name_and_index()
            new_node = self._create_node(node_name)
            self.nodes.append(new_node)
            new_nodes.append(new_node)
        return new_nodes

    @staticmethod
    def install_scylla_monitoring_prereqs(node):  # pylint: disable=invalid-name
        # since running local, don't install anything, just the monitor
        pass

    @property
    def monitor_install_path_base(self):
        if not self._monitor_install_path_base:
            self._monitor_install_path_base = self.nodes[0].logdir
        return self._monitor_install_path_base

    def _create_node(self, node_name):
        return DummyMonitoringNode(name=node_name,
                                   parent_cluster=self,
                                   base_logdir=self.logdir,
                                   node_prefix=self.node_prefix)

    def get_backtraces(self):
        pass

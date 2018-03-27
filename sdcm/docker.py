import json
import time
import logging
import os
from avocado.utils import process

import cluster
from sdcm.utils import retrying

logger = logging.getLogger(__name__)

BASE_NAME = 'db-node'
LOADER_NAME = 'loader-node'
MONITOR_NAME = 'monitor-node'


class DockerCommandError(Exception):
    pass


class DockerContainerNotExists(Exception):
    pass


class DockerContainerNotRunning(Exception):
    pass


def _cmd(cmd, timeout=10, sudo=False):
    res = process.run('docker {}'.format(cmd), ignore_status=True, timeout=timeout, sudo=sudo)
    if res.exit_status:
        if 'No such container:' in res.stderr:
            raise DockerContainerNotExists(res.stderr)
        raise DockerCommandError('command: {}, error: {}, output: {}'.format(cmd, res.stderr, res.stdout))
    return res.stdout


class DockerNode(cluster.BaseNode):

    def __init__(self, name, credentials, base_logdir=None, node_prefix=None):
        ssh_login_info = {'hostname': None,
                          'user': 'scylla-test',
                          'key_file': credentials.key_file}
        super(DockerNode, self).__init__(name=name,
                                         base_logdir=base_logdir,
                                         ssh_login_info=ssh_login_info,
                                         node_prefix=node_prefix)
        self.wait_for_status_running()
        self.wait_public_ip()

    def _get_public_ip_address(self):
        if not self._public_ip_address:
            out = _cmd("inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {}".format(self.name))
            self._public_ip_address = out.strip()
        return self._public_ip_address

    @retrying(n=10, sleep_time=2, allowed_exceptions=(DockerContainerNotRunning,))
    def wait_for_status_running(self):
        out = _cmd("inspect --format='{{{{json .State.Running}}}}' {}".format(self.name))
        state = json.loads(out)
        if not state:
            raise DockerContainerNotRunning(self.name)

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_public_ip_address()

    def run_nodetool(self, cmd):
        logger.debug('run nodetool %s' % cmd)
        return _cmd('exec {} nodetool {}'.format(self.name, cmd))

    def wait_public_ip(self):
        while not self._public_ip_address:
            self._get_public_ip_address()
            time.sleep(1)

    def start(self):
        _cmd('start {}'.format(self.name))

    def restart(self, timeout=30):
        _cmd('restart {}'.format(self.name), timeout=timeout)

    def stop(self, timeout=30):
        _cmd('stop {}'.format(self.name), timeout=timeout)

    def destroy(self, force=True):
        force_param = '-f' if force else ''
        _cmd('rm {} -v {}'.format(force_param, self.name))


class DockerCluster(cluster.BaseCluster):

    def __init__(self, **kwargs):
        self._image = kwargs.get('docker_image', 'scylladb/scylla-nightly')
        self.nodes = []
        self.credentials = kwargs.get('credentials')
        self._node_img_tag = 'scylla-sct-img'
        self._context_path = os.path.join(os.path.dirname(__file__), '../docker/scylla-sct')
        self._create_node_image()
        super(DockerCluster, self).__init__(node_prefix=kwargs.get('node_prefix'),
                                            n_nodes=kwargs.get('n_nodes'),
                                            params=kwargs.get('params'),
                                            region_names=["localhost-dc"])  # no multi dc currently supported

    def _create_node_image(self):
        self._update_image()
        _cmd('build --build-arg SOURCE_IMAGE={} -t {} {}'.format(self._image, self._node_img_tag, self._context_path),
             timeout=300)

    def _clean_old_images(self):
        images = _cmd('images -f "dangling=true" -q')
        if images:
            _cmd('rmi {}'.format(images), timeout=90)

    def _update_image(self):
        logger.debug('update scylla image')
        _cmd('pull {}'.format(self._image), timeout=300)
        self._clean_old_images()

    def _create_node(self, node_name, is_seed=False, seed_ip=None):
        cmd = 'run --name {} -d {}'.format(node_name, self._node_img_tag)
        if not is_seed and seed_ip:
            cmd = '{} --seeds="{}"'.format(cmd, seed_ip)
        _cmd(cmd, timeout=30)
        return DockerNode(node_name,
                          credentials=self.credentials[0],
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
            else:
                node_index += 1

    def add_nodes(self, count, dc_idx=0):
        new_nodes = []
        for _ in xrange(count):
            node_name, node_index = self._get_node_name_and_index()
            is_seed = (node_index == 0)
            seed_ip = self.nodes[0].public_ip_address if not is_seed else None
            new_node = self._create_node(node_name, is_seed, seed_ip)
            self.nodes.append(new_node)
            new_nodes.append(new_node)
        return new_nodes

    def destroy(self):
        logger.info('Destroy nodes')
        for node in self.nodes:
            node.destroy(force=True)


class ScyllaDockerCluster(DockerCluster, cluster.BaseScyllaCluster):

    def __init__(self, **kwargs):
        self._user_prefix = kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX)
        self._node_prefix = '%s-%s' % (self._user_prefix, BASE_NAME)
        super(ScyllaDockerCluster, self).__init__(node_prefix=kwargs.get('node_prefix', self._node_prefix),
                                                  **kwargs)

    @retrying(n=30, sleep_time=3, allowed_exceptions=(cluster.ClusterNodesNotReady, DockerCommandError))
    def wait_for_init(self, node_list=None, verbose=False, timeout=None):
        node_list = node_list if node_list else self.nodes
        for node in node_list:
            node.wait_for_status_running()
        return self.check_nodes_up_and_normal(node_list)


class LoaderSetDocker(DockerCluster, cluster.BaseLoaderSet):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), LOADER_NAME)
        super(LoaderSetDocker, self).__init__(node_prefix=self._node_prefix, **kwargs)


class MonitorSetDocker(DockerCluster, cluster.BaseMonitorSet):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), MONITOR_NAME)
        super(MonitorSetDocker, self).__init__(node_prefix=self._node_prefix, **kwargs)

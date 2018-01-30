import json
import time
import logging
import os
from avocado.utils import process

import cluster

logger = logging.getLogger(__name__)

BASE_NAME = 'db-node'
LOADER_NAME = 'loader-node'
MONITOR_NAME = 'monitor-node'


class DockerCommandError(Exception):
    pass


class DockerContainerNotExists(Exception):
    pass


def _cmd(cmd, timeout=10, sudo=False):
    res = process.run('docker {}'.format(cmd), ignore_status=True, timeout=timeout, sudo=sudo)
    if res.exit_status:
        if 'No such container:' in res.stderr:
            raise DockerContainerNotExists(res.stderr)
        raise DockerCommandError('command: {}, error: {}, output: {}'.format(cmd, res.stderr, res.stdout))
    return res.stdout


class DockerNode(cluster.BaseNode):

    def __init__(self, name, credentials, base_logdir=None):
        ssh_login_info = {'hostname': None,
                          'user': 'scylla-test',
                          'key_file': credentials.key_file}
        super(DockerNode, self).__init__(name=name,
                                         base_logdir=base_logdir,
                                         ssh_login_info=ssh_login_info)
        self.wait_for_status_running()
        self.wait_public_ip()

    def _get_public_ip_address(self):
        if not self._public_ip_address:
            out = _cmd("inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {}".format(self.name))
            self._public_ip_address = out.strip()
        return self._public_ip_address

    def _is_running(self):
        out = _cmd("inspect --format='{{{{json .State.Running}}}}' {}".format(self.name))
        return json.loads(out)

    @property
    def public_ip_address(self):
        return self._get_public_ip_address()

    @property
    def private_ip_address(self):
        return self._get_public_ip_address()

    def run_nodetool(self, cmd):
        logger.debug('run nodetool %s' % cmd)
        return _cmd('exec {} nodetool {}'.format(self.name, cmd))

    def wait_for_status_running(self):
        try_cnt = 0
        while not self._is_running() and try_cnt < 10:
            time.sleep(2)
            try_cnt += 1

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

    def destroy(self, force=False):
        force_param = '-f' if force else ''
        _cmd('rm {} {}'.format(force_param, self.name))


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
                                            params=kwargs.get('params'))

    def _create_node_image(self):
        self._update_image()
        _cmd('build --build-arg SOURCE_IMAGE={} -t {} {}'.format(self._image, self._node_img_tag, self._context_path))

    def _clean_old_images(self):
        images = _cmd('images -f "dangling=true" -q')
        if images:
            _cmd('rmi {}'.format(images), timeout=90)

    def _update_image(self):
        logger.debug('update scylla image')
        _cmd('pull {}'.format(self._image), timeout=90)
        self._clean_old_images()

    def _create_node(self, node_name, is_seed=False, seed_ip=None):
        if is_seed:
            _cmd('run --name {} -d {}'.format(node_name, self._node_img_tag))
        else:
            _cmd('run --name {} -d {} --seeds="{}"'.format(node_name, self._node_img_tag, seed_ip))
        return DockerNode(node_name,
                          credentials=self.credentials[0],
                          base_logdir=self.logdir)

    def add_nodes(self, count, dc_idx=0):
        for node_index in xrange(count):
            node_name = '%s-%s' % (self.node_prefix, node_index)
            is_seed = (node_index == 0)
            seed_ip = self.nodes[0].public_ip_address if not is_seed else None
            self.nodes.append(self._create_node(node_name, is_seed, seed_ip))

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

    def wait_for_init(self):
        self.nodes[0].wait_for_status_running()
        node_ips = [node.public_ip_address for node in self.nodes]
        up_nodes = list()
        try_cnt = 0
        while len(up_nodes) < len(node_ips) and try_cnt < 30:
            try:
                res = node.run_nodetool('status')
                for line in res.splitlines():
                    for ip in node_ips:
                        if ip in line and ip not in up_nodes:
                            if line.split()[0].strip() == 'UN':
                                up_nodes.append(ip)
            except Exception as ex:
                logger.debug('Failed getting node status: %s', ex)
                pass
            time.sleep(2)
            try_cnt += 1
        return len(up_nodes) == len(node_ips)


class LoaderSetDocker(DockerCluster, cluster.BaseLoaderSet):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), LOADER_NAME)
        super(LoaderSetDocker, self).__init__(node_prefix=self._node_prefix, **kwargs)


class MonitorSetDocker(DockerCluster, cluster.BaseMonitorSet):

    def __init__(self, **kwargs):
        self._node_prefix = '%s-%s' % (kwargs.get('user_prefix', cluster.DEFAULT_USER_PREFIX), MONITOR_NAME)
        super(MonitorSetDocker, self).__init__(node_prefix=self._node_prefix, **kwargs)

import logging
import shlex
import socket
from functools import cached_property, cache
from pathlib import Path

from invoke.exceptions import UnexpectedExit

from sdcm.cluster import BaseNode
from sdcm.remote.libssh2_client import UnexpectedExit as Libssh2_UnexpectedExit
from sdcm.utils.common import get_data_dir_path
from sdcm.utils.docker_utils import docker_hub_login
from sdcm.utils.net import resolve_ip_to_dns

LOGGER = logging.getLogger(__name__)


class RemoteDocker(BaseNode):
    def __init__(self, node, image_name, ports=None, command_line="tail -f /dev/null", extra_docker_opts="", docker_network=None):
        self.node = node
        self._internal_ip_address = None
        self.log = LOGGER
        ports = " ".join([f'-p {port}:{port}' for port in ports]) if ports else ""
        if docker_network:
            extra_docker_opts += f" --network {docker_network}"
            self.create_network(docker_network)
        extra_docker_opts += f" --label TestId={node.test_config.test_id()}"
        res = self.node.remoter.run(
            f'{self.sudo_needed} docker run {extra_docker_opts} -d {ports} {image_name} {command_line}',
            verbose=True, retry=3)
        self.docker_id = res.stdout.strip()
        self.image_name = image_name
        self.docker_network = docker_network
        super().__init__(name=image_name, parent_cluster=node.parent_cluster)

    @property
    def internal_ip_address(self):
        if not self._internal_ip_address:
            if self.docker_network:
                self._internal_ip_address = self.node.remoter.run(
                    "docker inspect"
                    f" --format='{{{{ (index .NetworkSettings.Networks \"{self.docker_network}\").IPAddress }}}}'"
                    f" {self.docker_id}").stdout.strip()
            else:
                self._internal_ip_address = self.node.remoter.run(
                    f"docker inspect --format='{{{{ .NetworkSettings.Networks.bridge.IPAddress }}}}' {self.docker_id}").stdout.strip()
        return self._internal_ip_address

    @property
    def ip_address(self):
        return self.internal_ip_address

    @property
    def external_address(self):
        return self.internal_ip_address

    @property
    def private_ip_address(self):
        return self.internal_ip_address

    @property
    def cql_address(self):
        return self.internal_ip_address

    @property
    def private_dns_name(self):
        raise NotImplementedError()

    @property
    def public_dns_name(self) -> str:
        try:
            return resolve_ip_to_dns(self.external_address)
        except (ValueError, socket.herror):
            return self.external_address

    @staticmethod
    @cache
    def running_in_docker(node):
        ok = node.remoter.run("test /.dockerenv", ignore_status=True).ok
        ok |= 'docker' in node.remoter.run('ls /proc/self/cgroup', ignore_status=True).stdout
        return ok

    @staticmethod
    @cache
    def running_in_podman(node) -> bool:
        return 'podman' in node.remoter.run('echo $container', ignore_status=True).stdout

    @cached_property
    def sudo_needed(self):
        return 'sudo ' if self.running_in_docker(self.node) and not self.running_in_podman(self.node) else ''

    def create_network(self, docker_network):
        try:
            ret = self.node.remoter.run(
                f"docker network create {docker_network} --label 'com.docker.compose.network=default'").stdout.strip()
            LOGGER.debug(ret)
        except (UnexpectedExit, Libssh2_UnexpectedExit) as ex:
            if 'already exists' in str(ex):
                pass
            else:
                raise

    def get_port(self, internal_port):
        """
        get specific port mapping

        :param internal_port: port exposed by docker
        :return: the external port automatically open by docker
        """
        external_port = self.node.remoter.run(f"docker port {self.docker_id} {internal_port}").stdout.strip()
        return external_port.splitlines()[0]

    def get_log(self):
        return self.node.remoter.run(f"{self.sudo_needed} docker logs {self.docker_id}").stdout.strip()

    def run(self, cmd, *args, **kwargs):
        return self.node.remoter.run(f'{self.sudo_needed} docker exec {self.docker_id} /bin/sh -c {shlex.quote(cmd)}', *args, **kwargs)

    def kill(self):
        return self.node.remoter.run(f"{self.sudo_needed} docker rm -f {self.docker_id}", verbose=False, ignore_status=True)

    def send_files(self, src, dst, **kwargs):
        remote_tempfile = self.node.remoter.run("mktemp", verbose=kwargs.get('verbose')).stdout.strip()
        result = self.node.remoter.send_files(src, remote_tempfile, **kwargs)
        result &= self.run(f'mkdir -p {Path(dst).parent}', ignore_status=True, verbose=kwargs.get('verbose')).ok
        result &= self.node.remoter.run(f"{self.sudo_needed} docker cp {remote_tempfile} {self.docker_id}:{dst}",
                                        verbose=kwargs.get('verbose'), ignore_status=True).ok
        return result

    def receive_files(self, src, dst, **kwargs):
        remote_tempfile = self.node.remoter.run("mktemp").stdout.strip()

        result = self.node.remoter.run(f"{self.sudo_needed} docker cp {self.docker_id}:{src} {remote_tempfile}",
                                       verbose=kwargs.get('verbose'), ignore_status=True).ok
        result &= self.node.remoter.receive_files(remote_tempfile, dst, **kwargs)
        return result

    def _get_ipv6_ip_address(self):
        pass

    def _refresh_instance_state(self):
        pass

    def check_spot_termination(self):
        pass

    @property
    def region(self):
        return "eu-north-1"

    def restart(self):
        pass

    @property
    def ssl_conf_dir(self):
        return Path(get_data_dir_path('ssl_conf'))

    def __str__(self):
        return f'RemoteDocker [{self.image_name}] on [{self.node}]'

    @staticmethod
    @cache
    def pull_image(node, image):
        # Login docker-hub before pull, in case node authentication is expired or not logged-in.
        use_sudo = node.is_docker() and (RemoteDocker.running_in_docker(node) and not RemoteDocker.running_in_podman(node))
        docker_hub_login(remoter=node.remoter, use_sudo=use_sudo)
        remote_cmd = node.remoter.sudo if use_sudo else node.remoter.run
        remote_cmd(f"docker pull {image}", verbose=True, retry=3)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kill()

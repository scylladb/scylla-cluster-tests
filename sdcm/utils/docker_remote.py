import logging
import shlex
from functools import cached_property

from sdcm.cluster import BaseNode

LOGGER = logging.getLogger(__name__)


class RemoteDocker(BaseNode):
    def __init__(self, node, image_name, ports=None, command_line="tail -f /dev/null", extra_docker_opts=""):  # pylint: disable=too-many-arguments
        self.node = node
        self._internal_ip_address = None
        self.log = LOGGER
        ports = " ".join([f'-p {port}:{port}' for port in ports]) if ports else ""
        res = self.node.remoter.run(
            f'{self.sudo_needed} docker run {extra_docker_opts} -d {ports} {image_name} {command_line}', verbose=True)
        self.docker_id = res.stdout.strip()
        self.image_name = image_name
        super().__init__(name=image_name, parent_cluster=node.parent_cluster)

    @property
    def internal_ip_address(self):
        if not self._internal_ip_address:
            self._internal_ip_address = self.node.remoter.run(
                f"docker inspect --format='{{{{ .NetworkSettings.IPAddress }}}}' {self.docker_id}").stdout.strip()
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
    def cql_ip_address(self):
        return self.internal_ip_address

    @cached_property
    def running_in_docker(self):
        ok = self.node.remoter.run("test /.dockerenv", ignore_status=True).ok
        ok |= 'docker' in self.node.remoter.run('ls /proc/self/cgroup', ignore_status=True).stdout
        return ok

    @cached_property
    def sudo_needed(self):
        return 'sudo ' if self.running_in_docker else ''

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
        result = self.node.remoter.send_files(src, src, **kwargs)
        result &= self.node.remoter.run(f"{self.sudo_needed} docker cp {src} {self.docker_id}:{dst}",
                                        verbose=kwargs.get('verbose'), ignore_status=True).ok
        return result

    def receive_files(self, src, dst, **kwargs):  # pylint: disable=unused-argument
        remote_tempfile = self.node.remoter.run("mktemp").stdout.strip()

        result = self.node.remoter.run(f"{self.sudo_needed} docker cp {self.docker_id}:{src} {remote_tempfile}",
                                       verbose=kwargs.get('verbose'), ignore_status=True).ok
        result &= self.node.remoter.receive_files(remote_tempfile, dst, **kwargs)
        return result

    def is_port_used(self, port: int, service_name: str) -> bool:
        try:
            # Path to `ss' is /usr/sbin/ss for RHEL-like distros and /bin/ss for Debian-based.  Unfortunately,
            # /usr/sbin is not always in $PATH, so need to set it explicitly.
            #
            # Output of `ss -ln' command in case of used port:
            #   $ ss -ln '( sport = :8000 )'
            #   Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port
            #   tcp   LISTEN     0      5                      *:8000                               *:*
            #
            # And if there are no processes listening on the port:
            #   $ ss -ln '( sport = :8001 )'
            #   Netid State      Recv-Q Send-Q     Local Address:Port                    Peer Address:Port
            #
            # Can't avoid the header by using `-H' option because of ss' core on Ubuntu 18.04.
            cmd = f"PATH=/bin:/usr/sbin ss -ln '( sport = :{port} )'"
            return len(self.remoter.run(cmd, verbose=False).stdout.splitlines()) > 1
        except Exception as details:  # pylint: disable=broad-except
            self.log.error("Error checking for '%s' on port %s: %s", service_name, port, details)
            return False

    def _get_ipv6_ip_address(self):
        pass

    def _refresh_instance_state(self):
        pass

    def check_spot_termination(self):
        pass

    @ property
    def region(self):
        return "docker"

    def restart(self):
        pass

    def __str__(self):
        return f'RemoteDocker [{self.image_name}] on [{self.node}]'

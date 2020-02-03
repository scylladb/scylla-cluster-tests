import os
import atexit


def running_in_docker():
    path = '/proc/self/cgroup'
    with open(path) as cgroup:
        return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in cgroup)
        )


class RemoteDocker:
    def __init__(self, node, image_name, ports=None, command_line="tail -f /dev/null", extra_docker_opts=""):  # pylint: disable=too-many-arguments
        self.node = node
        self._internal_ip_address = None

        ports = " ".join([f'-p {port}:{port}' for port in ports]) if ports else ""
        res = self.node.remoter.run(
            f'docker run {extra_docker_opts} -d {ports} {image_name} {command_line}', verbose=True)
        self.docker_id = res.stdout.strip()
        atexit.register(self.atexit)

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
    def remoter(self):
        return self.run

    def get_port(self, internal_port):
        """
        get specific port mapping

        :param internal_port: port exposed by docker
        :return: the external port automatically open by docker
        """
        external_port = self.node.remoter.run(f"docker port {self.docker_id} {internal_port}").stdout.strip()
        return external_port

    def run(self, cmd, *args, **kwargs):
        return self.node.remoter.run(f"docker exec -i {self.docker_id} /bin/bash -c '{cmd}'", *args, **kwargs)

    def atexit(self):
        return self.node.remoter.run(f"docker rm -f {self.docker_id}", verbose=False, ignore_status=True)

    def send_files(self, src, dst):
        self.node.remoter.send_files(src, src)
        self.node.remoter.run(f"docker cp {src} {self.docker_id}:{dst}", verbose=False, ignore_status=True)

    def receive_files(self, src, dst):
        self.node.remoter.run(f"docker cp {self.docker_id}:{src} {dst}", verbose=False, ignore_status=True)
        self.node.remoter.receive_files(dst, dst)

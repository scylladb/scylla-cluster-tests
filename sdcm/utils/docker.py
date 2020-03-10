# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2020 ScyllaDB

import os
import re
import logging
from types import SimpleNamespace
from typing import List, Optional, Union, Any, Tuple

import docker
from docker.errors import DockerException, NotFound
from docker.models.containers import Container

from sdcm.remote import LOCALRUNNER
from sdcm.utils.common import deprecation, retrying


LOGGER = logging.getLogger(__name__)


# TODO: remove this wrapper when migrated to Docker Python module completely.
class DockerClient(docker.DockerClient):
    def __call__(self, cmd, timeout=10):
        deprecation("consider to use Docker Python module instead of using Docker CLI commands")

        res = LOCALRUNNER.run('docker {}'.format(cmd), ignore_status=True, timeout=timeout)
        if res.exit_status:
            if 'No such container:' in res.stderr:
                raise NotFound(res.stderr)
            raise DockerException('command: {}, error: {}, output: {}'.format(cmd, res.stderr, res.stdout))
        return res


_docker = DockerClient.from_env()  # pylint: disable=invalid-name


class _Name(SimpleNamespace):
    # pylint: disable=too-few-public-methods

    def __init__(self, name: str) -> None:
        family, *member = name.split(":", 1)
        super().__init__(full=name, family=family, member=member[0] if member else None, member_as_args=tuple(member))

    def __str__(self) -> str:
        return self.full


class ContainerManager:
    """A set of methods for manipulating containers associated with a node.

    This manager expects that node instance has two dicts: `_containers' and `tags'.

    `_containers' dict uses keys as unique id of container inside the node (e.g., "node" or "auto_ssh") and values are
    instances of `docker.models.Container'.  Keys can be like "auto_ssh:0" or "auto_ssh:remote_browser".  In this case,
    part of the key before `:' called `family' and after `:' -- `member'.

    Also node instance can have methods/properties which return:
        a) node-wide docker client (`docker_client')
        b) custom Docker client (`{name}_docker_client')
        c) additional parameters for a container run method (`{name}_container_run_args')
        d) logfile (`{name}_container_logfile')

    where `name' is a key for `_containers' dict.
    """

    # pylint: disable=protected-access

    keep_alive_suffix = "---KEEPALIVE"
    default_docker_client = _docker

    @classmethod
    def get_docker_client(cls, instance: object, name: str) -> DockerClient:
        container = cls.get_container(instance, name, raise_not_found_exc=False)
        if container is None:
            return cls._get_docker_client_for_new_container(instance, _Name(name))
        return container.client

    @classmethod
    def _get_docker_client_for_new_container(cls, instance: object, name: _Name) -> DockerClient:

        # Try to get Docker client for container family or node-wide Docker client.
        for attr, args in ((f"{name.family}_docker_client", name.member_as_args), ("docker_client", ()), ):
            docker_client = getattr(instance, attr, None)
            if callable(docker_client):
                docker_client = docker_client(*args)
            if docker_client is not None:
                return docker_client

        return cls.default_docker_client

    @staticmethod
    def get_container(instance: object, name: str, raise_not_found_exc: bool = True) -> Optional[Container]:
        container = instance._containers.get(name)
        if container is None and raise_not_found_exc:
            raise NotFound(f"There is no container `{name}' for {instance}.")
        if container:
            container.reload()
        return container

    @classmethod
    def run_container(cls, instance: object, name: str, **extra_run_args) -> Container:
        name = _Name(name)
        container = cls.get_container(instance, name.full, raise_not_found_exc=False)
        if container is None:
            run_args = {"detach": True,
                        "labels": instance.tags, }
            run_args.update(getattr(instance, f"{name.family}_container_run_args", cls._run_args)(**extra_run_args))
            if name.member and "name" in run_args:
                run_args["name"] += f"-{name.member}"
            container = instance._containers[name.full] = \
                cls._get_docker_client_for_new_container(instance, name).containers.run(**run_args)
            LOGGER.info("Container %s started.", container)
        else:
            LOGGER.warning("Tried to run container which ran already: %s", container)
        return container

    @staticmethod
    def _run_args(**kwargs) -> dict:
        return kwargs

    @classmethod
    def destroy_container(cls, instance: object, name: str, ignore_keepalive: bool = False) -> None:
        name = _Name(name)
        container = cls.get_container(instance, name.full)
        logfile = getattr(instance, f"{name.family}_container_logfile", None)
        if callable(logfile):
            logfile = logfile(*name.member_as_args)
        if logfile:
            with open(logfile, "ab") as log:
                log.write(container.logs())
            LOGGER.info("Container %s logs written to %s", container, logfile)
        if ignore_keepalive or not container.name.endswith(cls.keep_alive_suffix):
            container.remove(v=True, force=True)
            del instance._containers[name.full]
            LOGGER.info("Container %s destroyed.", container)
        else:
            LOGGER.info("Container %s has keep-alive tag and not destroyed", container)

    @classmethod
    def destroy_all_containers(cls, instance: object, ignore_keepalive: bool = False) -> None:
        for name in tuple(instance._containers.keys()):
            cls.destroy_container(instance, name, ignore_keepalive=ignore_keepalive)

    @classmethod
    def is_running(cls, instance: object, name: str) -> bool:
        return cls.get_container(instance, name).status == "running"

    @classmethod
    def set_keep_alive(cls, instance: object, name: str) -> None:
        container = cls.get_container(instance, name)
        if container.name.endswith(cls.keep_alive_suffix):
            return
        container.rename(container.name + cls.keep_alive_suffix)
        LOGGER.info("Container %s set to keep alive.", container)

    @classmethod
    def get_containers_by_prefix(cls, prefix: str, docker_client: DockerClient = None) -> List[Container]:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.list(all=True, filters={"name": prefix})

    @classmethod
    def get_container_name_by_id(cls, c_id: str, docker_client: DockerClient = None) -> str:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.get(c_id).name

    @classmethod
    @retrying(n=20, sleep_time=2, allowed_exceptions=(AssertionError, ))
    def wait_for_status(cls, instance: object, name: str, status: str) -> None:
        assert cls.get_container(instance, name).status == status

    @classmethod
    @retrying(n=10, sleep_time=1, allowed_exceptions=(AssertionError, ))
    def get_ip_address(cls, instance: object, name: str) -> str:
        ip_address = cls.get_container(instance, name).attrs["NetworkSettings"]["IPAddress"]
        assert ip_address
        return ip_address

    @classmethod
    def get_container_port(cls, instance: object, name: str, port: Union[int, str]) -> Optional[int]:
        container = cls.get_container(instance, name)
        try:
            return int(container.ports[f"{port}/tcp"][0]["HostPort"])
        except (IndexError, KeyError, ):
            return None

    @classmethod
    def get_environ(cls, instance: object, name: str) -> dict:
        container = cls.get_container(instance, name)

        def normalize(key: Any, value: Any = None) -> Tuple[Any, Any]:
            return (key, value, )
        return dict(normalize(*(item.split("=", 1))) for item in container.attrs["Config"]["Env"])


class RemoteDocker:
    def __init__(self, node, image_name, ports=None, command_line="tail -f /dev/null", extra_docker_opts=""):  # pylint: disable=too-many-arguments
        self.node = node
        self._internal_ip_address = None

        ports = " ".join([f'-p {port}:{port}' for port in ports]) if ports else ""
        res = self.node.remoter.run(
            f'docker run {extra_docker_opts} -d {ports} {image_name} {command_line}', verbose=True)
        self.docker_id = res.stdout.strip()

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

    def get_log(self):
        return self.node.remoter.run(f"docker logs {self.docker_id}").stdout.strip()

    def run(self, cmd, *args, **kwargs):
        return self.node.remoter.run(f"docker exec -i {self.docker_id} /bin/bash -c '{cmd}'", *args, **kwargs)

    def kill(self):
        return self.node.remoter.run(f"docker rm -f {self.docker_id}", verbose=False, ignore_status=True)

    def send_files(self, src, dst):
        self.node.remoter.send_files(src, src)
        self.node.remoter.run(f"docker cp {src} {self.docker_id}:{dst}", verbose=False, ignore_status=True)

    def receive_files(self, src, dst):
        self.node.remoter.run(f"docker cp {self.docker_id}:{src} {dst}", verbose=False, ignore_status=True)
        self.node.remoter.receive_files(dst, dst)


def running_in_docker():
    path = '/proc/self/cgroup'
    with open(path) as cgroup:
        return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in cgroup)
        )


def get_docker_bridge_gateway(remoter):
    result = remoter.run(
        "docker inspect -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' bridge",
        ignore_status=True
    )
    if result.exit_status != 0 or not result.stdout:
        return None
    address = result.stdout.splitlines()[0]
    match = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", address)
    if not match:
        return None
    return match.group()

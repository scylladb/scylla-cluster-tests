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
from pprint import pformat
from types import SimpleNamespace
from typing import List, Optional, Union, Any, Tuple

import docker
import paramiko  # pylint: disable=wrong-import-order; false warning because of docker import (local file vs. package)
from docker.errors import DockerException, NotFound, ImageNotFound, NullResource
from docker.models.images import Image
from docker.models.containers import Container

from sdcm.remote import LOCALRUNNER
from sdcm.utils.common import deprecation
from sdcm.utils.decorators import retrying, Retry


LOGGER = logging.getLogger(__name__)


class ContainerAlreadyRegistered(DockerException):
    pass


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

    def __init__(self, name: Optional[str]) -> None:
        family, *member = (None, ) if name is None else name.split(":", 1)
        super().__init__(full=name, family=family, member=member[0] if member else None, member_as_args=tuple(member))

    def __str__(self) -> str:
        return str(self.full)

    def __bool__(self):
        return self.full is not None


class ContainerManager:
    """A set of methods for manipulating containers associated with a node.

    This manager expects that node instance has two dicts: `_containers' and `tags'.

    `_containers' dict uses keys as an unique id of container inside the node (e.g., "node" or "auto_ssh") and values
    are instances of `docker.models.Container'.  Keys can be like "auto_ssh:0" or "auto_ssh:remote_browser".  In this
    case, part of the key before `:' called `family' and after `:' -- `member'.

    There two types of hooks which can be defined in a node instance: attributes and methods.  Attributes can be
    properties or methods with zero parameters if it is node-wide attribute or one optional `member' parameter
    if hook defined for a container family.  E.g.,

    >>> class Node:
    ...     def __init__(self):
    ...         self._containers = {}
    ...         self.tags = {"key1", "value1", }
    ...
    ...     def attr_as_a_method(self):
    ...         return "hello"
    ...
    ...     @property
    ...     def attr_as_a_property(self):
    ...         return "hello"
    ...
    ...     def auto_ssh_attr_as_a_method(self, member=None):
    ...         return f"hello, {member}"
    ...
    ...     @property
    ...     def auto_ssh_attr_as_property(self):
    ...         return "hello"
    ...
    ...     def auto_ssh_method_args(self, arg1, arg2="value2"):
    ...         return dict(option1=arg1, option2=arg2)

    Attributes:
        1. Client attributes:
            a) docker_client
            b) {name.family}_docker_client

        2. Container attributes:
            a) {name.family}_container_logfile

        3. Image building attributes:
            a) {name.family}_container_image_tag
            b) {name.family}_container_image_dockerfile_args

    Methods:
        1. Container methods:
            a) {name.family}_container_run_args(**kwargs)

        2. Image building methods:
            a) {name.family}_container_image_build_args(**kwargs)

    where `name' is a key for `_containers' dict.
    """

    # pylint: disable=protected-access

    keep_alive_suffix = "---KEEPALIVE"
    default_docker_client = _docker

    @classmethod
    def get_docker_client(cls, instance: object, name: Optional[str] = None) -> DockerClient:
        container = None if name is None else cls.get_container(instance, name, raise_not_found_exc=False)
        if container is None:
            return cls._get_docker_client_for_new_container(instance, _Name(name))
        return container.client

    @classmethod
    def _get_docker_client_for_new_container(cls, instance: object, name: _Name) -> DockerClient:
        return cls._get_attr_for_name(instance, name, "docker_client", default=cls.default_docker_client)

    @staticmethod
    def _get_attr_for_name(instance: object,
                           name: _Name,
                           attr: str,
                           default: Optional[Any] = None,
                           name_only_lookup: bool = False) -> Optional[Any]:
        attr_candidate_list = []

        # Try to get attribute value for container family.
        if name:
            attr_candidate_list.append((f"{name.family}_{attr}", name.member_as_args))

        # Try to get instance-wide attribute value.
        if not name_only_lookup:
            attr_candidate_list.append((attr, ()))

        for attr_candidate, args in attr_candidate_list:
            attr_candidate = getattr(instance, attr_candidate, None)
            if callable(attr_candidate):
                attr_candidate = attr_candidate(*args)
            if attr_candidate is not None:
                return attr_candidate

        return default

    @staticmethod
    def get_container(instance: object, name, raise_not_found_exc: bool = True) -> Optional[Container]:
        assert name is not None, "None is not allowed as a container name"

        container = instance._containers.get(name)
        if container is None and raise_not_found_exc:
            raise NotFound(f"There is no container `{name}' for {instance}.")
        if container:
            container.reload()
        return container

    @classmethod
    def run_container(cls, instance: object, name: str, **extra_run_args) -> Container:
        container = cls.get_container(instance, name, raise_not_found_exc=False)
        if container is None:
            name = _Name(name)
            run_args = {"detach": True,
                        "labels": instance.tags, }
            run_args.update(getattr(instance, f"{name.family}_container_run_args", cls._run_args)(**extra_run_args))
            docker_client = cls._get_docker_client_for_new_container(instance, name)
            if name.member and "name" in run_args:
                run_args["name"] += f"-{name.member}"
            try:
                container = docker_client.containers.get(run_args.get("name"))
                LOGGER.info("Found container %s, re-use it and re-run", container)
                container.start()
            except (NotFound, NullResource, ):
                container = docker_client.containers.run(**run_args)
            instance._containers[name.full] = container
            LOGGER.info("Container %s started.", container)
        else:
            LOGGER.warning("Re-run container %s", container)
            container.start()
        return container

    @staticmethod
    def _run_args(**kwargs) -> dict:
        return kwargs

    @classmethod
    def destroy_container(cls, instance: object, name: str, ignore_keepalive: bool = False) -> bool:
        container = cls.get_container(instance, name)
        logfile = cls._get_attr_for_name(instance, _Name(name), "container_logfile", name_only_lookup=True)
        if logfile:
            try:
                with open(logfile, "ab") as log:
                    log.write(container.logs())
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("Unable to write container logs to %s", logfile, exc_info=exc)
            else:
                LOGGER.info("Container %s logs written to %s", container, logfile)
        if ignore_keepalive or not container.name.endswith(cls.keep_alive_suffix):
            cls.unregister_container(instance, name)
            container.remove(v=True, force=True)
            LOGGER.info("Container %s destroyed", container)
            return True
        LOGGER.info("Container %s has keep-alive tag and not destroyed", container)
        return False

    @classmethod
    def destroy_all_containers(cls, instance: object, ignore_keepalive: bool = False) -> None:
        for name in tuple(instance._containers.keys()):
            try:
                cls.destroy_container(instance, name, ignore_keepalive=ignore_keepalive)
            except Exception as exc:  # pylint: disable=broad-except
                LOGGER.error("%s: some exception raised during container `%s' destroying", instance, name, exc_info=exc)

    @classmethod
    def is_running(cls, instance: object, name: str) -> bool:
        return cls.get_container(instance, name).status == "running"

    @classmethod
    def set_container_keep_alive(cls, instance: object, name: str) -> None:
        container = cls.get_container(instance, name)
        if container.name.endswith(cls.keep_alive_suffix):
            return
        container.rename(container.name + cls.keep_alive_suffix)
        LOGGER.info("Container %s set to keep alive.", container)

    @classmethod
    def set_all_containers_keep_alive(cls, instance: object) -> None:
        for name in tuple(instance._containers.keys()):
            cls.set_container_keep_alive(instance, name)

    @classmethod
    def ssh_copy_id(cls,  # pylint: disable=too-many-arguments
                    instance: object,
                    name: str,
                    user: str,
                    key_file: str,
                    comment: str = "test@local") -> None:
        container = cls.get_container(instance, name)
        pub_key = paramiko.rsakey.RSAKey.from_private_key_file(os.path.expanduser(key_file)).get_base64()
        shell_command = f"umask 077 && echo 'ssh-rsa {pub_key} {comment}' >> ~/.ssh/authorized_keys"
        res = container.exec_run(["sh", "-c", shell_command], user=user)
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")

    @classmethod
    @retrying(n=20, sleep_time=2, allowed_exceptions=(Retry, ))
    def wait_for_status(cls, instance: object, name: str, status: str) -> None:
        if cls.get_container(instance, name).status != status:
            raise Retry

    @classmethod
    @retrying(n=10, sleep_time=1, allowed_exceptions=(Retry, ))
    def get_ip_address(cls, instance: object, name: str) -> str:
        ip_address = cls.get_container(instance, name).attrs["NetworkSettings"]["IPAddress"]
        if not ip_address:
            raise Retry
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

    @classmethod
    def register_container(cls, instance: object, name: str, container: Container, replace: bool = False) -> None:
        assert name is not None, "None is not allowed as a container name"

        if container in instance._containers.values():
            raise ContainerAlreadyRegistered(f"{instance}: container `{container}' registered already")

        if replace or ContainerManager.get_container(instance, name, raise_not_found_exc=False) is None:
            instance._containers[name] = container
        else:
            raise ContainerAlreadyRegistered(f"{instance}: there is another container registered for `{name}'")

    @staticmethod
    def unregister_container(instance: object, name: str) -> None:
        if name not in instance._containers:
            raise NotFound(f"{instance}: there is no container registered for name `{name}'")
        del instance._containers[name]
        LOGGER.debug("%s: container `%s' unregistered", instance, name)

    @classmethod
    def build_container_image(cls, instance: object, name: str, **extra_build_args) -> Image:
        assert name is not None, "None is not allowed as a container name"

        if cls.get_container(instance, name, raise_not_found_exc=False) is not None:
            raise ContainerAlreadyRegistered(f"Can't build a Docker image for a registered container (name: `{name}')")

        name = _Name(name)

        docker_client = cls._get_docker_client_for_new_container(instance, name)
        image_tag = cls._get_container_image_tag(instance, name)

        assert image_tag is not None, f"Please, define image tag property in your class for `{name}' container."

        try:
            return docker_client.images.get(image_tag)
        except ImageNotFound:
            pass

        LOGGER.info("Build Docker image `%s'", image_tag)

        dockerfile_args = cls._get_container_image_dockerfile_args(instance, name)

        assert dockerfile_args is not None, \
            f"Please, define Dockerfile opts property in your class for `{name}' container."

        build_args = dict(rm=True,  # Remove intermediate containers.
                          pull=True,  # Update source image.
                          labels=instance.tags)
        build_args.update(
            getattr(instance, f"{name.family}_container_image_build_args", cls._build_args)(**extra_build_args))

        LOGGER.debug("Build arguments for Docker image `%s':\n%s,", image_tag, pformat(build_args, indent=8))
        image, logs = docker_client.images.build(tag=image_tag, **dockerfile_args, **build_args)

        LOGGER.debug(">>> Build log for Docker image `%s': >>>", image_tag)
        for entry in logs:
            if "stream" in entry:
                LOGGER.debug(entry["stream"].rstrip())
        LOGGER.debug("<<<")

        return image

    @classmethod
    def _get_container_image_tag(cls, instance: object, name: _Name) -> Optional[str]:
        return cls._get_attr_for_name(instance, name, "container_image_tag", name_only_lookup=True)

    @classmethod
    def _get_container_image_dockerfile_args(cls, instance: object, name: _Name) -> Optional[dict]:
        return cls._get_attr_for_name(instance, name, "container_image_dockerfile_args", name_only_lookup=True)

    @staticmethod
    def _build_args(**kwargs):
        return kwargs

    @classmethod
    def get_containers_by_prefix(cls, prefix: str, docker_client: DockerClient = None) -> List[Container]:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.list(all=True, filters={"name": prefix})

    @classmethod
    def get_container_name_by_id(cls, c_id: str, docker_client: DockerClient = None) -> str:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.get(c_id).name


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

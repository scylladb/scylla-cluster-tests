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
import warnings
from pprint import pformat
from types import SimpleNamespace
from typing import List, Optional, Union, Any, Tuple, Protocol
from functools import cache
import itertools

import docker
from docker.errors import DockerException, NotFound, ImageNotFound, NullResource, BuildError
from docker.models.images import Image
from docker.models.containers import Container
from docker.utils.json_stream import json_stream

from sdcm.remote import LOCALRUNNER
from sdcm.remote.base import CommandRunner
from sdcm.remote.remote_file import remote_file
from sdcm.keystore import pub_key_from_private_key_file, KeyStore
from sdcm.utils.decorators import retrying, Retry

DOCKER_API_CALL_TIMEOUT = 180  # seconds

LOGGER = logging.getLogger(__name__)


# Monkey-patch `Container' class in `docker' module for prettier output.
setattr(docker.models.containers.Container, "__str__", lambda self: f"<{self.short_id} {self.name}>")


def deprecation(message):
    warnings.warn(message, DeprecationWarning, stacklevel=3)


def get_ip_address_of_container(container: Container) -> str:
    """
    Get the IP address of a Docker container.
    Takes into account https://docs.docker.com/engine/deprecated/#top-level-network-properties-in-networksettings.

    :param container: Docker container object
    :return: IP address string or None if not available
    """
    network_settings = container.attrs.get("NetworkSettings", {})
    ip_address = network_settings.get("IPAddress")
    if ip_address:
        return ip_address
    networks = network_settings.get("Networks", {})
    if networks:
        # Get the IP address from the first network, if available
        first_network = next(iter(networks.values()), None)
        if first_network:
            return first_network.get("IPAddress")
    return None


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


_docker = DockerClient.from_env(timeout=DOCKER_API_CALL_TIMEOUT)


class _Name(SimpleNamespace):

    def __init__(self, name: Optional[str]) -> None:
        family, *member = (None, ) if name is None else name.split(":", 1)
        super().__init__(full=name, family=family, member=member[0] if member else None, member_as_args=tuple(member))

    def __str__(self) -> str:
        return str(self.full)

    def __bool__(self):
        return self.full is not None


class INodeWithContainerManager(Protocol):

    _containers: dict[str, Container]
    tags: dict[str, str]
    parent_cluster: Any


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

    keep_alive_suffix = "---KEEPALIVE"
    default_docker_client = _docker

    @classmethod
    def get_docker_client(cls, instance: INodeWithContainerManager, name: Optional[str] = None) -> DockerClient:
        container = None if name is None else cls.get_container(instance, name, raise_not_found_exc=False)
        if container is None:
            return cls._get_docker_client_for_new_container(instance, _Name(name))
        return container.client

    @classmethod
    def _get_docker_client_for_new_container(cls, instance: INodeWithContainerManager, name: _Name) -> DockerClient:
        return cls._get_attr_for_name(instance, name, "docker_client", default=cls.default_docker_client)

    @staticmethod
    def _get_attr_for_name(instance: INodeWithContainerManager,
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

        for _attr_candidate, args in attr_candidate_list:
            attr_candidate = getattr(instance, _attr_candidate, None)
            if callable(attr_candidate):
                attr_candidate = attr_candidate(*args)
            if attr_candidate is not None:
                return attr_candidate

        return default

    @classmethod
    def get_container(cls, instance: INodeWithContainerManager, name, raise_not_found_exc: bool = True) -> Optional[Container]:
        assert name is not None, "None is not allowed as a container name"

        container = instance._containers.get(name)
        if container is None and raise_not_found_exc:
            raise NotFound(f"There is no container `{name}' for {instance}.")
        if container:
            try:
                container.reload()
            except OSError:
                LOGGER.warning("Container failed to refresh, checking docker connection...")
                docker_client = cls.get_docker_client(instance)
                docker_client.ping()
                LOGGER.info("Docker version: %s\nActive containers: %s", docker_client.version(),
                            [c.name for c in docker_client.containers.list()])
                container = docker_client.containers.get(name)
                LOGGER.debug("Found container %s, reloading...", container)
                container.reload()
                instance._containers[name] = container

        return container

    @classmethod
    def run_container(cls, instance: INodeWithContainerManager, name: str, **extra_run_args) -> Container:
        container = cls.get_container(instance, name, raise_not_found_exc=False)
        if container is None:
            name = _Name(name)
            run_args = {"detach": True,
                        "labels": instance.tags, }
            if image_tag := cls._get_container_image_tag(instance, name):
                run_args["image"] = image_tag
            run_args.update(getattr(instance, f"{name.family}_container_run_args", cls._run_args)(**extra_run_args))
            docker_client = cls._get_docker_client_for_new_container(instance, name)
            if name.member and "name" in run_args:
                run_args["name"] += f"-{name.member}"
            try:
                container = docker_client.containers.get(run_args.get("name"))
                LOGGER.debug("Found container %s, re-use it and re-run", container)
                container.start()
            except (NotFound, NullResource, ):
                if run_args.pop("pull", None):
                    docker_client.images.pull(*image_tag.split(":", maxsplit=1))
                container = docker_client.containers.run(**run_args)
            instance._containers[name.full] = container
            LOGGER.debug("Container %s started.", container)
        elif container.status != 'running':
            LOGGER.warning("Re-run container %s", container)
            container.start()
            LOGGER.debug('Container %s status %s', container, container.status)
        else:
            LOGGER.debug("Container %s is running already.", container)
        return container

    @staticmethod
    def _run_args(**kwargs) -> dict:
        return kwargs

    @classmethod
    def destroy_container(cls, instance: INodeWithContainerManager, name: str, ignore_keepalive: bool = False) -> bool:
        container = cls.get_container(instance, name)
        logfile = cls._get_attr_for_name(instance, _Name(name), "container_logfile", name_only_lookup=True)
        if logfile:
            try:
                with open(logfile, "ab") as log:
                    log.write(container.logs())
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("Unable to write container logs to %s", logfile, exc_info=exc)
            else:
                LOGGER.debug("Container %s logs written to %s", container, logfile)
        if ignore_keepalive or not container.name.endswith(cls.keep_alive_suffix):
            cls.unregister_container(instance, name)
            container.remove(v=True, force=True)
            LOGGER.debug("Container %s destroyed", container)
            return True
        LOGGER.info("Container %s has keep-alive tag and not destroyed", container)
        return False

    @classmethod
    def destroy_all_containers(cls, instance: INodeWithContainerManager, ignore_keepalive: bool = False) -> None:
        for name in tuple(instance._containers.keys()):
            try:
                cls.destroy_container(instance, name, ignore_keepalive=ignore_keepalive)
            except Exception as exc:  # noqa: BLE001
                LOGGER.error("%s: some exception raised during container `%s' destroying", instance, name, exc_info=exc)

    @classmethod
    def is_running(cls, instance: INodeWithContainerManager, name: str) -> bool:
        return cls.get_container(instance, name).status == "running"

    @classmethod
    def set_container_keep_alive(cls, instance: INodeWithContainerManager, name: str) -> None:
        container = cls.get_container(instance, name)
        if container.name.endswith(cls.keep_alive_suffix):
            return
        container.rename(container.name + cls.keep_alive_suffix)
        LOGGER.debug("Container %s set to keep alive.", container)

    @classmethod
    def set_all_containers_keep_alive(cls, instance: INodeWithContainerManager) -> None:
        for name in tuple(instance._containers.keys()):
            cls.set_container_keep_alive(instance, name)

    @classmethod
    def ssh_copy_id(cls,
                    instance: INodeWithContainerManager,
                    name: str,
                    user: str,
                    key_file: str,
                    comment: str = "test@local") -> None:
        container = cls.get_container(instance, name)
        pub_key, key_type = pub_key_from_private_key_file(key_file)
        shell_command = f"umask 077 && echo '{key_type} {pub_key} {comment}' >> ~/.ssh/authorized_keys"
        res = container.exec_run(["sh", "-c", shell_command], user=user)
        if res.exit_code:
            raise DockerException(f"{container}: {res.output.decode('utf-8')}")

    @classmethod
    @retrying(n=20, sleep_time=2, allowed_exceptions=(Retry, ))
    def wait_for_status(cls, instance: INodeWithContainerManager, name: str, status: str) -> None:
        if cls.get_container(instance, name).status != status:
            raise Retry

    @classmethod
    @retrying(n=10, sleep_time=1, allowed_exceptions=(Retry, ))
    def get_ip_address(cls, instance: INodeWithContainerManager, name: str) -> str:
        if docker_network := instance.parent_cluster.params.get('docker_network'):
            ip_address = cls.get_container(
                instance, name).attrs["NetworkSettings"]["Networks"][docker_network]["IPAddress"]
        else:
            ip_address = get_ip_address_of_container(cls.get_container(instance, name))
        if not ip_address:
            raise Retry
        return ip_address

    @classmethod
    def get_container_port(cls, instance: INodeWithContainerManager, name: str, port: Union[int, str]) -> Optional[int]:
        container = cls.get_container(instance, name)
        try:
            return int(container.ports[f"{port}/tcp"][0]["HostPort"])
        except (IndexError, KeyError, ):
            return None

    @classmethod
    def get_host_volume_path(cls, instance: INodeWithContainerManager, container_name: str, path_in_container: str) -> str:
        """
            Return directory on the host that is mounted into the container to path `path_in_container`
        """
        container = cls.get_container(instance, container_name, raise_not_found_exc=False)
        if not container:
            return ''
        for mount in container.attrs.get('Mounts', []):
            if mount.get('Destination') == path_in_container:
                return mount.get('Source', '')
        return ''

    @classmethod
    def get_environ(cls, instance: INodeWithContainerManager, name: str) -> dict:
        container = cls.get_container(instance, name)

        def normalize(key: Any, value: Any = None) -> Tuple[Any, Any]:
            return (key, value, )
        return dict(normalize(*(item.split("=", 1))) for item in container.attrs["Config"]["Env"])

    @classmethod
    def register_container(cls, instance: INodeWithContainerManager, name: str, container: Container, replace: bool = False) -> None:
        assert name is not None, "None is not allowed as a container name"

        if container in instance._containers.values():
            raise ContainerAlreadyRegistered(f"{instance}: container `{container}' registered already")

        if replace or ContainerManager.get_container(instance, name, raise_not_found_exc=False) is None:
            instance._containers[name] = container
        else:
            raise ContainerAlreadyRegistered(f"{instance}: there is another container registered for `{name}'")

    @staticmethod
    def unregister_container(instance: INodeWithContainerManager, name: str) -> None:
        if name not in instance._containers:
            raise NotFound(f"{instance}: there is no container registered for name `{name}'")
        del instance._containers[name]
        LOGGER.debug("%s: container `%s' unregistered", instance, name)

    @classmethod
    def destroy_unregistered_containers(
            cls, instance: INodeWithContainerManager, docker_client: DockerClient = None) -> None:
        """Destroy, if any, containers that were created for the instance during previous test run(s)"""
        docker_client = docker_client or cls.default_docker_client
        filters = {
            "label": [
                f"TestId={instance.tags['TestId']}",
                f"Name={instance.name}",
            ]
        }
        containers = docker_client.containers.list(all=True, filters=filters)
        for container in containers:
            if container not in instance._containers.values():
                container.remove(v=True, force=True)
                LOGGER.debug(
                    "Container '%s' labeled with '%s' node is not registered for this node. It is likely a "
                    "leftover container from previous test run and was destroyed.", container.name, instance.name)

    @classmethod
    def build_with_progress_prints(cls, docker_client, image_tag, **kwargs):
        """
        build image while printing the progress to log

        this is using the low level api.build, and the logic
        is copied from images.build command, but with added logging
        so user following the log could see the progress as
        it happening
        """
        resp = docker_client.api.build(tag=image_tag, **kwargs)
        last_event = None
        image_id = None
        LOGGER.info(">>> Build log for Docker image `%s': >>>", image_tag)
        result_stream, internal_stream = itertools.tee(json_stream(resp))
        for chunk in internal_stream:
            if 'error' in chunk:
                raise BuildError(chunk['error'], result_stream)
            if 'stream' in chunk:
                LOGGER.info(chunk["stream"].rstrip())
                match = re.search(
                    r'(^Successfully built |sha256:)([0-9a-f]+)$',
                    chunk['stream']
                )
                if match:
                    image_id = match.group(2)
            last_event = chunk
        LOGGER.info("<<<")
        if image_id:
            return docker_client.images.get(image_id)
        else:
            raise BuildError(last_event or 'Unknown', result_stream)

    @classmethod
    def build_container_image(cls, instance: INodeWithContainerManager, name: str, **extra_build_args) -> Image:
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

        image = cls.build_with_progress_prints(docker_client=docker_client,
                                               image_tag=image_tag, **dockerfile_args, **build_args)
        return image

    @classmethod
    def _get_container_image_tag(cls, instance: INodeWithContainerManager, name: _Name) -> Optional[str]:
        return cls._get_attr_for_name(instance, name, "container_image_tag", name_only_lookup=True)

    @classmethod
    def _get_container_image_dockerfile_args(cls, instance: INodeWithContainerManager, name: _Name) -> Optional[dict]:
        return cls._get_attr_for_name(instance, name, "container_image_dockerfile_args", name_only_lookup=True)

    @staticmethod
    def _build_args(**kwargs):
        return kwargs

    @classmethod
    def get_containers_by_prefix(cls, prefix: str, docker_client: DockerClient = None) -> List[Container]:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.list(all=True, filters={"name": f'{prefix}*'})

    @classmethod
    def get_container_name_by_id(cls, c_id: str, docker_client: DockerClient = None) -> str:
        docker_client = docker_client or cls.default_docker_client
        return docker_client.containers.get(c_id).name

    @classmethod
    def pause_container(cls, instance: INodeWithContainerManager, name: str) -> None:
        cls.get_container(instance, name).pause()

    @classmethod
    def unpause_container(cls, instance: INodeWithContainerManager, name: str) -> None:
        cls.get_container(instance, name).unpause()


def running_in_docker():
    path = '/proc/self/cgroup'
    with open(path, encoding="utf-8") as cgroup:
        return (
            os.path.exists('/.dockerenv') or
            os.path.isfile(path) and any('docker' in line for line in cgroup)
        )


def running_in_podman():
    return os.environ.get('container') == 'podman'


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


@cache
def get_docker_hub_credentials() -> dict:
    LOGGER.debug("Get Docker Hub credentials")
    return KeyStore().get_docker_hub_credentials()


def docker_hub_login(remoter: CommandRunner, use_sudo: bool = False) -> None:
    """
    Logs into Docker Hub (if not already logged in) using credentials from the KeyStore.

    :param remoter: CommandRunner, command runner instance
    :param use_sudo: bool, whether to use sudo for remote commands. Optional, defaults to False.
    """
    remote_cmd = remoter.sudo if use_sudo else remoter.run
    docker_info = remote_cmd("docker info", ignore_status=True)
    if docker_info.failed:
        remoter.log.error("Can't get docker info, probably there is no running Docker daemon on the host")
        return
    if match := re.search(r"^\s+Username: (.+)$", docker_info.stdout, re.MULTILINE):
        remoter.log.debug("Docker daemon is already logged in as `%s'.", match.group(1))
        return
    if "Podman Engine" in remote_cmd("docker version", ignore_status=True).stdout:
        remoter.log.info("When Podman daemon is used we don't login")
        return
    if not os.environ.get('JENKINS_URL'):
        remoter.log.debug("not in jenkins, skipping login to docker hub")
        return
    docker_hub_creds = get_docker_hub_credentials()
    password_file = remote_cmd("mktemp").stdout.strip()
    if use_sudo:
        remote_cmd(f"chmod 644 '{password_file}'")

    with remote_file(remoter=remoter, remote_path=password_file, sudo=use_sudo) as fobj:
        fobj.write(docker_hub_creds["password"])
    remoter.log.debug("Login to Docker Hub as `%s'", docker_hub_creds["username"])
    remote_cmd(f"docker login --username {docker_hub_creds['username']} --password-stdin < '{password_file}'")
    remote_cmd(f"rm '{password_file}'")

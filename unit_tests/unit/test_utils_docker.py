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


from __future__ import absolute_import

import contextlib
import http.client
import io
import os
import socket
from collections import namedtuple
from types import SimpleNamespace
from unittest.mock import ANY, Mock, mock_open, patch, sentinel

import pytest

from sdcm.utils.docker_utils import (
    ContainerAlreadyRegistered,
    ContainerManager,
    DockerClient,
    DockerException,
    ImageNotFound,
    NotFound,
    NullResource,
    Retry,
    _Name,
)

# Docker sends 101 upgrade headers, then multiplexed frames on the same connection.
# http.client reads through an io.BufferedReader, so frame bytes can already be in that buffer.
HIJACK_HEADERS = (
    b"HTTP/1.1 101 UPGRADED\r\n"
    b"Content-Type: application/vnd.docker.multiplexed-stream\r\n"
    b"Connection: Upgrade\r\n"
    b"Upgrade: tcp\r\n"
    b"Api-Version: 1.49\r\n"
    b"\r\n"
)
# Frame header: stream id 1, three padding bytes, then the payload length as a big-endian
# uint32. The declared length has to match the payload, 0x10 for these 16 bytes.
MKTEMP_FRAME = b"\x01\x00\x00\x00\x00\x00\x00\x10" + b"/tmp/tmp.abcdef\n"

build_args = {}


class DummyDockerClient:
    """Docker client stub exposing api, containers and images."""

    class api:
        @staticmethod
        def build(*args, **kwargs):
            """Record the build args and return a successful build log."""
            build_args["123456"] = (args, kwargs)
            return """{"stream": "Successfully built 123456"}"""

    class containers:
        @staticmethod
        def list(*args, **kwargs):
            """Return the args it was called with."""
            return args, kwargs

        @staticmethod
        def get(c_id):
            """Return a container for deadbeef, raise NullResource or NotFound otherwise."""
            if c_id is None:
                raise NullResource("Resource ID was not provided")
            if c_id == "deadbeef":
                return namedtuple("_", "name")("deadbeef-blah")
            raise NotFound("No such container")

        @staticmethod
        def run(*args, **kwargs):
            """Return a DummyContainer that remembers the run args."""
            container = DummyContainer()
            container.run_args = args, kwargs
            return container

    class images:
        @staticmethod
        def build(*args, **kwargs):
            """Return the build args and a build log stream."""
            return (
                args,
                kwargs,
            ), [{"stream": "blah"}]

        @staticmethod
        def get(image_tag):
            """Return the build args recorded for tag 123456, raise ImageNotFound otherwise."""
            if image_tag == "123456":
                return build_args["123456"]

            raise ImageNotFound("not found")


class DummyContainer:
    """Container stub with a settable status."""

    _status = "exited"
    name = "dummy"
    client = sentinel.c1_docker_client

    @staticmethod
    def reload():
        """No-op reload."""

    @property
    def status(self):
        """Current status, see get_status."""
        return self.get_status()

    def get_status(self):
        """Return the status stub value."""
        return self._status

    @property
    def attrs(self):
        """Container attributes from get_attrs."""
        return self.get_attrs()

    @staticmethod
    def get_attrs():
        """Return empty container attributes."""
        return {}

    def rename(self, name):
        """Set the container name."""
        self.name = name

    @staticmethod
    def remove(*_, **__):
        """No-op remove."""

    @staticmethod
    def logs():
        """Return fixed log output."""
        return "container logs"

    @staticmethod
    def start():
        """No-op start."""


class DummyNode:
    """Node stub with container registry, tags and empty cluster params."""

    tags = {
        "key1": "value1",
    }

    def __init__(self):
        self._containers = {}
        # empty params, now part of ContainerManager api to allow access to SCT configuration
        self.parent_cluster = namedtuple("cluster", field_names="params")(params={})

    @staticmethod
    def c3_container_run_args(**kwargs):
        """Return kwargs with keys and values swapped."""
        return {v: k for k, v in kwargs.items()}


dummy_default_client = DummyDockerClient()


@pytest.fixture()
def default_docker_client(monkeypatch):
    """Make ContainerManager fall back to dummy_default_client when a node gives no docker client."""
    monkeypatch.setattr("sdcm.utils.docker_utils._default_docker_client", lambda: dummy_default_client)


@pytest.fixture()
def container():
    """Exited DummyContainer, registered as "c1" on the node fixture."""
    return DummyContainer()


@pytest.fixture()
def node(default_docker_client, container):
    """DummyNode with the container registered as "c1" and the default docker client patched."""
    dummy_node = DummyNode()
    dummy_node._containers["c1"] = container
    return dummy_node


@contextlib.contextmanager
def hijacked_response(frames: bytes):
    """Yield a response where headers and frames arrive in the same recv()."""
    server, client = socket.socketpair()
    try:
        server.sendall(HIJACK_HEADERS + frames)
        # signal EOF after the last frame
        server.shutdown(socket.SHUT_WR)
        http_response = http.client.HTTPResponse(client)
        http_response.begin()
        assert isinstance(http_response.fp, io.BufferedReader), "http.client must buffer the stream"

        yield SimpleNamespace(
            raw=SimpleNamespace(_fp=http_response),
            raise_for_status=lambda: None,
            close=lambda: None,
        )
    finally:
        client.close()
        server.close()


def test_name_none():
    """_Name(None) has no parts and is falsy."""
    name = _Name(None)
    assert name.full is None
    assert name.family is None
    assert name.member is None
    assert name.member_as_args == ()
    assert str(name) == "None"
    assert not name


def test_name_empty_string():
    """An empty name is its own empty family and is truthy."""
    name = _Name("")
    assert name.family == ""
    assert name.member is None
    assert name.member_as_args == ()
    assert str(name) == ""
    assert name  # Yep, empty name is True too.


def test_name_without_member():
    """A name without ':' is its own family with no member."""
    name = _Name("blah")
    assert name.full == "blah"
    assert name.family == "blah"
    assert name.member is None
    assert name.member_as_args == ()
    assert str(name) == "blah"
    assert name


def test_name_with_member():
    """A 'family:member' name splits into family and member."""
    name = _Name("foo:bar")
    assert name.full == "foo:bar"
    assert name.family == "foo"
    assert name.member == "bar"
    assert name.member_as_args == ("bar",)
    assert str(name) == "foo:bar"
    assert name


def test_get_docker_client_default(node):
    """Unknown container falls back to the default docker client."""
    assert ContainerManager.get_docker_client(node, "c2") == dummy_default_client


def test_get_docker_client_without_name(node):
    """Without a container name the node-wide docker_client is used."""
    node.None_docker_client = Mock()
    node.docker_client = sentinel.none_docker_client
    assert ContainerManager.get_docker_client(node) == sentinel.none_docker_client
    node.None_docker_client.assert_not_called()


def test_get_docker_client_from_existent_container(node):
    """A registered container returns its own docker client."""
    assert ContainerManager.get_docker_client(node, "c1") == sentinel.c1_docker_client


def test_get_docker_client_node_wide_none(node):
    """A None node-wide docker_client falls back to the default client."""
    node.docker_client = None
    assert ContainerManager.get_docker_client(node, "c2") == dummy_default_client


def test_get_docker_client_node_wide_not_callable(node):
    """A non-callable node-wide docker_client is returned as is."""
    node.docker_client = sentinel.node_property_docker_client
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.node_property_docker_client


def test_get_docker_client_node_wide_callable_returns_none(node):
    """A node-wide callable returning None falls back to the default client."""
    node.docker_client = Mock(return_value=None)
    assert ContainerManager.get_docker_client(node, "c2") == dummy_default_client
    node.docker_client.assert_called_once_with()


def test_get_docker_client_node_wide_callable(node):
    """A node-wide docker_client callable is called and its result returned."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.node_callable_docker_client
    node.docker_client.assert_called_once_with()


def test_get_docker_client_node_wide_callable_with_member(node):
    """The member part of the name does not change the node-wide call."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    assert ContainerManager.get_docker_client(node, "c2:blah") == sentinel.node_callable_docker_client
    node.docker_client.assert_called_once_with()


def test_get_docker_client_per_family_none(node):
    """A None per-family client falls through to the node-wide client."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    node.c2_docker_client = None
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.node_callable_docker_client
    node.docker_client.assert_called_once_with()


def test_get_docker_client_per_family_not_callable(node):
    """A non-callable per-family client wins over the node-wide one."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    node.c2_docker_client = sentinel.c2_property_docker_client
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.c2_property_docker_client
    node.docker_client.assert_not_called()


def test_get_docker_client_per_family_callable_returns_none(node):
    """A per-family callable returning None falls through to the node-wide client."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    node.c2_docker_client = Mock(return_value=None)
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.node_callable_docker_client
    node.c2_docker_client.assert_called_once_with()
    node.docker_client.assert_called_once_with()


def test_get_docker_client_per_family_callable(node):
    """A per-family callable wins over the node-wide client."""
    node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
    node.c2_docker_client = Mock(return_value=sentinel.c2_callable_docker_client)
    assert ContainerManager.get_docker_client(node, "c2") == sentinel.c2_callable_docker_client
    node.c2_docker_client.assert_called_once_with()
    node.docker_client.assert_not_called()


def test_get_docker_client_per_family_callable_with_member(node):
    """The per-family callable gets the member, and None results fall back to the default client."""
    node.docker_client = Mock(return_value=None)
    node.c2_docker_client = Mock(return_value=None)
    assert ContainerManager.get_docker_client(node, "c2:blah") == dummy_default_client
    node.c2_docker_client.assert_called_once_with("blah")
    node.docker_client.assert_called_once_with()


def test_get_container_existent(node, container):
    """get_container returns the registered container."""
    assert ContainerManager.get_container(node, "c1") is container


def test_get_container_nonexistent_no_exception(node):
    """get_container returns None for an unknown name when raise_not_found_exc is False."""
    assert ContainerManager.get_container(node, "c2", raise_not_found_exc=False) is None


def test_get_container_nonexistent_raises(node):
    """get_container raises NotFound for an unknown name by default."""
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c2")


def test_is_running_exited_container(node):
    """is_running is False for an exited container."""
    assert not ContainerManager.is_running(node, "c1")


def test_is_running_running_container(node, container):
    """is_running is True for a running container."""
    container._status = "running"
    assert ContainerManager.is_running(node, "c1")


def test_is_running_nonexistent_raises(node):
    """is_running raises NotFound for an unknown container."""
    with pytest.raises(NotFound):
        ContainerManager.is_running(node, "c2")


@patch("time.sleep", int)
def test_wait_for_status_nonexistent_raises(node):
    """wait_for_status raises NotFound for an unknown container."""
    with pytest.raises(NotFound):
        ContainerManager.wait_for_status(node, "c2", status="exited")


@patch("time.sleep", int)
def test_wait_for_status_running(node, container):
    """wait_for_status polls until the container is running."""
    statuses = iter(("exited", "exited", "running", "mark1"))
    container.get_status = lambda: next(statuses)
    ContainerManager.wait_for_status(node, "c1", status="running")
    assert next(statuses) == "mark1"


@patch("time.sleep", int)
def test_wait_for_status_exited(node, container):
    """wait_for_status polls until the container has exited."""
    statuses = iter(("running", "running", "exited", "mark2"))
    container.get_status = lambda: next(statuses)
    ContainerManager.wait_for_status(node, "c1", status="exited")
    assert next(statuses) == "mark2"


@patch("time.sleep", int)
def test_wait_for_status_too_many_retries(node, container):
    """wait_for_status raises Retry when the status never matches."""
    statuses = iter(("exited",) * 20)
    container.get_status = lambda: next(statuses)
    with pytest.raises(Retry):
        ContainerManager.wait_for_status(node, "c1", status="running")


@patch("time.sleep", int)
def test_get_ip_address(node, container):
    """get_ip_address raises for an unknown container, retries until an address appears, and raises Retry when none does."""
    no_ip_address = dict(NetworkSettings=dict(Networks=dict(bridge=dict(IPAddress=""))))
    ip_address = dict(NetworkSettings=dict(Networks=dict(bridge=dict(IPAddress="10.0.0.1"))))
    ip_addresses = iter(
        (
            no_ip_address,
            no_ip_address,
            ip_address,
            "mark",
        )
        + (no_ip_address,) * 10
    )

    def attrs():
        return next(ip_addresses)

    container.get_attrs = attrs

    # Try to get IP address of non-existent container
    with pytest.raises(NotFound):
        ContainerManager.get_ip_address(node, "c2")

    # Get IP address
    assert ContainerManager.get_ip_address(node, "c1") == "10.0.0.1"
    assert attrs() == "mark"

    # Test too many retries
    with pytest.raises(Retry):
        ContainerManager.get_ip_address(node, "c1")


def test_get_container_port(node, container):
    """get_container_port returns the host port, None when unpublished, and accepts int or str ports."""
    container.ports = {
        "9999/tcp": [
            {"HostIp": "0.0.0.0", "HostPort": 1111},
        ]
    }

    # Try to get port for non-existent container
    with pytest.raises(NotFound):
        ContainerManager.get_container_port(node, "c2", 9999)

    # No port
    assert ContainerManager.get_container_port(node, "c1", 8888) is None

    # Open port
    assert ContainerManager.get_container_port(node, "c1", 9999) == 1111
    assert ContainerManager.get_container_port(node, "c1", "9999") == 1111


def test_get_environ(node, container):
    """get_environ parses container env into a dict, with None for bare names."""
    container.get_attrs = lambda: {
        "Config": {
            "Env": [
                "A",
                "B=1",
            ]
        }
    }

    # Try to get env for non-existent container
    with pytest.raises(NotFound):
        ContainerManager.get_environ(node, "c2")

    # Get env for existent container
    assert ContainerManager.get_environ(node, "c1") == {"A": None, "B": "1"}


@pytest.mark.usefixtures("default_docker_client")
def test_get_containers_by_prefix():
    """get_containers_by_prefix lists all containers filtered by name prefix."""
    assert ContainerManager.get_containers_by_prefix("blah") == (
        (),
        {
            "all": True,
            "filters": {"name": "blah*"},
        },
    )


@pytest.mark.usefixtures("default_docker_client")
def test_get_container_name_by_id():
    """get_container_name_by_id returns the name and raises NotFound for an unknown id."""
    # Try to get name of non-existent container
    with pytest.raises(NotFound):
        ContainerManager.get_container_name_by_id("not_found")

    # Get name by id
    assert ContainerManager.get_container_name_by_id("deadbeef") == "deadbeef-blah"


def test_run_container_existing(node, container):
    """run_container on a registered name starts and returns the same container."""
    assert ContainerManager.run_container(node, "c1") == container


def test_run_container_no_hook(node):
    """run_container creates and registers a container with node tags and the given args."""
    container2 = ContainerManager.run_container(node, "c2", arg1="value1", arg2="value2")
    assert container2.run_args == (
        (),
        {
            "detach": True,
            "labels": node.tags,
            "arg1": "value1",
            "arg2": "value2",
        },
    )
    assert ContainerManager.get_container(node, "c2") == container2


def test_run_container_no_hook_with_member(node):
    """Names with different members register separate containers."""
    container2 = ContainerManager.run_container(node, "c2", arg1="value1", arg2="value2")
    c2another = ContainerManager.run_container(node, "c2:another", arg1="value1", arg2="value2")
    assert container2 != c2another
    assert c2another.run_args == (
        (),
        {
            "detach": True,
            "labels": node.tags,
            "arg1": "value1",
            "arg2": "value2",
        },
    )
    assert ContainerManager.get_container(node, "c2") == container2
    assert ContainerManager.get_container(node, "c2:another") == c2another


def test_run_container_with_hook(node):
    """run_container applies the *_container_run_args hook to the args."""
    container3 = ContainerManager.run_container(node, "c3", blah="name")
    assert container3.run_args == (
        (),
        {
            "detach": True,
            "labels": node.tags,
            "name": "blah",
        },
    )
    assert ContainerManager.get_container(node, "c3") == container3


def test_run_container_with_hook_and_member(node):
    """The run args hook receives the member part of the name."""
    c3another = ContainerManager.run_container(node, "c3:another", blah="name")
    assert c3another.run_args == (
        (),
        {
            "detach": True,
            "labels": node.tags,
            "name": "blah-another",
        },
    )
    assert ContainerManager.get_container(node, "c3:another") == c3another


def test_rerun_existing_container(node, container):
    """run_container on an existing container starts it again."""
    container.start = Mock()
    ContainerManager.run_container(node, "c1")
    container.start.assert_called_once_with()


def test_set_container_keep_alive_nonexistent_raises(node):
    """set_container_keep_alive raises NotFound for an unknown container."""
    with pytest.raises(NotFound):
        ContainerManager.set_container_keep_alive(node, "c2")


def test_set_container_keep_alive_sets_name(node, container):
    """set_container_keep_alive renames the container with the keep-alive suffix."""
    ContainerManager.set_container_keep_alive(node, "c1")
    assert container.name == "dummy---KEEPALIVE"


def test_set_container_keep_alive_idempotent(node, container):
    """Setting keep-alive twice keeps a single suffix."""
    ContainerManager.set_container_keep_alive(node, "c1")
    ContainerManager.set_container_keep_alive(node, "c1")
    assert container.name == "dummy---KEEPALIVE"


def test_set_all_containers_keep_alive(node, container):
    """set_all_containers_keep_alive marks every node container."""
    ContainerManager.set_all_containers_keep_alive(node)
    assert container.name == "dummy---KEEPALIVE"


def test_ssh_copy_id_nonexistent_raises(node):
    """ssh_copy_id raises NotFound for an unknown container."""
    with pytest.raises(NotFound):
        ContainerManager.ssh_copy_id(node, "c2", user="root", key_file="/root/.ssh/id_rsa")


def test_ssh_copy_id_success(node, container):
    """ssh_copy_id appends the public key to authorized_keys as the given user."""
    container.exec_run = Mock()
    container.exec_run.return_value.exit_code = 0

    with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
        rsa_key_mock.return_value.get_base64.return_value = "0123456789"
        ContainerManager.ssh_copy_id(node, "c1", user=sentinel.ssh_user, key_file="~/lala.key")

    rsa_key_mock.assert_called_once_with(os.path.expanduser("~/lala.key"))
    rsa_key_mock.return_value.get_base64.assert_called_once_with()
    container.exec_run.assert_called_once_with(["sh", "-c", ANY], user=sentinel.ssh_user)

    assert " 0123456789 " in " ".join(container.exec_run.call_args[0][0])
    assert container.exec_run.call_args[1]["user"] == sentinel.ssh_user


def test_ssh_copy_id_exec_failure_raises(node, container):
    """ssh_copy_id raises DockerException with the exec output when it fails."""
    container.exec_run = Mock()
    container.exec_run.return_value.exit_code = 126
    container.exec_run.return_value.output = b"blah"

    with patch("paramiko.rsakey.RSAKey.from_private_key_file"), pytest.raises(DockerException, match="blah"):
        ContainerManager.ssh_copy_id(
            node,
            "c1",
            user=sentinel.ssh_user,
            key_file="lala.key",
        )


def test_register_container(node, container):
    """register_container refuses to overwrite unless replace is set, and tracks the new container."""
    c2_container = DummyContainer()

    # Try to register registered container
    with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
        ContainerManager.register_container(
            node,
            "c2",
            container=container,
        )

    # Try to register registered container with replace=True
    with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
        ContainerManager.register_container(
            node,
            "c2",
            container=container,
            replace=True,
        )

    # Try to replace container
    with pytest.raises(ContainerAlreadyRegistered, match="another container registered"):
        ContainerManager.register_container(
            node,
            "c1",
            container=c2_container,
        )

    # Force container replacement
    ContainerManager.register_container(node, "c1", container=c2_container, replace=True)
    assert ContainerManager.get_container(node, "c1") == c2_container

    # Register container
    ContainerManager.register_container(node, "c2", container=container)
    assert ContainerManager.get_container(node, "c2") == container

    # Try to force container replacement with container registered with another name
    with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
        ContainerManager.register_container(
            node,
            "c1",
            container=container,
            replace=True,
        )


def test_unregister_container(node):
    """unregister_container removes the name and raises NotFound for unknown names."""
    # Unregister non-existent name
    with pytest.raises(NotFound):
        ContainerManager.unregister_container(node, "c2")

    # Unregister container
    ContainerManager.unregister_container(node, "c1")
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")


def test_destroy(node):
    """destroy_container removes a container and returns True, then raises NotFound."""
    # Try to destroy non-existent container
    with pytest.raises(NotFound):
        ContainerManager.destroy_container(node, "c2")

    # without *_container_logfile hook
    assert ContainerManager.destroy_container(node, "c1")
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")
    with pytest.raises(NotFound):
        ContainerManager.destroy_container(node, "c1")


def test_build_container_image(node):
    """build_container_image validates tag and Dockerfile args, then builds with node tags."""
    # Try to build image for existent container
    with pytest.raises(ContainerAlreadyRegistered):
        ContainerManager.build_container_image(node, "c1")

    # Node-wide container image tag
    node.container_image_tag = Mock(return_value="blah")
    with pytest.raises(AssertionError, match="image tag"):
        ContainerManager.build_container_image(node, "c2")

    # Node-wide dockerfile args
    node.c2_container_image_tag = Mock(return_value="hello-world:latest")
    node.container_image_dockerfile_args = Mock(return_value="blah")
    with pytest.raises(AssertionError, match="Dockerfile"):
        ContainerManager.build_container_image(node, "c2")
    node.c2_container_image_tag.assert_called_once_with()

    # No build args
    node.c2_container_image_tag.reset_mock()
    node.c2_container_image_dockerfile_args = Mock(return_value={"path": "."})
    image = ContainerManager.build_container_image(node, "c2:another", arg1="value1", arg2="value2")
    assert image == (
        (),
        dict(
            tag="hello-world:latest",
            path=".",
            labels=node.tags,
            pull=True,
            rm=True,
            arg1="value1",
            arg2="value2",
        ),
    )
    node.c2_container_image_tag.assert_called_once_with("another")
    node.c2_container_image_dockerfile_args.assert_called_once_with("another")

    # No build args
    node.c2_container_image_tag.reset_mock()
    node.c2_container_image_dockerfile_args.reset_mock()
    node.c2_container_image_build_args = lambda **kw: {v: k for k, v in kw.items()}
    image = ContainerManager.build_container_image(node, "c2:another", arg1="value1", arg2="value2")
    assert image == (
        (),
        dict(
            tag="hello-world:latest",
            path=".",
            labels=node.tags,
            pull=True,
            rm=True,
            value1="arg1",
            value2="arg2",
        ),
    )
    node.c2_container_image_tag.assert_called_once_with("another")
    node.c2_container_image_dockerfile_args.assert_called_once_with("another")


def test_destroy_logfile(node):
    """destroy_container writes container logs to the *_container_logfile before removing it."""
    node.c1_container_logfile = "container.log"

    with patch("builtins.open", mock_open()) as mock_file:
        assert ContainerManager.destroy_container(node, "c1")

    mock_file.assert_called_once_with("container.log", "ab")
    mock_file().write.assert_called_once_with("container logs")

    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")
    with pytest.raises(NotFound):
        ContainerManager.destroy_container(node, "c1")


def test_destroy_logfile_callable(node):
    """A callable *_container_logfile receives the member and picks the log file name."""
    members = []

    def logfile(member=None):
        members.append(member)
        return member

    node.c1_container_logfile = logfile

    c1another = ContainerManager.run_container(node, "c1:another")
    ContainerManager.set_container_keep_alive(node, "c1:another")

    with patch("builtins.open", mock_open()) as mock_file:
        assert ContainerManager.destroy_container(node, "c1")
        assert not ContainerManager.destroy_container(node, "c1:another")

    mock_file.assert_called_once_with("another", "ab")
    mock_file().write.assert_called_once_with("container logs")

    assert members == [
        None,
        "another",
    ]
    assert ContainerManager.get_container(node, "c1:another") == c1another
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")
    with pytest.raises(NotFound):
        ContainerManager.destroy_container(node, "c1")


def test_destroy_keep_alive(node, container):
    """destroy_container skips keep-alive containers unless ignore_keepalive is set."""
    ContainerManager.set_container_keep_alive(node, "c1")

    # Try to destroy container with keep-alive tag
    assert not ContainerManager.destroy_container(node, "c1")
    assert ContainerManager.get_container(node, "c1") == container

    # Ignore keep-alive tag
    assert ContainerManager.destroy_container(node, "c1", ignore_keepalive=True)
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")


def test_destroy_all(node, container):
    """destroy_all_containers removes non keep-alive containers, and all with ignore_keepalive."""
    ContainerManager.run_container(node, "c2")
    ContainerManager.set_container_keep_alive(node, "c1")

    # Destroy all containers without keep-alive tag
    ContainerManager.destroy_all_containers(node)
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c2")
    assert ContainerManager.get_container(node, "c1") == container

    # Ignore keep-alive tag
    ContainerManager.destroy_all_containers(node, ignore_keepalive=True)
    with pytest.raises(NotFound):
        ContainerManager.get_container(node, "c1")


@pytest.mark.usefixtures("default_docker_client")
def test_destroy_unregistered_containers():
    """destroy_unregistered_containers removes labeled containers the node does not register."""
    tags = {"TestId": "test123", "Name": "nodeA"}

    r_c1, r_c2, nr_c = [DummyContainer() for _ in range(3)]
    r_c1.get_attrs = lambda: {"Config": {"Labels": {}}}
    r_c2.get_attrs = nr_c.get_attrs = lambda: {"Config": {"Labels": tags}}

    node = DummyNode()
    node.tags, node.name = tags, tags["Name"]
    node._containers["r_c1"] = r_c1  # register not-labeled container for the node
    node._containers["r_c2"] = r_c2  # register labeled container for the node

    def stub_list(*args, **kwargs):
        filters = kwargs.get("filters", {})
        filter_dict = {}
        for label in filters["label"]:
            key, val = label.split("=")
            filter_dict[key] = val

        filtered_containers = []
        for container in (r_c1, r_c2, nr_c):
            container_labels = container.attrs["Config"]["Labels"]
            if all(container_labels.get(key) == val for key, val in filter_dict.items()):
                filtered_containers.append(container)

        return filtered_containers

    r_c1.remove, r_c2.remove, nr_c.remove = Mock(), Mock(), Mock()
    # temporarily patch dummy default_docker_client.containers.list to be able to filter containers by labels
    with patch.object(dummy_default_client.containers, "list", new=stub_list):
        ContainerManager.destroy_unregistered_containers(node)

    # Not-registered labeled container destroyed
    nr_c.remove.assert_called_once_with(v=True, force=True)

    # Registered labeled container not destroyed
    r_c2.remove.assert_not_called()

    # Registered not labeled container not destroyed
    r_c2.remove.assert_not_called()


def test_frame_buffered_with_the_headers_is_not_lost():
    """Ensure a frame buffered with 101 headers is still read."""
    docker_client = DockerClient(base_url="unix:///var/run/docker.sock", version="1.49")

    with hijacked_response(MKTEMP_FRAME) as response:
        stdout, stderr = docker_client.api._read_from_socket(response, stream=False, tty=False, demux=True)

    assert stdout == b"/tmp/tmp.abcdef\n"
    assert stderr is None

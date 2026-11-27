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
from unittest.mock import Mock, mock_open, patch, sentinel

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
    """Docker client stand-in whose api, containers and images return their call arguments or canned objects."""

    class api:
        @staticmethod
        def build(*args, **kwargs):
            """Record the build arguments under image id 123456 and return a successful build stream."""
            build_args["123456"] = (args, kwargs)
            return """{"stream": "Successfully built 123456"}"""

    class containers:
        @staticmethod
        def list(*args, **kwargs):
            """Return the positional and keyword arguments of the call."""
            return args, kwargs

        @staticmethod
        def get(c_id):
            """Return a named container for 'deadbeef', raising NullResource for None and NotFound otherwise."""
            if c_id is None:
                raise NullResource("Resource ID was not provided")
            if c_id == "deadbeef":
                return namedtuple("_", "name")("deadbeef-blah")
            raise NotFound("No such container")

        @staticmethod
        def run(*args, **kwargs):
            """Return a new DummyContainer carrying the arguments of the call."""
            container = DummyContainer()
            container.run_args = args, kwargs
            return container

    class images:
        @staticmethod
        def build(*args, **kwargs):
            """Return the arguments of the call and a one-entry build log."""
            return (
                args,
                kwargs,
            ), [{"stream": "blah"}]

        @staticmethod
        def get(image_tag):
            """Return the recorded build arguments for image 123456, raising ImageNotFound for any other tag."""
            if image_tag == "123456":
                return build_args["123456"]

            raise ImageNotFound("not found")


class DummyContainer:
    """Docker container stand-in, exited by default, with an overridable status and attrs."""

    _status = "exited"
    name = "dummy"
    client = sentinel.c1_docker_client

    @staticmethod
    def reload():
        """Stub: accept the reload and return immediately."""

    @property
    def status(self):
        """Return the container status via get_status()."""
        return self.get_status()

    def get_status(self):
        """Return the current fake status."""
        return self._status

    @property
    def attrs(self):
        """Return the container attrs via get_attrs()."""
        return self.get_attrs()

    @staticmethod
    def get_attrs():
        """Return empty container attrs."""
        return {}

    def rename(self, name):
        """Rename the container."""
        self.name = name

    @staticmethod
    def remove(*_, **__):
        """Accept the container removal."""

    @staticmethod
    def logs():
        """Return fixed container logs."""
        return "container logs"

    @staticmethod
    def start():
        """Accept the container start."""


class DummyNode:
    """Node stand-in with tags, an empty container registry and empty SCT params."""

    tags = {
        "key1": "value1",
    }

    def __init__(self):
        self._containers = {}
        # empty params, now part of ContainerManager api to allow access to SCT configuration
        self.parent_cluster = namedtuple("cluster", field_names="params")(params={})

    @staticmethod
    def c3_container_run_args(**kwargs):
        """Run-args hook for the c3 container family that swaps keys and values of the given kwargs."""
        return {v: k for k, v in kwargs.items()}


class TestName:
    """Parsing of container names in the 'family:member' format."""

    def test_none(self):
        """A None name has no full name, family or member, and is falsy."""
        name = _Name(None)
        assert name.full is None
        assert name.family is None
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == "None"
        assert not name

    def test_empty_name(self):
        """An empty name has an empty family, no member, and is truthy."""
        name = _Name("")
        assert name.family == ""
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == ""
        assert name  # Yep, empty name is True too.

    def test_name_without_member(self):
        """A name without ':' is its own family with no member."""
        name = _Name("blah")
        assert name.full == "blah"
        assert name.family == "blah"
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == "blah"
        assert name

    def test_name_with_member(self):
        """A 'foo:bar' name splits into family 'foo' and member 'bar'."""
        name = _Name("foo:bar")
        assert name.full == "foo:bar"
        assert name.family == "foo"
        assert name.member == "bar"
        assert name.member_as_args == ("bar",)
        assert str(name) == "foo:bar"
        assert name


dummy_default_client = DummyDockerClient()


@patch("sdcm.utils.docker_utils._default_docker_client", new=lambda: dummy_default_client)
class TestContainerManager:
    """Tests for ContainerManager docker client resolution.

    ContainerManager resolves docker clients by checking (in order):
    per-container, per-family, then node-wide attributes.  When none of
    these provide a client (attribute missing, None, or callable returning
    None), the manager falls back to ``dummy_default_client`` — the patched
    module-level default.  Tests that assert ``== dummy_default_client``
    verify this fallback behaviour.
    """

    def setup_method(self) -> None:
        """Create a node with an exited container registered as 'c1'."""
        self.node = DummyNode()
        self.node._containers["c1"] = self.container = DummyContainer()

    def test_get_docker_client_default(self):
        """An unknown container with no client attributes on the node gets the default docker client."""
        assert ContainerManager.get_docker_client(self.node, "c2") == dummy_default_client

    def test_get_docker_client_without_name(self):
        """Without a container name, the node-wide docker_client is used."""
        self.node.None_docker_client = Mock()
        self.node.docker_client = sentinel.none_docker_client
        assert ContainerManager.get_docker_client(self.node) == sentinel.none_docker_client
        self.node.None_docker_client.assert_not_called()

    def test_get_docker_client_from_existent_container(self):
        """A registered container's own docker client is returned."""
        assert ContainerManager.get_docker_client(self.node, "c1") == sentinel.c1_docker_client

    def test_get_docker_client_node_wide_none(self):
        """A None node-wide docker_client falls back to the default client."""
        self.node.docker_client = None
        assert ContainerManager.get_docker_client(self.node, "c2") == dummy_default_client

    def test_get_docker_client_node_wide_not_callable(self):
        """A non-callable node-wide docker_client is returned as is."""
        self.node.docker_client = sentinel.node_property_docker_client
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_property_docker_client

    def test_get_docker_client_node_wide_callable_returns_none(self):
        """A node-wide docker_client callable returning None falls back to the default client."""
        self.node.docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2") == dummy_default_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_node_wide_callable(self):
        """A node-wide docker_client callable is called and its result returned."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_node_wide_callable_with_member(self):
        """A node-wide docker_client callable is called without the member name."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2:blah") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_none(self):
        """A None per-family client falls back to the node-wide docker_client."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = None
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_not_callable(self):
        """A non-callable per-family client wins over the node-wide one."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = sentinel.c2_property_docker_client
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.c2_property_docker_client
        self.node.docker_client.assert_not_called()

    def test_get_docker_client_per_family_callable_returns_none(self):
        """A per-family callable returning None falls back to the node-wide docker_client."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.c2_docker_client.assert_called_once_with()
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_callable(self):
        """A per-family callable's result wins over the node-wide docker_client."""
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = Mock(return_value=sentinel.c2_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.c2_callable_docker_client
        self.node.c2_docker_client.assert_called_once_with()
        self.node.docker_client.assert_not_called()

    def test_get_docker_client_per_family_callable_with_member(self):
        """A per-family callable receives the member name; with every hook returning None the default client is used."""
        self.node.docker_client = Mock(return_value=None)
        self.node.c2_docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2:blah") == dummy_default_client
        self.node.c2_docker_client.assert_called_once_with("blah")
        self.node.docker_client.assert_called_once_with()

    def test_get_container_existent(self):
        """get_container returns the registered container."""
        assert ContainerManager.get_container(self.node, "c1") is self.container

    def test_get_container_nonexistent_no_exception(self):
        """get_container returns None for an unknown name when raise_not_found_exc=False."""
        assert ContainerManager.get_container(self.node, "c2", raise_not_found_exc=False) is None

    def test_get_container_nonexistent_raises(self):
        """get_container raises NotFound for an unknown name."""
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c2")

    def test_is_running_exited_container(self):
        """is_running is False for an exited container."""
        assert not ContainerManager.is_running(self.node, "c1")

    def test_is_running_running_container(self):
        """is_running is True for a running container."""
        self.container._status = "running"
        assert ContainerManager.is_running(self.node, "c1")

    def test_is_running_nonexistent_raises(self):
        """is_running raises NotFound for an unknown container."""
        with pytest.raises(NotFound):
            ContainerManager.is_running(self.node, "c2")

    @patch("time.sleep", int)
    def test_wait_for_status_nonexistent_raises(self):
        """wait_for_status raises NotFound for an unknown container."""
        with pytest.raises(NotFound):
            ContainerManager.wait_for_status(self.node, "c2", status="exited")

    @patch("time.sleep", int)
    def test_wait_for_status_running(self):
        """wait_for_status polls until the container reaches 'running', then stops."""
        statuses = iter(("exited", "exited", "running", "mark1"))
        self.container.get_status = lambda: next(statuses)
        ContainerManager.wait_for_status(self.node, "c1", status="running")
        assert next(statuses) == "mark1"

    @patch("time.sleep", int)
    def test_wait_for_status_exited(self):
        """wait_for_status polls until the container reaches 'exited', then stops."""
        statuses = iter(("running", "running", "exited", "mark2"))
        self.container.get_status = lambda: next(statuses)
        ContainerManager.wait_for_status(self.node, "c1", status="exited")
        assert next(statuses) == "mark2"

    @patch("time.sleep", int)
    def test_wait_for_status_too_many_retries(self):
        """wait_for_status raises Retry when the status never reaches the requested one."""
        statuses = iter(("exited",) * 20)
        self.container.get_status = lambda: next(statuses)
        with pytest.raises(Retry):
            ContainerManager.wait_for_status(self.node, "c1", status="running")

    @patch("time.sleep", int)
    def test_get_ip_address(self):
        """get_ip_address raises NotFound for unknown containers, polls until an IP shows, and raises Retry on none."""
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

        self.container.get_attrs = attrs

        # Try to get IP address of non-existent container
        with pytest.raises(NotFound):
            ContainerManager.get_ip_address(self.node, "c2")

        # Get IP address
        assert ContainerManager.get_ip_address(self.node, "c1") == "10.0.0.1"
        assert attrs() == "mark"

        # Test too many retries
        with pytest.raises(Retry):
            ContainerManager.get_ip_address(self.node, "c1")

    def test_get_container_port(self):
        """get_container_port maps a container port, int or str, to its host port, or None when unpublished."""
        self.container.ports = {
            "9999/tcp": [
                {"HostIp": "0.0.0.0", "HostPort": 1111},
            ]
        }

        # Try to get port for non-existent container
        with pytest.raises(NotFound):
            ContainerManager.get_container_port(self.node, "c2", 9999)

        # No port
        assert ContainerManager.get_container_port(self.node, "c1", 8888) is None

        # Open port
        assert ContainerManager.get_container_port(self.node, "c1", 9999) == 1111
        assert ContainerManager.get_container_port(self.node, "c1", "9999") == 1111

    def test_get_environ(self):
        """get_environ parses the container env into a dict, with None for variables without a value."""
        self.container.get_attrs = lambda: {
            "Config": {
                "Env": [
                    "A",
                    "B=1",
                ]
            }
        }

        # Try to get env for non-existent container
        with pytest.raises(NotFound):
            ContainerManager.get_environ(self.node, "c2")

        # Get env for existent container
        assert ContainerManager.get_environ(self.node, "c1") == {"A": None, "B": "1"}

    def test_get_containers_by_prefix(self):
        """get_containers_by_prefix lists all containers whose name matches the prefix."""
        assert ContainerManager.get_containers_by_prefix("blah") == (
            (),
            {
                "all": True,
                "filters": {"name": "blah*"},
            },
        )

    def test_get_container_name_by_id(self):
        """get_container_name_by_id returns the container name, raising NotFound for an unknown id."""
        # Try to get name of non-existent container
        with pytest.raises(NotFound):
            ContainerManager.get_container_name_by_id("not_found")

        # Get name by id
        assert ContainerManager.get_container_name_by_id("deadbeef") == "deadbeef-blah"

    def test_run_container_existing(self):
        """run_container returns the already registered container."""
        assert ContainerManager.run_container(self.node, "c1") == self.container

    def test_run_container_no_hook(self):
        """run_container without a run-args hook starts a detached container labeled with node tags and registers it."""
        container2 = ContainerManager.run_container(self.node, "c2", arg1="value1", arg2="value2")
        assert container2.run_args == (
            (),
            {
                "detach": True,
                "labels": self.node.tags,
                "arg1": "value1",
                "arg2": "value2",
            },
        )
        assert ContainerManager.get_container(self.node, "c2") == container2

    def test_run_container_no_hook_with_member(self):
        """A 'family:member' container runs and registers separately from its family's base container."""
        container2 = ContainerManager.run_container(self.node, "c2", arg1="value1", arg2="value2")
        c2another = ContainerManager.run_container(self.node, "c2:another", arg1="value1", arg2="value2")
        assert container2 != c2another
        assert c2another.run_args == (
            (),
            {
                "detach": True,
                "labels": self.node.tags,
                "arg1": "value1",
                "arg2": "value2",
            },
        )
        assert ContainerManager.get_container(self.node, "c2") == container2
        assert ContainerManager.get_container(self.node, "c2:another") == c2another

    def test_run_container_with_hook(self):
        """The family's run-args hook transforms the kwargs passed to the container run."""
        container3 = ContainerManager.run_container(self.node, "c3", blah="name")
        assert container3.run_args == (
            (),
            {
                "detach": True,
                "labels": self.node.tags,
                "name": "blah",
            },
        )
        assert ContainerManager.get_container(self.node, "c3") == container3

    def test_run_container_with_hook_and_member(self):
        """The run-args hook result gets the member name appended to the container name."""
        c3another = ContainerManager.run_container(self.node, "c3:another", blah="name")
        assert c3another.run_args == (
            (),
            {
                "detach": True,
                "labels": self.node.tags,
                "name": "blah-another",
            },
        )
        assert ContainerManager.get_container(self.node, "c3:another") == c3another

    def test_rerun_existing_container(self):
        """run_container starts an already registered container."""
        self.container.start = Mock()
        ContainerManager.run_container(self.node, "c1")
        self.container.start.assert_called_once_with()

    def test_set_container_keep_alive_nonexistent_raises(self):
        """set_container_keep_alive raises NotFound for an unknown container."""
        with pytest.raises(NotFound):
            ContainerManager.set_container_keep_alive(self.node, "c2")

    def test_set_container_keep_alive_sets_name(self):
        """set_container_keep_alive appends the ---KEEPALIVE suffix to the container name."""
        ContainerManager.set_container_keep_alive(self.node, "c1")
        assert self.container.name == "dummy---KEEPALIVE"

    def test_set_container_keep_alive_idempotent(self):
        """set_container_keep_alive appends the ---KEEPALIVE suffix only once."""
        ContainerManager.set_container_keep_alive(self.node, "c1")
        ContainerManager.set_container_keep_alive(self.node, "c1")
        assert self.container.name == "dummy---KEEPALIVE"

    def test_set_all_containers_keep_alive(self):
        """set_all_containers_keep_alive marks the registered container as keep-alive."""
        ContainerManager.set_all_containers_keep_alive(self.node)
        assert self.container.name == "dummy---KEEPALIVE"

    def test_ssh_copy_id_nonexistent_raises(self):
        """ssh_copy_id raises NotFound for an unknown container."""
        with pytest.raises(NotFound):
            ContainerManager.ssh_copy_id(self.node, "c2", user="root", key_file="/root/.ssh/id_rsa")

    def test_ssh_copy_id_success(self):
        """ssh_copy_id adds the key file's public key in the container as the given user."""
        self.container.exec_run = Mock()
        self.container.exec_run.return_value.exit_code = 0

        with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
            rsa_key_mock.return_value.get_base64.return_value = "0123456789"
            ContainerManager.ssh_copy_id(self.node, "c1", user=sentinel.ssh_user, key_file="~/lala.key")

        rsa_key_mock.assert_called_once_with(os.path.expanduser("~/lala.key"))
        rsa_key_mock.return_value.get_base64.assert_called_once_with()
        self.container.exec_run.assert_called_once()

        assert " 0123456789 " in " ".join(self.container.exec_run.call_args[0][0])
        assert self.container.exec_run.call_args[1]["user"] == sentinel.ssh_user

    def test_ssh_copy_id_exec_failure_raises(self):
        """A failing exec in ssh_copy_id raises DockerException with the command output."""
        self.container.exec_run = Mock()
        self.container.exec_run.return_value.exit_code = 126
        self.container.exec_run.return_value.output = b"blah"

        with patch("paramiko.rsakey.RSAKey.from_private_key_file"):
            with pytest.raises(DockerException, match="blah"):
                ContainerManager.ssh_copy_id(
                    self.node,
                    "c1",
                    user=sentinel.ssh_user,
                    key_file="lala.key",
                )

    def test_register_container(self):
        """register_container rejects a container already registered under any name, or a taken name.

        With replace=True, a container registered nowhere else replaces the one under a taken name.
        """
        c2_container = DummyContainer()

        # Try to register registered container
        with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
            ContainerManager.register_container(
                self.node,
                "c2",
                container=self.container,
            )

        # Try to register registered container with replace=True
        with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
            ContainerManager.register_container(
                self.node,
                "c2",
                container=self.container,
                replace=True,
            )

        # Try to replace container
        with pytest.raises(ContainerAlreadyRegistered, match="another container registered"):
            ContainerManager.register_container(
                self.node,
                "c1",
                container=c2_container,
            )

        # Force container replacement
        ContainerManager.register_container(self.node, "c1", container=c2_container, replace=True)
        assert ContainerManager.get_container(self.node, "c1") == c2_container

        # Register container
        ContainerManager.register_container(self.node, "c2", container=self.container)
        assert ContainerManager.get_container(self.node, "c2") == self.container

        # Try to force container replacement with container registered with another name
        with pytest.raises(ContainerAlreadyRegistered, match="container .* registered already"):
            ContainerManager.register_container(
                self.node,
                "c1",
                container=self.container,
                replace=True,
            )

    def test_unregister_container(self):
        """unregister_container removes the name from the registry, raising NotFound for an unknown name."""
        # Unregister non-existent name
        with pytest.raises(NotFound):
            ContainerManager.unregister_container(self.node, "c2")

        # Unregister container
        ContainerManager.unregister_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")

    def test_destroy(self):
        """destroy_container removes and unregisters the container, raising NotFound for an unknown name."""
        # Try to destroy non-existent container
        with pytest.raises(NotFound):
            ContainerManager.destroy_container(self.node, "c2")

        # without *_container_logfile hook
        assert ContainerManager.destroy_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.destroy_container(self.node, "c1")

    def test_build_container_image(self):
        """build_container_image needs a per-family image tag and Dockerfile args.

        It builds with node tags as labels, passing kwargs as is or through the family build-args hook.
        """
        # Try to build image for existent container
        with pytest.raises(ContainerAlreadyRegistered):
            ContainerManager.build_container_image(self.node, "c1")

        # Node-wide container image tag
        self.node.container_image_tag = Mock(return_value="blah")
        with pytest.raises(AssertionError, match="image tag"):
            ContainerManager.build_container_image(self.node, "c2")

        # Node-wide dockerfile args
        self.node.c2_container_image_tag = Mock(return_value="hello-world:latest")
        self.node.container_image_dockerfile_args = Mock(return_value="blah")
        with pytest.raises(AssertionError, match="Dockerfile"):
            ContainerManager.build_container_image(self.node, "c2")
        self.node.c2_container_image_tag.assert_called_once_with()

        # No build args
        self.node.c2_container_image_tag.reset_mock()
        self.node.c2_container_image_dockerfile_args = Mock(return_value={"path": "."})
        image = ContainerManager.build_container_image(self.node, "c2:another", arg1="value1", arg2="value2")
        assert image == (
            (),
            dict(
                tag="hello-world:latest",
                path=".",
                labels=self.node.tags,
                pull=True,
                rm=True,
                arg1="value1",
                arg2="value2",
            ),
        )
        self.node.c2_container_image_tag.assert_called_once_with("another")
        self.node.c2_container_image_dockerfile_args.assert_called_once_with("another")

        # No build args
        self.node.c2_container_image_tag.reset_mock()
        self.node.c2_container_image_dockerfile_args.reset_mock()
        self.node.c2_container_image_build_args = lambda **kw: {v: k for k, v in kw.items()}
        image = ContainerManager.build_container_image(self.node, "c2:another", arg1="value1", arg2="value2")
        assert image == (
            (),
            dict(
                tag="hello-world:latest",
                path=".",
                labels=self.node.tags,
                pull=True,
                rm=True,
                value1="arg1",
                value2="arg2",
            ),
        )
        self.node.c2_container_image_tag.assert_called_once_with("another")
        self.node.c2_container_image_dockerfile_args.assert_called_once_with("another")

    def test_destroy_logfile(self):
        """destroy_container appends the container logs to the family's logfile before removing it."""
        self.node.c1_container_logfile = "container.log"

        with patch("builtins.open", mock_open()) as mock_file:
            assert ContainerManager.destroy_container(self.node, "c1")

        mock_file.assert_called_once_with("container.log", "ab")
        mock_file().write.assert_called_once_with("container logs")

        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.destroy_container(self.node, "c1")

    def test_destroy_logfile_callable(self):
        """A callable logfile hook gets the member name and names the log file; keep-alive members remain."""
        members = []

        def logfile(member=None):
            members.append(member)
            return member

        self.node.c1_container_logfile = logfile

        c1another = ContainerManager.run_container(self.node, "c1:another")
        ContainerManager.set_container_keep_alive(self.node, "c1:another")

        with patch("builtins.open", mock_open()) as mock_file:
            assert ContainerManager.destroy_container(self.node, "c1")
            assert not ContainerManager.destroy_container(self.node, "c1:another")

        mock_file.assert_called_once_with("another", "ab")
        mock_file().write.assert_called_once_with("container logs")

        assert members == [
            None,
            "another",
        ]
        assert ContainerManager.get_container(self.node, "c1:another") == c1another
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.destroy_container(self.node, "c1")

    def test_destroy_keep_alive(self):
        """A keep-alive container survives destroy_container unless ignore_keepalive=True."""
        ContainerManager.set_container_keep_alive(self.node, "c1")

        # Try to destroy container with keep-alive tag
        assert not ContainerManager.destroy_container(self.node, "c1")
        assert ContainerManager.get_container(self.node, "c1") == self.container

        # Ignore keep-alive tag
        assert ContainerManager.destroy_container(self.node, "c1", ignore_keepalive=True)
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")

    def test_destroy_all(self):
        """destroy_all_containers removes containers without keep-alive, and all of them with ignore_keepalive=True."""
        ContainerManager.run_container(self.node, "c2")
        ContainerManager.set_container_keep_alive(self.node, "c1")

        # Destroy all containers without keep-alive tag
        ContainerManager.destroy_all_containers(self.node)
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c2")
        assert ContainerManager.get_container(self.node, "c1") == self.container

        # Ignore keep-alive tag
        ContainerManager.destroy_all_containers(self.node, ignore_keepalive=True)
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")

    def test_destroy_unregistered_containers(self):
        """destroy_unregistered_containers removes a node-labeled unregistered container and keeps a registered one."""
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
        nr_c.remove.assert_called_once()

        # Registered labeled container not destroyed
        r_c2.remove.assert_not_called()

        # Registered not labeled container not destroyed
        r_c2.remove.assert_not_called()


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


def test_frame_buffered_with_the_headers_is_not_lost():
    """Ensure a frame buffered with 101 headers is still read."""
    docker_client = DockerClient(base_url="unix:///var/run/docker.sock", version="1.49")

    with hijacked_response(MKTEMP_FRAME) as response:
        stdout, stderr = docker_client.api._read_from_socket(response, stream=False, tty=False, demux=True)

    assert stdout == b"/tmp/tmp.abcdef\n"
    assert stderr is None

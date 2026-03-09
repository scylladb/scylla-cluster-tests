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

import os
import pytest
from unittest.mock import Mock, patch, mock_open, sentinel
from collections import namedtuple

from sdcm.utils.docker_utils import (
    _Name,
    ContainerManager,
    DockerException,
    NotFound,
    ImageNotFound,
    NullResource,
    Retry,
    ContainerAlreadyRegistered,
)

build_args = {}


class DummyDockerClient:
    class api:
        @staticmethod
        def build(*args, **kwargs):
            build_args["123456"] = (args, kwargs)
            return """{"stream": "Successfully built 123456"}"""

    class containers:
        @staticmethod
        def list(*args, **kwargs):
            return args, kwargs

        @staticmethod
        def get(c_id):
            if c_id is None:
                raise NullResource("Resource ID was not provided")
            if c_id == "deadbeef":
                return namedtuple("_", "name")("deadbeef-blah")
            raise NotFound("No such container")

        @staticmethod
        def run(*args, **kwargs):
            container = DummyContainer()
            container.run_args = args, kwargs
            return container

    class images:
        @staticmethod
        def build(*args, **kwargs):
            return (
                args,
                kwargs,
            ), [{"stream": "blah"}]

        @staticmethod
        def get(image_tag):
            if image_tag == "123456":
                return build_args["123456"]

            raise ImageNotFound("not found")


class DummyContainer:
    _status = "exited"
    name = "dummy"
    client = sentinel.c1_docker_client

    @staticmethod
    def reload():
        pass

    @property
    def status(self):
        return self.get_status()

    def get_status(self):
        return self._status

    @property
    def attrs(self):
        return self.get_attrs()

    @staticmethod
    def get_attrs():
        return {}

    def rename(self, name):
        self.name = name

    @staticmethod
    def remove(*_, **__):
        pass

    @staticmethod
    def logs():
        return "container logs"

    @staticmethod
    def start():
        pass


class DummyNode:
    tags = {
        "key1": "value1",
    }

    def __init__(self):
        self._containers = {}
        # empty params, now part of ContainerManager api to allow access to SCT configuration
        self.parent_cluster = namedtuple("cluster", field_names="params")(params={})

    @staticmethod
    def c3_container_run_args(**kwargs):
        return {v: k for k, v in kwargs.items()}


class TestName:
    def test_none(self):
        name = _Name(None)
        assert name.full is None
        assert name.family is None
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == "None"
        assert not name

    def test_empty_name(self):
        name = _Name("")
        assert name.family == ""
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == ""
        assert name  # Yep, empty name is True too.

    def test_name_without_member(self):
        name = _Name("blah")
        assert name.full == "blah"
        assert name.family == "blah"
        assert name.member is None
        assert name.member_as_args == ()
        assert str(name) == "blah"
        assert name

    def test_name_with_member(self):
        name = _Name("foo:bar")
        assert name.full == "foo:bar"
        assert name.family == "foo"
        assert name.member == "bar"
        assert name.member_as_args == ("bar",)
        assert str(name) == "foo:bar"
        assert name


@patch("sdcm.utils.docker_utils.ContainerManager.default_docker_client", DummyDockerClient())
class TestContainerManager:
    def setup_method(self) -> None:
        self.node = DummyNode()
        self.node._containers["c1"] = self.container = DummyContainer()

    def test_get_docker_client_default(self):
        assert ContainerManager.get_docker_client(self.node, "c2") == ContainerManager.default_docker_client

    def test_get_docker_client_without_name(self):
        self.node.None_docker_client = Mock()
        self.node.docker_client = sentinel.none_docker_client
        assert ContainerManager.get_docker_client(self.node) == sentinel.none_docker_client
        self.node.None_docker_client.assert_not_called()

    def test_get_docker_client_from_existent_container(self):
        assert ContainerManager.get_docker_client(self.node, "c1") == sentinel.c1_docker_client

    def test_get_docker_client_node_wide_none(self):
        self.node.docker_client = None
        assert ContainerManager.get_docker_client(self.node, "c2") == ContainerManager.default_docker_client

    def test_get_docker_client_node_wide_not_callable(self):
        self.node.docker_client = sentinel.node_property_docker_client
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_property_docker_client

    def test_get_docker_client_node_wide_callable_returns_none(self):
        self.node.docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2") == ContainerManager.default_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_node_wide_callable(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_node_wide_callable_with_member(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2:blah") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_none(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = None
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_not_callable(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = sentinel.c2_property_docker_client
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.c2_property_docker_client
        self.node.docker_client.assert_not_called()

    def test_get_docker_client_per_family_callable_returns_none(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.node_callable_docker_client
        self.node.c2_docker_client.assert_called_once_with()
        self.node.docker_client.assert_called_once_with()

    def test_get_docker_client_per_family_callable(self):
        self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
        self.node.c2_docker_client = Mock(return_value=sentinel.c2_callable_docker_client)
        assert ContainerManager.get_docker_client(self.node, "c2") == sentinel.c2_callable_docker_client
        self.node.c2_docker_client.assert_called_once_with()
        self.node.docker_client.assert_not_called()

    def test_get_docker_client_per_family_callable_with_member(self):
        self.node.docker_client = Mock(return_value=None)
        self.node.c2_docker_client = Mock(return_value=None)
        assert ContainerManager.get_docker_client(self.node, "c2:blah") == ContainerManager.default_docker_client
        self.node.c2_docker_client.assert_called_once_with("blah")
        self.node.docker_client.assert_called_once_with()

    def test_get_container(self):
        # Get existent container
        assert ContainerManager.get_container(self.node, "c1") is self.container

        # Try to get non-existent container without exception
        assert ContainerManager.get_container(self.node, "c2", raise_not_found_exc=False) is None

        # Check NotFound exception
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c2")

    def test_is_running(self):
        # Check exited container
        assert not ContainerManager.is_running(self.node, "c1")

        # Check running container
        self.container._status = "running"
        assert ContainerManager.is_running(self.node, "c1")

        # Try to get status of non-existent container
        with pytest.raises(NotFound):
            ContainerManager.is_running(self.node, "c2")

    @patch("time.sleep", int)
    def test_wait_for_status(self):
        statuses = iter(
            (
                "exited",
                "exited",
                "running",
                "mark1",
                "running",
                "running",
                "exited",
                "mark2",
            )
            + ("exited",) * 20
        )

        def status():
            return next(statuses)

        self.container.get_status = status

        # Try to get status of non-existent container
        with pytest.raises(NotFound):
            ContainerManager.wait_for_status(self.node, "c2", status="exited")

        # Wait for `running' status
        ContainerManager.wait_for_status(self.node, "c1", status="running")
        assert status() == "mark1"

        # Wait for `exited' status
        ContainerManager.wait_for_status(self.node, "c1", status="exited")
        assert status() == "mark2"

        # Test too many retries
        with pytest.raises(Retry):
            ContainerManager.wait_for_status(self.node, "c1", status="running")

    @patch("time.sleep", int)
    def test_get_ip_address(self):
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
        assert ContainerManager.get_containers_by_prefix("blah") == (
            (),
            {
                "all": True,
                "filters": {"name": "blah*"},
            },
        )

    def test_get_container_name_by_id(self):
        # Try to get name of non-existent container
        with pytest.raises(NotFound):
            ContainerManager.get_container_name_by_id("not_found")

        # Get name by id
        assert ContainerManager.get_container_name_by_id("deadbeef") == "deadbeef-blah"

    def test_run_container(self):
        # Try to run existent container
        assert ContainerManager.run_container(self.node, "c1") == self.container

        # Test no *_container_run_args hook available
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

        # Test no *_container_run_args hook available and member name
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

        # Test with *_container_run_args hook
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

        # Test with *_container_run_args hook and member name
        c3another = ContainerManager.run_container(self.node, "c3:another", blah="name")
        assert c3another.run_args == (
            (),
            {
                "detach": True,
                "labels": self.node.tags,
                "name": "blah-another",
            },
        )
        assert ContainerManager.get_container(self.node, "c3") == container3
        assert ContainerManager.get_container(self.node, "c3:another") == c3another

        # Verify that all containers are there
        set1 = {self.container, container2, c2another, container3, c3another}
        set2 = {
            ContainerManager.get_container(self.node, name) for name in ("c1", "c2", "c2:another", "c3", "c3:another")
        }
        assert set1 == set2
        assert len(set1) == 5

    def test_rerun_existing_container(self):
        self.container.start = Mock()
        ContainerManager.run_container(self.node, "c1")
        self.container.start.assert_called_once_with()

    def test_set_container_keep_alive(self):
        # Set keep alive to non-existent container
        with pytest.raises(NotFound):
            ContainerManager.set_container_keep_alive(self.node, "c2")

        # Set keep alive
        ContainerManager.set_container_keep_alive(self.node, "c1")
        assert self.container.name == "dummy---KEEPALIVE"

        # Set keep alive again
        ContainerManager.set_container_keep_alive(self.node, "c1")
        assert self.container.name == "dummy---KEEPALIVE"

    def test_set_all_containers_keep_alive(self):
        ContainerManager.set_all_containers_keep_alive(self.node)
        assert self.container.name == "dummy---KEEPALIVE"

    def test_ssh_copy_id(self):
        # Copy SSH pub key to non-existent container
        with pytest.raises(NotFound):
            ContainerManager.ssh_copy_id(self.node, "c2", user="root", key_file="/root/.ssh/id_rsa")

        # Copy SSH pub key
        self.container.exec_run = Mock()
        self.container.exec_run.return_value.exit_code = 0

        with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
            rsa_key_mock.return_value.get_base64.return_value = "0123456789"
            ContainerManager.ssh_copy_id(self.node, "c1", user=sentinel.ssh_user, key_file="~/lala.key")

        rsa_key_mock.assert_called_once_with(os.path.expanduser("~/lala.key"))
        rsa_key_mock.return_value.get_base64.assert_called_once_with()
        self.container.exec_run.assert_called_once()

        # disable this message for .call_args[]
        assert " 0123456789 " in " ".join(self.container.exec_run.call_args[0][0])
        assert self.container.exec_run.call_args[1]["user"] == sentinel.ssh_user

        # Check exec_run failure
        self.container.exec_run.reset_mock()
        self.container.exec_run.return_value.exit_code = 126
        self.container.exec_run.return_value.output = b"blah"

        with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
            with pytest.raises(DockerException, match="blah"):
                ContainerManager.ssh_copy_id(
                    self.node,
                    "c1",
                    user=sentinel.ssh_user,
                    key_file="lala.key",
                )

    def test_register_container(self):
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
        # Unregister non-existent name
        with pytest.raises(NotFound):
            ContainerManager.unregister_container(self.node, "c2")

        # Unregister container
        ContainerManager.unregister_container(self.node, "c1")
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")

    def test_destroy(self):
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
        ContainerManager.set_container_keep_alive(self.node, "c1")

        # Try to destroy container with keep-alive tag
        assert not ContainerManager.destroy_container(self.node, "c1")
        assert ContainerManager.get_container(self.node, "c1") == self.container

        # Ignore keep-alive tag
        assert ContainerManager.destroy_container(self.node, "c1", ignore_keepalive=True)
        with pytest.raises(NotFound):
            ContainerManager.get_container(self.node, "c1")

    def test_destroy_all(self):
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
        with patch.object(ContainerManager.default_docker_client.containers, "list", new=stub_list):
            ContainerManager.destroy_unregistered_containers(node)

        # Not-registered labeled container destroyed
        nr_c.remove.assert_called_once()

        # Registered labeled container not destroyed
        r_c2.remove.assert_not_called()

        # Registered not labeled container not destroyed
        r_c2.remove.assert_not_called()

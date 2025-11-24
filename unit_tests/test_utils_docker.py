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
import unittest
from unittest.mock import Mock, patch, mock_open, sentinel
from collections import namedtuple

from sdcm.utils.docker_utils import _Name, ContainerManager, \
    DockerException, NotFound, ImageNotFound, NullResource, Retry, ContainerAlreadyRegistered

build_args = {}


class DummyDockerClient:
    class api:
        @staticmethod
        def build(*args, **kwargs):
            build_args['123456'] = (args, kwargs)
            return '''{"stream": "Successfully built 123456"}'''

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
            return (args, kwargs,), [{"stream": "blah"}]

        @staticmethod
        def get(image_tag):
            if image_tag == "123456":
                return build_args['123456']

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
    tags = {"key1": "value1", }

    def __init__(self):
        self._containers = {}
        # empty params, now part of ContainerManager api to allow access to SCT configuration
        self.parent_cluster = namedtuple('cluster', field_names='params')(params={})

    @staticmethod
    def c3_container_run_args(**kwargs):
        return {v: k for k, v in kwargs.items()}


class TestName(unittest.TestCase):
    def test_none(self):
        name = _Name(None)
        self.assertIs(name.full, None)
        self.assertIs(name.family, None)
        self.assertIs(name.member, None)
        self.assertEqual(name.member_as_args, ())
        self.assertEqual(str(name), "None")
        self.assertFalse(name)

    def test_empty_name(self):
        name = _Name("")
        self.assertEqual(name.family, "")
        self.assertIs(name.member, None)
        self.assertEqual(name.member_as_args, ())
        self.assertEqual(str(name), "")
        self.assertTrue(name)  # Yep, empty name is True too.

    def test_name_without_member(self):
        name = _Name("blah")
        self.assertEqual(name.full, "blah")
        self.assertEqual(name.family, "blah")
        self.assertIs(name.member, None)
        self.assertEqual(name.member_as_args, ())
        self.assertEqual(str(name), "blah")
        self.assertTrue(name)

    def test_name_with_member(self):
        name = _Name("foo:bar")
        self.assertEqual(name.full, "foo:bar")
        self.assertEqual(name.family, "foo")
        self.assertEqual(name.member, "bar")
        self.assertEqual(name.member_as_args, ("bar", ))
        self.assertEqual(str(name), "foo:bar")
        self.assertTrue(name)


@patch("sdcm.utils.docker_utils.ContainerManager.default_docker_client", DummyDockerClient())
class TestContainerManager(unittest.TestCase):
    def setUp(self) -> None:
        self.node = DummyNode()
        self.node._containers["c1"] = self.container = DummyContainer()

    def test_get_docker_client(self):
        with self.subTest("Default Docker client"):
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"),
                             ContainerManager.default_docker_client)

        with self.subTest("Docker client without name argument"):
            self.node.None_docker_client = Mock()
            self.node.docker_client = sentinel.none_docker_client
            self.assertEqual(ContainerManager.get_docker_client(self.node), sentinel.none_docker_client)
            self.node.None_docker_client.assert_not_called()

        with self.subTest("Docker client from existent container"):
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c1"), sentinel.c1_docker_client)

        with self.subTest("Node-wide Docker client (None)"):
            self.node.docker_client = None
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"),
                             ContainerManager.default_docker_client)

        with self.subTest("Node-wide Docker client (not callable)"):
            self.node.docker_client = sentinel.node_property_docker_client
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.node_property_docker_client)

        with self.subTest("Node-wide Docker client (callable, return None)"):
            self.node.docker_client = Mock(return_value=None)
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"),
                             ContainerManager.default_docker_client)
            self.node.docker_client.assert_called_once_with()

        with self.subTest("Node-wide Docker client (callable)"):
            self.node.docker_client = Mock(return_value=sentinel.node_callable_docker_client)
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.node_callable_docker_client)
            self.node.docker_client.assert_called_once_with()

        with self.subTest("Node-wide Docker client (callable, with member)"):
            self.node.docker_client.reset_mock()
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2:blah"),
                             sentinel.node_callable_docker_client)
            self.node.docker_client.assert_called_once_with()

        with self.subTest("Docker client per container family (None)"):
            self.node.docker_client.reset_mock()
            self.node.c2_docker_client = None
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.node_callable_docker_client)
            self.node.docker_client.assert_called_once_with()

        with self.subTest("Docker client per container family (not callable)"):
            self.node.docker_client.reset_mock()
            self.node.c2_docker_client = sentinel.c2_property_docker_client
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.c2_property_docker_client)
            self.node.docker_client.assert_not_called()

        with self.subTest("Docker client per container family (callable, return None)"):
            self.node.docker_client.reset_mock()
            self.node.c2_docker_client = Mock(return_value=None)
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.node_callable_docker_client)
            self.node.c2_docker_client.assert_called_once_with()
            self.node.docker_client.assert_called_once_with()

        with self.subTest("Docker client per container family (callable)"):
            self.node.docker_client.reset_mock()
            self.node.c2_docker_client = Mock(return_value=sentinel.c2_callable_docker_client)
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2"), sentinel.c2_callable_docker_client)
            self.node.c2_docker_client.assert_called_once_with()
            self.node.docker_client.assert_not_called()

        with self.subTest("Docker client per container family (callable, with member)"):
            self.node.docker_client = Mock(return_value=None)
            self.node.c2_docker_client = Mock(return_value=None)
            self.assertEqual(ContainerManager.get_docker_client(self.node, "c2:blah"),
                             ContainerManager.default_docker_client)
            self.node.c2_docker_client.assert_called_once_with("blah")
            self.node.docker_client.assert_called_once_with()

    def test_get_container(self):
        with self.subTest("Get existent container"):
            self.assertIs(ContainerManager.get_container(self.node, "c1"), self.container)

        with self.subTest("Try to get non-existent container without exception"):
            self.assertIs(ContainerManager.get_container(self.node, "c2", raise_not_found_exc=False), None)

        with self.subTest("Check NotFound exception"):
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c2")

    def test_is_running(self):
        with self.subTest("Check exited container"):
            self.assertFalse(ContainerManager.is_running(self.node, "c1"))

        with self.subTest("Check running container"):
            self.container._status = "running"
            self.assertTrue(ContainerManager.is_running(self.node, "c1"))

        with self.subTest("Try to get status of non-existent container"):
            self.assertRaises(NotFound, ContainerManager.is_running, self.node, "c2")

    @patch("time.sleep", int)
    def test_wait_for_status(self):
        statuses = iter(("exited", "exited", "running", "mark1",
                         "running", "running", "exited", "mark2", ) +
                        ("exited", ) * 20)

        def status():
            return next(statuses)

        self.container.get_status = status

        with self.subTest("Try to get status of non-existent container"):
            self.assertRaises(NotFound, ContainerManager.wait_for_status, self.node, "c2", status="exited")

        with self.subTest("Wait for `running' status"):
            ContainerManager.wait_for_status(self.node, "c1", status="running")
            self.assertEqual(status(), "mark1")

        with self.subTest("Wait for `exited' status"):
            ContainerManager.wait_for_status(self.node, "c1", status="exited")
            self.assertEqual(status(), "mark2")

        with self.subTest("Test too many retries"):
            self.assertRaises(Retry, ContainerManager.wait_for_status, self.node, "c1", status="running")

    @patch("time.sleep", int)
    def test_get_ip_address(self):
        no_ip_address = dict(NetworkSettings=dict(Networks=dict(bridge=dict(IPAddress=""))))
        ip_address = dict(NetworkSettings=dict(Networks=dict(bridge=dict(IPAddress="10.0.0.1"))))
        ip_addresses = iter((no_ip_address, no_ip_address, ip_address, "mark", ) + (no_ip_address, ) * 10)

        def attrs():
            return next(ip_addresses)

        self.container.get_attrs = attrs

        with self.subTest("Try to get IP address of non-existent container"):
            self.assertRaises(NotFound, ContainerManager.get_ip_address, self.node, "c2")

        with self.subTest("Get IP address"):
            self.assertEqual(ContainerManager.get_ip_address(self.node, "c1"), "10.0.0.1")
            self.assertEqual(attrs(), "mark")

        with self.subTest("Test too many retries"):
            self.assertRaises(Retry, ContainerManager.get_ip_address, self.node, "c1")

    def test_get_container_port(self):
        self.container.ports = {"9999/tcp": [{"HostIp": "0.0.0.0", "HostPort": 1111}, ]}

        with self.subTest("Try to get port for non-existent container"):
            self.assertRaises(NotFound, ContainerManager.get_container_port, self.node, "c2", 9999)

        with self.subTest("No port"):
            self.assertIs(ContainerManager.get_container_port(self.node, "c1", 8888), None)

        with self.subTest("Open port"):
            self.assertEqual(ContainerManager.get_container_port(self.node, "c1", 9999), 1111)
            self.assertEqual(ContainerManager.get_container_port(self.node, "c1", "9999"), 1111)

    def test_get_environ(self):
        self.container.get_attrs = lambda: {"Config": {"Env": ["A", "B=1", ]}}

        with self.subTest("Try to get env for non-existent container"):
            self.assertRaises(NotFound, ContainerManager.get_environ, self.node, "c2")

        with self.subTest("Get env for existent container"):
            self.assertEqual(ContainerManager.get_environ(self.node, "c1"), {"A": None, "B": "1"})

    def test_get_containers_by_prefix(self):
        self.assertEqual(ContainerManager.get_containers_by_prefix("blah"),
                         ((), {"all": True, "filters": {"name": "blah*"}, }, ))

    def test_get_container_name_by_id(self):
        with self.subTest("Try to get name of non-existent container"):
            self.assertRaises(NotFound, ContainerManager.get_container_name_by_id, "not_found")

        with self.subTest("Get name by id"):
            self.assertEqual(ContainerManager.get_container_name_by_id("deadbeef"), "deadbeef-blah")

    def test_run_container(self):
        with self.subTest("Try to run existent container"):
            self.assertEqual(ContainerManager.run_container(self.node, "c1"), self.container)

        with self.subTest("Test no *_container_run_args hook available"):
            container2 = ContainerManager.run_container(self.node, "c2", arg1="value1", arg2="value2")
            self.assertEqual(container2.run_args,
                             ((), {"detach": True, "labels": self.node.tags, "arg1": "value1", "arg2": "value2", }, ))
            self.assertEqual(ContainerManager.get_container(self.node, "c2"), container2)

        with self.subTest("Test no *_container_run_args hook available and member name"):
            c2another = ContainerManager.run_container(self.node, "c2:another", arg1="value1", arg2="value2")
            self.assertNotEqual(container2, c2another)
            self.assertEqual(c2another.run_args,
                             ((), {"detach": True, "labels": self.node.tags, "arg1": "value1", "arg2": "value2", }, ))
            self.assertEqual(ContainerManager.get_container(self.node, "c2"), container2)
            self.assertEqual(ContainerManager.get_container(self.node, "c2:another"), c2another)

        with self.subTest("Test with *_container_run_args hook"):
            container3 = ContainerManager.run_container(self.node, "c3", blah="name")
            self.assertEqual(container3.run_args,
                             ((), {"detach": True, "labels": self.node.tags, "name": "blah", }, ))
            self.assertEqual(ContainerManager.get_container(self.node, "c3"), container3)

        with self.subTest("Test with *_container_run_args hook and member name"):
            c3another = ContainerManager.run_container(self.node, "c3:another", blah="name")
            self.assertEqual(c3another.run_args,
                             ((), {"detach": True, "labels": self.node.tags, "name": "blah-another", }, ))
            self.assertEqual(ContainerManager.get_container(self.node, "c3"), container3)
            self.assertEqual(ContainerManager.get_container(self.node, "c3:another"), c3another)

        with self.subTest("Verify that all containers are there"):
            set1 = {self.container, container2, c2another, container3, c3another}
            set2 = {ContainerManager.get_container(self.node, name)
                    for name in ("c1", "c2", "c2:another", "c3", "c3:another")}
            self.assertSetEqual(set1, set2)
            self.assertEqual(len(set1), 5)

        with self.subTest("Re-run container"):
            self.container.start = Mock()
            ContainerManager.run_container(self.node, "c1")
            self.container.start.assert_called_once_with()

    def test_set_container_keep_alive(self):
        with self.subTest("Set keep alive to non-existent container"):
            self.assertRaises(NotFound, ContainerManager.set_container_keep_alive, self.node, "c2")

        with self.subTest("Set keep alive"):
            ContainerManager.set_container_keep_alive(self.node, "c1")
            self.assertEqual(self.container.name, "dummy---KEEPALIVE")

        with self.subTest("Set keep alive again"):
            ContainerManager.set_container_keep_alive(self.node, "c1")
            self.assertEqual(self.container.name, "dummy---KEEPALIVE")

    def test_set_all_containers_keep_alive(self):
        ContainerManager.set_all_containers_keep_alive(self.node)
        self.assertEqual(self.container.name, "dummy---KEEPALIVE")

    def test_ssh_copy_id(self):
        with self.subTest("Copy SSH pub key to non-existent container"):
            self.assertRaises(NotFound,
                              ContainerManager.ssh_copy_id, self.node, "c2", user="root", key_file="/root/.ssh/id_rsa")

        with self.subTest("Copy SSH pub key"):
            self.container.exec_run = Mock()
            self.container.exec_run.return_value.exit_code = 0

            with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
                rsa_key_mock.return_value.get_base64.return_value = "0123456789"
                ContainerManager.ssh_copy_id(self.node, "c1", user=sentinel.ssh_user, key_file="~/lala.key")

            rsa_key_mock.assert_called_once_with(os.path.expanduser("~/lala.key"))
            rsa_key_mock.return_value.get_base64.assert_called_once_with()
            self.container.exec_run.assert_called_once()

            # disable this message for .call_args[]
            self.assertIn(" 0123456789 ", " ".join(self.container.exec_run.call_args[0][0]))
            self.assertEqual(self.container.exec_run.call_args[1]["user"], sentinel.ssh_user)

        with self.subTest("Check exec_run failure"):
            self.container.exec_run.reset_mock()
            self.container.exec_run.return_value.exit_code = 126
            self.container.exec_run.return_value.output = b"blah"

            with patch("paramiko.rsakey.RSAKey.from_private_key_file") as rsa_key_mock:
                self.assertRaisesRegex(DockerException, "blah",
                                       ContainerManager.ssh_copy_id,
                                       self.node, "c1", user=sentinel.ssh_user, key_file="lala.key")

    def test_register_container(self):
        c2_container = DummyContainer()

        with self.subTest("Try to register registered container"):
            self.assertRaisesRegex(ContainerAlreadyRegistered, "container .* registered already",
                                   ContainerManager.register_container,
                                   self.node, "c2", container=self.container)

        with self.subTest("Try to register registered container with replace=True"):
            self.assertRaisesRegex(ContainerAlreadyRegistered, "container .* registered already",
                                   ContainerManager.register_container,
                                   self.node, "c2", container=self.container, replace=True)

        with self.subTest("Try to replace container"):
            self.assertRaisesRegex(ContainerAlreadyRegistered, "another container registered",
                                   ContainerManager.register_container,
                                   self.node, "c1", container=c2_container)

        with self.subTest("Force container replacement"):
            ContainerManager.register_container(self.node, "c1", container=c2_container, replace=True)
            self.assertEqual(ContainerManager.get_container(self.node, "c1"), c2_container)

        with self.subTest("Register container"):
            ContainerManager.register_container(self.node, "c2", container=self.container)
            self.assertEqual(ContainerManager.get_container(self.node, "c2"), self.container)

        with self.subTest("Try to force container replacement with container registered with another name"):
            self.assertRaisesRegex(ContainerAlreadyRegistered, "container .* registered already",
                                   ContainerManager.register_container,
                                   self.node, "c1", container=self.container, replace=True)

    def test_unregister_container(self):
        with self.subTest("Unregister non-existent name"):
            self.assertRaises(NotFound, ContainerManager.unregister_container, self.node, "c2")

        with self.subTest("Unregister container"):
            ContainerManager.unregister_container(self.node, "c1")
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")

    def test_destroy(self):
        with self.subTest("Try to destroy non-existent container"):
            self.assertRaises(NotFound, ContainerManager.destroy_container, self.node, "c2")

        with self.subTest("without *_container_logfile hook"):
            self.assertTrue(ContainerManager.destroy_container(self.node, "c1"))
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")
            self.assertRaises(NotFound, ContainerManager.destroy_container, self.node, "c1")

    def test_build_container_image(self):
        with self.subTest("Try to build image for existent container"):
            self.assertRaises(ContainerAlreadyRegistered, ContainerManager.build_container_image, self.node, "c1")

        with self.subTest("Node-wide container image tag"):
            self.node.container_image_tag = Mock(return_value="blah")
            self.assertRaisesRegex(AssertionError, "image tag", ContainerManager.build_container_image, self.node, "c2")

        with self.subTest("Node-wide dockerfile args"):
            self.node.c2_container_image_tag = Mock(return_value="hello-world:latest")
            self.node.container_image_dockerfile_args = Mock(return_value="blah")
            self.assertRaisesRegex(AssertionError, "Dockerfile",
                                   ContainerManager.build_container_image, self.node, "c2")
            self.node.c2_container_image_tag.assert_called_once_with()

        with self.subTest("No build args"):
            self.node.c2_container_image_tag.reset_mock()
            self.node.c2_container_image_dockerfile_args = Mock(return_value={"path": "."})
            image = ContainerManager.build_container_image(self.node, "c2:another", arg1="value1", arg2="value2")
            self.assertEqual(image, ((), dict(tag="hello-world:latest",
                                              path=".",
                                              labels=self.node.tags,
                                              pull=True,
                                              rm=True,
                                              arg1="value1",
                                              arg2="value2")))
            self.node.c2_container_image_tag.assert_called_once_with("another")
            self.node.c2_container_image_dockerfile_args.assert_called_once_with("another")

        with self.subTest("No build args"):
            self.node.c2_container_image_tag.reset_mock()
            self.node.c2_container_image_dockerfile_args.reset_mock()
            self.node.c2_container_image_build_args = lambda **kw: {v: k for k, v in kw.items()}
            image = ContainerManager.build_container_image(self.node, "c2:another", arg1="value1", arg2="value2")
            self.assertEqual(image, ((), dict(tag="hello-world:latest",
                                              path=".",
                                              labels=self.node.tags,
                                              pull=True,
                                              rm=True,
                                              value1="arg1",
                                              value2="arg2")))
            self.node.c2_container_image_tag.assert_called_once_with("another")
            self.node.c2_container_image_dockerfile_args.assert_called_once_with("another")

    def test_destroy_logfile(self):
        self.node.c1_container_logfile = "container.log"

        with patch("builtins.open", mock_open()) as mock_file:
            self.assertTrue(ContainerManager.destroy_container(self.node, "c1"))

        mock_file.assert_called_once_with("container.log", "ab")
        mock_file().write.assert_called_once_with("container logs")

        self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")
        self.assertRaises(NotFound, ContainerManager.destroy_container, self.node, "c1")

    def test_destroy_logfile_callable(self):
        members = []

        def logfile(member=None):
            members.append(member)
            return member

        self.node.c1_container_logfile = logfile

        c1another = ContainerManager.run_container(self.node, "c1:another")
        ContainerManager.set_container_keep_alive(self.node, "c1:another")

        with patch("builtins.open", mock_open()) as mock_file:
            self.assertTrue(ContainerManager.destroy_container(self.node, "c1"))
            self.assertFalse(ContainerManager.destroy_container(self.node, "c1:another"))

        mock_file.assert_called_once_with("another", "ab")
        mock_file().write.assert_called_once_with("container logs")

        self.assertEqual(members, [None, "another", ])
        self.assertEqual(ContainerManager.get_container(self.node, "c1:another"), c1another)
        self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")
        self.assertRaises(NotFound, ContainerManager.destroy_container, self.node, "c1")

    def test_destroy_keep_alive(self):
        ContainerManager.set_container_keep_alive(self.node, "c1")

        with self.subTest("Try to destroy container with keep-alive tag"):
            self.assertFalse(ContainerManager.destroy_container(self.node, "c1"))
            self.assertEqual(ContainerManager.get_container(self.node, "c1"), self.container)

        with self.subTest("Ignore keep-alive tag"):
            self.assertTrue(ContainerManager.destroy_container(self.node, "c1", ignore_keepalive=True))
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")

    def test_destroy_all(self):
        ContainerManager.run_container(self.node, "c2")
        ContainerManager.set_container_keep_alive(self.node, "c1")

        with self.subTest("Destroy all containers without keep-alive tag"):
            ContainerManager.destroy_all_containers(self.node)
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c2")
            self.assertEqual(ContainerManager.get_container(self.node, "c1"), self.container)

        with self.subTest("Ignore keep-alive tag"):
            ContainerManager.destroy_all_containers(self.node, ignore_keepalive=True)
            self.assertRaises(NotFound, ContainerManager.get_container, self.node, "c1")

    def test_destroy_unregistered_containers(self):
        tags = {'TestId': 'test123', 'Name': 'nodeA'}

        r_c1, r_c2, nr_c = [DummyContainer() for _ in range(3)]
        r_c1.get_attrs = lambda: {"Config": {"Labels": {}}}
        r_c2.get_attrs = nr_c.get_attrs = lambda: {"Config": {"Labels": tags}}

        node = DummyNode()
        node.tags, node.name = tags, tags['Name']
        node._containers["r_c1"] = r_c1  # register not-labeled container for the node
        node._containers["r_c2"] = r_c2  # register labeled container for the node

        def stub_list(*args, **kwargs):
            filters = kwargs.get('filters', {})
            filter_dict = {}
            for label in filters['label']:
                key, val = label.split('=')
                filter_dict[key] = val

            filtered_containers = []
            for container in (r_c1, r_c2, nr_c):
                container_labels = container.attrs['Config']['Labels']
                if all(container_labels.get(key) == val for key, val in filter_dict.items()):
                    filtered_containers.append(container)

            return filtered_containers

        r_c1.remove, r_c2.remove, nr_c.remove = Mock(), Mock(), Mock()
        # temporarily patch dummy default_docker_client.containers.list to be able to filter containers by labels
        with patch.object(ContainerManager.default_docker_client.containers, 'list', new=stub_list):
            ContainerManager.destroy_unregistered_containers(node)

        with self.subTest("Not-registered labeled container destroyed"):
            nr_c.remove.assert_called_once()

        with self.subTest("Registered labeled container not destroyed"):
            r_c2.remove.assert_not_called()

        with self.subTest("Registered not labeled container not destroyed"):
            r_c2.remove.assert_not_called()

# pylint: disable=too-few-public-methods

from __future__ import print_function

from __future__ import absolute_import
import unittest
import tempfile
import logging
import shutil
import os.path
import json
from weakref import proxy as weakproxy

from invoke import Result

from sdcm.cluster import BaseNode
from sdcm.sct_events import start_events_device, stop_events_device
from sdcm.sct_events import EVENTS_PROCESSES
from sdcm.utils.distro import Distro

from unit_tests.dummy_remote import DummyRemote


class DummyNode(BaseNode):  # pylint: disable=abstract-method
    _database_log = None
    is_enterprise = False
    distro = Distro.CENTOS7

    def init(self):
        super().init()
        self.remoter.stop()

    @property
    def private_ip_address(self):
        return '127.0.0.1'

    @property
    def public_ip_address(self):
        return '127.0.0.1'

    def start_task_threads(self):
        # disable all background threads
        pass

    @property
    def database_log(self):
        return self._database_log

    @database_log.setter
    def database_log(self, x):
        self._database_log = x

    def set_hostname(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestBaseNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir, timeout=5)

        cls.node = DummyNode(name='test_node', parent_cluster=None,
                             base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.init()
        cls.node.remoter = DummyRemote()

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database.log')

    def test_search_database_log(self):
        critical_errors = self.node.search_database_log()
        self.assertEqual(len(critical_errors), 34)

        for _, line in critical_errors:
            print(line)

    def test_search_database_log_teardown(self):  # pylint: disable=invalid-name
        critical_errors = self.node.search_database_log(start_from_beginning=True, publish_events=False)
        self.assertEqual(len(critical_errors), 36)

        for _, line in critical_errors:
            print(line)

    def test_search_database_log_specific_log(self):  # pylint: disable=invalid-name
        errors = self.node.search_database_log(
            search_pattern='Failed to load schema version', start_from_beginning=True, publish_events=False)
        self.assertEqual(len(errors), 2)

        for line_number, line in errors:
            print(line_number, line)

    def test_search_database_interlace_reactor_stall(self):  # pylint: disable=invalid-name
        self.node.database_log = os.path.join(os.path.dirname(__file__), 'test_data', 'database_interlace_stall.log')

        _ = self.node.search_database_log()

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')
        events = []
        for line in events_file.readlines():

            events.append(json.loads(line))

        event_a, event_b = events[-2], events[-1]
        print(event_a)
        print(event_b)

        assert event_a["type"] == "REACTOR_STALLED"
        assert event_a["line_number"] == 1
        assert event_b["type"] == "REACTOR_STALLED"
        assert event_b["line_number"] == 4

    def test_search_database_suppressed_messages(self):  # pylint: disable=invalid-name
        self.node.database_log = os.path.join(os.path.dirname(
            __file__), 'test_data', 'database_suppressed_messages.log')

        _ = self.node.search_database_log(start_from_beginning=True)

        events_file = open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r')

        events = [json.loads(line) for line in events_file.readlines()]

        event_a = events[-1]
        print(event_a)

        assert event_a["type"] == "SUPPRESSED_MESSAGES", 'Not expected event type {}'.format(event_a["type"])
        assert event_a["line_number"] == 6, 'Not expected event line number {}'.format(event_a["line_number"])


class VersionDummyRemote:
    def __init__(self, test, results):
        self.test = weakproxy(test)
        self.results = iter(results)

    def run(self, cmd, *_, **__):
        expected_cmd, result = next(self.results)
        self.test.assertEqual(cmd, expected_cmd)
        return Result(exited=result[0], stdout=result[1], stderr=result[2])


class TestBaseNodeGetScyllaVersion(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.node = DummyNode(name='test_node',
                              parent_cluster=None,
                              base_logdir=self.temp_dir,
                              ssh_login_info=dict(key_file='~/.ssh/scylla-test'))

    def test_no_scylla_binary_rhel_like(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("rpm --query --queryformat '%{VERSION}' scylla", (0, "3.3.rc1", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "3.3.rc1")
        self.assertEqual(self.node.scylla_version, "3.3.rc1")

    def test_no_scylla_binary_other(self):
        self.node.distro = Distro.DEBIAN9
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("dpkg-query --show --showformat '${Version}' scylla", (0, "3.3~rc1-0.20200209.0d0c1d43188-1", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "3.3.rc1")
        self.assertEqual(self.node.scylla_version, "3.3.rc1")

    def test_unparsable_version(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "asdfasdfadf", "")),
            ("rpm --query --queryformat '%{VERSION}' scylla", (0, "asdfasdff", "")),
        ))
        self.assertIs(self.node.get_scylla_version(), None)
        self.assertIs(self.node.scylla_version, None)

    def test_scylla(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "3.3.rc1")
        self.assertEqual(self.node.scylla_version, "3.3.rc1")

    def test_scylla_master(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "666.development-0.20200205.2816404f575\n", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "666.development")
        self.assertEqual(self.node.scylla_version, "666.development")

    def test_scylla_enterprise(self):
        self.node.is_enterprise = True
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "2019.1.4-0.20191217.b59e92dbd\n", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "2019.1.4")
        self.assertEqual(self.node.scylla_version, "2019.1.4")

    def test_scylla_enterprise_no_scylla_binary(self):
        self.node.is_enterprise = True
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("rpm --query --queryformat '%{VERSION}' scylla-enterprise", (0, "2019.1.4", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "2019.1.4")
        self.assertEqual(self.node.scylla_version, "2019.1.4")

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

from sdcm.cluster import BaseNode, BaseCluster, BaseMonitorSet
from sdcm.sct_events import start_events_device, stop_events_device
from sdcm.sct_events import EVENTS_PROCESSES
from sdcm.utils.distro import Distro

from unit_tests.dummy_remote import DummyRemote


class DummyNode(BaseNode):  # pylint: disable=abstract-method
    _system_log = None
    is_enterprise = False
    distro = Distro.CENTOS7

    def init(self):
        super().init()
        self.remoter.stop()

    def _get_private_ip_address(self):
        return '127.0.0.1'

    def _get_public_ip_address(self):
        return '127.0.0.1'

    def start_task_threads(self):
        # disable all background threads
        pass

    @property
    def system_log(self):
        return self._system_log

    @system_log.setter
    def system_log(self, log):
        self._system_log = log

    def set_hostname(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500):
        pass


class DummyDbCluster(BaseCluster):
    def __init__(self, nodes):
        self.nodes = nodes


logging.basicConfig(format="%(asctime)s - %(levelname)-8s - %(name)-10s: %(message)s", level=logging.DEBUG)


class TestBaseNode(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.mkdtemp()
        start_events_device(cls.temp_dir)

        cls.node = DummyNode(name='test_node', parent_cluster=None,
                             base_logdir=cls.temp_dir, ssh_login_info=dict(key_file='~/.ssh/scylla-test'))
        cls.node.init()
        cls.node.remoter = DummyRemote()

    @classmethod
    def tearDownClass(cls):
        stop_events_device()
        shutil.rmtree(cls.temp_dir)

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    def test_search_system_log(self):
        critical_errors = list(self.node.follow_system_log(start_from_beginning=True))
        self.assertEqual(36, len(critical_errors))

    def test_search_system_log_specific_log(self):  # pylint: disable=invalid-name
        errors = list(self.node.follow_system_log(
            patterns=['Failed to load schema version'], start_from_beginning=True))
        self.assertEqual(len(errors), 2)

    def test_search_system_interlace_reactor_stall(self):  # pylint: disable=invalid-name
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_interlace_stall.log')

        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

            event_a, event_b = events[-2], events[-1]
            print(event_a)
            print(event_b)

            assert event_a["type"] == "REACTOR_STALLED"
            assert event_a["line_number"] == 0
            assert event_b["type"] == "REACTOR_STALLED"
            assert event_b["line_number"] == 3

    def test_search_system_suppressed_messages(self):  # pylint: disable=invalid-name
        self.node.system_log = os.path.join(os.path.dirname(
            __file__), 'test_data', 'system_suppressed_messages.log')

        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

            event_a = events[-1]
            print(event_a)

            assert event_a["type"] == "SUPPRESSED_MESSAGES", 'Not expected event type {}'.format(event_a["type"])
            assert event_a["line_number"] == 6, 'Not expected event line number {}'.format(event_a["line_number"])

    def test_search_one_line_backtraces(self):  # pylint: disable=invalid-name
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_one_line_backtrace.log')

        self.node._read_system_log_and_publish_events(start_from_beginning=True)

        with open(EVENTS_PROCESSES['MainDevice'].raw_events_filename, 'r') as events_file:
            events = [json.loads(line) for line in events_file]

            event_backtrace1, event_backtrace2 = events[22], events[23]
            print(event_backtrace1)
            print(event_backtrace2)

            assert event_backtrace1["type"] == "DATABASE_ERROR"
            assert event_backtrace1["raw_backtrace"]
            assert event_backtrace2["type"] == "DATABASE_ERROR"
            assert event_backtrace2["raw_backtrace"]


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

    def test_scylla(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
            ("readelf -n /usr/bin/scylla", (0, "Build ID: xxx", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "3.3.rc1")
        self.assertEqual(self.node.scylla_version, "3.3.rc1")
        self.assertEqual(self.node.scylla_version_detailed, "3.3.rc1-0.20200209.0d0c1d43188 with build-id xxx")

    def test_scylla_master(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "666.development-0.20200205.2816404f575\n", "")),
            ("readelf -n /usr/bin/scylla", (0, "Build ID: xxx", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "666.development")
        self.assertEqual(self.node.scylla_version, "666.development")

    def test_scylla_master_new_format(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("readelf -n /usr/bin/scylla", (0, "Build ID: xxx", "")),
        ))
        self.assertEqual(self.node.get_scylla_version(), "4.4.dev")
        self.assertEqual(self.node.scylla_version, "4.4.dev")

    def test_scylla_enterprise(self):
        self.node.is_enterprise = True
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "2019.1.4-0.20191217.b59e92dbd\n", "")),
            ("readelf -n /usr/bin/scylla", (0, "Build ID: xxx", "")),
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


class TestBaseMonitorSet(unittest.TestCase):
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
        self.db_cluster = DummyDbCluster([self.node])
        self.monitor_cluster = BaseMonitorSet({"db_cluster": self.db_cluster}, {})
        self.monitor_cluster.log = logging

    def test_monitoring_version(self):
        """
        verify that dev version are mapped to monitor master version
        """
        self.node.remoter = VersionDummyRemote(self, (
            ("scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("readelf -n /usr/bin/scylla", (0, "Build ID: xxx", "")),
        ))
        self.assertEqual(self.monitor_cluster.monitoring_version, "master")

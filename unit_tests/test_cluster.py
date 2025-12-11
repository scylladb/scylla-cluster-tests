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

import json
import logging
import os.path
import re
import shutil
import tempfile
import time
import unittest
from datetime import datetime
from functools import cached_property
from typing import List
from weakref import proxy as weakproxy

import pytest
from invoke import Result

from sdcm import sct_config
from sdcm.cluster import BaseNode, BaseCluster, BaseMonitorSet, BaseScyllaCluster
from sdcm.db_log_reader import DbLogReader
from sdcm.sct_events import Severity
from sdcm.sct_events.database import SYSTEM_ERROR_EVENTS_PATTERNS
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.sct_events.group_common_events import ignore_upgrade_schema_errors
from sdcm.sct_events.system import InstanceStatusEvent
from sdcm.utils.common import (
    get_keyspace_partition_ranges,
    keyspace_min_max_tokens,
    parse_nodetool_listsnapshots,
)
from sdcm.utils.distro import Distro
from sdcm.utils.nemesis_utils.indexes import get_column_names
from sdcm.utils.version_utils import ComparableScyllaVersion
from unit_tests.dummy_remote import DummyRemote
from unit_tests.lib.events_utils import EventsUtilsMixin
from unit_tests.test_utils_common import DummyNode


class DummyDbCluster(BaseCluster, BaseScyllaCluster):

    def __init__(self, nodes, params=None):
        self.nodes = nodes
        self.params = params or sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.added_password_suffix = False
        self.log = logging.getLogger(__name__)
        self.node_type = "scylla-db"
        self.name = 'dummy_db_cluster'

    def add_nodes(self, count, ec2_user_data='', dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        for _ in range(count):
            self.nodes += [self.nodes[-1]]

    def wait_for_init(*_, node_list=None, verbose=False, timeout=None, **__):
        pass

    def validate_seeds_on_all_nodes(self):
        pass


class TestBaseNode(unittest.TestCase, EventsUtilsMixin):
    @classmethod
    def setUpClass(cls):
        cls.setup_events_processes(events_device=True, events_main_device=False, registry_patcher=True)

    @cached_property
    def node(self):
        dummy_node = DummyNode(
            name='test_node',
            parent_cluster=None,
            base_logdir=self.temp_dir,
            ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
        )
        dummy_node.parent_cluster = DummyDbCluster(nodes=[dummy_node])
        dummy_node.init()
        dummy_node.remoter = DummyRemote()
        return dummy_node

    @cached_property
    def _db_log_reader(self):
        return DbLogReader(
            system_log=self.node.system_log,
            remoter=self.node.remoter,
            node_name=str(self),
            system_event_patterns=SYSTEM_ERROR_EVENTS_PATTERNS,
            decoding_queue=None,
            log_lines=False,
            backtrace_stall_decoding=True,
            backtrace_decoding_disable_regex=None,
        )

    def _read_and_publish_events(self, log_text=None):
        if log_text:
            with tempfile.NamedTemporaryFile(mode='wt') as temp_log:
                self.node.system_log = temp_log.name

                for line in log_text.splitlines(keepends=True):
                    temp_log.write(line)
                    temp_log.flush()
                    self._db_log_reader._read_and_publish_events()
        else:
            self._db_log_reader._read_and_publish_events()

    @classmethod
    def tearDownClass(cls):
        cls.teardown_events_processes()

    def setUp(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system.log')

    def test_search_system_log(self):
        critical_errors = list(self.node.follow_system_log(start_from_beginning=True))
        self.assertEqual(33, len(critical_errors))

    def test_search_system_log_specific_log(self):
        errors = list(self.node.follow_system_log(
            patterns=['Failed to load schema version'], start_from_beginning=True))
        self.assertEqual(len(errors), 2)

    def test_search_system_interlace_reactor_stall(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_interlace_stall.log')

        self._read_and_publish_events()

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            event_a, event_b = events[-2], events[-1]
            print(event_a)
            print(event_b)

            assert event_a["type"] == "REACTOR_STALLED"
            assert event_a["line_number"] == 0
            assert event_b["type"] == "REACTOR_STALLED"
            assert event_b["line_number"] == 3

    def test_search_kernel_callstack(self):
        self.node.parent_cluster = {'params': {'print_kernel_callstack': True}}
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'kernel_callstack.log')
        self._read_and_publish_events()
        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            event_a, event_b = events[-2], events[-1]
            print(event_a)
            print(event_b)

            assert event_a["type"] == "KERNEL_CALLSTACK"
            assert event_a["line_number"] == 2
            assert event_b["type"] == "KERNEL_CALLSTACK"
            assert event_b["line_number"] == 5

    def test_search_cdc_invalid_request(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_cdc_invalid_request.log')
        with unittest.mock.patch("sdcm.sct_events.group_common_events.TestConfig"):
            with unittest.mock.patch("sdcm.sct_events.group_common_events.SkipPerIssues") as skip_per_issues:
                skip_per_issues.return_value = False
                with ignore_upgrade_schema_errors():
                    self._read_and_publish_events()

        time.sleep(0.2)
        with self.get_events_logger().events_logs_by_severity[Severity.ERROR].open() as events_file:
            cdc_err_events = [line for line in events_file if 'cdc - Could not retrieve CDC streams' in line]
            assert cdc_err_events != []

    def test_search_power_off(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'power_off.log')
        with DbEventsFilter(db_event=InstanceStatusEvent.POWER_OFF, node=self.node):
            self._read_and_publish_events()

        InstanceStatusEvent.POWER_OFF().add_info(
            node="A", line_number=22,
            line=f"{datetime.utcfromtimestamp(time.time() + 1):%Y-%m-%dT%H:%M:%S+00:00} "
                 "longevity-large-collections-12h-mas-db-node-c6a4e04e-1 !INFO    | systemd-logind: Powering Off..."
        ).publish()

        time.sleep(0.1)
        with self.get_events_logger().events_logs_by_severity[Severity.WARNING].open() as events_file:
            events = [line for line in events_file if 'Powering Off' in line]
            assert events

    def test_search_system_suppressed_messages(self):
        self.node.system_log = os.path.join(os.path.dirname(
            __file__), 'test_data', 'system_suppressed_messages.log')

        self._read_and_publish_events()

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            event_a = events[-1]
            print(event_a)

            assert event_a["type"] == "SUPPRESSED_MESSAGES", 'Not expected event type {}'.format(event_a["type"])
            assert event_a["line_number"] == 6, 'Not expected event line number {}'.format(event_a["line_number"])

    def test_search_one_line_backtraces(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'system_one_line_backtrace.log')

        self._read_and_publish_events()

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            backtraces = [event for event in events if event["type"] == "BACKTRACE"]
            assert len(backtraces) == 2
            for event_backtrace in backtraces:
                assert event_backtrace["raw_backtrace"]

            oversized_events = [event for event in events if event["type"] == "OVERSIZED_ALLOCATION"]
            assert len(oversized_events) == 2
            for event_oversized in oversized_events:
                print(event_oversized)
                assert event_oversized["raw_backtrace"]

            print(events[-1])

    def test_gate_closed_ignored_exception_is_catched(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'gate_closed_ignored_exception.log')

        self._read_and_publish_events()

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            event_backtrace1, event_backtrace2 = events[-2], events[-1]
            print(event_backtrace1)
            print(event_backtrace2)

            assert event_backtrace1["type"] == "GATE_CLOSED"
            assert event_backtrace1["line_number"] == 1
            assert event_backtrace2["type"] == "GATE_CLOSED"
            assert event_backtrace2["line_number"] == 3

    def test_compaction_stopped_exception_is_catched(self):
        self.node.system_log = os.path.join(os.path.dirname(__file__), 'test_data', 'compaction_stopped_exception.log')

        self._read_and_publish_events()

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]

            event_backtrace1, event_backtrace2 = events[-3], events[-2]
            print(event_backtrace1)
            print(event_backtrace2)

            assert event_backtrace1["type"] == "COMPACTION_STOPPED"
            assert event_backtrace1["line_number"] == 0
            assert event_backtrace2["type"] == "COMPACTION_STOPPED"
            assert event_backtrace2["line_number"] == 1

    def test_appending_to_log(self):
        logs = """
INFO  2022-07-14 09:28:34,095 [shard 1] database - Flushing non-system tables
Reactor stalled for 32 ms on shard 1. Backtrace: 0x4e0d6e2 0x4e0c340 0x4e0d5f0 0x7f230af20a1f 0x14de339 0x14dc3b0 0x14d680c 0x14d5ff6 0x14e4d52 0x14d0238 0x1aec3c6 0x508c7e1
kernel callstack:
INFO  2022-07-14 09:28:35,102 [shard 1] database - Flushed non-system tables
        """

        self._read_and_publish_events(logs)

        with self.get_raw_events_log().open() as events_file:
            events = [json.loads(line) for line in events_file]
            reactor_stalls = [event for event in events if event["type"] == "REACTOR_STALLED"]
            assert len(reactor_stalls) == 1
            event = reactor_stalls[0]
            assert event["type"] == "REACTOR_STALLED"
            assert event["line_number"] == 2
            assert 'Reactor stalled for 32 ms on shard 1' in event['line']


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
        self.node.parent_cluster = DummyDbCluster([self.node])

    def test_no_scylla_binary_rhel_like(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("rpm --query --queryformat '%{VERSION}' scylla", (0, "3.3.rc1", "")),
        ))
        self.assertEqual("3.3.rc1", self.node.scylla_version)
        self.assertEqual("3.3.rc1", self.node.scylla_version_detailed)

    def test_no_scylla_binary_other(self):
        self.node.distro = Distro.DEBIAN11
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("dpkg-query --show --showformat '${Version}' scylla", (0, "3.3~rc1-0.20200209.0d0c1d43188-1", "")),
        ))
        self.assertEqual("3.3.rc1", self.node.scylla_version)
        self.assertEqual("3.3.rc1-0.20200209.0d0c1d43188-1", self.node.scylla_version_detailed)

    def test_scylla(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("3.3.rc1", self.node.scylla_version)
        self.assertEqual("3.3.rc1-0.20200209.0d0c1d43188 with build-id xxx", self.node.scylla_version_detailed)

    def test_scylla_master(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "666.development-0.20200205.2816404f575\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("666.development", self.node.scylla_version)
        self.assertEqual("666.development-0.20200205.2816404f575 with build-id xxx", self.node.scylla_version_detailed)

    def test_scylla_master_new_format(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("4.4.dev", self.node.scylla_version)
        self.assertEqual("4.4.dev-0.20200205.2816404f575 with build-id xxx", self.node.scylla_version_detailed)

    def test_scylla_enterprise(self):
        self.node.is_enterprise = True
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "2019.1.4-0.20191217.b59e92dbd\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("2019.1.4", self.node.scylla_version)
        self.assertEqual("2019.1.4-0.20191217.b59e92dbd with build-id xxx", self.node.scylla_version_detailed)

    def test_scylla_enterprise_no_scylla_binary(self):
        self.node.is_enterprise = True
        self.node.is_product_enterprise = True
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("rpm --query --queryformat '%{VERSION}' scylla-enterprise", (0, "2019.1.4", "")),
        ))
        self.assertEqual("2019.1.4", self.node.scylla_version)
        self.assertEqual("2019.1.4", self.node.scylla_version_detailed)

    def test_scylla_binary_version_unparseable(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "x.y.z\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertIsNone(self.node.scylla_version)
        self.assertEqual("x.y.z with build-id xxx", self.node.scylla_version_detailed)

    def test_get_scylla_version_from_second_attempt(self):
        self.node.distro = Distro.DEBIAN11
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("dpkg-query --show --showformat '${Version}' scylla",
             (1, "", "dpkg-query: no packages found matching scylla\n")),
            ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
            ("dpkg-query --show --showformat '${Version}' scylla",
             (1, "", "dpkg-query: no packages found matching scylla\n")),
        ))
        self.assertIsNone(self.node.scylla_version)
        self.assertIsNone(self.node.scylla_version_detailed)

        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("4.4.dev", self.node.scylla_version)
        self.assertEqual("4.4.dev-0.20200205.2816404f575 with build-id xxx", self.node.scylla_version_detailed)

    def test_forget_scylla_version(self):
        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual("4.4.dev", self.node.scylla_version)
        self.assertEqual("4.4.dev-0.20200205.2816404f575 with build-id xxx", self.node.scylla_version_detailed)

        self.node.remoter = VersionDummyRemote(self, (
            ("/usr/bin/scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))

        self.assertEqual("4.4.dev", self.node.scylla_version)
        self.assertEqual("4.4.dev-0.20200205.2816404f575 with build-id xxx", self.node.scylla_version_detailed)

        self.node.forget_scylla_version()

        self.assertEqual("3.3.rc1", self.node.scylla_version)
        self.assertEqual("3.3.rc1-0.20200209.0d0c1d43188 with build-id xxx", self.node.scylla_version_detailed)


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
            ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
            ("/usr/bin/scylla --build-id", (0, "xxx", "")),
        ))
        self.assertEqual(self.monitor_cluster.monitoring_version, "master")


class NodetoolDummyNode(BaseNode):

    def __init__(self, resp, myregion=None, myname=None, myrack=None, db_up=True):
        self.resp = resp
        self.myregion = myregion
        self.myname = myname
        self.parent_cluster = None
        self.rack = myrack
        self._db_up = db_up

    @property
    def region(self):
        return self.myregion

    @property
    def name(self):
        return self.myname

    def run_nodetool(self, *args, **kwargs):
        return Result(exited=0, stderr="", stdout=self.resp)

    def db_up(self):
        """ return True if the database is up
        Couldn't be a property, because BaseNode.db_up is a method
        """
        return self._db_up


class DummyScyllaCluster(BaseScyllaCluster, BaseCluster):
    nodes: List['NodetoolDummyNode']

    def __init__(self, params):
        self.nodes = params
        self.name = 'dummy_cluster'
        self.added_password_suffix = False
        self.log = logging.getLogger(self.name)

    def get_ip_to_node_map(self):
        """returns {ip: node} map for all nodes in cluster to get node by ip"""
        return {node.myname: node for node in self.nodes}


class TestNodetoolStatus(unittest.TestCase):

    def test_can_get_nodetool_status_typical(self):
        resp = "\n".join(["Datacenter: eastus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                          "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a"
                          ]
                         )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {'eastus':
                          {'10.0.59.34':
                           {'state': 'UN', 'load': '21.71GB', 'tokens': '256', 'owns': '?',
                            'host_id': 'e5bcb094-e4de-43aa-8dc9-b1bf74b3b346', 'rack': '1a'},
                           '10.0.198.153': {'state': 'UN', 'load': '?', 'tokens': '256', 'owns': '?',
                                            'host_id': 'fba174cd-917a-40f6-ab62-cc58efaaf301', 'rack': '1a'}}}

    def test_can_get_nodetool_status_typical_with_one_space_after_host_id(self):
        """case for https://github.com/scylladb/scylla-cluster-tests/issues/7274"""
        resp = "\n".join(["Datacenter: datacenter1",
                          "=======================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "-- Address    Load      Tokens Owns Host ID                              Rack ",
                          "UN 172.17.0.2 202.92 KB 256    ?    7b8f86bf-c70c-4246-a273-146057e12431 rack1",
                          ]
                         )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {'datacenter1':
                          {'172.17.0.2':
                           {'state': 'UN', 'load': '202.92KB', 'tokens': '256', 'owns': '?',
                            'host_id': '7b8f86bf-c70c-4246-a273-146057e12431', 'rack': 'rack1'},
                           }}

    def test_datacenter_name_per_region(self):
        resp = "\n".join(["Datacenter: eastus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                          "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a",
                          "Datacenter: westus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a"
                          ]
                         )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34")
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname='10.1.59.34')
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {'east-us': 'eastus', 'west-us': 'westus'}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node1])
        assert datacenter_name_per_region == {'east-us': 'eastus'}

    def test_datacenter_name_per_region_when_region_doesnt_have_live_nodes(self):
        resp = "\n".join(["Datacenter: eastus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                          "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a",
                          "Datacenter: westus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "DN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a"
                          ]
                         )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34")
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname='10.1.59.34', db_up=False)
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {'east-us': 'eastus'}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node3])
        assert datacenter_name_per_region == {}

    def test_datacenter_name_per_region_when_first_node_is_dn(self):
        resp = "\n".join(["Datacenter: eastus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "DN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                          "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a",
                          "Datacenter: westus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a"
                          ]
                         )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34", db_up=False)
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname='10.1.59.34')
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {'east-us': 'eastus', 'west-us': 'westus'}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node1, node2])
        assert datacenter_name_per_region == {'east-us': 'eastus'}

    def test_get_rack_names_per_datacenter_and_rack_idx(self):
        resp = "\n".join(["Datacenter: eastus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                          "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a",
                          "Datacenter: westus",
                          "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a"
                          ]
                         )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34", myrack=1)
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153", myrack=1)
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname='10.1.59.34', myrack=2)
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_rack_names_per_datacenter_and_rack_idx()
        assert datacenter_name_per_region == {('east-us', '1'): '1a', ('west-us', '2'): '2a'}

        datacenter_name_per_region = db_cluster.get_rack_names_per_datacenter_and_rack_idx(db_nodes=[node1])
        assert datacenter_name_per_region == {('east-us', '1'): '1a'}

        nodes_per_region = db_cluster.get_nodes_per_datacenter_and_rack_idx()
        assert nodes_per_region == {('east-us', '1'): [node1, node2], ('west-us', '2'): [node3, ]}

    def test_can_get_nodetool_status_ipv6(self):
        resp = "\n".join(["Datacenter: eu-north",
                          "====================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address                                 Load       Tokens       Owns    Host ID                Rack",
                          "UN  2a05:d016:cf8:de00:e07d:5832:c5c0:36a0  774 KB     256          ?       e2ed6943  1a",
                          "UN  2a05:d016:cf8:de00:339e:d0d:9446:1980   1.04 MB    256          ?       d67e8502  1a",
                          ]
                         )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {'eu-north':
                          {'2a05:d016:cf8:de00:e07d:5832:c5c0:36a0':
                           {'state': 'UN', 'load': '774KB', 'tokens': '256', 'owns': '?',
                            'host_id': 'e2ed6943', 'rack': '1a'},
                           '2a05:d016:cf8:de00:339e:d0d:9446:1980': {'state': 'UN', 'load': '1.04MB', 'tokens': '256', 'owns': '?',
                                                                     'host_id': 'd67e8502', 'rack': '1a'}}}

    def test_can_get_nodetool_status_azure(self):
        resp = "\n".join(["Datacenter: eastus",
                         "==================",
                          "Status=Up/Down",
                          "|/ State=Normal/Leaving/Joining/Moving",
                          "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                          "UN  10.0.0.4  431 KB     256          ?       ed6af9a0-8c22-4813-ac9b-6fbeb462b687  ",
                          "UN  10.0.0.5  612 KB     256          ?       caa15869-cfb4-4229-85d7-0f4832986237  1",
                          "UN  10.0.0.6  806 KB     256          ?       3046ded9-ce17-4a3a-ac44-a3ada6916972  ",
                          ""
                          ]
                         )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {'eastus': {'10.0.0.4': {'host_id': 'ed6af9a0-8c22-4813-ac9b-6fbeb462b687',
                                                  'load': '431KB',
                                                  'owns': '?',
                                                  'rack': '',
                                                  'state': 'UN',
                                                  'tokens': '256'},
                                     '10.0.0.5': {'host_id': 'caa15869-cfb4-4229-85d7-0f4832986237',
                                                  'load': '612KB',
                                                  'owns': '?',
                                                  'rack': '1',
                                                  'state': 'UN',
                                                  'tokens': '256'},
                                     '10.0.0.6': {'host_id': '3046ded9-ce17-4a3a-ac44-a3ada6916972',
                                                  'load': '806KB',
                                                  'owns': '?',
                                                  'rack': '',
                                                  'state': 'UN',
                                                  'tokens': '256'}
                                     }
                          }


@pytest.mark.parametrize("cat_results,expected_core_number", (
    ("1", 1), ("2", 1), ("9", 1), ("10", 1), ("11", 1),

    ("1-7", 7), ("2-7", 6), ("1-6", 6), ("2-6", 5),

    ("1-7,9", 8), ("1,9-15", 8), ("1-7,9-15", 14), ("2-7,10-15", 12),

    ("1-7,9-15,17-23", 21),
    ("1-7,9-15,17", 15),
    ("1-7,9,17-23", 15),
    ("1,9-15,17-23", 15),
    ("1,9,17", 3),
    ("1,9-15,17", 9),
    ("1-7,9,17", 9),
    ("1,9,17-23", 9),

    ("1-7,9-15,17-23,25-31", 28),
    ("1,9-15,17-23,25-31", 22),
    ("1-7,9,17-23,25-31", 22),
    ("1-7,9-15,23,25-31", 22),
    ("1-7,9-15,17-23,31", 22),
    ("1-7,15,17,25-31", 16),
    ("1,9-15,17-23,31", 16),
    ("1,15,17-23,31", 10),
    ("1,15,17,31", 4),
    ("1,9-15,17,25-31", 16),
    ("1-7,9,17-23,25", 16),
))
def test_base_node_cpuset(cat_results, expected_core_number):
    dummy_node = DummyNode(
        name='dummy_node',
        parent_cluster=None,
        base_logdir=tempfile.mkdtemp(),
        ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
    )
    dummy_node.parent_cluster = DummyDbCluster([dummy_node])
    dummy_node.init()
    cat_results_obj = type("FakeGrepResults", (), {
        "stdout": f'#\n# some comment\nCPUSET="--cpuset {cat_results} "'
    })
    dummy_node.remoter = type("FakeRemoter", (), {
        "run": (lambda *args, **kwargs: cat_results_obj),
    })

    cpuset_value = dummy_node.cpuset

    assert isinstance(cpuset_value, int)
    assert cpuset_value == expected_core_number


@pytest.mark.parametrize("cat_results", (
    "",
    "# one comment-line file",
    "# first comment line\n# second comment line",
    "# first comment line\n# second comment linei\n # third comment line",
    "# CPUSET=\"--cpuset 1-7,9-15,17-23,31' \"",
))
def test_base_node_cpuset_not_configured(cat_results):
    dummy_node = DummyNode(
        name='dummy_node',
        parent_cluster=None,
        base_logdir=tempfile.mkdtemp(),
        ssh_login_info=dict(key_file='~/.ssh/scylla-test'),
    )
    dummy_node.parent_cluster = DummyDbCluster([dummy_node])
    dummy_node.init()
    cat_results_obj = type("FakeCatResults", (), {"stdout": cat_results})
    dummy_node.remoter = type("FakeRemoter", (), {
        "run": (lambda *args, **kwargs: cat_results_obj),
    })
    assert dummy_node.cpuset == ""


@pytest.mark.integration
def test_get_any_ks_cf_list(docker_scylla, params, events):

    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        session.execute(
            "CREATE MATERIALIZED VIEW mview.users_by_first_name AS SELECT * FROM mview.users WHERE first_name "
            "IS NOT NULL and username IS NOT NULL PRIMARY KEY (first_name, username)")
        session.execute(
            "CREATE MATERIALIZED VIEW mview.users_by_last_name AS SELECT * FROM mview.users WHERE last_name "
            "IS NOT NULL and username IS NOT NULL PRIMARY KEY (last_name, username)")
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')")
        session.execute(
            "CREATE KEYSPACE \"123_keyspace\" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}")
        session.execute(
            "CREATE TABLE \"123_keyspace\".users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        session.execute(
            "CREATE TABLE \"123_keyspace\".\"120users\" (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        session.execute(
            "INSERT INTO \"123_keyspace\".users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')")
        session.execute(
            "INSERT INTO \"123_keyspace\".\"120users\" (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')")

    docker_scylla.run_nodetool('flush')

    table_names = cluster.get_any_ks_cf_list(docker_scylla, filter_empty_tables=False)
    assert set(table_names) == {'system.runtime_info', 'system_distributed.cdc_generation_timestamps',
                                'system.config', 'system.local', 'system.token_ring', 'system.clients',
                                'system.commitlog_cleanups', 'system.discovery', 'system.group0_history',
                                'system.raft', 'system.raft_snapshot_config', 'system.raft_snapshots', 'system.raft_state',
                                'system_schema.tables', 'system_schema.columns', 'system.compaction_history',
                                'system.cdc_local', 'system.versions', 'system.view_build_status_v2',
                                'system_distributed_everywhere.cdc_generation_descriptions_v2',
                                'system_replicated_keys.encrypted_keys',
                                'system.scylla_local', 'system.cluster_status', 'system.protocol_servers',
                                'system_distributed.cdc_streams_descriptions_v2', 'system_schema.keyspaces',
                                'system.size_estimates', 'system_schema.scylla_tables',
                                'system.scylla_table_schema_history', 'system_schema.views',
                                'system_distributed.view_build_status', 'system.built_views',
                                'mview.users_by_first_name', 'mview.users_by_last_name', 'mview.users',
                                'system."IndexInfo"', 'system.batchlog', 'system.compactions_in_progress',
                                'system.hints', 'system.large_cells', 'system.large_partitions', 'system.large_rows',
                                'system.paxos', 'system.peer_events', 'system.peers', 'system.range_xfers', 'system.repair_history',
                                'system.scylla_views_builds_in_progress', 'system.snapshots', 'system.sstable_activity',
                                'system.truncated', 'system.views_builds_in_progress',
                                'system_distributed.service_levels', 'system_schema.aggregates',
                                'system_schema.computed_columns', 'system_schema.dropped_columns', 'system_schema.functions',
                                'system_schema.indexes', 'system_schema.scylla_aggregates', 'system_schema.scylla_keyspaces',
                                'system_schema.triggers', 'system_schema.types', 'system_schema.view_virtual_columns',
                                'system_traces.events', 'system_traces.node_slow_log', 'system_traces.node_slow_log_time_idx',
                                'system_traces.sessions', 'system_traces.sessions_time_idx',
                                'system.role_attributes', 'system.role_members', 'system.role_permissions',
                                'system.roles', 'system.service_levels_v2',
                                'system.topology', 'system.topology_requests',
                                'system.cdc_generations_v3', 'system.tablets', 'system.view_build_status_v2',
                                'system_replicated_keys.encrypted_keys', 'system.dicts',
                                '"123_keyspace"."120users"', '"123_keyspace".users'}

    table_names = cluster.get_non_system_ks_cf_list(docker_scylla, filter_empty_tables=False, filter_out_mv=True)
    assert set(table_names) == {'mview.users', '"123_keyspace"."120users"', '"123_keyspace".users'}

    table_names = cluster.get_non_system_ks_cf_list(docker_scylla, filter_empty_tables=False)
    assert set(table_names) == {'mview.users_by_first_name', 'mview.users_by_last_name',
                                'mview.users', '"123_keyspace"."120users"', '"123_keyspace".users'}

    table_names = cluster.get_non_system_ks_cf_list(docker_scylla, filter_empty_tables=True, filter_out_mv=True)
    assert set(table_names) == {'mview.users', '"123_keyspace"."120users"', '"123_keyspace".users'}


@pytest.fixture
def prepared_keyspaces(docker_scylla, params):

    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        ks = "testks"
        base_cf = "test_table"
        counter_cf = "counter_table"

        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {ks}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
        """)

        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {ks}.{base_cf} (
                id uuid PRIMARY KEY,
                txt text,
                tags list<text>,
                kv map<text, text>,
                nums set<int>,
                d duration
            );
        """)

        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {ks}.{counter_cf} (
                id uuid PRIMARY KEY,
                cnt counter
            );
        """)

    return cluster, ks, base_cf, counter_cf


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # All regular (non-PK) columns
        ({"is_primary_key": False}, {"txt", "tags", "kv", "nums", "d"}),

        # Filter out collections
        ({"is_primary_key": False, "filter_out_collections": True}, {"txt", "d"}),

        # Filter out unsupported types
        ({"is_primary_key": False, "filter_out_column_types": ["duration"]}, {"txt", "tags", "kv", "nums"}),

        # Filter out collections + unsupported types
        (
            {"is_primary_key": False, "filter_out_collections": True, "filter_out_column_types": ["duration"]},
            {"txt"}
        ),

        # Get PK column
        ({"is_primary_key": True}, {"id"}),

        # Filter out PK column by type
        ({"is_primary_key": True, "filter_out_column_types": ["uuid"]}, set()),
    ]
)
@pytest.mark.integration
def test_general_table_column_filtering(prepared_keyspaces, kwargs, expected):
    cluster, ks, base_cf, _ = prepared_keyspaces
    with cluster.cql_connection_patient(cluster.nodes[0]) as session:
        result = set(get_column_names(session, ks, base_cf, **kwargs))
        assert result == expected


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        # Regular counter column (non-PK)
        ({"is_primary_key": False}, {"cnt"}),

        # Filter out counter type (non-PK)
        ({"is_primary_key": False, "filter_out_column_types": ["counter"]}, set()),

        # Get PK column
        ({"is_primary_key": True}, {"id"}),

        # Filter out PK column by type
        ({"is_primary_key": True, "filter_out_column_types": ["uuid"]}, set()),
    ]
)
@pytest.mark.integration
def test_counter_table_column_filtering(prepared_keyspaces, kwargs, expected):
    cluster, ks, _, counter_cf = prepared_keyspaces
    with cluster.cql_connection_patient(cluster.nodes[0]) as session:
        result = set(get_column_names(session, ks, counter_cf, **kwargs))
        assert result == expected


@pytest.mark.integration
def test_filter_out_ks_with_rf_one(docker_scylla, params, events):

    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}")
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')")
        docker_scylla.run_nodetool('flush')

        table_names = cluster.get_non_system_ks_cf_list(docker_scylla, filter_func=cluster.is_ks_rf_one)
        assert table_names == []


@pytest.mark.integration
def test_is_table_has_no_sstables(docker_scylla, params, events):
    """
    test is_table_has_no_sstables filter function, as it would be used in `disrupt_snapshot_operations` nemesis
    """
    cluster = DummyScyllaCluster([docker_scylla])
    cluster.params = params

    with cluster.cql_connection_patient(docker_scylla) as session:
        session.execute(
            "CREATE KEYSPACE mview WITH replication = {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'} "
            "AND durable_writes = true AND tablets = {'enabled': false}")
        session.execute(
            "CREATE TABLE mview.users (username text, first_name text, last_name text, password text, email text, "
            "last_access timeuuid, PRIMARY KEY(username))")
        session.execute(
            "INSERT INTO mview.users (username, first_name, last_name, password) VALUES "
            "('fruch', 'Israel', 'Fruchter', '1111')")
        docker_scylla.run_nodetool('flush')

        def is_virtual_tables_get_snapshot():
            """
            scylla commit https://github.com/scylladb/scylladb/commit/24589cf00cf8f1fae0b19a2ac1bd7b637061301a
            has stopped creating snapshots for virtual tables.
            hence we need to filter them out when compare tables to snapshot content.
            """
            if docker_scylla.is_enterprise:
                return ComparableScyllaVersion(docker_scylla.scylla_version) >= "2024.3.0-dev"
            else:
                return ComparableScyllaVersion(docker_scylla.scylla_version) >= "6.3.0-dev"

        if is_virtual_tables_get_snapshot():
            ks_cf = cluster.get_any_ks_cf_list(
                docker_scylla, filter_func=cluster.is_table_has_no_sstables, filter_empty_tables=False)
        else:
            ks_cf = cluster.get_any_ks_cf_list(docker_scylla, filter_empty_tables=False)

        keyspace_table = []
        ks_cf = [k_c.replace('"', '') for k_c in ks_cf]
        keyspace_table.extend([k_c.split('.') for k_c in ks_cf])

        result = docker_scylla.run_nodetool('snapshot')
        snapshot_name = re.findall(r'(\d+)', result.stdout.split("snapshot name")[1])[0]

        result = docker_scylla.run_nodetool('listsnapshots')
        logging.debug(result)
        snapshots_content = parse_nodetool_listsnapshots(listsnapshots_output=result.stdout)
        snapshot_content = snapshots_content[snapshot_name]
        logging.debug(snapshot_content)

        snapshot_content_list = [[elem.keyspace_name, elem.table_name] for elem in snapshot_content]
        if sorted(keyspace_table) != sorted(snapshot_content_list):
            raise AssertionError(f"Snapshot content not as expected. \n"
                                 f"Expected content: {sorted(keyspace_table)} \n "
                                 f"Actual snapshot content: {sorted(snapshot_content_list)}")


@pytest.mark.integration
def test_exclusive_connection(docker_scylla, docker_scylla_2, params, events):
    """
    Test exclusive CQL connection creation for each node in the cluster.
    Ensures that the session connects to the correct node.
    Run 10 times to increase the chance of catching intermittent issues.
    """
    cluster = DummyScyllaCluster([docker_scylla, docker_scylla_2])
    cluster.params = params

    for i in range(10):
        for node in cluster.nodes:
            with cluster.cql_connection_patient_exclusive(node) as session:
                print(f"Iteration {i}, Node {node.cql_address}")
                local = session.execute("SELECT host_id, rpc_address FROM system.local").one()
                peers = session.execute("SELECT host_id, peer, rpc_address FROM system.peers").one()
                assert local.rpc_address == node.cql_address, (
                    f"Local rpc_address: {local.rpc_address}, expected: {node.cql_address}"
                )
                assert peers.rpc_address != node.cql_address, (
                    f"Peers rpc_address: {peers.rpc_address}, expected not: {node.cql_address}"
                )


class TestNodetool(unittest.TestCase):
    def test_describering_parsing(self):
        """ Test "nodetool describering" output parsing """
        resp = "\n".join(["Schema Version:00703362-03ed-3b41-afcb-ed34c1d1586c TokenRange:",
                          "TokenRange(start_token:-9193109213506951143, end_token:9202125676696964746, "
                          "endpoints:[127.0.49.3], rpc_endpoints:[127.0.49.3], "
                          "endpoint_details:[EndpointDetails(host:127.0.49.3, datacenter:datacenter1, rack:rack1)])",
                          "TokenRange(start_token:9154793403047166459, end_token:9156354786201613199, "
                          "endpoints:[127.0.49.3], rpc_endpoints:[127.0.49.3], "
                          "endpoint_details:[EndpointDetails(host:127.0.49.3, datacenter:datacenter1, rack:rack1)])",
                          ]
                         )
        node = NodetoolDummyNode(resp=resp)
        ranges = get_keyspace_partition_ranges(node=node, keyspace="")
        assert ranges == [{'start_token': -9193109213506951143,
                           'details': [{'host': '127.0.49.3', 'datacenter': 'datacenter1', 'rack': 'rack1'}],
                           'end_token': 9202125676696964746, 'endpoints': '127.0.49.3', 'rpc_endpoints': '127.0.49.3'},
                          {'start_token': 9154793403047166459,
                           'details': [{'host': '127.0.49.3', 'datacenter': 'datacenter1', 'rack': 'rack1'}],
                           'end_token': 9156354786201613199, 'endpoints': '127.0.49.3', 'rpc_endpoints': '127.0.49.3'}]

        min_token, max_token = keyspace_min_max_tokens(node=node, keyspace="")
        assert min_token == -9193109213506951143
        assert max_token == 9202125676696964746

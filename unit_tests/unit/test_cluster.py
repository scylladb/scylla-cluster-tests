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

import importlib
import inspect
import logging
import tempfile
import time
import unittest.mock
from datetime import datetime, timezone

import pytest
from invoke import Result

from sdcm.cluster import BaseCluster, BaseMonitorSet, BaseNode
from sdcm.db_log_reader import DbLogReader
from sdcm.sct_events.database import SYSTEM_ERROR_EVENTS_PATTERNS
from sdcm.sct_events.filters import DbEventsFilter
from sdcm.sct_events.group_common_events import ignore_upgrade_schema_errors
from sdcm.sct_events.system import InstanceStatusEvent
from sdcm.utils.common import (
    get_keyspace_partition_ranges,
    keyspace_min_max_tokens,
)
from sdcm.utils.distro import Distro
from sdcm.remote import LocalCmdRunner
from sdcm.sct_config import SCTConfiguration
from unit_tests.lib.dummy_remote import DummyRemote, LocalNode
from unit_tests.lib.fake_cluster import DummyDbCluster, DummyNode, DummyScyllaCluster, NodetoolDummyNode


class TestBaseNode:
    @pytest.fixture(autouse=True)
    def setup_events(self, events_function_scope, tmp_path):
        """Use per-test events fixture for proper isolation between tests."""
        self._events = events_function_scope
        self._tmp_path = tmp_path
        self._init_node()

    def _init_node(self):
        if not hasattr(self, "_node"):
            self._node = DummyNode(
                name="test_node",
                parent_cluster=None,
                base_logdir=str(self._tmp_path),
                ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
            )
            self._node.parent_cluster = DummyDbCluster(nodes=[self._node])
            self._node.init()
            self._node.remoter = DummyRemote()

    @pytest.fixture(autouse=True)
    def inject_test_data_dir(self, setup_events, test_data_dir):  # noqa: ARG002
        self.test_data_dir = test_data_dir
        self.node.system_log = str(test_data_dir / "system.log")

    @property
    def node(self):
        return self._node

    @property
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
            with tempfile.NamedTemporaryFile(mode="wt") as temp_log:
                self.node.system_log = temp_log.name
                db_log_reader = self._db_log_reader

                for line in log_text.splitlines(keepends=True):
                    temp_log.write(line)
                    temp_log.flush()
                    db_log_reader._read_and_publish_events()
        else:
            self._db_log_reader._read_and_publish_events()

    def test_search_system_log(self):
        critical_errors = list(self.node.follow_system_log(start_from_beginning=True))
        assert 33 == len(critical_errors)

    def test_search_system_log_specific_log(self):
        errors = list(
            self.node.follow_system_log(patterns=["Failed to load schema version"], start_from_beginning=True)
        )
        assert len(errors) == 2

    def test_search_system_interlace_reactor_stall(self):
        self.node.system_log = str(self.test_data_dir / "system_interlace_stall.log")

        self._read_and_publish_events()

        events = self._events.published_events

        event_a, event_b = events[-2], events[-1]
        print(event_a)
        print(event_b)

        assert event_a["type"] == "REACTOR_STALLED"
        assert event_a["line_number"] == 0
        assert event_b["type"] == "REACTOR_STALLED"
        assert event_b["line_number"] == 3

    def test_search_kernel_callstack(self):
        self.node.parent_cluster = {"params": {"print_kernel_callstack": True}}
        self.node.system_log = str(self.test_data_dir / "kernel_callstack.log")
        self._read_and_publish_events()

        events = self._events.published_events

        event_a, event_b = events[-2], events[-1]
        print(event_a)
        print(event_b)

        assert event_a["type"] == "KERNEL_CALLSTACK"
        assert event_a["line_number"] == 2
        assert event_b["type"] == "KERNEL_CALLSTACK"
        assert event_b["line_number"] == 5

    def test_search_cdc_invalid_request(self):
        self.node.system_log = str(self.test_data_dir / "system_cdc_invalid_request.log")
        with unittest.mock.patch("sdcm.sct_events.group_common_events.TestConfig"):
            with unittest.mock.patch("sdcm.sct_events.group_common_events.SkipPerIssues") as skip_per_issues:
                skip_per_issues.return_value = False
                with ignore_upgrade_schema_errors():
                    self._read_and_publish_events()

        cdc_err_events = [
            line
            for line in self._events.get_events_by_category()["ERROR"]
            if "cdc - Could not retrieve CDC streams" in line
        ]
        assert cdc_err_events != []

    def test_search_power_off(self):
        self.node.system_log = str(self.test_data_dir / "power_off.log")
        with DbEventsFilter(db_event=InstanceStatusEvent.POWER_OFF, node=self.node):
            self._read_and_publish_events()

        InstanceStatusEvent.POWER_OFF().add_info(
            node="A",
            line_number=22,
            line=f"{datetime.fromtimestamp(time.time() + 1, tz=timezone.utc):%Y-%m-%dT%H:%M:%S+00:00} "
            "longevity-large-collections-12h-mas-db-node-c6a4e04e-1 !INFO    | systemd-logind: Powering Off...",
        ).publish()

        events = [line for line in self._events.get_events_by_category()["WARNING"] if "Powering Off" in line]
        assert events

    def test_search_system_suppressed_messages(self):
        self.node.system_log = str(self.test_data_dir / "system_suppressed_messages.log")

        self._read_and_publish_events()

        events = self._events.published_events

        event_a = events[-1]
        print(event_a)

        assert event_a["type"] == "SUPPRESSED_MESSAGES", "Not expected event type {}".format(event_a["type"])
        assert event_a["line_number"] == 6, "Not expected event line number {}".format(event_a["line_number"])

    def test_search_one_line_backtraces(self):
        self.node.system_log = str(self.test_data_dir / "system_one_line_backtrace.log")

        self._read_and_publish_events()

        events = self._events.published_events

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
        self.node.system_log = str(self.test_data_dir / "gate_closed_ignored_exception.log")

        self._read_and_publish_events()

        events = self._events.published_events

        event_backtrace1, event_backtrace2 = events[-2], events[-1]
        print(event_backtrace1)
        print(event_backtrace2)

        assert event_backtrace1["type"] == "GATE_CLOSED"
        assert event_backtrace1["line_number"] == 1
        assert event_backtrace2["type"] == "GATE_CLOSED"
        assert event_backtrace2["line_number"] == 3

    def test_compaction_stopped_exception_is_catched(self):
        self.node.system_log = str(self.test_data_dir / "compaction_stopped_exception.log")

        self._read_and_publish_events()

        events = self._events.published_events

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

        events = self._events.published_events
        reactor_stalls = [event for event in events if event["type"] == "REACTOR_STALLED"]
        assert len(reactor_stalls) == 1
        event = reactor_stalls[0]
        assert event["type"] == "REACTOR_STALLED"
        assert event["line_number"] == 2
        assert "Reactor stalled for 32 ms on shard 1" in event["line"]


class VersionDummyRemote:
    def __init__(self, test, results):
        self.results = iter(results)

    def run(self, cmd, *_, **__):
        expected_cmd, result = next(self.results)
        assert cmd == expected_cmd
        return Result(exited=result[0], stdout=result[1], stderr=result[2])


class TestBaseNodeGetScyllaVersion:
    @pytest.fixture(autouse=True, scope="class")
    def _class_tmp_dir(self, tmp_path_factory):
        TestBaseNodeGetScyllaVersion.temp_dir = str(tmp_path_factory.mktemp("scylla_version"))

    def setup_method(self):
        self.node = DummyNode(
            name="test_node",
            parent_cluster=None,
            base_logdir=self.temp_dir,
            ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
        )
        self.node.parent_cluster = DummyDbCluster([self.node])

    def test_no_scylla_binary_rhel_like(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
                ("rpm --query --queryformat '%{VERSION}' scylla", (0, "3.3.rc1", "")),
            ),
        )
        assert "3.3.rc1" == self.node.scylla_version
        assert "3.3.rc1" == self.node.scylla_version_detailed

    def test_no_scylla_binary_other(self):
        self.node.distro = Distro.DEBIAN11
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
                ("dpkg-query --show --showformat '${Version}' scylla", (0, "3.3~rc1-0.20200209.0d0c1d43188-1", "")),
            ),
        )
        assert "3.3.rc1" == self.node.scylla_version
        assert "3.3.rc1-0.20200209.0d0c1d43188-1" == self.node.scylla_version_detailed

    def test_scylla(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "3.3.rc1" == self.node.scylla_version
        assert "3.3.rc1-0.20200209.0d0c1d43188 with build-id xxx" == self.node.scylla_version_detailed

    def test_scylla_master(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "666.development-0.20200205.2816404f575\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "666.development" == self.node.scylla_version
        assert "666.development-0.20200205.2816404f575 with build-id xxx" == self.node.scylla_version_detailed

    def test_scylla_master_new_format(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "4.4.dev" == self.node.scylla_version
        assert "4.4.dev-0.20200205.2816404f575 with build-id xxx" == self.node.scylla_version_detailed

    def test_scylla_enterprise(self):
        self.node.is_enterprise = True
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "2019.1.4-0.20191217.b59e92dbd\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "2019.1.4" == self.node.scylla_version
        assert "2019.1.4-0.20191217.b59e92dbd with build-id xxx" == self.node.scylla_version_detailed

    def test_scylla_enterprise_no_scylla_binary(self):
        self.node.is_enterprise = True
        self.node.is_product_enterprise = True
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
                ("rpm --query --queryformat '%{VERSION}' scylla-enterprise", (0, "2019.1.4", "")),
            ),
        )
        assert "2019.1.4" == self.node.scylla_version
        assert "2019.1.4" == self.node.scylla_version_detailed

    def test_scylla_binary_version_unparseable(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "x.y.z\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert self.node.scylla_version is None
        assert "x.y.z with build-id xxx" == self.node.scylla_version_detailed

    def test_get_scylla_version_from_second_attempt(self):
        self.node.distro = Distro.DEBIAN11
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
                (
                    "dpkg-query --show --showformat '${Version}' scylla",
                    (1, "", "dpkg-query: no packages found matching scylla\n"),
                ),
                ("/usr/bin/scylla --version", (127, "", "bash: scylla: command not found\n")),
                (
                    "dpkg-query --show --showformat '${Version}' scylla",
                    (1, "", "dpkg-query: no packages found matching scylla\n"),
                ),
            ),
        )
        assert self.node.scylla_version is None
        assert self.node.scylla_version_detailed is None

        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "4.4.dev" == self.node.scylla_version
        assert "4.4.dev-0.20200205.2816404f575 with build-id xxx" == self.node.scylla_version_detailed

    def test_forget_scylla_version(self):
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert "4.4.dev" == self.node.scylla_version
        assert "4.4.dev-0.20200205.2816404f575 with build-id xxx" == self.node.scylla_version_detailed

        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "3.3.rc1-0.20200209.0d0c1d43188\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )

        assert "4.4.dev" == self.node.scylla_version
        assert "4.4.dev-0.20200205.2816404f575 with build-id xxx" == self.node.scylla_version_detailed

        self.node.forget_scylla_version()

        assert "3.3.rc1" == self.node.scylla_version
        assert "3.3.rc1-0.20200209.0d0c1d43188 with build-id xxx" == self.node.scylla_version_detailed


class TestBaseMonitorSet:
    @pytest.fixture(autouse=True, scope="class")
    def _class_tmp_dir(self, tmp_path_factory):
        TestBaseMonitorSet.temp_dir = str(tmp_path_factory.mktemp("monitor_set"))

    def setup_method(self):
        self.node = DummyNode(
            name="test_node",
            parent_cluster=None,
            base_logdir=self.temp_dir,
            ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
        )
        self.db_cluster = DummyDbCluster([self.node])
        self.monitor_cluster = BaseMonitorSet({"db_cluster": self.db_cluster}, params=SCTConfiguration())
        self.monitor_cluster.log = logging

    def test_monitoring_version(self):
        """
        verify that dev version are mapped to monitor master version
        """
        self.node.remoter = VersionDummyRemote(
            self,
            (
                ("/usr/bin/scylla --version", (0, "4.4.dev-0.20200205.2816404f575\n", "")),
                ("/usr/bin/scylla --build-id", (0, "xxx", "")),
            ),
        )
        assert self.monitor_cluster.monitoring_version == "master"


class TestNodetoolStatus:
    def test_can_get_nodetool_status_typical(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
                "==================",
                "Status=Up/Down",
                "|/ State=Normal/Leaving/Joining/Moving",
                "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                "UN  10.0.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a",
                "UN  10.0.198.153  ?          256          ?       fba174cd-917a-40f6-ab62-cc58efaaf301  1a",
            ]
        )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {
            "eastus": {
                "10.0.59.34": {
                    "state": "UN",
                    "load": "21.71GB",
                    "tokens": "256",
                    "owns": "?",
                    "host_id": "e5bcb094-e4de-43aa-8dc9-b1bf74b3b346",
                    "rack": "1a",
                },
                "10.0.198.153": {
                    "state": "UN",
                    "load": "?",
                    "tokens": "256",
                    "owns": "?",
                    "host_id": "fba174cd-917a-40f6-ab62-cc58efaaf301",
                    "rack": "1a",
                },
            }
        }

    def test_can_get_nodetool_status_typical_with_one_space_after_host_id(self):
        """case for https://github.com/scylladb/scylla-cluster-tests/issues/7274"""
        resp = "\n".join(
            [
                "Datacenter: datacenter1",
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

        assert status == {
            "datacenter1": {
                "172.17.0.2": {
                    "state": "UN",
                    "load": "202.92KB",
                    "tokens": "256",
                    "owns": "?",
                    "host_id": "7b8f86bf-c70c-4246-a273-146057e12431",
                    "rack": "rack1",
                },
            }
        }

    def test_datacenter_name_per_region(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
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
                "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a",
            ]
        )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34")
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname="10.1.59.34")
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {"east-us": "eastus", "west-us": "westus"}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node1])
        assert datacenter_name_per_region == {"east-us": "eastus"}

    def test_datacenter_name_per_region_when_region_doesnt_have_live_nodes(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
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
                "DN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a",
            ]
        )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34")
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname="10.1.59.34", db_up=False)
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {"east-us": "eastus"}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node3])
        assert datacenter_name_per_region == {}

    def test_datacenter_name_per_region_when_first_node_is_dn(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
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
                "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a",
            ]
        )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34", db_up=False)
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153")
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname="10.1.59.34")
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region()
        assert datacenter_name_per_region == {"east-us": "eastus", "west-us": "westus"}

        datacenter_name_per_region = db_cluster.get_datacenter_name_per_region(db_nodes=[node1, node2])
        assert datacenter_name_per_region == {"east-us": "eastus"}

    def test_get_rack_names_per_datacenter_and_rack_idx(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
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
                "UN  10.1.59.34    21.71 GB   256          ?       e5bcb094-e4de-43aa-8dc9-b1bf74546346  2a",
            ]
        )
        node1 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.59.34", myrack=1)
        node2 = NodetoolDummyNode(resp=resp, myregion="east-us", myname="10.0.198.153", myrack=1)
        node3 = NodetoolDummyNode(resp=resp, myregion="west-us", myname="10.1.59.34", myrack=2)
        db_cluster = DummyScyllaCluster([node1, node2, node3])
        setattr(db_cluster, "params", {"use_zero_nodes": False})
        node1.parent_cluster = node2.parent_cluster = node3.parent_cluster = db_cluster
        datacenter_name_per_region = db_cluster.get_rack_names_per_datacenter_and_rack_idx()
        assert datacenter_name_per_region == {("east-us", "1"): "1a", ("west-us", "2"): "2a"}

        datacenter_name_per_region = db_cluster.get_rack_names_per_datacenter_and_rack_idx(db_nodes=[node1])
        assert datacenter_name_per_region == {("east-us", "1"): "1a"}

        nodes_per_region = db_cluster.get_nodes_per_datacenter_and_rack_idx()
        assert nodes_per_region == {
            ("east-us", "1"): [node1, node2],
            ("west-us", "2"): [
                node3,
            ],
        }

    def test_can_get_nodetool_status_ipv6(self):
        resp = "\n".join(
            [
                "Datacenter: eu-north",
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

        assert status == {
            "eu-north": {
                "2a05:d016:cf8:de00:e07d:5832:c5c0:36a0": {
                    "state": "UN",
                    "load": "774KB",
                    "tokens": "256",
                    "owns": "?",
                    "host_id": "e2ed6943",
                    "rack": "1a",
                },
                "2a05:d016:cf8:de00:339e:d0d:9446:1980": {
                    "state": "UN",
                    "load": "1.04MB",
                    "tokens": "256",
                    "owns": "?",
                    "host_id": "d67e8502",
                    "rack": "1a",
                },
            }
        }

    def test_can_get_nodetool_status_azure(self):
        resp = "\n".join(
            [
                "Datacenter: eastus",
                "==================",
                "Status=Up/Down",
                "|/ State=Normal/Leaving/Joining/Moving",
                "--  Address   Load       Tokens       Owns    Host ID                               Rack",
                "UN  10.0.0.4  431 KB     256          ?       ed6af9a0-8c22-4813-ac9b-6fbeb462b687  ",
                "UN  10.0.0.5  612 KB     256          ?       caa15869-cfb4-4229-85d7-0f4832986237  1",
                "UN  10.0.0.6  806 KB     256          ?       3046ded9-ce17-4a3a-ac44-a3ada6916972  ",
                "",
            ]
        )
        node = NodetoolDummyNode(resp=resp)
        db_cluster = DummyScyllaCluster([node])

        status = db_cluster.get_nodetool_status()

        assert status == {
            "eastus": {
                "10.0.0.4": {
                    "host_id": "ed6af9a0-8c22-4813-ac9b-6fbeb462b687",
                    "load": "431KB",
                    "owns": "?",
                    "rack": "",
                    "state": "UN",
                    "tokens": "256",
                },
                "10.0.0.5": {
                    "host_id": "caa15869-cfb4-4229-85d7-0f4832986237",
                    "load": "612KB",
                    "owns": "?",
                    "rack": "1",
                    "state": "UN",
                    "tokens": "256",
                },
                "10.0.0.6": {
                    "host_id": "3046ded9-ce17-4a3a-ac44-a3ada6916972",
                    "load": "806KB",
                    "owns": "?",
                    "rack": "",
                    "state": "UN",
                    "tokens": "256",
                },
            }
        }


@pytest.mark.parametrize(
    "cat_results,expected_core_number",
    (
        ("1", 1),
        ("2", 1),
        ("9", 1),
        ("10", 1),
        ("11", 1),
        ("1-7", 7),
        ("2-7", 6),
        ("1-6", 6),
        ("2-6", 5),
        ("1-7,9", 8),
        ("1,9-15", 8),
        ("1-7,9-15", 14),
        ("2-7,10-15", 12),
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
    ),
)
def test_base_node_cpuset(cat_results, expected_core_number, tmp_path):
    dummy_node = DummyNode(
        name="dummy_node",
        parent_cluster=None,
        base_logdir=str(tmp_path),
        ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
    )
    dummy_node.parent_cluster = DummyDbCluster([dummy_node])
    dummy_node.init()
    cat_results_obj = type("FakeGrepResults", (), {"stdout": f'#\n# some comment\nCPUSET="--cpuset {cat_results} "'})
    dummy_node.remoter = type(
        "FakeRemoter",
        (),
        {
            "run": (lambda *args, **kwargs: cat_results_obj),
        },
    )

    cpuset_value = dummy_node.cpuset

    assert isinstance(cpuset_value, int)
    assert cpuset_value == expected_core_number


@pytest.mark.parametrize(
    "cat_results",
    (
        "",
        "# one comment-line file",
        "# first comment line\n# second comment line",
        "# first comment line\n# second comment linei\n # third comment line",
        '# CPUSET="--cpuset 1-7,9-15,17-23,31\' "',
    ),
)
def test_base_node_cpuset_not_configured(cat_results, tmp_path):
    dummy_node = DummyNode(
        name="dummy_node",
        parent_cluster=None,
        base_logdir=str(tmp_path),
        ssh_login_info=dict(key_file="~/.ssh/scylla-test"),
    )
    dummy_node.parent_cluster = DummyDbCluster([dummy_node])
    dummy_node.init()
    cat_results_obj = type("FakeCatResults", (), {"stdout": cat_results})
    dummy_node.remoter = type(
        "FakeRemoter",
        (),
        {
            "run": (lambda *args, **kwargs: cat_results_obj),
        },
    )
    assert dummy_node.cpuset == ""


class TestNodetool:
    def test_describering_parsing(self):
        """Test "nodetool describering" output parsing"""
        resp = "\n".join(
            [
                "Schema Version:00703362-03ed-3b41-afcb-ed34c1d1586c TokenRange:",
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
        assert ranges == [
            {
                "start_token": -9193109213506951143,
                "details": [{"host": "127.0.49.3", "datacenter": "datacenter1", "rack": "rack1"}],
                "end_token": 9202125676696964746,
                "endpoints": "127.0.49.3",
                "rpc_endpoints": "127.0.49.3",
            },
            {
                "start_token": 9154793403047166459,
                "details": [{"host": "127.0.49.3", "datacenter": "datacenter1", "rack": "rack1"}],
                "end_token": 9156354786201613199,
                "endpoints": "127.0.49.3",
                "rpc_endpoints": "127.0.49.3",
            },
        ]

        min_token, max_token = keyspace_min_max_tokens(node=node, keyspace="")
        assert min_token == -9193109213506951143
        assert max_token == 9202125676696964746


def test_base_node_init_with_none_ssh_login_info():
    """Verify that node initialization does not crash when ssh_login_info is not defined."""
    node = LocalNode(
        name="test_local_node",
        parent_cluster=DummyDbCluster(nodes=[]),
    )

    node.init()

    assert isinstance(node.remoter, LocalCmdRunner), f"Expected LocalCmdRunner, got {type(node.remoter)}"


# backend modules whose `BaseNode` subclasses must be loaded into the class hierarchy
_BACKEND_MODULES = (
    "sdcm.cluster_aws",
    "sdcm.cluster_azure",
    "sdcm.cluster_baremetal",
    "sdcm.cluster_cloud",
    "sdcm.cluster_docker",
    "sdcm.cluster_gce",
    "sdcm.cluster_k8s",
    "sdcm.cluster_k8s.eks",
    "sdcm.cluster_k8s.gke",
    "sdcm.cluster_k8s.mini_k8s",
    "sdcm.cluster_oci",
    "sdcm.utils.docker_remote",
)


def _all_base_node_subclasses() -> list[type]:
    """Return all `BaseNode` subclasses from the `sdcm` package, excluding test fixtures."""
    for module_name in _BACKEND_MODULES:
        importlib.import_module(module_name)

    all_subclasses = set()
    stack = list(BaseNode.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls not in all_subclasses:
            all_subclasses.add(cls)
            stack.extend(cls.__subclasses__())

    production = [cls for cls in all_subclasses if cls.__module__.startswith("sdcm.")]
    return sorted(production, key=lambda c: c.__name__)


# classes that use cloud-SDK clients in __init__ and need those patched out during construction
_INIT_CONSTRUCT_PATCHES: dict[str, tuple[str, ...]] = {
    "GCENode": ("sdcm.cluster_gce.GceLoggingClient",),
}

# `BaseNode.__init__` kwargs a subclass intentionally does NOT accept.
_NO_SSH = frozenset({"ssh_login_info"})
_NO_DC_RACK = frozenset({"dc_idx", "rack"})
_NARROWED_KWARGS: dict[str, frozenset[str]] = {
    "AWSNode": _NO_SSH,
    "AzureNode": _NO_SSH,
    "BasePodContainer": _NO_SSH,
    "CloudManagerNode": _NO_SSH,
    "CloudNode": _NO_SSH,
    "CloudVSNode": _NO_SSH,
    "GCENode": _NO_SSH,
    "LoaderPodContainer": _NO_SSH,
    "OciNode": _NO_SSH,
    "VectorStoreAWSNode": _NO_SSH,
    "DockerMonitoringNode": _NO_DC_RACK,
    "DockerNode": _NO_DC_RACK,
    "VectorStoreDockerNode": _NO_DC_RACK,
    "PhysicalMachineNode": _NO_DC_RACK | _NO_SSH,
    "RemoteDocker": _NO_DC_RACK | _NO_SSH | frozenset({"base_logdir", "node_prefix"}),
}


def _build_init_kwargs(cls: type) -> dict:
    """Build the minimal kwargs needed to construct `cls` for this test."""
    kwargs: dict[str, object] = {}
    constructor_params = inspect.signature(cls.__init__).parameters

    for name, param in constructor_params.items():
        if name == "self" or param.default is not inspect.Parameter.empty:
            continue
        if name == "cloud_instance_data":
            kwargs[name] = {}
            continue
        if name == "parent_cluster":
            parent_cluster = unittest.mock.MagicMock(name="parent_cluster")
            parent_cluster.params = {}
            kwargs[name] = parent_cluster
            continue
        kwargs[name] = unittest.mock.MagicMock(name=name)

    narrowed_kwargs = _NARROWED_KWARGS.get(cls.__name__, frozenset())
    for name, param in inspect.signature(BaseNode.__init__).parameters.items():
        if param.default is inspect.Parameter.empty or name in kwargs or name in narrowed_kwargs:
            continue
        kwargs[name] = param.default

    return kwargs


@pytest.mark.parametrize("cls", _all_base_node_subclasses(), ids=lambda cls: cls.__name__)
def test_base_node_subclass_constructs_with_forwarded_kwargs(cls, monkeypatch):
    """Verify each `BaseNode` subclass can be constructed polymorphically."""
    if "__init__" not in cls.__dict__:
        pytest.skip(f"{cls.__name__} inherits __init__ from a parent")

    for target in _INIT_CONSTRUCT_PATCHES.get(cls.__name__, ()):
        monkeypatch.setattr(target, unittest.mock.MagicMock())

    cls(**_build_init_kwargs(cls))


def _cluster_classes_overriding_create_node() -> list[type]:
    """Return all `BaseCluster` subclasses from the `sdcm` package that override `_create_node`."""
    for module_name in _BACKEND_MODULES:
        importlib.import_module(module_name)

    all_subclasses = set()
    stack = list(BaseCluster.__subclasses__())
    while stack:
        cls = stack.pop()
        if cls not in all_subclasses:
            all_subclasses.add(cls)
            stack.extend(cls.__subclasses__())

    production = [
        cls for cls in all_subclasses if cls.__module__.startswith("sdcm.") and "_create_node" in cls.__dict__
    ]
    return sorted(production, key=lambda c: c.__name__)


def _parent_create_node_kwargs(cls: type) -> dict | None:
    """Build kwargs from the closest ancestor's `_create_node` signature, or None if no ancestor defines it."""
    for ancestor in cls.__mro__[1:]:
        if "_create_node" not in ancestor.__dict__:
            continue

        signature = inspect.signature(ancestor._create_node)
        return {
            name: (
                param.default if param.default is not inspect.Parameter.empty else unittest.mock.MagicMock(name=name)
            )
            for name, param in signature.parameters.items()
            if name != "self"
        }
    return None


@pytest.mark.parametrize("cls", _cluster_classes_overriding_create_node(), ids=lambda cls: cls.__name__)
def test_cluster_create_node_accepts_parent_kwargs(cls):
    """Verify every `_create_node` override accepts the kwargs its parent's `add_nodes` forwards.

    Bypasses cluster `__init__` via `__new__` — Python validates kwargs at the call
    boundary before the body runs, so any TypeError about unexpected/missing/duplicate
    kwargs surfaces a signature mismatch. Other exceptions come from the body running
    against the bare cluster, which is unrelated to what we're checking.
    """
    kwargs = _parent_create_node_kwargs(cls)
    if kwargs is None:
        pytest.skip(f"{cls.__name__} has no ancestor with `_create_node`")

    try:
        cls._create_node(cls.__new__(cls), **kwargs)
    except TypeError as exc:
        if any(phrase in str(exc) for phrase in ("unexpected keyword argument", "got multiple values", "missing ")):
            raise
    except Exception:  # noqa: BLE001
        pass

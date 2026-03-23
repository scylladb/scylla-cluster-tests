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

"""Shared dummy/fake cluster and node implementations for unit tests.

These classes were extracted from test_cluster.py and test_utils_common.py
to break cross-test imports. They provide lightweight stubs of BaseCluster,
BaseScyllaCluster, and BaseNode for tests that don't need real infrastructure.
"""

import logging
from typing import List

from invoke import Result

from sdcm import sct_config
from sdcm.cluster import BaseNode, BaseCluster, BaseScyllaCluster
from sdcm.utils.distro import Distro


class FakeSstableRemoter:
    """Minimal remoter stub that writes predefined sstable-loader lines to a log file."""

    def __init__(self, system_log):
        self.system_log = system_log

    def run(self, *args, **kwargs):
        lines = [
            "[shard 11] sstables_loader - load_and_stream: started ops_uuid=a2661989-6836-418f-aa67-2c5466499848, process [0-1] out",
            "[shard  2] sstables_loader - Done loading new SSTables for keyspace=keyspace1, table=standard1, load_and_stream=true, "
            "primary_replica_only=false, status=succeeded",
        ]
        for line in lines:
            with open(self.system_log, "a", encoding="utf-8") as file:
                file.write(f"{line}\n")


class DummyNode(BaseNode):
    """Lightweight BaseNode stub for unit tests.

    Returns 127.0.0.1 for all addresses and disables background threads.
    Uses FakeSstableRemoter as a fake remoter after init().
    """

    _system_log = None
    is_enterprise = False
    is_product_enterprise = False
    distro = Distro.CENTOS7

    def init(self):
        super().init()
        self.remoter.stop()
        self.remoter = FakeSstableRemoter(self.system_log)

    def do_default_installations(self):
        pass  # we don't need to install anything for these unit tests

    def _set_keep_duration(self, duration_in_hours: int) -> None:
        pass

    def _get_private_ip_address(self) -> str:
        return "127.0.0.1"

    def _get_public_ip_address(self) -> str:
        return "127.0.0.1"

    @property
    def cql_address(self):
        return "127.0.0.1"

    def start_task_threads(self) -> None:
        # disable all background threads
        pass

    @property
    def system_log(self) -> str:
        return self._system_log

    @system_log.setter
    def system_log(self, log: str):
        self._system_log = log

    def wait_for_cloud_init(self):
        pass

    def set_hostname(self) -> None:
        pass

    def configure_remote_logging(self):
        pass

    def wait_ssh_up(self, verbose=True, timeout=500) -> None:
        pass

    @property
    def is_nonroot_install(self) -> bool:
        return False

    @property
    def scylla_shards(self):
        return 0

    @property
    def cpu_cores(self) -> int:
        return 0


class DummyDbCluster(BaseCluster, BaseScyllaCluster):
    """Lightweight BaseCluster + BaseScyllaCluster stub for unit tests.

    Merged from test_cluster.py and test_utils_common.py versions.
    Accepts an optional params argument; creates a fresh SCTConfiguration when omitted.
    """

    def __init__(self, nodes, params=None):
        self.nodes = nodes
        self.params = params or sct_config.SCTConfiguration()
        self.params["region_name"] = "test_region"
        self.racks_count = 0
        self.added_password_suffix = False
        self.log = logging.getLogger(__name__)
        self.node_type = "scylla-db"
        self.name = "dummy_db_cluster"
        self.vector_store_cluster = None

    def add_nodes(self, count, ec2_user_data="", dc_idx=0, rack=0, enable_auto_bootstrap=False, instance_type=None):
        for _ in range(count):
            self.nodes += [self.nodes[-1]]

    def wait_for_init(self, *_, node_list=None, verbose=False, timeout=None, **__):
        pass

    def validate_seeds_on_all_nodes(self):
        pass

    def start_nemesis(self):
        pass


class NodetoolDummyNode(BaseNode):
    """Stub node that returns a canned nodetool stdout response."""

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
        """return True if the database is up
        Couldn't be a property, because BaseNode.db_up is a method
        """
        return self._db_up


class DummyScyllaCluster(BaseScyllaCluster, BaseCluster):
    """Stub Scylla cluster backed by NodetoolDummyNode instances."""

    nodes: List["NodetoolDummyNode"]

    def __init__(self, params):
        self.nodes = params
        self.name = "dummy_cluster"
        self.added_password_suffix = False
        self.log = logging.getLogger(self.name)

    def get_ip_to_node_map(self):
        """returns {ip: node} map for all nodes in cluster to get node by ip"""
        return {node.myname: node for node in self.nodes}

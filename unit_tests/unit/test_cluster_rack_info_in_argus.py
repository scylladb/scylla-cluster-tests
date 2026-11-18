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
# Copyright (c) 2026 ScyllaDB

"""`wait_for_init` reports the datacenter and the rack name of every node it started to Argus."""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.cluster import BaseCluster, wait_for_init_wrap
from unit_tests.lib.fake_cluster import DummyScyllaCluster, NodetoolDummyNode

STATUS_HEADER = (
    "Status=Up/Down\n|/ State=Normal/Leaving/Joining/Moving\n--  Address  Load  Tokens  Owns  Host ID  Rack\n"
)
STATUS = (
    f"Datacenter: eu-west\n===================\n{STATUS_HEADER}"
    "UN  10.0.0.1  1.2 MB  256  ?  e5bcb094-e4de-43aa-8dc9-b1bf74b3b346  1a\n"
    "UN  10.0.0.2  1.2 MB  256  ?  e5bcb094-e4de-43aa-8dc9-b1bf74b3b347  1b\n"
    f"Datacenter: us-east\n===================\n{STATUS_HEADER}"
    "UN  2a05:d018:223:f00::1  1.2 MB  256  ?  e5bcb094-e4de-43aa-8dc9-b1bf74b3b348  1c\n"
)
NAMES_IN_STATUS = {
    "10.0.0.1": ("eu-west", "1a"),
    "10.0.0.2": ("eu-west", "1b"),
    "2a05:d018:223:f00::1": ("us-east", "1c"),
}


class StartedNode(NodetoolDummyNode):
    """A DB node that `wait_for_init` starts: Scylla is down until `node_startup`, and Argus is mocked."""

    def __init__(self, ip, status=STATUS):
        super().__init__(resp=status, myregion="eu-west-1", myname=ip, myrack=0, db_up=False)
        self._datacenter_name = None
        self._node_rack = None
        self.remoter = MagicMock()
        self.log = MagicMock()
        self.run_nodetool = MagicMock(wraps=self.run_nodetool)
        self.update_rack_info_in_argus = MagicMock()
        self.update_shards_in_argus = MagicMock()

    def __str__(self):
        return self.name

    def reported_names(self):
        """The names of the last report of this node to Argus."""
        return self.update_rack_info_in_argus.call_args.args


class StartedScyllaCluster(DummyScyllaCluster):
    """A Scylla cluster that goes through `wait_for_init_wrap`, with the node setup and startup left out.

    The real `node_setup` and `node_startup` install and configure Scylla over SSH. The real UN check runs
    `nodetool status` too, and retries for minutes when the status does not list a node.
    """

    node_type = "scylla-db"
    parallel_startup = True

    def update_db_binary(self, *_, **__):
        pass

    def update_db_packages(self, *_, **__):
        pass

    def node_setup(self, node, **_):
        pass

    def node_startup(self, node, **_):
        node._db_up = True

    def wait_for_nodes_up_and_normal(self, *_, **__):
        pass

    @wait_for_init_wrap
    def wait_for_init(self, node_list=None, timeout=None):
        pass


@pytest.fixture
def db_cluster():
    """A cluster whose three nodes are all in the `nodetool status` that each of them returns."""
    cluster = StartedScyllaCluster([StartedNode(ip) for ip in NAMES_IN_STATUS])
    for node in cluster.nodes:
        node.parent_cluster = cluster
    return cluster


def test_names_of_all_nodes_come_from_one_nodetool_status_of_the_first_node(db_cluster):
    """One `nodetool status` of the first node gives the names of the nodes of both datacenters."""
    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert [node.run_nodetool.call_count for node in db_cluster.nodes] == [1, 0, 0]
    assert {node.name: node.reported_names() for node in db_cluster.nodes} == NAMES_IN_STATUS


def test_node_that_the_status_does_not_list_asks_itself(db_cluster):
    """A node that is missing from the status of the first node runs `nodetool status` on itself."""
    own_status = f"Datacenter: eu-west\n===================\n{STATUS_HEADER}UN  10.0.0.9  1.2 MB  256  ?  e5bcb094-e4de-43aa-8dc9-b1bf74b3b349  1z\n"
    missing = StartedNode("10.0.0.9", status=own_status)
    missing.parent_cluster = db_cluster
    db_cluster.nodes.append(missing)

    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert missing.run_nodetool.call_count == 2  # one for the datacenter name, one for the rack name
    assert missing.reported_names() == ("eu-west", "1z")
    assert [node.run_nodetool.call_count for node in db_cluster.nodes[:3]] == [1, 0, 0]
    assert db_cluster.nodes[1].reported_names() == NAMES_IN_STATUS["10.0.0.2"]


def test_first_node_that_cannot_run_scylla_commands_is_not_asked(db_cluster):
    """No `nodetool status` runs on a node with no SSH connection, as on Scylla Cloud without SSH access."""
    for node in db_cluster.nodes:
        node.remoter = None

    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert [node.run_nodetool.call_count for node in db_cluster.nodes] == [0, 0, 0]
    assert [node.reported_names() for node in db_cluster.nodes] == [(None, None)] * 3


def test_failed_read_of_the_names_does_not_fail_wait_for_init(db_cluster):
    """`wait_for_init` passes when the status of the first node raises, and the other nodes ask themselves."""
    db_cluster.nodes[0].get_nodes_status = MagicMock(side_effect=RuntimeError("no events device"))

    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert [node.run_nodetool.call_count for node in db_cluster.nodes] == [0, 2, 2]
    assert db_cluster.nodes[1].reported_names() == NAMES_IN_STATUS["10.0.0.2"]
    assert db_cluster.nodes[2].reported_names() == NAMES_IN_STATUS["2a05:d018:223:f00::1"]


def test_node_whose_report_fails_does_not_stop_the_others(db_cluster):
    """`wait_for_init` passes and the other nodes are reported when the report of one node raises."""
    db_cluster.nodes[1].update_rack_info_in_argus.side_effect = RuntimeError("argus is down")

    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert db_cluster.nodes[0].reported_names() == NAMES_IN_STATUS["10.0.0.1"]
    assert db_cluster.nodes[2].reported_names() == NAMES_IN_STATUS["2a05:d018:223:f00::1"]


def test_reports_of_the_nodes_run_at_the_same_time(db_cluster):
    """The reports of the three nodes overlap: each one waits until all three have started."""
    all_reports_started = threading.Barrier(len(db_cluster.nodes), timeout=5)
    overlapped = []

    def report(dc_name, _rack_name):
        if dc_name:  # the report after the startup; the one of the node setup has no names yet
            all_reports_started.wait()
            overlapped.append(dc_name)

    for node in db_cluster.nodes:
        node.update_rack_info_in_argus.side_effect = report

    db_cluster.wait_for_init(critical_node_setup_events=[])

    assert len(overlapped) == len(db_cluster.nodes)


def test_nodes_of_a_cluster_that_is_not_a_scylla_cluster_are_reported_one_by_one():
    """A loader set reports the names that each of its nodes gives, without the parallel run."""
    loaders = [MagicMock(datacenter="eu-west-1", node_rack=str(idx)) for idx in range(3)]
    loader_set = MagicMock(spec=BaseCluster, nodes=loaders, parallel_startup=True, log=MagicMock())

    wait_for_init_wrap(MagicMock())(loader_set, critical_node_setup_events=[])

    for idx, loader in enumerate(loaders):
        loader.update_rack_info_in_argus.assert_called_with("eu-west-1", str(idx))
        assert loader.update_rack_info_in_argus.call_count == 2  # once in the node setup, once after the startup
    loader_set.run_func_parallel.assert_not_called()

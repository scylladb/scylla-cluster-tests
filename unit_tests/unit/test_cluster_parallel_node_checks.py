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

"""Tests for the per-node checks of BaseScyllaCluster that run on all nodes at the same time.

Covers `wait_for_schema_agreement` and `wait_all_nodes_un`.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest
from invoke import Result

from sdcm.cluster import ClusterNodesNotReady
from sdcm.utils.health_checker import CHECK_NODE_HEALTH_RETRIES
from unit_tests.lib.fake_cluster import DummyScyllaCluster

# how long a check waits for the checks of the other nodes, a serial run fails after it
OVERLAP_TIMEOUT = 10
SCHEMA_AGREEMENT_RETRIES = 10
WAIT_ALL_NODES_UN_RETRIES = 30
GET_NODETOOL_STATUS_RETRIES = 3

NODETOOL_STATUS_HEADER = """\
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
-- Address    Load      Tokens Owns Host ID                              Rack
"""


@pytest.fixture(autouse=True)
def _no_retry_sleep(monkeypatch):
    """Skip the sleeps between the retries of `retrying` and of the schema agreement check."""
    monkeypatch.setattr("sdcm.utils.decorators.time.sleep", lambda *_, **__: None)
    monkeypatch.setattr("sdcm.utils.health_checker.CHECK_NODE_HEALTH_RETRY_DELAY", 0)


def _make_nodes(count):
    nodes = []
    for idx in range(count):
        node = MagicMock()
        node.name = f"node-{idx}"
        node.ip_address = f"10.0.0.{idx + 1}"
        node.running_nemesis = None
        nodes.append(node)
    return nodes


def _run_together(barrier, result):
    """Build a side effect that returns `result` only after all the parties of `barrier` called it."""

    def side_effect(*_, **__):
        barrier.wait()
        return result

    return side_effect


# --- wait_for_schema_agreement ---


def _gossip_info(nodes, schemas=None):
    schemas = schemas or {}
    return {node: {"schema": schemas.get(node.name, "v1"), "status": "NORMAL", "dc": "dc1"} for node in nodes}


def _set_schema_info(nodes, disagreeing=()):
    """Make every node report the gossip and the peers info, the `disagreeing` ones see two schema versions."""
    for node in nodes:
        schemas = {nodes[0].name: "v2"} if node.name in disagreeing else {}
        node.get_gossip_info.return_value = _gossip_info(nodes, schemas)
        node.get_peers_info.return_value = {peer: {"schema_version": "v1"} for peer in nodes if peer is not node}


def test_wait_for_schema_agreement_checks_nodes_concurrently(caplog):
    nodes = _make_nodes(5)
    _set_schema_info(nodes)
    barrier = threading.Barrier(len(nodes), timeout=OVERLAP_TIMEOUT)
    for node in nodes:
        node.get_gossip_info.side_effect = _run_together(barrier, node.get_gossip_info.return_value)

    with caplog.at_level(logging.DEBUG, logger="dummy_cluster"):
        assert DummyScyllaCluster(nodes).wait_for_schema_agreement() is True

    for node in nodes:
        node.get_gossip_info.assert_called_once()
        node.get_peers_info.assert_called_once()
    assert "Schema agreement is reached" in caplog.messages


def test_wait_for_schema_agreement_disagreeing_nodes_raise_assertion_error_of_first_node():
    nodes = _make_nodes(5)
    _set_schema_info(nodes, disagreeing=("node-1", "node-3"))
    node_3_checked = threading.Event()

    def node_1_gossip_info():
        # a later node that fails earlier must not replace the error of the first failed node
        if not node_3_checked.wait(OVERLAP_TIMEOUT):
            raise TimeoutError("node-3 was not checked while node-1 was")
        return _gossip_info(nodes, {"node-0": "v2"})

    def node_3_gossip_info():
        node_3_checked.set()
        return _gossip_info(nodes, {"node-0": "v2"})

    nodes[1].get_gossip_info.side_effect = node_1_gossip_info
    nodes[3].get_gossip_info.side_effect = node_3_gossip_info

    with pytest.raises(
        AssertionError, match=r"on the node node-1 .* Schema version is not same on nodes in the gossip"
    ):
        DummyScyllaCluster(nodes).wait_for_schema_agreement()

    assert nodes[1].get_gossip_info.call_count == SCHEMA_AGREEMENT_RETRIES * CHECK_NODE_HEALTH_RETRIES


def test_wait_for_schema_agreement_retries_until_nodes_agree():
    nodes = _make_nodes(3)
    _set_schema_info(nodes)
    disagreement = _gossip_info(nodes, {"node-0": "v2"})
    agreement = _gossip_info(nodes)
    # the first pass sees the disagreement on all the attempts of the check, the second one sees the agreement
    nodes[2].get_gossip_info.side_effect = [disagreement] * CHECK_NODE_HEALTH_RETRIES + [agreement]

    assert DummyScyllaCluster(nodes).wait_for_schema_agreement() is True

    assert nodes[2].get_gossip_info.call_count == CHECK_NODE_HEALTH_RETRIES + 1


def test_wait_for_schema_agreement_skips_nodes_running_nemesis():
    nodes = _make_nodes(4)
    _set_schema_info(nodes)
    nodes[1].running_nemesis = "DecommissionMonkey"
    nodes[2].running_nemesis = "RestartMonkey"

    assert DummyScyllaCluster(nodes).wait_for_schema_agreement() is True

    for node in (nodes[0], nodes[3]):
        node.get_gossip_info.assert_called_once()
    for node in (nodes[1], nodes[2]):
        node.get_gossip_info.assert_not_called()
        node.get_peers_info.assert_not_called()


def test_wait_for_schema_agreement_all_nodes_running_nemesis_raises_assertion_error():
    nodes = _make_nodes(2)
    for node in nodes:
        node.running_nemesis = "RestartMonkey"

    with pytest.raises(AssertionError, match="All nodes are under active nemesis"):
        DummyScyllaCluster(nodes).wait_for_schema_agreement()

    for node in nodes:
        node.get_gossip_info.assert_not_called()


# --- wait_all_nodes_un ---


def _nodetool_status(nodes, down=()):
    rows = [
        f"{'DN' if node.name in down else 'UN'} {node.ip_address} 202.92 KB 256    ?    "
        f"7b8f86bf-c70c-4246-a273-{idx:012d} rack1"
        for idx, node in enumerate(nodes)
    ]
    return Result(stdout=NODETOOL_STATUS_HEADER + "\n".join(rows), exited=0)


def test_wait_all_nodes_un_checks_nodes_concurrently():
    nodes = _make_nodes(5)
    barrier = threading.Barrier(len(nodes), timeout=OVERLAP_TIMEOUT)
    for node in nodes:
        node.run_nodetool.side_effect = _run_together(barrier, _nodetool_status(nodes))

    DummyScyllaCluster(nodes).wait_all_nodes_un()

    for node in nodes:
        node.run_nodetool.assert_called_once_with("status", publish_event=False)


def test_wait_all_nodes_un_down_node_raises_cluster_nodes_not_ready():
    nodes = _make_nodes(4)
    for node in nodes:
        node.run_nodetool.return_value = _nodetool_status(nodes)
    # only node-2 sees node-3 as down
    nodes[2].run_nodetool.return_value = _nodetool_status(nodes, down=("node-3",))

    with pytest.raises(ClusterNodesNotReady) as exc_info:
        DummyScyllaCluster(nodes).wait_all_nodes_un()

    assert exc_info.value.args[1] == "node-3"
    assert nodes[2].run_nodetool.call_count == WAIT_ALL_NODES_UN_RETRIES


def test_wait_all_nodes_un_retries_until_nodes_are_up():
    nodes = _make_nodes(3)
    for node in nodes:
        node.run_nodetool.return_value = _nodetool_status(nodes)
    nodes[0].run_nodetool.side_effect = [_nodetool_status(nodes, down=("node-1",)), _nodetool_status(nodes)]

    DummyScyllaCluster(nodes).wait_all_nodes_un()

    assert nodes[0].run_nodetool.call_count == 2


def test_wait_all_nodes_un_unexpected_error_is_not_retried():
    nodes = _make_nodes(3)
    for node in nodes:
        node.run_nodetool.return_value = _nodetool_status(nodes)
    nodes[1].run_nodetool.side_effect = ConnectionError("ssh is down")

    with pytest.raises(ConnectionError, match="ssh is down"):
        DummyScyllaCluster(nodes).wait_all_nodes_un()

    # only `get_nodetool_status` retried it, `wait_all_nodes_un` did not start another pass
    assert nodes[1].run_nodetool.call_count == GET_NODETOOL_STATUS_RETRIES


def test_wait_all_nodes_un_failed_check_stops_checks_of_other_nodes(monkeypatch):
    monkeypatch.setattr("sdcm.cluster.NODES_CHECK_MAX_WORKERS", 2)
    nodes = _make_nodes(6)
    # the two checks that run first fail at the same time, as when no node sees the cluster as ready
    barrier = threading.Barrier(2, timeout=OVERLAP_TIMEOUT)
    for node in nodes:
        node.run_nodetool.side_effect = _run_together(barrier, _nodetool_status(nodes, down=("node-5",)))

    with pytest.raises(ClusterNodesNotReady):
        DummyScyllaCluster(nodes).wait_all_nodes_un()

    for node in nodes[:2]:
        assert node.run_nodetool.call_count == WAIT_ALL_NODES_UN_RETRIES
    for node in nodes[2:]:
        node.run_nodetool.assert_not_called()


def test_wait_all_nodes_un_no_nodes_does_nothing():
    DummyScyllaCluster([]).wait_all_nodes_un()


# --- the cap of the concurrent checks ---


@pytest.mark.parametrize(
    "nodes_count, max_workers, expected_workers",
    [
        pytest.param(3, 20, 3, id="fewer-nodes-than-cap"),
        pytest.param(6, 2, 2, id="more-nodes-than-cap"),
    ],
)
def test_wait_all_nodes_un_caps_concurrent_checks(monkeypatch, nodes_count, max_workers, expected_workers):
    monkeypatch.setattr("sdcm.cluster.NODES_CHECK_MAX_WORKERS", max_workers)
    nodes = _make_nodes(nodes_count)
    lock = threading.Lock()
    running = []
    max_running = []
    # the checks pass only in groups of `expected_workers`
    barrier = threading.Barrier(expected_workers, timeout=OVERLAP_TIMEOUT)

    def run_nodetool(*_, **__):
        with lock:
            running.append(1)
            max_running.append(len(running))
        try:
            barrier.wait()
        finally:
            with lock:
                running.pop()
        return _nodetool_status(nodes)

    for node in nodes:
        node.run_nodetool.side_effect = run_nodetool

    with patch("sdcm.cluster.ThreadPoolExecutor", wraps=ThreadPoolExecutor) as executor:
        DummyScyllaCluster(nodes).wait_all_nodes_un()

    assert executor.call_args.kwargs["max_workers"] == expected_workers
    assert max(max_running) == expected_workers

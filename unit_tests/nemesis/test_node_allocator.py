"""
This module tests core responsibilities of NemesisNodeAllocator entity -
nodes selection, release; state management and thread safety.
"""

import threading
import time
from unittest.mock import MagicMock

import pytest
import random

from sdcm.cluster import BaseNode
from sdcm.utils.nemesis_utils import NEMESIS_TARGET_POOLS
from sdcm.utils.nemesis_utils.node_allocator import NemesisNodeAllocator, NemesisNodeAllocationError, AllNodesRunNemesisError


@pytest.fixture(autouse=True)
def reset_node_allocator_singleton():
    """Reset NemesisNodeAllocator singleton state before each test"""
    if hasattr(NemesisNodeAllocator.__class__, '_instances'):
        NemesisNodeAllocator.__class__._instances.clear()
    yield


class MockNode(BaseNode):
    def __init__(self, name, node_type="data_nodes", is_seed=False, dc_idx=0, rack=0):
        self.name = name
        self.node_type = node_type
        self.is_seed = is_seed
        self.dc_idx = dc_idx
        self.rack = rack
        self._is_zero_token_node = (node_type == "zero_nodes")
        self.parent_cluster = MagicMock()
        self.parent_cluster.params = MagicMock()

    def __repr__(self):
        return f"MockNode({self.name})"


class MockTester:
    def __init__(self, nodes):
        self._nodes = list(nodes)

    @property
    def all_db_nodes(self):
        return self._nodes

    def add_node(self, node):
        self._nodes.append(node)

    def remove_node(self, node):
        if node in self._nodes:
            self._nodes.remove(node)


@pytest.fixture
def allocator_with_nodes():
    """Provides an allocator instance with a set of mock nodes."""
    nodes = [
        MockNode("node-1", dc_idx=0, rack=0, is_seed=True),
        MockNode("node-2", dc_idx=0, rack=0),
        MockNode("node-3", dc_idx=0, rack=1),
        MockNode("z-node-1", node_type="zero_nodes", dc_idx=0, rack=0),
    ]
    tester = MockTester(nodes)
    allocator = NemesisNodeAllocator(tester)
    return allocator, tester, nodes


def test_nodes_list(allocator_with_nodes):
    allocator, tester, nodes = allocator_with_nodes
    assert len(allocator._get_pool_type_nodes(NEMESIS_TARGET_POOLS.all_nodes)) == 4

    tester.remove_node(nodes[0])
    assert len(allocator._get_pool_type_nodes(NEMESIS_TARGET_POOLS.all_nodes)) == 3

    tester.add_node(nodes[0])
    assert len(allocator._get_pool_type_nodes(NEMESIS_TARGET_POOLS.all_nodes)) == 4


def test_select_and_release_target_node(allocator_with_nodes):
    allocator, _, _ = allocator_with_nodes

    # select a node
    selected_node = allocator.select_target_node(
        nemesis_name="TestNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)
    assert selected_node is not None
    assert selected_node in allocator.active_nemesis_on_nodes
    assert allocator.active_nemesis_on_nodes[selected_node] == "TestNemesis"

    # select another node
    second_node = allocator.select_target_node(
        nemesis_name="SecondNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)
    assert second_node is not None
    assert second_node != selected_node

    # release the first node
    allocator.unset_running_nemesis(selected_node, "TestNemesis")
    assert selected_node not in allocator.active_nemesis_on_nodes


def test_no_available_nodes(allocator_with_nodes):
    allocator, _, nodes = allocator_with_nodes

    data_nodes = [n for n in nodes if n.node_type == "data_nodes"]
    for i, node in enumerate(data_nodes):
        allocator.set_running_nemesis(node, f"Nemesis-{i}")

    with pytest.raises(AllNodesRunNemesisError):
        allocator.select_target_node(
            nemesis_name="ExtraNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)


def test_nodes_filtering(allocator_with_nodes):
    allocator, _, _ = allocator_with_nodes

    # filter seed node
    seed_node = allocator.select_target_node(
        "SeedNemesis", NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False, is_seed=True)
    assert seed_node.is_seed

    # filter a rack
    rack1_node = allocator.select_target_node(
        "RackNemesis", NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False, rack=1)
    assert rack1_node.rack == 1

    # filter a pool type
    znode = allocator.select_target_node("ZNodeNemesis", NEMESIS_TARGET_POOLS.zero_nodes, filter_seed=False)
    assert znode.node_type == "zero_nodes"


def test_switch_target_node(allocator_with_nodes):
    allocator, _, nodes = allocator_with_nodes

    allocator.set_running_nemesis(nodes[1], "SwitchNemesis")
    assert nodes[1] in allocator.active_nemesis_on_nodes

    switch_ok = allocator.switch_target_node(old_node=nodes[1], new_node=nodes[2], nemesis_name="SwitchNemesis")
    assert switch_ok
    assert nodes[1] not in allocator.active_nemesis_on_nodes
    assert nodes[2] in allocator.active_nemesis_on_nodes
    assert allocator.active_nemesis_on_nodes[nodes[2]] == "SwitchNemesis"


def test_switch_to_reserved_node_fails(allocator_with_nodes):
    allocator, _, nodes = allocator_with_nodes

    allocator.set_running_nemesis(nodes[1], "NemesisA")
    allocator.set_running_nemesis(nodes[2], "NemesisB")

    with pytest.raises(NemesisNodeAllocationError, match="Requested node is already running nemesis"):
        allocator.switch_target_node(old_node=nodes[1], new_node=nodes[2], nemesis_name="NemesisA")
    assert allocator.active_nemesis_on_nodes[nodes[1]] == "NemesisA"
    assert allocator.active_nemesis_on_nodes[nodes[2]] == "NemesisB"


def test_thread_safety():
    """Check for race conditions under pressure."""
    nodes = [MockNode(f"node-{i}") for i in range(5)]
    tester = MockTester(nodes)
    allocator = NemesisNodeAllocator(tester)

    results, errors = [], []

    def worker_thread(worker_id):
        try:
            for _ in range(10):
                nemesis_name = f"Nemesis-{worker_id}"
                node = allocator.select_target_node(nemesis_name, NEMESIS_TARGET_POOLS.data_nodes, False)
                if node:
                    time.sleep(random.uniform(0.05, 0.1))
                    results.append(node)
                    allocator.unset_running_nemesis(node, nemesis_name)
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker_thread, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Threads failed with errors: {errors}"
    # number of successful node selections should be 4 workers * 10 iterations
    assert len(results) == 40


def test_context_manager_sets_and_releases_single_node(allocator_with_nodes):
    allocator, _, nodes = allocator_with_nodes
    node_to_mark = nodes[0]

    with allocator.nodes_running_nemesis(node_to_mark, "CtxNemesis"):
        assert allocator.active_nemesis_on_nodes.get(node_to_mark) == "CtxNemesis"

    assert node_to_mark not in allocator.active_nemesis_on_nodes


def test_context_manager_sets_and_releases_multiple_nodes(allocator_with_nodes):
    allocator, _, nodes = allocator_with_nodes
    nodes_to_mark = [nodes[0], nodes[1]]

    with allocator.nodes_running_nemesis(nodes_to_mark, "MultiCtxNemesis"):
        assert all(node in allocator.active_nemesis_on_nodes for node in nodes_to_mark)

    assert all(node not in allocator.active_nemesis_on_nodes for node in nodes_to_mark)


class ProtectedMockNode(MockNode):
    def __init__(self, *args, is_protected=False, **kwargs):
        super().__init__(*args, **kwargs)

        # override the is_protected property to simulate protected nodes
        self.is_protected = is_protected


def test_protected_nodes_are_not_selected():
    # Create nodes, some protected
    nodes = [
        ProtectedMockNode("node-1", is_protected=True),
        ProtectedMockNode("node-2", is_protected=False),
        ProtectedMockNode("node-3", is_protected=True),
        ProtectedMockNode("node-4", is_protected=False),
    ]
    tester = MockTester(nodes)
    allocator = NemesisNodeAllocator(tester)

    # Try to select nodes multiple times, should never select protected

    for _ in range(2):
        node = allocator.select_target_node(
            nemesis_name="TestNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)
        assert not getattr(node, "is_protected", False)

    # After all unprotected nodes are used, should raise error
    with pytest.raises(AllNodesRunNemesisError):
        allocator.select_target_node(
            nemesis_name="TestNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)


def test_unprotecting_node_allows_selection():
    nodes = [
        ProtectedMockNode("node-1", is_protected=True),
        ProtectedMockNode("node-2", is_protected=True),
    ]
    tester = MockTester(nodes)
    allocator = NemesisNodeAllocator(tester)

    # All nodes protected: should raise error
    with pytest.raises(AllNodesRunNemesisError):
        allocator.select_target_node(
            nemesis_name="TestNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)

    # Unprotect one node
    nodes[1].is_protected = False
    node = allocator.select_target_node(
        nemesis_name="TestNemesis", pool_type=NEMESIS_TARGET_POOLS.data_nodes, filter_seed=False)
    assert node == nodes[1]

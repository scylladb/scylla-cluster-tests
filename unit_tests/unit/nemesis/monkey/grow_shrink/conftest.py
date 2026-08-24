"""Shared fixtures for the grow/shrink nemesis tests."""

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def events(events):
    """Every test in this package exercises code that emits SCT events."""
    return events


@pytest.fixture()
def make_data_node():
    """Factory returning a mock data node placed in the given datacenter."""

    def _make(name, dc_idx=0):
        node = MagicMock()
        node.name = name
        node.dc_idx = dc_idx
        node._is_zero_token_node = False
        return node

    return _make


@pytest.fixture()
def runner(base_runner):
    """``base_runner`` extended with the attributes grow/shrink helpers rely on."""
    base_runner.interval = 0
    base_runner.current_disruption = "GrowShrinkCluster-deadbeef"
    base_runner.node_allocator = MagicMock()
    base_runner.monitoring_set = MagicMock()
    base_runner._is_it_on_kubernetes = MagicMock(return_value=False)
    base_runner.decommission_nodes = MagicMock()
    base_runner.add_new_nodes = MagicMock(return_value=[])
    base_runner.set_target_node = MagicMock()
    base_runner.set_target_node_pool_type = MagicMock()
    base_runner.cluster.parallel_node_operations = True
    base_runner.cluster.racks_count = 2
    base_runner.target_node.dc_idx = 0
    base_runner.target_node._is_zero_token_node = False
    base_runner.tester.params = {}
    base_runner.cluster.params = {}
    return base_runner

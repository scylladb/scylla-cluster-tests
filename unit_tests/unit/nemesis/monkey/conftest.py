"""Shared fixtures for monkey (individual nemesis) unit tests.

Provides a ``base_runner`` fixture with a pre-configured ``TestRunner``
that individual test modules can extend for their specific needs.
"""

from unittest.mock import MagicMock

import pytest

from unit_tests.unit.nemesis import TestRunner


def make_mock_node(name="node1", rack="rack1", is_seed=False):
    """Create a ``MagicMock`` that behaves like a minimal cluster node.

    Monkey code calls arbitrary methods on every node in ``data_nodes``
    (``run_nodetool``, ``stop_scylla``, ``get_list_of_sstables``, …), so a
    plain dataclass is not sufficient — ``MagicMock`` auto-stubs all of them.
    """
    node = MagicMock()
    node.name = name
    node.rack = rack
    node.is_seed = is_seed
    return node


@pytest.fixture()
def base_runner():
    """A ``TestRunner`` with deterministic random behaviour and a two-node rack.

    **Random mocking** — ``runner.random.choice`` always picks the first
    element, so tests that rely on a table/node being selected get
    predictable results without extra setup.

    **Node / rack setup** — two data-nodes are placed in ``rack1`` so that
    monkey guards like "only one node in rack" do not fire by default.
    ``target_node`` is set to the first node in the list.

    Test modules that need a customised runner should define their own
    fixture that builds on top of this one, e.g.::

        @pytest.fixture()
        def runner(base_runner):
            base_runner.target_node.host_id = "aaaa-bbbb-cccc"
            return base_runner
    """
    runner = TestRunner()

    # -- deterministic random ------------------------------------------------
    runner.random.choice.side_effect = lambda seq: seq[0]

    # -- basic two-node rack -------------------------------------------------
    node1 = make_mock_node(name="node1", rack="rack1")
    node2 = make_mock_node(name="node2", rack="rack1")

    runner.cluster.data_nodes = [node1, node2]
    runner.target_node = node1

    return runner

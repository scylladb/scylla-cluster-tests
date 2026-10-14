"""Tests for the seeded node order of BaseScyllaCluster.restart_scylla."""

import random
from unittest.mock import MagicMock

from unit_tests.lib.fake_cluster import DummyDbCluster


def test_restart_scylla_random_order_replays_from_the_same_seed():
    """Two generators with the same seed restart the nodes in the same order, so the random-order
    rolling restart nemesis replays from nemesis_seed.
    """
    cluster = DummyDbCluster(nodes=[MagicMock() for _ in range(4)])
    restarted = []
    for node in cluster.nodes:
        node.stop_scylla.side_effect = lambda node=node, **_: restarted.append(node)

    cluster.restart_scylla(random_order=True, random_generator=random.Random(3))
    first_order = restarted.copy()
    restarted.clear()
    cluster.restart_scylla(random_order=True, random_generator=random.Random(3))

    assert restarted == first_order

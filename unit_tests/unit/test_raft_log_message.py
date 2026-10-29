"""Tests for RaftFeatureOperations.get_random_log_message seeding behaviour."""

import random
from unittest.mock import MagicMock

import pytest

from sdcm.utils.raft import RaftFeature, TopologyOperations

DRAWS = 10


@pytest.fixture
def raft():
    """RaftFeature over a mock node whose cluster runs on the aws backend, so timeouts resolve."""
    node = MagicMock()
    node.parent_cluster.params = {"cluster_backend": "aws"}
    return RaftFeature(node)


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(TopologyOperations.DECOMMISSION, id="decommission"),
        pytest.param(TopologyOperations.BOOTSTRAP, id="bootstrap"),
    ],
)
def test_get_random_log_message_same_seed_replays_a_varying_sequence(raft, operation):
    """Two generators with the same seed pick the same patterns in the same order, and the picks vary
    along the run; the helper used to re-seed before every draw and so returned one pattern forever."""
    generator_a, generator_b = random.Random(11), random.Random(11)

    picks_a = [raft.get_random_log_message(operation, random_generator=generator_a) for _ in range(DRAWS)]
    picks_b = [raft.get_random_log_message(operation, random_generator=generator_b) for _ in range(DRAWS)]

    assert picks_a == picks_b
    assert len(set(picks_a)) > 1

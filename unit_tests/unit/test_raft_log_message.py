"""Tests for RaftFeatureOperations.get_random_log_message seeding behaviour."""

import random
from unittest.mock import MagicMock

import pytest

from sdcm.utils.raft import DECOMMISSION_WITH_REPAIR_LOG_MESSAGE, RaftFeature, TopologyOperations

DRAWS = 10


def make_raft(scylla_config: dict[str, str | None]) -> RaftFeature:
    """RaftFeature over a mock node whose cluster runs on the aws backend, so timeouts resolve."""
    node = MagicMock()
    node.parent_cluster.params = {"cluster_backend": "aws"}
    node.get_scylla_config_param.side_effect = lambda name, verbose=True: scylla_config.get(name)
    return RaftFeature(node)


@pytest.fixture
def raft():
    """RaftFeature over a node whose repair-based node ops cover decommission, so every pattern stays in the pick."""
    return make_raft({"enable_repair_based_node_ops": "true", "allowed_repair_based_node_ops": "decommission"})


@pytest.mark.parametrize(
    "operation",
    [
        pytest.param(TopologyOperations.DECOMMISSION, id="decommission"),
        pytest.param(TopologyOperations.BOOTSTRAP, id="bootstrap"),
    ],
)
def test_get_random_log_message_same_seed_replays_a_varying_sequence(raft, operation):
    """Two generators with the same seed pick the same patterns in the same order, and the picks vary
    along the run; the helper used to re-seed before every draw and so returned one pattern forever.
    """
    generator_a, generator_b = random.Random(11), random.Random(11)

    picks_a = [raft.get_random_log_message(operation, random_generator=generator_a) for _ in range(DRAWS)]
    picks_b = [raft.get_random_log_message(operation, random_generator=generator_b) for _ in range(DRAWS)]

    assert picks_a == picks_b
    assert len(set(picks_a)) > 1


def decommission_picks(raft: RaftFeature) -> set[str]:
    generator = random.Random(7)
    return {
        raft.get_random_log_message(TopologyOperations.DECOMMISSION, random_generator=generator).log_message
        for _ in range(200)
    }


@pytest.mark.parametrize(
    "scylla_config",
    [
        pytest.param(
            {"enable_repair_based_node_ops": "true", "allowed_repair_based_node_ops": '"rebuild,removenode,replace"'},
            id="decommission-not-allowed",
        ),
        pytest.param(
            {"enable_repair_based_node_ops": "false", "allowed_repair_based_node_ops": '"decommission"'},
            id="rbno-disabled",
        ),
    ],
)
def test_decommission_pick_skips_repair_pattern_without_rbno_decommission(scylla_config):
    """Scylla never logs decommission_with_repair when RBNO does not cover decommission (SCT-1610)."""
    picks = decommission_picks(make_raft(scylla_config))

    assert DECOMMISSION_WITH_REPAIR_LOG_MESSAGE not in picks
    assert len(picks) > 1


@pytest.mark.parametrize(
    "scylla_config",
    [
        pytest.param(
            {"enable_repair_based_node_ops": "true", "allowed_repair_based_node_ops": '"decommission,replace"'},
            id="decommission-allowed",
        ),
        pytest.param({}, id="config-unknown"),
    ],
)
def test_decommission_pick_keeps_repair_pattern_with_rbno_decommission(scylla_config):
    assert DECOMMISSION_WITH_REPAIR_LOG_MESSAGE in decommission_picks(make_raft(scylla_config))

"""Tests for AddRemoveDcNemesis.precheck()."""

from sdcm.nemesis.monkey import AddRemoveDcNemesis


def test_precheck_skips_for_multi_region(base_runner):
    """MULTI_REGION runs are pruned before the nemesis is ever scheduled."""
    base_runner.cluster.test_config.MULTI_REGION = True

    assert AddRemoveDcNemesis(base_runner).precheck(node=base_runner.cluster.nodes[0]) == (
        "Skipped for multi-dc scenario (https://github.com/scylladb/scylla-cluster-tests/issues/5369)"
    )


def test_precheck_keeps_for_single_region(base_runner):
    """Single-region runs keep the nemesis in the rotation."""
    base_runner.cluster.test_config.MULTI_REGION = False

    assert AddRemoveDcNemesis(base_runner).precheck(node=base_runner.cluster.nodes[0]) is None

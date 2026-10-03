"""Tests for AddRemoveRackNemesis."""

from unittest.mock import patch

import pytest

from sdcm.nemesis.monkey.grow_shrink import AddRemoveRackNemesis
from unit_tests.unit.nemesis.monkey.grow_shrink import MODULE

pytestmark = pytest.mark.usefixtures("events")


def test_grows_and_shrinks_a_brand_new_rack(runner):
    """On k8s the nemesis operates on a rack index one above the highest existing rack."""
    runner._is_it_on_kubernetes.return_value = True
    runner.cluster.racks = [0, 1]

    with patch(f"{MODULE}.grow_cluster") as grow, patch(f"{MODULE}.shrink_cluster") as shrink:
        AddRemoveRackNemesis(runner).disrupt()

    grow.assert_called_once_with(runner, 2)
    shrink.assert_called_once_with(runner, 2)


def test_precheck_passes_on_kubernetes(runner):
    runner._is_it_on_kubernetes.return_value = True

    assert AddRemoveRackNemesis(runner).precheck(runner.target_node) is None


def test_precheck_rejects_non_kubernetes_backends(runner):
    """Adding a rack is a scylla-operator feature, so non-k8s backends are skipped."""
    reason = AddRemoveRackNemesis(runner).precheck(runner.target_node)

    assert reason == "Adding new rack is not supported for non-k8s Scylla clusters"

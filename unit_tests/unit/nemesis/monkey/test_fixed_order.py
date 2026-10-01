"""Tests for FixedOrderMonkey: configured order, repeat, and unknown-name handling."""

import pytest

from sdcm.nemesis.monkey.runners import FixedOrderMonkey
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.unit.nemesis import CustomNemesisA, CustomNemesisB, CustomNemesisC, TestNemesisClass
from unit_tests.unit.nemesis.fake_cluster import Cluster, FakeTester, Node, PARAMS
from unit_tests.lib.fake_tester import ClusterTesterForTests


class FakeFixedOrderMonkey(FixedOrderMonkey, TestNemesisClass):
    """Override FixedOrderMonkey with the test-only disruption tree."""


@pytest.fixture()
def tester(tmp_path):
    """Shared tester fixture with a fake 2-node cluster, for get_nemesis_class() tests."""
    cluster_tester = ClusterTesterForTests()
    cluster_tester._init_logging(tmp_path)
    cluster_tester._init_params()
    cluster_tester.db_cluster = Cluster(nodes=[Node(), Node()])
    cluster_tester.db_cluster.params = cluster_tester.params
    cluster_tester.params["nemesis_multiply_factor"] = 1
    cluster_tester.nemesis_allocator = NemesisNodeAllocator(cluster_tester)
    return cluster_tester


def build_fixed_order_monkey(order, multiply_factor=None):
    """Build via an explicit nemesis_fixed_order kwarg, as add_nemesis() wires it per thread."""
    params = dict(PARAMS)
    if multiply_factor is not None:
        params["nemesis_multiply_factor"] = multiply_factor
    return FakeFixedOrderMonkey(FakeTester(params=params), None, nemesis_fixed_order=order)


def test_resolved_order_matches_configured_order_not_registry_order():
    nemesis = build_fixed_order_monkey(["CustomNemesisC", "CustomNemesisA", "CustomNemesisB"])
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisC, CustomNemesisA, CustomNemesisB]


def test_repeated_name_yields_one_entry_per_occurrence_reusing_one_instance():
    nemesis = build_fixed_order_monkey(["CustomNemesisA", "CustomNemesisB", "CustomNemesisA"])
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisA, CustomNemesisB, CustomNemesisA]
    assert nemesis.disruptions_list[0] is nemesis.disruptions_list[2]


def test_unknown_name_raises_at_construction_and_names_it():
    with pytest.raises(ValueError, match="DoesNotExist"):
        build_fixed_order_monkey(["DoesNotExist"])


def test_sequence_repeats_from_first_entry_via_infinite_cycle():
    nemesis = build_fixed_order_monkey(["CustomNemesisA", "CustomNemesisB"])
    order = [next(nemesis.infinite_cycle).__class__ for _ in range(5)]
    assert order == [CustomNemesisA, CustomNemesisB, CustomNemesisA, CustomNemesisB, CustomNemesisA]


def test_multiply_factor_above_one_leaves_sequence_unchanged():
    nemesis = build_fixed_order_monkey(["CustomNemesisA", "CustomNemesisB"], multiply_factor=3)
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisA, CustomNemesisB]


def test_precheck_prunes_entry_while_remaining_order_is_unchanged(events_function_scope):
    nemesis = build_fixed_order_monkey(["CustomNemesisA", "CustomNemesisB", "CustomNemesisC"])
    nemesis.disruptions_list[0].precheck = lambda node: "not feasible"

    excluded = nemesis.precheck_nemesis()

    assert excluded == [("CustomNemesisA", "not feasible")]
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisB, CustomNemesisC]


# ---------------------------------------------------------------------------
# get_nemesis_class(): per-thread distribution, mirroring nemesis_selector/nemesis_seed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("thread_count", "configured_fixed_order", "expected_fixed_orders"),
    (
        pytest.param(2, [["A", "B"], ["C", "D"]], [["A", "B"], ["C", "D"]], id="one_order_per_thread"),
        pytest.param(3, [["A", "B"]], [["A", "B"], ["A", "B"], ["A", "B"]], id="broadcasts_single_order"),
        pytest.param(1, None, [[]], id="defaults_to_empty_when_unset"),
    ),
)
def test_get_nemesis_class_distributes_fixed_order_per_thread(
    tester, thread_count, configured_fixed_order, expected_fixed_orders
):
    tester.params["nemesis_class_name"] = ["FixedOrderMonkey"] * thread_count
    if configured_fixed_order is not None:
        tester.params["nemesis_fixed_order"] = configured_fixed_order

    nemeses = tester.get_nemesis_class()

    assert [n["nemesis_fixed_order"] for n in nemeses] == expected_fixed_orders

"""Tests for FixedOrderMonkey: configured order, repeat, and unknown-name handling."""

import pytest

from sdcm.nemesis.monkey.runners import FixedOrderMonkey
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.lib.fake_tester import ClusterTesterForTests
from unit_tests.unit.nemesis import CustomNemesisA, CustomNemesisB, CustomNemesisC, TestNemesisClass
from unit_tests.unit.nemesis.fake_cluster import FakeTester, PARAMS, Cluster, Node


class FakeFixedOrderMonkey(FixedOrderMonkey, TestNemesisClass):
    """Override FixedOrderMonkey with the test-only disruption tree."""


def build_fixed_order_monkey(order, separator=", "):
    """Build via a comma-separated nemesis_selector, as the per-thread nemesis_selector."""
    params = dict(PARAMS)
    return FakeFixedOrderMonkey(FakeTester(params=params), None, nemesis_selector=separator.join(order))


@pytest.fixture()
def tester(tmp_path):
    cluster_tester = ClusterTesterForTests()
    cluster_tester._init_logging(tmp_path)
    cluster_tester._init_params()
    cluster_tester.db_cluster = Cluster(nodes=[Node(), Node()])
    cluster_tester.db_cluster.params = cluster_tester.params
    cluster_tester.nemesis_allocator = NemesisNodeAllocator(cluster_tester)
    return cluster_tester


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


def test_precheck_prunes_entry_while_remaining_order_is_unchanged(events_function_scope):
    nemesis = build_fixed_order_monkey(["CustomNemesisA", "CustomNemesisB", "CustomNemesisC"])
    nemesis.disruptions_list[0].precheck = lambda node: "not feasible"

    excluded = nemesis.precheck_nemesis()

    assert excluded == [("CustomNemesisA", "not feasible")]
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisB, CustomNemesisC]


@pytest.mark.parametrize(
    "selector",
    [
        pytest.param("CustomNemesisA,CustomNemesisB", id="no_spaces"),
        pytest.param("  CustomNemesisA ,  CustomNemesisB  ", id="padded"),
        pytest.param("CustomNemesisA,, CustomNemesisB,", id="empty_entries"),
    ],
)
def test_selector_whitespace_and_empty_entries_are_ignored(selector):
    nemesis = FakeFixedOrderMonkey(FakeTester(params=dict(PARAMS)), None, nemesis_selector=selector)
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisA, CustomNemesisB]


def test_single_name_selector_yields_single_entry():
    nemesis = build_fixed_order_monkey(["CustomNemesisB"])
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisB]


@pytest.mark.parametrize("selector", ["", " , ", None])
def test_empty_selector_raises_value_error(selector):
    with pytest.raises(ValueError, match="requires 'nemesis_selector'"):
        FakeFixedOrderMonkey(FakeTester(params=dict(PARAMS)), None, nemesis_selector=selector)


def test_flag_expression_selector_is_rejected_as_unknown_name():
    with pytest.raises(ValueError, match="not disruptive"):
        build_fixed_order_monkey(["not disruptive"])


def test_per_thread_selectors_map_one_to_one_to_fixed_order_threads(tester):
    tester.params["nemesis_class_name"] = ["FixedOrderMonkey", "FixedOrderMonkey"]
    tester.params["nemesis_selector"] = ["CustomNemesisA, CustomNemesisB", "CustomNemesisC"]

    threads = tester.get_nemesis_class()

    assert [t["nemesis"] for t in threads] == [FixedOrderMonkey, FixedOrderMonkey]
    assert [t["nemesis_selector"] for t in threads] == ["CustomNemesisA, CustomNemesisB", "CustomNemesisC"]


def test_single_selector_broadcasts_to_all_fixed_order_threads(tester):
    tester.params["nemesis_class_name"] = ["FixedOrderMonkey", "FixedOrderMonkey"]
    tester.params["nemesis_selector"] = ["CustomNemesisA, CustomNemesisB"]

    threads = tester.get_nemesis_class()

    assert [t["nemesis_selector"] for t in threads] == ["CustomNemesisA, CustomNemesisB"] * 2

"""Tests for FixedOrderMonkey: configured order, repeat, and unknown-name handling."""

import pytest
from pydantic import ValidationError

from sdcm.nemesis.monkey.runners import FixedOrderMonkey, FixedOrderMonkeyConfig
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.lib.fake_tester import ClusterTesterForTests
from unit_tests.unit.nemesis import CustomNemesisA, CustomNemesisB, CustomNemesisC, TestNemesisClass, runner_config
from unit_tests.unit.nemesis.fake_cluster import FakeTester, Cluster, Node


class FakeFixedOrderMonkey(FixedOrderMonkey, TestNemesisClass):
    """Override FixedOrderMonkey with the test-only disruption tree."""


def build_fixed_order_monkey(order):
    """Build from the `order` of a `nemesis.runners` entry."""
    config = runner_config(FixedOrderMonkeyConfig, order=order)
    return FakeFixedOrderMonkey(FakeTester(), None, config=config)


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


def test_single_name_yields_single_entry():
    nemesis = build_fixed_order_monkey(["CustomNemesisB"])
    assert [d.__class__ for d in nemesis.disruptions_list] == [CustomNemesisB]


@pytest.mark.parametrize("options", [{}, {"order": []}])
def test_missing_or_empty_order_fails_validation(options):
    with pytest.raises(ValidationError, match="order"):
        FixedOrderMonkeyConfig(**options)


@pytest.mark.parametrize("option", ["selector", "multiply_factor"])
def test_options_it_would_ignore_fail_validation(option):
    with pytest.raises(ValidationError, match=option):
        FixedOrderMonkeyConfig(order=["CustomNemesisA"], **{option: 1})


def test_each_fixed_order_entry_takes_its_own_order(tester):
    tester.params["nemesis"] = {
        "selector": "disruptive",
        "runners": [
            {"FixedOrderMonkey": {"order": ["StopStartMonkey", "NoCorruptRepairMonkey"]}},
            {"FixedOrderMonkey": {"order": ["StopStartMonkey"]}},
        ],
    }

    threads = tester.get_nemesis_class()

    assert [t.runner_class for t in threads] == [FixedOrderMonkey, FixedOrderMonkey]
    assert [t.config.order for t in threads] == [["StopStartMonkey", "NoCorruptRepairMonkey"], ["StopStartMonkey"]]
    assert not any(hasattr(t.config, "selector") for t in threads), "the `nemesis` selector default must not reach it"


@pytest.mark.parametrize("name", ["NoSuchMonkey", "not disruptive"])
def test_order_naming_unknown_nemesis_fails_config_load(tester, name):
    with pytest.raises(ValueError, match=rf"`order` names unknown nemesis classes: \['{name}'\]"):
        tester.params["nemesis"] = {"runners": [{"FixedOrderMonkey": {"order": [name]}}]}

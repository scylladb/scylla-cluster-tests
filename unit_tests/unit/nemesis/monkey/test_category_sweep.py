"""Tests for CategorySweepMonkey: the runner that sweeps every nemesis once, category by category.

Two levels are covered here:

- The real category table, checked against the real nemesis registry, is a partition: every
  discovered nemesis belongs to exactly one category. A new flag combination that escapes the
  four selectors fails this test rather than silently dropping a nemesis out of every sweep.
- The sweep behaviour itself, checked against the test-only nemesis tree with a category table
  built from its flags: ordering, selector intersection, one announcement per category, pruning,
  and the single pass that ends the nemesis thread.
"""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.exceptions import NemesisPassCompleted
from sdcm.nemesis import NemesisBaseClass, NemesisFlags
from sdcm.nemesis.monkey.runners import CategorySweepMonkey
from sdcm.nemesis.registry import NemesisRegistry
from unit_tests.unit.nemesis import CustomNemesisAD, CustomNemesisC, TestNemesisClass
from unit_tests.unit.nemesis.fake_cluster import FakeTester, PARAMS

# Category table over the test-only flag tree, shaped like the real one: ordered, and each entry
# excluding the ones before it.
#   flag-c -> CustomNemesisC
#   flag-a -> CustomNemesisA, CustomNemesisAD  (two members, so ordering inside a category shows)
#   rest   -> CustomNemesisB
TEST_CATEGORIES = (
    ("flag-c", "flag_c"),
    ("flag-a", "flag_a and not flag_c"),
    ("rest", "not flag_a and not flag_c"),
)

FULL_SWEEP = ["CustomNemesisC", "CustomNemesisA", "CustomNemesisAD", "CustomNemesisB"]


class FakeCategorySweepMonkey(CategorySweepMonkey, TestNemesisClass):
    """CategorySweepMonkey over the test nemesis tree.

    Multiple inheritance mirrors FakeSisyphusMonkey: TestNemesisClass swaps in the test registry
    after NemesisRunner but before the sweep logic builds its list.
    """

    CATEGORIES = TEST_CATEGORIES
    DISABLED_CATEGORIES = frozenset()

    def __init__(self, tester_obj, *args, termination_event=None, nemesis_selector=None, **kwargs):
        super().__init__(tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, **kwargs)


@pytest.fixture()
def sweep():
    """A sweep runner whose nemesis never really execute, so tests drive the order only."""

    def _create(params=None, nemesis_selector=None, runner_class=FakeCategorySweepMonkey):
        tester = FakeTester(params=params or PARAMS)
        tester.db_cluster.check_cluster_health = MagicMock()
        tester.db_cluster.test_config = MagicMock()
        runner = runner_class(tester, termination_event=threading.Event(), nemesis_selector=nemesis_selector)
        runner.interval = 0
        runner.executed = []
        runner.execute_nemesis = lambda nemesis: runner.executed.append(nemesis.__class__.__name__)
        return runner

    return _create


def info_messages(events_fixture) -> list[str]:
    """Messages of the InfoEvents published so far, in order."""
    return [event["message"] for event in events_fixture.published_events if event.get("base") == "InfoEvent"]


# ---------------------------------------------------------------------------
# The real category table is a partition of the real nemesis tree
# ---------------------------------------------------------------------------


def test_real_categories_partition_every_nemesis():
    """Every discovered nemesis falls into exactly one of the four categories."""
    registry = NemesisRegistry(base_class=NemesisBaseClass, flag_class=NemesisFlags)
    all_nemesis = {nemesis.__name__ for nemesis in registry.get_subclasses()}
    assert all_nemesis, "no nemesis discovered - the registry or the auto-discovery is broken"

    category_of = {}
    for label, selector in CategorySweepMonkey.CATEGORIES:
        members = {nemesis.__name__ for nemesis in registry.filter_subclasses(selector)}
        assert members, f"category {label!r} selects no nemesis at all"
        already_taken = {name: category_of[name] for name in members if name in category_of}
        assert not already_taken, f"category {label!r} overlaps earlier categories: {already_taken}"
        category_of.update(dict.fromkeys(members, label))

    assert category_of.keys() == all_nemesis, (
        f"nemesis in no category: {sorted(all_nemesis - category_of.keys())}; "
        f"unknown nemesis in a category: {sorted(category_of.keys() - all_nemesis)}"
    )


# ---------------------------------------------------------------------------
# Building the sweep list
# ---------------------------------------------------------------------------


def test_sweep_is_ordered_by_category_then_class_name(sweep):
    """Categories keep their declared order, and members are alphabetical inside a category."""
    runner = sweep()

    assert [nemesis.__class__.__name__ for nemesis in runner.disruptions_list] == FULL_SWEEP


def test_nemesis_selector_narrows_every_category(sweep):
    """A selector intersects each category, leaving the category order intact."""
    runner = sweep(nemesis_selector="flag_common and not flag_c")

    assert [nemesis.__class__.__name__ for nemesis in runner.disruptions_list] == [
        "CustomNemesisA",
        "CustomNemesisAD",
        "CustomNemesisB",
    ]


def test_disabled_category_is_held_out_of_the_sweep(sweep):
    """A disabled category contributes no nemesis, and the rest keep their order."""

    class SweepWithoutFlagA(FakeCategorySweepMonkey):
        DISABLED_CATEGORIES = frozenset({"flag-a"})

    runner = sweep(runner_class=SweepWithoutFlagA)

    assert [nemesis.__class__.__name__ for nemesis in runner.disruptions_list] == [
        "CustomNemesisC",
        "CustomNemesisB",
    ]


def test_topology_changes_is_disabled_for_now(sweep):
    """The topology-changes category is temporarily held out of the real sweep."""
    assert CategorySweepMonkey.DISABLED_CATEGORIES == frozenset({"topology-changes"})
    assert "topology-changes" in dict(CategorySweepMonkey.CATEGORIES), (
        "a disabled category must stay in CATEGORIES, otherwise its nemesis fall into another one"
    )


def test_multiply_factor_is_ignored(sweep):
    """nemesis_multiply_factor does not repeat the list - a sweep runs each nemesis once."""
    runner = sweep(params=dict(PARAMS, nemesis_multiply_factor=3))

    assert [nemesis.__class__.__name__ for nemesis in runner.disruptions_list] == FULL_SWEEP


# ---------------------------------------------------------------------------
# Sweeping
# ---------------------------------------------------------------------------


def test_each_category_is_announced_once(sweep, events_function_scope):
    """One InfoEvent per non-empty category, published as its first nemesis starts."""
    runner = sweep()

    with pytest.raises(NemesisPassCompleted):
        for _ in FULL_SWEEP:
            runner.call_next_nemesis()

    assert runner.executed == FULL_SWEEP
    assert info_messages(events_function_scope)[:3] == [
        f"{runner} starting nemesis category 'flag-c'",
        f"{runner} starting nemesis category 'flag-a'",
        f"{runner} starting nemesis category 'rest'",
    ]


def test_sweep_ends_on_its_last_nemesis(sweep, events_function_scope):
    """The pass is signalled by the call that runs the last nemesis, not by an extra idle call."""
    runner = sweep()

    for _ in FULL_SWEEP[:-1]:
        runner.call_next_nemesis()

    with pytest.raises(NemesisPassCompleted, match="completed its sweep of 4 nemesis"):
        runner.call_next_nemesis()
    assert runner.executed == FULL_SWEEP


def test_pruned_nemesis_do_not_break_category_boundaries(sweep, events_function_scope, monkeypatch):
    """A category that loses a member keeps its place, and is still announced exactly once."""
    monkeypatch.setattr(CustomNemesisAD, "precheck", lambda self, node: "not feasible here")
    runner = sweep()

    runner.precheck_nemesis()

    assert [nemesis.__class__.__name__ for nemesis in runner.disruptions_list] == [
        "CustomNemesisC",
        "CustomNemesisA",
        "CustomNemesisB",
    ]
    with pytest.raises(NemesisPassCompleted):
        for _ in range(len(runner.disruptions_list)):
            runner.call_next_nemesis()
    assert runner.executed == ["CustomNemesisC", "CustomNemesisA", "CustomNemesisB"]
    assert info_messages(events_function_scope)[:3] == [
        f"{runner} starting nemesis category 'flag-c'",
        f"{runner} starting nemesis category 'flag-a'",
        f"{runner} starting nemesis category 'rest'",
    ]


def test_emptied_category_is_not_announced(sweep, events_function_scope, monkeypatch):
    """A category whose only member is pruned is skipped, and does not end the sweep early."""
    monkeypatch.setattr(CustomNemesisC, "precheck", lambda self, node: "not feasible here")
    runner = sweep()

    runner.precheck_nemesis()

    with pytest.raises(NemesisPassCompleted):
        for _ in range(len(runner.disruptions_list)):
            runner.call_next_nemesis()
    assert runner.executed == ["CustomNemesisA", "CustomNemesisAD", "CustomNemesisB"]
    assert info_messages(events_function_scope)[:2] == [
        f"{runner} starting nemesis category 'flag-a'",
        f"{runner} starting nemesis category 'rest'",
    ]


def test_run_stops_the_thread_after_one_pass(sweep, events_function_scope):
    """run() returns once the sweep is done, without touching the shared termination event."""
    runner = sweep()

    runner.run()

    assert runner.executed == FULL_SWEEP
    assert not runner.termination_event.is_set(), "a finished sweep must not stop sibling nemesis threads"
    assert events_function_scope.get_events_by_category()["CRITICAL"] == []
    assert f"{runner} completed its sweep of 4 nemesis" in info_messages(events_function_scope)

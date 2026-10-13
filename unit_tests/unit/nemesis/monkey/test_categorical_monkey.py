"""Tests for CategoricalMonkey: weighted disruption selection and precheck() pruning."""

import threading
from unittest.mock import MagicMock

import pytest

from sdcm.nemesis.monkey.runners import CategoricalMonkey, CategoricalMonkeyConfig
from unit_tests.unit.nemesis import TestNemesisClass, runner_config
from unit_tests.unit.nemesis.fake_cluster import FakeTester


class FakeCategorialMonkey(CategoricalMonkey, TestNemesisClass):
    """Override CategoricalMonkey with a new disruption tree"""


def categorical_config(dist):
    """Config running only the disruptions listed in ``dist``."""
    return runner_config(CategoricalMonkeyConfig, dist=dist, default_weight=0)


@pytest.fixture()
def categorical_runner():
    """A FakeCategorialMonkey wired like a real NemesisRunner for run() integration."""
    tester = FakeTester()
    tester.db_cluster.check_cluster_health = MagicMock()

    def build(dist):
        instance = FakeCategorialMonkey(tester, threading.Event(), categorical_config(dist))
        instance.interval = 0
        return instance

    return build


def _member_named(nemesis, name):
    """Find the disruption_distribution population member with the given class name."""
    population, _ = nemesis.disruption_distribution
    return next(member for member in population if member.__class__.__name__ == name)


@pytest.mark.parametrize(
    "dist, output",
    [
        ({"CustomNemesisA": 1}, "called test function a\n"),
        ({"CustomNemesisC": 0.5}, "called test function c\n"),
        ({"CustomNemesisAD": 1, "CustomNemesisA": 0}, "called test function d\n"),
    ],
)
def test_categorical_monkey_simple(dist, output, capsys):
    nemesis = FakeCategorialMonkey(FakeTester(), None, categorical_config(dist))
    method = nemesis.select_next_nemesis()

    method.disrupt()
    captured = capsys.readouterr()
    assert output in captured.out


def test_categorical_monkey_default_weight_applies_to_unlisted_disruptions():
    config = runner_config(CategoricalMonkeyConfig, dist={"CustomNemesisC": 2, "CustomNemesisA": 0}, default_weight=0.5)

    nemesis = FakeCategorialMonkey(FakeTester(), None, config)

    population, weights = nemesis.disruption_distribution
    assert dict(zip((member.__class__.__name__ for member in population), weights)) == {
        "CustomNemesisC": 2.0,
        "CustomNemesisB": 0.5,
        "CustomNemesisAD": 0.5,
    }


# ---------------------------------------------------------------------------
# precheck() pruning of disruption_distribution (via disruptions_list)
# ---------------------------------------------------------------------------


def test_categorical_monkey_precheck_prunes_distribution(events_function_scope):
    """precheck() removes an infeasible member from both population and weights,
    keeping them zipped, and leaves the runnable member's weight untouched."""
    nemesis = FakeCategorialMonkey(FakeTester(), None, categorical_config({"CustomNemesisA": 1, "CustomNemesisC": 2}))
    _member_named(nemesis, "CustomNemesisA").precheck = MagicMock(return_value="not feasible")

    excluded = nemesis.precheck_nemesis()

    assert excluded == [("CustomNemesisA", "not feasible")]
    population, weights = nemesis.disruption_distribution
    assert [m.__class__.__name__ for m in population] == ["CustomNemesisC"]
    assert weights == [2.0]
    assert nemesis.disruptions_list == population


def test_categorical_monkey_precheck_all_excluded(events_function_scope):
    """When every member is excluded, the distribution ends up empty on both sides."""
    nemesis = FakeCategorialMonkey(FakeTester(), None, categorical_config({"CustomNemesisA": 1, "CustomNemesisC": 2}))
    _member_named(nemesis, "CustomNemesisA").precheck = MagicMock(return_value="not feasible")
    _member_named(nemesis, "CustomNemesisC").precheck = MagicMock(return_value="also not feasible")

    excluded = nemesis.precheck_nemesis()

    assert {name for name, _ in excluded} == {"CustomNemesisA", "CustomNemesisC"}
    assert nemesis.disruption_distribution == ([], [])
    assert nemesis.disruptions_list == []


# ---------------------------------------------------------------------------
# run() integration: empty-rotation CRITICAL applies to CategoricalMonkey too
# ---------------------------------------------------------------------------


def test_categorical_monkey_run_all_pruned_emits_critical_and_stops(categorical_runner, events_function_scope):
    """run() stops cleanly with a CRITICAL TestFrameworkEvent when precheck empties
    the whole distribution, instead of select_next_nemesis() hitting its bare assert."""
    nemesis = categorical_runner({"CustomNemesisA": 1})
    _member_named(nemesis, "CustomNemesisA").precheck = MagicMock(return_value="not feasible")

    nemesis.run(cycles_count=5)

    assert nemesis.disruption_distribution == ([], [])
    framework_events = [
        event
        for event in events_function_scope.published_events
        if event.get("base") == "TestFrameworkEvent" and event.get("severity") == "CRITICAL"
    ]
    assert len(framework_events) == 1
    assert "CustomNemesisA" in framework_events[0]["message"]


def test_categorical_monkey_run_partial_prune_runs_survivors_without_critical(
    categorical_runner, events_function_scope, capsys
):
    """A partial prune keeps the runnable member selectable and runs it, no CRITICAL event."""
    nemesis = categorical_runner({"CustomNemesisA": 1, "CustomNemesisC": 1})
    _member_named(nemesis, "CustomNemesisA").precheck = MagicMock(return_value="not feasible")

    nemesis.run(cycles_count=1)

    population, _ = nemesis.disruption_distribution
    assert [m.__class__.__name__ for m in population] == ["CustomNemesisC"]
    assert events_function_scope.get_events_by_category()["CRITICAL"] == []

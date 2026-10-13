"""
This module tests Nemesis/SisyphusMonkey specific class feature directly, with custom Subclass tree
Should not be dependent on the implementation of Nemesis class

"""

from contextlib import nullcontext

import pytest

from sdcm.nemesis.monkey.runners import SisyphusMonkey
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.unit.nemesis import TestNemesisClass, runner_config
from unit_tests.unit.nemesis.fake_cluster import FakeTester, PARAMS, Cluster, Node
from unit_tests.lib.fake_tester import ClusterTesterForTests


# Use multiple inheritance to ensure we override registry after Nemesis but before Sisyphus
class FakeSisyphusMonkey(SisyphusMonkey, TestNemesisClass):
    def __init__(self, tester_obj, termination_event, config):
        super().__init__(tester_obj, termination_event, config=config)


@pytest.fixture()
def get_sisyphus():
    def _create_sisyphus(params=PARAMS):
        return FakeSisyphusMonkey(FakeTester(params=params), None, runner_config())

    return _create_sisyphus


@pytest.fixture()
def tester(tmp_path):
    """Shared tester fixture with a fake 2-node cluster."""
    cluster_tester = ClusterTesterForTests()
    cluster_tester._init_logging(tmp_path)
    cluster_tester._init_params()
    cluster_tester.db_cluster = Cluster(nodes=[Node(), Node()])
    cluster_tester.db_cluster.params = cluster_tester.params
    cluster_tester.params["nemesis_multiply_factor"] = 1
    cluster_tester.nemesis_allocator = NemesisNodeAllocator(cluster_tester)
    return cluster_tester


def test_disruptions_list(get_sisyphus):
    nemesis = get_sisyphus()
    assert set(method.__class__.__name__ for method in nemesis.disruptions_list) == {
        "CustomNemesisA",
        "CustomNemesisB",
        "CustomNemesisC",
        "CustomNemesisAD",
    }


class CustomNemesisByName(TestNemesisClass):
    """Override Nemesis with a new disruption tree, built from the given names."""

    def __init__(self, *args, disruptions, **kwargs):
        super().__init__(*args, **kwargs)
        self.disruptions_list = self.build_disruptions_by_name(disruptions)


@pytest.mark.parametrize(
    "disruptions, expected_error",
    [
        pytest.param(["CustomNemesisA", "CustomNemesisAD"], None, id="valid_disruptions"),
        pytest.param(["CustomNemesisAD", "CustomNemesisA"], None, id="valid_disruptions_reversed_order"),
        pytest.param(["CustomNemesisX", "CustomNemesisAD"], ValueError, id="unknown_disruption"),
    ],
)
def test_build_disruptions_by_name(disruptions, expected_error):
    """
    Tests the build_disruptions_by_name method of CategoricalMonkey.
    It checks if the method correctly builds disruptions from given names, regardless of
    whether that order matches nemesis-registry discovery order, and raises an error for a
    name that matches no known nemesis class.
    """
    tester = FakeTester()
    ctx = pytest.raises(expected_error) if expected_error else nullcontext()
    with ctx:
        nemesis = CustomNemesisByName(tester, None, runner_config(), disruptions=disruptions)
        if expected_error is None:
            assert set(nemesis._disruption_list_names) == set(disruptions)


def test_build_disruptions_by_name_selector_excludes_one():
    """A name that exists but is filtered out by the active nemesis_selector is skipped
    with a warning, not raised as an error -- distinct from a name that matches no class
    at all."""
    tester = FakeTester()
    # flag_b only matches CustomNemesisB, so CustomNemesisA is excluded by the selector.
    nemesis = CustomNemesisByName(
        tester, None, runner_config(selector="flag_b"), disruptions=["CustomNemesisA", "CustomNemesisB"]
    )
    assert nemesis._disruption_list_names == ["CustomNemesisB"]


def test_build_disruptions_by_name_selector_excludes_all():
    """When the active selector excludes every requested name, construction raises instead
    of silently leaving an empty disruptions_list for call_next_nemesis to assert on later."""
    tester = FakeTester()
    with pytest.raises(ValueError, match="No nemesis left"):
        CustomNemesisByName(
            tester,
            None,
            runner_config(selector="flag_d"),
            disruptions=["CustomNemesisA", "CustomNemesisB"],
        )


def test_nested_runners_build_one_thread_per_entry(tester):
    tester.params["nemesis"] = {
        "interval": 7,
        "runners": [{"SisyphusMonkey": {"selector": "flag_c"}}, {"SisyphusMonkey": {"interval": 15, "seed": 456}}],
    }

    nemeses = tester.get_nemesis_class()

    assert [thread.runner_class for thread in nemeses] == [SisyphusMonkey, SisyphusMonkey]
    assert [(thread.config.selector, thread.config.interval) for thread in nemeses] == [("flag_c", 7), (None, 15)]
    assert nemeses[1].config.seed == 456


def test_runner_reads_options_from_its_config(tester):
    tester.params["nemesis"] = {
        "runners": [{"SisyphusMonkey": {"selector": "flag_c", "interval": 15, "seed": 456, "multiply_factor": 3}}]
    }
    (nemesis_thread,) = tester.get_nemesis_class()

    runner = FakeSisyphusMonkey(tester, None, nemesis_thread.config)

    assert runner.interval == 15 * 60
    assert runner.nemesis_seed == 456
    assert runner.nemesis_selector == "flag_c"
    assert [disrupt.__class__.__name__ for disrupt in runner.disruptions_list] == ["CustomNemesisC"] * 3

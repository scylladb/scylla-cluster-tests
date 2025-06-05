"""
This module tests Nemesis/SisyphusMonkey specific class feature directly, with custom Subclass tree
Should not be dependent on the implementation of Nemesis class

"""
import pytest

from sdcm.nemesis import Nemesis, SisyphusMonkey
from sdcm.nemesis_registry import NemesisRegistry
from unit_tests.nemesis import TestBaseClass
from unit_tests.nemesis.fake_cluster import FakeTester, PARAMS, Cluster, Node
from unit_tests.test_tester import ClusterTesterForTests


class TestNemesisClass(Nemesis):
    COMMON_STRING = "called test function "
    kubernetes = False
    flag_a = False
    flag_b = False
    flag_c = False
    flag_common = False

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(tester_obj, termination_event, *args, nemesis_selector=nemesis_selector,
                         nemesis_seed=nemesis_seed, **kwargs)
        self.nemesis_registry = NemesisRegistry(base_class=TestBaseClass)


# Use multiple inheritance to ensure we overide registry after Nemesis but before Sisyphus
class FakeSisyphusMonkey(SisyphusMonkey, TestNemesisClass):
    def __init__(self, tester_obj, *args, termination_event=None, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(tester_obj, termination_event, *args, nemesis_selector=nemesis_selector,
                         nemesis_seed=nemesis_seed, **kwargs)


@pytest.fixture()
def get_sisyphus():
    def _create_sisyphus(params=PARAMS):
        return FakeSisyphusMonkey(FakeTester(params=params))
    return _create_sisyphus


@pytest.mark.parametrize(
    "params, expected",
    [
        pytest.param({"nemesis_exclude_disabled": True},
                     {'CustomNemesisD', 'CustomNemesisA', 'CustomNemesisC'},
                     id="exclude_disabled"),
        pytest.param({"nemesis_exclude_disabled": False},
                     {'CustomNemesisA',
                      'DisabledNemesis',
                      'CustomNemesisC',
                      'CustomNemesisD'},
                     id="disabled"),
    ]
)
def test_disruptions_list(get_sisyphus, params, expected):
    if params:
        params.update(PARAMS)
    nemesis = get_sisyphus(params=params)
    assert expected == set(method.__name__ for method in nemesis.disruptions_list)


def test_add_sisyphus_with_filter_in_parallel_nemesis_run():
    tester = ClusterTesterForTests()
    tester._init_params()
    tester.db_cluster = Cluster(nodes=[Node(), Node()])
    tester.db_cluster.params = tester.params
    tester.params["nemesis_class_name"] = "SisyphusMonkey:1 SisyphusMonkey:2"
    tester.params["nemesis_selector"] = ["flag_common",
                                         "flag_common and not flag_a",
                                         "flag_b"]
    tester.params["nemesis_exclude_disabled"] = True
    tester.params["nemesis_multiply_factor"] = 1
    nemesises = tester.get_nemesis_class()

    expected_selectors = ["flag_common", "flag_common and not flag_a",  "flag_b"]
    for i, nemesis_settings in enumerate(nemesises):
        assert nemesis_settings['nemesis'] == SisyphusMonkey, \
            f"Wrong instance of nemesis class {nemesis_settings['nemesis']} expected SisyphusMonkey"
        assert nemesis_settings['nemesis_selector'] == expected_selectors[i], \
            f"Wrong nemesis filter selectors {nemesis_settings['nemesis_selector']} expected {expected_selectors[i]}"

    active_nemesis = []
    for nemesis in nemesises:
        sisyphus = FakeSisyphusMonkey(tester, nemesis_selector=nemesis["nemesis_selector"])
        active_nemesis.append(sisyphus)

    expected_methods = [{'CustomNemesisD', 'CustomNemesisA', 'CustomNemesisC'},
                        {"CustomNemesisC"},
                        set()]
    for i, nem in enumerate(active_nemesis):
        assert {disrupt.__name__ for disrupt in nem.disruptions_list} == expected_methods[i]

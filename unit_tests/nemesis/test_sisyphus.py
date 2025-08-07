"""
This module tests Nemesis/SisyphusMonkey specific class feature directly, with custom Subclass tree
Should not be dependent on the implementation of Nemesis class

"""
from contextlib import nullcontext

import pytest

from sdcm.nemesis import Nemesis, SisyphusMonkey
from sdcm.nemesis_registry import NemesisRegistry
from sdcm.utils.nemesis_utils.node_allocator import NemesisNodeAllocator
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
    __test__ = False  # Prevent pytest from treating this as a test class

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(tester_obj, termination_event, *args, nemesis_selector=nemesis_selector,
                         nemesis_seed=nemesis_seed, **kwargs)
        self.nemesis_registry = NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)


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
                     {"CustomNemesisAD", "CustomNemesisA", "CustomNemesisC"},
                     id="exclude_disabled"),
        pytest.param({"nemesis_exclude_disabled": False},
                     {"CustomNemesisA",
                      "CustomNemesisB",
                      "CustomNemesisC",
                      "CustomNemesisAD"},
                     id="disabled"),
    ]
)
def test_disruptions_list(get_sisyphus, params, expected):
    if params:
        params.update(PARAMS)
    nemesis = get_sisyphus(params=params)
    assert set(method.__class__.__name__ for method in nemesis.disruptions_list) == expected


def test_add_sisyphus_with_filter_in_parallel_nemesis_run(tmp_path):
    tester = ClusterTesterForTests()
    tester._init_logging(tmp_path)
    tester._init_params()
    tester.db_cluster = Cluster(nodes=[Node(), Node()])
    tester.db_cluster.params = tester.params
    tester.params["nemesis_class_name"] = "SisyphusMonkey:1 SisyphusMonkey:2"
    tester.params["nemesis_selector"] = ["flag_common",
                                         "flag_common and not flag_c",
                                         "flag_c"]
    tester.params["nemesis_exclude_disabled"] = True
    tester.params["nemesis_multiply_factor"] = 1

    tester.nemesis_allocator = NemesisNodeAllocator(tester)

    nemesises = tester.get_nemesis_class()

    expected_selectors = ["flag_common", "flag_common and not flag_c",  "flag_c"]
    for i, nemesis_settings in enumerate(nemesises):
        assert nemesis_settings['nemesis'] == SisyphusMonkey, \
            f"Wrong instance of nemesis class {nemesis_settings['nemesis']} expected SisyphusMonkey"
        assert nemesis_settings['nemesis_selector'] == expected_selectors[i], \
            f"Wrong nemesis filter selecters {nemesis_settings['nemesis_selector']} expected {expected_selectors[i]}"

    active_nemesis = []
    for nemesis in nemesises:
        sisyphus = FakeSisyphusMonkey(tester, nemesis_selector=nemesis["nemesis_selector"])
        active_nemesis.append(sisyphus)

    expected_methods = [{"CustomNemesisA", "CustomNemesisAD", "CustomNemesisC"},
                        {"CustomNemesisA", "CustomNemesisAD"},
                        {"CustomNemesisC"}]
    for i, nem in enumerate(active_nemesis):
        assert {disrupt.__class__.__name__ for disrupt in nem.disruptions_list} == expected_methods[i]


@pytest.mark.parametrize("disruptions, expected_error",
                         [
                             pytest.param(["CustomNemesisA", "CustomNemesisAD"], None, id="valid_disruptions"),
                             pytest.param(["CustomNemesisX", "CustomNemesisAD"],
                                          AssertionError, id="invalid_disruptions"),
                             pytest.param(["CustomNemesisB", "CustomNemesisC"],
                                          AssertionError, id="disabled_disruption"),
                         ]
                         )
def test_build_disruptions_by_name(disruptions, expected_error):
    """
    Tests the build_disruptions_by_name method of CategoricalMonkey.
    It checks if the method correctly builds disruptions from given names
    and raises an error for invalid or disabled disruptions.
    """

    class CustomNemesis(TestNemesisClass):
        """Override Nemesis with a new disruption tree"""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.disruptions_list = self.build_disruptions_by_name(disruptions)

    tester = FakeTester()
    tester.params["nemesis_exclude_disabled"] = True
    ctx = pytest.raises(expected_error) if expected_error else nullcontext()
    with ctx:
        CustomNemesis(tester, None)

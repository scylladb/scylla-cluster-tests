"""
This module tests Nemesis/SisyphusMonkey specific class feature directly, with custom Subclass tree
Should not be dependent on the implementation of Nemesis class

"""
import pytest

from sdcm.nemesis import Nemesis, SisyphusMonkey
from sdcm.nemesis_registry import NemesisRegistry
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
        self.nemesis_registry = NemesisRegistry(base_class=TestNemesisClass)

    def disrupt_method_a(self):
        print(self.COMMON_STRING + "a")

    def disrupt_method_b(self):
        print(self.COMMON_STRING + "b")

    def disrupt_method_c(self):
        print(self.COMMON_STRING + "c")


class AddRemoveDCMonkey(TestNemesisClass):
    flag_common = True

    @TestNemesisClass.add_disrupt_method
    def disrupt_rnd_method(self):
        print("disrupt_rnd_method")

    def disrupt(self):
        self.disrupt_rnd_method()


class DisabledMonkey(TestNemesisClass):
    disabled = True
    flag_b = True
    flag_common = True

    def disrupt(self):
        self.disrupt_method_b()


class DisruptAMonkey(TestNemesisClass):
    flag_a = True
    flag_common = True

    def disrupt(self):
        self.disrupt_method_a()


class DisruptCMonkey(TestNemesisClass):
    flag_c = True

    def disrupt(self):
        self.disrupt_method_c()


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
                     {"disrupt_method_a", "disrupt_method_c", "disrupt_rnd_method"},
                     id="exclude_disabled"),
        pytest.param({"nemesis_exclude_disabled": False},
                     {"disrupt_method_a", "disrupt_method_c", "disrupt_rnd_method", "disrupt_method_b"},
                     id="disabled"),
    ]
)
def test_disruptions_list(get_sisyphus, params, expected):
    if params:
        params.update(PARAMS)
    nemesis = get_sisyphus(params=params)
    assert expected == set(method.__name__ for method in nemesis.disruptions_list)


def test_list_nemesis_of_added_disrupt_methods(get_sisyphus, capsys):
    nemesis = get_sisyphus()
    nemesis.disruptions_list = nemesis.build_disruptions_by_name(['disrupt_rnd_method'])
    nemesis.call_next_nemesis()
    captured = capsys.readouterr()
    assert "disrupt_rnd_method" in captured.out


def test_add_sisyphus_with_filter_in_parallel_nemesis_run():
    tester = ClusterTesterForTests()
    tester._init_params()
    tester.db_cluster = Cluster(nodes=[Node(), Node()])
    tester.db_cluster.params = tester.params
    tester.params["nemesis_class_name"] = "SisyphusMonkey:1 SisyphusMonkey:2"
    tester.params["nemesis_selector"] = ["flag_common",
                                         "flag_common and not flag_a",
                                         "flag_c"]
    tester.params["nemesis_exclude_disabled"] = True
    tester.params["nemesis_multiply_factor"] = 1
    nemesises = tester.get_nemesis_class()

    expected_selectors = ["flag_common", "flag_common and not flag_a",  "flag_c"]
    for i, nemesis_settings in enumerate(nemesises):
        assert nemesis_settings['nemesis'] == SisyphusMonkey, \
            f"Wrong instance of nemesis class {nemesis_settings['nemesis']} expected SisyphusMonkey"
        assert nemesis_settings['nemesis_selector'] == expected_selectors[i], \
            f"Wrong nemesis filter selecters {nemesis_settings['nemesis_selector']} expected {expected_selectors[i]}"

    active_nemesis = []
    for nemesis in nemesises:
        sisyphus = FakeSisyphusMonkey(tester, nemesis_selector=nemesis["nemesis_selector"])
        active_nemesis.append(sisyphus)

    expected_methods = [{"disrupt_method_a", "disrupt_rnd_method"},
                        {"disrupt_rnd_method"},
                        {"disrupt_method_c"}]
    for i, nem in enumerate(active_nemesis):
        assert {disrupt.__name__ for disrupt in nem.disruptions_list} == expected_methods[i]

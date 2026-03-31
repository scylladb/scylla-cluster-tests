"""
This module tests Nemesis/SisyphusMonkey specific class feature directly, with custom Subclass tree
Should not be dependent on the implementation of Nemesis class

"""

from contextlib import nullcontext

import pytest

from sdcm.nemesis.monkey.runners import SisyphusMonkey
from sdcm.nemesis import NemesisRunner
from sdcm.nemesis.registry import NemesisRegistry
from sdcm.nemesis.utils.node_allocator import NemesisNodeAllocator
from unit_tests.nemesis import TestBaseClass
from unit_tests.nemesis.fake_cluster import FakeTester, PARAMS, Cluster, Node
from unit_tests.test_tester import ClusterTesterForTests


class TestNemesisClass(NemesisRunner):
    COMMON_STRING = "called test function "
    kubernetes = False
    flag_a = False
    flag_b = False
    flag_c = False
    flag_common = False
    __test__ = False  # Prevent pytest from treating this as a test class

    def __init__(self, tester_obj, termination_event, *args, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )
        self.nemesis_registry = NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)


# Use multiple inheritance to ensure we overide registry after Nemesis but before Sisyphus
class FakeSisyphusMonkey(SisyphusMonkey, TestNemesisClass):
    def __init__(self, tester_obj, *args, termination_event=None, nemesis_selector=None, nemesis_seed=None, **kwargs):
        super().__init__(
            tester_obj, termination_event, *args, nemesis_selector=nemesis_selector, nemesis_seed=nemesis_seed, **kwargs
        )


@pytest.fixture()
def get_sisyphus():
    def _create_sisyphus(params=PARAMS):
        return FakeSisyphusMonkey(FakeTester(params=params))

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
    # Disable the HEAD-only "exclude disabled" augmentation so upstream-style
    # selector expectations hold (individual tests can re-enable as needed).
    cluster_tester.params["nemesis_exclude_disabled"] = False
    cluster_tester.nemesis_allocator = NemesisNodeAllocator(cluster_tester)
    return cluster_tester


@pytest.mark.parametrize(
    "params, expected",
    [
        pytest.param(
            {"nemesis_exclude_disabled": True},
            {"CustomNemesisAD", "CustomNemesisA", "CustomNemesisC"},
            id="exclude_disabled",
        ),
        pytest.param(
            {"nemesis_exclude_disabled": False},
            {"CustomNemesisA", "CustomNemesisB", "CustomNemesisC", "CustomNemesisAD"},
            id="disabled",
        ),
    ],
)
def test_disruptions_list(get_sisyphus, params, expected):
    if params:
        params.update(PARAMS)
    nemesis = get_sisyphus(params=params)
    assert set(method.__class__.__name__ for method in nemesis.disruptions_list) == expected


def test_add_sisyphus_with_filter_in_parallel_nemesis_run(tester):
    """5 explicit threads (explicit list format), 1 selector per thread."""
    tester.params["nemesis_class_name"] = [
        "SisyphusMonkey",
        "SisyphusMonkey",
        "SisyphusMonkey",
        "SisyphusMonkey",
        "SisyphusMonkey",
    ]
    tester.params["nemesis_selector"] = [
        "flag_common",
        "flag_common and not flag_c",
        "flag_c",
        "CustomNemesisC",
        "CustomNemesisA or CustomNemesisC",
    ]

    nemeses = tester.get_nemesis_class()

    assert len(nemeses) == 5, f"Expected 5 nemesis threads, got {len(nemeses)}"

    expected_selectors = [
        "flag_common",
        "flag_common and not flag_c",
        "flag_c",
        "CustomNemesisC",
        "CustomNemesisA or CustomNemesisC",
    ]
    for i, nemesis_settings in enumerate(nemeses):
        assert nemesis_settings["nemesis"] == SisyphusMonkey, (
            f"Thread {i}: wrong class {nemesis_settings['nemesis']}, expected SisyphusMonkey"
        )
        assert nemesis_settings["nemesis_selector"] == expected_selectors[i], (
            f"Thread {i}: wrong selector {nemesis_settings['nemesis_selector']!r}, expected {expected_selectors[i]!r}"
        )

    active_nemesis = [FakeSisyphusMonkey(tester, nemesis_selector=n["nemesis_selector"]) for n in nemeses]

    expected_methods = [
        {"CustomNemesisA", "CustomNemesisB", "CustomNemesisAD", "CustomNemesisC"},
        {"CustomNemesisA", "CustomNemesisB", "CustomNemesisAD"},
        {"CustomNemesisC"},
        {"CustomNemesisC"},
        {"CustomNemesisA", "CustomNemesisC"},
    ]
    for i, nem in enumerate(active_nemesis):
        assert {disrupt.__class__.__name__ for disrupt in nem.disruptions_list} == expected_methods[i], (
            f"Thread {i}: wrong disruptions list"
        )


@pytest.mark.parametrize(
    "class_names,selectors,expected",
    [
        pytest.param(["SisyphusMonkey", "SisyphusMonkey"], [], ["", ""], id="no_selectors_empty_per_thread"),
        pytest.param(
            ["SisyphusMonkey", "SisyphusMonkey", "SisyphusMonkey"],
            ["flag_common"],
            ["flag_common"] * 3,
            id="single_selector_broadcast",
        ),
        pytest.param(
            ["SisyphusMonkey", "SisyphusMonkey", "SisyphusMonkey"],
            ["flag_a", "flag_b", "flag_c"],
            ["flag_a", "flag_b", "flag_c"],
            id="exact_length_one_to_one_three_threads",
        ),
    ],
)
def test_selector_assignment_behaviour(tester, class_names, selectors, expected):
    """Parametrized: no selectors -> empty, single -> broadcast, N->1:1 mapping."""
    tester.params["nemesis_class_name"] = class_names
    tester.params["nemesis_selector"] = selectors

    nemeses = tester.get_nemesis_class()
    assert len(nemeses) == len(class_names), f"Expected {len(class_names)} nemesis threads, got {len(nemeses)}"

    for i, n in enumerate(nemeses):
        assert n["nemesis_selector"] == expected[i], (
            f"Thread {i}: wrong selector {n['nemesis_selector']!r}, expected {expected[i]!r}"
        )


@pytest.mark.parametrize(
    "config_selector, explicit_selector, expected_selector, expected_disruptions",
    [
        pytest.param(["flag_c"], None, "flag_c", {"CustomNemesisC"}, id="config_only"),
        pytest.param(
            ["flag_c"],
            "",
            "",
            {"CustomNemesisA", "CustomNemesisB", "CustomNemesisC", "CustomNemesisAD"},
            id="explicit_empty_overrides",
        ),
        pytest.param(
            ["flag_c"],
            "flag_a",
            "flag_a",
            {"CustomNemesisA", "CustomNemesisAD", "CustomNemesisC"},
            id="explicit_non_empty_overrides",
        ),
    ],
)
def test_direct_instantiation_selector_precedence(
    tester, config_selector, explicit_selector, expected_selector, expected_disruptions
):
    """Parametrized: direct construction uses config selector unless an explicit selector is provided.

    Cases:
    - config-only: config selector is used
    - explicit empty: explicit empty selector overrides config and runs all disruptions
    - explicit non-empty: explicit selector overrides config
    """
    tester.params["nemesis_selector"] = config_selector

    kwargs = {}
    if explicit_selector is not None:
        kwargs["nemesis_selector"] = explicit_selector

    runner = FakeSisyphusMonkey(tester, None, **kwargs)

    assert runner.nemesis_selector == expected_selector
    assert {d.__class__.__name__ for d in runner.disruptions_list} == expected_disruptions


@pytest.mark.parametrize(
    "class_names,seeds,expected",
    [
        pytest.param(["SisyphusMonkey", "SisyphusMonkey"], None, [None, None], id="no_seeds"),
        pytest.param(
            ["SisyphusMonkey", "SisyphusMonkey", "SisyphusMonkey"],
            17,
            [17, 17, 17],
            id="single_seed_broadcast",
        ),
        pytest.param(
            ["SisyphusMonkey", "SisyphusMonkey", "SisyphusMonkey"],
            [17, 23, 42],
            [17, 23, 42],
            id="exact_length_seed_mapping",
        ),
    ],
)
def test_seed_assignment_behaviour(tester, class_names, seeds, expected):
    """Parametrized: no seeds -> None, single -> broadcast, N->1:1 mapping."""
    tester.params["nemesis_class_name"] = class_names
    if seeds is not None:
        tester.params["nemesis_seed"] = seeds

    nemeses = tester.get_nemesis_class()
    assert len(nemeses) == len(class_names), f"Expected {len(class_names)} nemesis threads, got {len(nemeses)}"

    for i, n in enumerate(nemeses):
        assert n["nemesis_seed"] == expected[i], (
            f"Thread {i}: wrong seed {n['nemesis_seed']!r}, expected {expected[i]!r}"
        )


@pytest.mark.parametrize(
    "class_name, expected_error_fragment",
    [
        pytest.param(
            "SisyphusMonkey:3",
            "Class:N' count syntax is no longer supported",
            id="count_syntax",
        ),
        pytest.param(
            "SisyphusMonkey SisyphusMonkey",
            "Space-separated 'nemesis_class_name' values are no longer supported",
            id="space_separated",
        ),
    ],
)
def test_legacy_class_name_syntax_raises_value_error(tester, class_name, expected_error_fragment):
    """Both ':count' and space-separated syntaxes are rejected with a clear error."""
    tester.params["nemesis_class_name"] = class_name

    with pytest.raises(ValueError, match=expected_error_fragment):
        tester.get_nemesis_class()


@pytest.mark.parametrize(
    "disruptions, expected_error",
    [
        pytest.param(["CustomNemesisA", "CustomNemesisAD"], None, id="valid_disruptions"),
        pytest.param(["CustomNemesisX", "CustomNemesisAD"], AssertionError, id="invalid_disruptions"),
        pytest.param(["CustomNemesisB", "CustomNemesisC"], AssertionError, id="disabled_disruption"),
    ],
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

"""
This module tests NemesisRegistry method on custom Subclass tree
Should not be dependent on the implementation of Nemesis class
"""

import pytest
from sdcm.nemesis_registry import NemesisRegistry


class FlagClass:
    flag_a = False
    flag_b = False
    flag_c = False
    flag_d = False
    flag_common = False
    flag_true = True


class TestNemesis(FlagClass):
    COMMON_STRING = "called test function "

    def disrupt_method_a(self):
        print(self.COMMON_STRING + "a")

    def disrupt_method_b(self):
        print(self.COMMON_STRING + "b")

    def disrupt_method_c(self):
        print(self.COMMON_STRING + "c")

    def disrupt_method_d(self):
        print(self.COMMON_STRING + "d")


class CustomNemesisA(TestNemesis):
    flag_a = True
    flag_common = True
    flag_true = False

    def disrupt(self):
        self.disrupt_method_a()


class CustomNemesisB(TestNemesis):
    flag_b = True
    flag_common = True

    def disrupt(self):
        self.disrupt_method_b()


class CustomNemesisC(CustomNemesisA):
    flag_c = True

    def disrupt(self):
        self.disrupt_method_c()


class CustomNemesisAD(CustomNemesisB):
    flag_d = True
    flag_a = True
    flag_common = True

    def disrupt(self):
        self.disrupt_method_d()


@pytest.fixture
def registry():
    return NemesisRegistry(base_class=TestNemesis, flag_class=FlagClass)


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("flag_a", {CustomNemesisA, CustomNemesisAD}),
        ("flag_b", {CustomNemesisB}),
        ("flag_c", {CustomNemesisC}),
        ("flag_d", {CustomNemesisAD}),
        ("flag_common", {CustomNemesisA, CustomNemesisB, CustomNemesisAD}),
        ("", {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}),
        (None, {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}),
        ("flag_true", {CustomNemesisB, CustomNemesisC, CustomNemesisAD}),  # flag_true is set to False in CustomNemesisA
    ],
)
def test_filter_subclasses_by_single_flag(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("flag_a and flag_d", {CustomNemesisAD}),
        ("flag_common and not flag_c", {CustomNemesisA, CustomNemesisB, CustomNemesisAD}),
        ("CustomNemesisA or flag_d", {CustomNemesisA, CustomNemesisAD}),
    ],
)
def test_filter_subclasses_by_combined_flags(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("(CustomNemesisA or CustomNemesisD) and not flag_d", {CustomNemesisA}),
        ("flag_common and not flag_a", {CustomNemesisB}),
    ],
)
def test_filter_subclasses_with_complex_expression(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


def test_get_subclasses(registry):
    subclasses = registry.get_subclasses()
    assert set(subclasses) == {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}


def test_gather_properties(registry):
    class_properties, method_properties = registry.gather_properties()

    expected_class_properties = {
        "CustomNemesisA": {"flag_a": True, "flag_b": False, "flag_c": False, "flag_d": False, "flag_true": False, "flag_common": True},
        "CustomNemesisB": {"flag_a": False, "flag_b": True, "flag_c": False, "flag_d": False, "flag_true": True, "flag_common": True},
        "CustomNemesisC": {"flag_a": True, "flag_b": False, "flag_c": True, "flag_d": False, "flag_true": False, "flag_common": True},
        "CustomNemesisAD": {"flag_a": True, "flag_b": True, "flag_c": False, "flag_d": True, "flag_true": True, "flag_common": True},
    }

    expected_method_properties = {
        "disrupt_method_a": {"flag_a": True, "flag_b": False, "flag_c": False, "flag_d": False, "flag_true": False, "flag_common": True},
        "disrupt_method_b": {"flag_a": False, "flag_b": True, "flag_c": False, "flag_d": False, "flag_true": True, "flag_common": True},
        "disrupt_method_c": {"flag_a": True, "flag_b": False, "flag_c": True, "flag_d": False, "flag_true": False, "flag_common": True},
        "disrupt_method_d": {"flag_a": True, "flag_b": True, "flag_c": False, "flag_d": True, "flag_true": True, "flag_common": True},
    }

    assert class_properties == expected_class_properties
    assert method_properties == expected_method_properties


@pytest.mark.parametrize(
    "logical_phrase, expected_methods",
    [
        ("flag_a", {CustomNemesisA.disrupt_method_a, CustomNemesisAD.disrupt_method_d}),
        ("flag_b", {CustomNemesisB.disrupt_method_b}),
        ("flag_common and not flag_b", {CustomNemesisA.disrupt_method_a, CustomNemesisAD.disrupt_method_d}),
        ("flag_c", {CustomNemesisC.disrupt_method_c}),
        ("flag_d", {CustomNemesisAD.disrupt_method_d}),
    ],
)
def test_get_disrupt_methods(registry, logical_phrase, expected_methods):
    disrupt_methods = registry.get_disrupt_methods(logical_phrase)
    assert set(disrupt_methods) == expected_methods


def test_get_disrupt_method_execution(registry, capsys):
    """Tests how you can use get_disrupt_methods to actually call returned methods"""
    disrupt_methods = registry.get_disrupt_methods("flag_common and not flag_b")
    for disrupt_method in disrupt_methods:
        disrupt_method(TestNemesis())

    captured = capsys.readouterr()
    assert "called test function a" in captured.out
    assert "called test function d" in captured.out

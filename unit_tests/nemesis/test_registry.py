"""
This module tests NemesisRegistry method on custom Subclass tree
Should not be dependent on the implementation of Nemesis class
"""

import pytest
from sdcm.nemesis_registry import NemesisRegistry
from unit_tests.nemesis import TestBaseClass, CustomNemesisD, CustomNemesisA, DisabledNemesis, CustomNemesisC, \
    TestRunner


@pytest.fixture
def registry():
    return NemesisRegistry(base_class=TestBaseClass)


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("flag_a", {CustomNemesisA, CustomNemesisD}),
        ("disabled", {DisabledNemesis}),
        ("flag_c", {CustomNemesisC}),
        ("flag_d", {CustomNemesisD}),
        ("flag_common", {CustomNemesisA, DisabledNemesis, CustomNemesisD, CustomNemesisC}),
    ],
)
def test_filter_subclasses_by_single_flag(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("flag_a and flag_d", {CustomNemesisD}),
        ("flag_common and not flag_c", {CustomNemesisA, DisabledNemesis, CustomNemesisD}),
        ("CustomNemesisA or flag_d", {CustomNemesisA, CustomNemesisD}),
    ],
)
def test_filter_subclasses_by_combined_flags(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("(CustomNemesisA or CustomNemesisD) and not flag_d", {CustomNemesisA}),
        ("flag_common and not flag_a", {CustomNemesisC, DisabledNemesis}),
    ],
)
def test_filter_subclasses_with_complex_expression(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(registry.get_subclasses(), logical_phrase)
    assert set(filtered) == expected_classes


def test_get_subclasses(registry):
    subclasses = registry.get_subclasses()
    assert set(subclasses) == {CustomNemesisA, DisabledNemesis, CustomNemesisC, CustomNemesisD}


def test_gather_properties(registry):
    class_properties = registry.gather_properties()

    expected_class_properties = {
        "CustomNemesisA": {"flag_a": True, "flag_common": True},
        "DisabledNemesis": {"flag_common": True, "disabled": True},
        "CustomNemesisC": {"flag_c": True, "flag_common": True},
        "CustomNemesisD": {"flag_a": True, "flag_d": True, "flag_common": True},
    }

    assert class_properties == expected_class_properties


@pytest.mark.parametrize(
    "logical_phrase, expected_methods",
    [
        ("flag_a", {CustomNemesisA, CustomNemesisD}),
        ("disabled", {DisabledNemesis}),
        ("flag_common and not disabled", {CustomNemesisA, CustomNemesisC, CustomNemesisD}),
        ("flag_c", {CustomNemesisC}),
        ("flag_d", {CustomNemesisD}),
        ("CustomNemesisA or CustomNemesisD", {CustomNemesisA, CustomNemesisD}),
    ],
)
def test_get_disrupt_methods(registry, logical_phrase, expected_methods):
    disrupt_methods = registry.get_disrupt_methods(logical_phrase)
    assert set(disrupt_methods) == expected_methods


def test_get_disrupt_method_execution(registry, capsys):
    """Tests how you can use get_disrupt_methods to actually call returned methods"""
    disrupt_methods = registry.get_disrupt_methods("flag_common and not flag_b")
    for disrupt_method in disrupt_methods:
        disrupt_method(TestRunner())

    captured = capsys.readouterr()
    assert "called test function a" in captured.out
    assert "called test function d" in captured.out

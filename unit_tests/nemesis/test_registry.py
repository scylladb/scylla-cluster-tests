"""
This module tests NemesisRegistry method on custom Subclass tree
Should not be dependent on the implementation of Nemesis class
"""

import pytest
from sdcm.nemesis_registry import NemesisRegistry
from unit_tests.nemesis import CustomNemesisA, CustomNemesisAD, CustomNemesisB, CustomNemesisC, TestBaseClass, \
    TestRunner


@pytest.fixture
def registry():
    return NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("flag_a", {CustomNemesisA, CustomNemesisAD, CustomNemesisC}),
        ("flag_b", {CustomNemesisB}),
        ("flag_c", {CustomNemesisC}),
        ("flag_d", {CustomNemesisAD}),
        ("flag_common", {CustomNemesisA, CustomNemesisB, CustomNemesisAD, CustomNemesisC}),
        ("", {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}),
        (None, {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}),
        ("flag_true", {CustomNemesisB, CustomNemesisAD}),  # flag_true is set to False in CustomNemesisA
    ],
)
def test_filter_subclasses_by_single_flag(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(logical_phrase)
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
    filtered = registry.filter_subclasses(logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        ("(CustomNemesisA or CustomNemesisD) and not flag_d", {CustomNemesisA}),
        ("flag_common and not flag_a", {CustomNemesisB}),
    ],
)
def test_filter_subclasses_with_complex_expression(registry, logical_phrase, expected_classes):
    filtered = registry.filter_subclasses(logical_phrase)
    assert set(filtered) == expected_classes


def test_get_subclasses(registry):
    subclasses = registry.get_subclasses()
    assert set(subclasses) == {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}


def test_gather_properties(registry):
    class_properties = registry.gather_properties()

    expected_class_properties = {
        "CustomNemesisA": {"flag_a": True, "flag_b": False, "flag_c": False, "flag_d": False, "flag_true": False, "flag_common": True, "disabled": False},
        "CustomNemesisB": {"flag_a": False, "flag_b": True, "flag_c": False, "flag_d": False, "flag_true": True, "flag_common": True, "disabled": True},
        "CustomNemesisC": {"flag_a": True, "flag_b": False, "flag_c": True, "flag_d": False, "flag_true": False, "flag_common": True, "disabled": False},
        "CustomNemesisAD": {"flag_a": True, "flag_b": False, "flag_c": False, "flag_d": True, "flag_true": True, "flag_common": True, "disabled": False},
    }

    assert class_properties == expected_class_properties


def test_get_disrupt_method_execution(registry, capsys):
    """Tests how you can use get_disrupt_methods to actually call returned methods"""
    subclasses = registry.filter_subclasses("flag_true and not flag_a")
    for subclass in subclasses:
        subclass(TestRunner()).disrupt()

    captured = capsys.readouterr()
    assert "called test function b" in captured.out

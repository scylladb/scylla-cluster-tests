"""
This module tests NemesisRegistry method on custom Subclass tree
Should not be dependent on the implementation of Nemesis class
"""

import pytest

from sdcm.nemesis.registry import NemesisRegistry
from unit_tests.unit.nemesis import (
    CustomNemesisA,
    CustomNemesisAD,
    CustomNemesisB,
    CustomNemesisC,
    TestBaseClass,
    TestRunner,
)


@pytest.fixture
def registry():
    """NemesisRegistry over the TestBaseClass subclass tree."""
    return NemesisRegistry(base_class=TestBaseClass, flag_class=TestBaseClass)


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        pytest.param("flag_a", {CustomNemesisA, CustomNemesisAD, CustomNemesisC}, id="flag-a"),
        pytest.param("flag_b", {CustomNemesisB}, id="flag-b"),
        pytest.param("flag_c", {CustomNemesisC}, id="flag-c"),
        pytest.param("flag_d", {CustomNemesisAD}, id="flag-d"),
        pytest.param(
            "flag_common", {CustomNemesisA, CustomNemesisB, CustomNemesisAD, CustomNemesisC}, id="flag-common"
        ),
        pytest.param("", {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}, id="empty-phrase"),
        pytest.param(None, {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}, id="none-phrase"),
        pytest.param(
            "flag_true",
            {CustomNemesisB, CustomNemesisAD},
            id="flag-true",  # flag_true is set to False in CustomNemesisA
        ),
        pytest.param("CustomNemesisA", {CustomNemesisA}, id="class-name"),
        pytest.param("CustomNemesisA or CustomNemesisAD", {CustomNemesisA, CustomNemesisAD}, id="class-name-or"),
    ],
)
def test_filter_subclasses_by_single_flag(registry, logical_phrase, expected_classes):
    """filter_subclasses selects the subclasses matching a single flag or class name."""
    filtered = registry.filter_subclasses(logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        pytest.param("flag_a and flag_d", {CustomNemesisAD}, id="flag-a-and-flag-d"),
        pytest.param(
            "flag_common and not flag_c", {CustomNemesisA, CustomNemesisB, CustomNemesisAD}, id="common-and-not-c"
        ),
        pytest.param("CustomNemesisA or flag_d", {CustomNemesisA, CustomNemesisAD}, id="class-or-flag-d"),
    ],
)
def test_filter_subclasses_by_combined_flags(registry, logical_phrase, expected_classes):
    """filter_subclasses combines flags and class names with and, or and not."""
    filtered = registry.filter_subclasses(logical_phrase)
    assert set(filtered) == expected_classes


@pytest.mark.parametrize(
    "logical_phrase, expected_classes",
    [
        pytest.param(
            "(CustomNemesisA or CustomNemesisD) and not flag_d",
            {CustomNemesisA},
            id="parenthesized-or-and-not",
        ),
        pytest.param("flag_common and not flag_a", {CustomNemesisB}, id="common-and-not-a"),
    ],
)
def test_filter_subclasses_with_complex_expression(registry, logical_phrase, expected_classes):
    """filter_subclasses honours parentheses and operator precedence."""
    filtered = registry.filter_subclasses(logical_phrase)
    assert set(filtered) == expected_classes


def test_get_subclasses(registry):
    """get_subclasses returns every subclass of the base class."""
    subclasses = registry.get_subclasses()
    assert set(subclasses) == {CustomNemesisA, CustomNemesisB, CustomNemesisC, CustomNemesisAD}


def test_gather_properties(registry):
    """gather_properties maps each subclass name to its flag values."""
    class_properties = registry.gather_properties()

    expected_class_properties = {
        "CustomNemesisA": {
            "flag_a": True,
            "flag_b": False,
            "flag_c": False,
            "flag_d": False,
            "flag_true": False,
            "flag_common": True,
        },
        "CustomNemesisB": {
            "flag_a": False,
            "flag_b": True,
            "flag_c": False,
            "flag_d": False,
            "flag_true": True,
            "flag_common": True,
        },
        "CustomNemesisC": {
            "flag_a": True,
            "flag_b": False,
            "flag_c": True,
            "flag_d": False,
            "flag_true": False,
            "flag_common": True,
        },
        "CustomNemesisAD": {
            "flag_a": True,
            "flag_b": False,
            "flag_c": False,
            "flag_d": True,
            "flag_true": True,
            "flag_common": True,
        },
    }

    assert class_properties == expected_class_properties


def test_get_disrupt_method_execution(registry, capsys):
    """Tests how you can use get_disrupt_methods to actually call returned methods"""
    subclasses = registry.filter_subclasses("flag_true and not flag_a")
    for subclass in subclasses:
        subclass(TestRunner()).disrupt()

    captured = capsys.readouterr()
    assert "called test function b" in captured.out

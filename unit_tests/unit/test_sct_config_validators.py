"""Tests for sct_config type-coercion validators."""

import pytest

from sdcm.sct_config import (
    boolean_or_space_separated_booleans,
    dict_or_str,
    int_or_space_separated_ints,
    str_or_list_or_eval,
)


# ---------------------------------------------------------------------------
# int_or_space_separated_ints
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, None),
        (1, [1]),
        (0, [0]),
        (-5, [-5]),
        ("3", [3]),
        ("10", [10]),
        ([1, 2, 3], [1, 2, 3]),
        ([1], [1]),
        (["1", "2", "3"], [1, 2, 3]),
        ("1 2 3", [1, 2, 3]),
        ("  4  5  6  ", [4, 5, 6]),
        ("42", [42]),
    ],
)
def test_int_or_space_separated_ints_valid(input_val, expected):
    assert int_or_space_separated_ints(input_val) == expected


@pytest.mark.parametrize(
    "input_val",
    [
        "not_a_number",
        "1 2 abc",
        [1, "abc"],
    ],
)
def test_int_or_space_separated_ints_invalid(input_val):
    with pytest.raises((ValueError, TypeError)):
        int_or_space_separated_ints(input_val)


# ---------------------------------------------------------------------------
# str_or_list_or_eval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, None),
        ("hello", ["hello"]),
        ("", []),
        ("['cmd1', 'cmd2']", ["cmd1", "cmd2"]),
        ("[1, 2]", [1, 2]),
        ("3", [3]),
        ("{'a': 1}", [{"a": 1}]),
        (["a", "b"], ["a", "b"]),
        (["['nested']", "plain"], [["nested"], "plain"]),
    ],
)
def test_str_or_list_or_eval_valid(input_val, expected):
    assert str_or_list_or_eval(input_val) == expected


def test_str_or_list_or_eval_invalid():
    with pytest.raises(ValueError):
        str_or_list_or_eval(123)


# ---------------------------------------------------------------------------
# boolean_or_space_separated_booleans
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, None),
        (True, [True]),
        (False, [False]),
        ("true", [True]),
        ("false", [False]),
        ("true false", [True, False]),
        ("yes no", [True, False]),
        ([True], [True]),
        ([False], [False]),
        ([True, False], [True, False]),
        (["true", "false"], [True, False]),
    ],
)
def test_boolean_or_space_separated_booleans_valid(input_val, expected):
    assert boolean_or_space_separated_booleans(input_val) == expected


@pytest.mark.parametrize(
    "input_val",
    [
        "not_a_bool",
        123,
    ],
)
def test_boolean_or_space_separated_booleans_invalid(input_val):
    with pytest.raises(ValueError):
        boolean_or_space_separated_booleans(input_val)


# ---------------------------------------------------------------------------
# dict_or_str
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, None),
        ({"key": "val"}, {"key": "val"}),
        ("{'a': 1}", {"a": 1}),
        ('{"b": 2}', {"b": 2}),
        ("key: value", {"key": "value"}),
    ],
)
def test_dict_or_str_valid(input_val, expected):
    assert dict_or_str(input_val) == expected


@pytest.mark.parametrize(
    "input_val",
    [
        "[1, 2]",
        "3",
        "plain string",
    ],
)
def test_dict_or_str_invalid(input_val):
    with pytest.raises(ValueError):
        dict_or_str(input_val)

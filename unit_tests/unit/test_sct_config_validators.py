"""Tests for sct_config type-coercion validators."""

import pytest

from sdcm.sct_config import (
    SCTConfiguration,
    bool_or_list_or_eval,
    dict_or_str,
    int_or_list_or_eval,
    str_or_list_or_eval,
)


# ---------------------------------------------------------------------------
# int_or_list_or_eval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        # config path: YAML-native types
        pytest.param(None, None, id="config_none"),
        # bare scalar int — the real YAML path when a config has `n_db_nodes: 3`
        pytest.param(3, [3], id="config_bare_int"),
        pytest.param(0, [0], id="config_bare_zero"),
        pytest.param(-5, [-5], id="config_bare_negative_int"),
        pytest.param([3], [3], id="config_int"),
        pytest.param([0], [0], id="config_zero"),
        pytest.param([3, 1], [3, 1], id="config_list_ints"),
        pytest.param([253, 328], [253, 328], id="config_list_ints_long"),
        # env path: raw strings from SCT_* environment variables
        pytest.param("3", [3], id="env_single_int"),
        pytest.param("0", [0], id="env_zero"),
        pytest.param("[3, 1]", [3, 1], id="env_list_literal"),
        pytest.param("[3,1]", [3, 1], id="env_list_literal_no_spaces"),
        pytest.param("[3, 3, 2]", [3, 3, 2], id="env_list_literal_three"),
        pytest.param("[1, 0]", [1, 0], id="env_list_literal_with_zero"),
        # negative integers
        pytest.param([-5], [-5], id="config_negative_int"),
        pytest.param("[-5]", [-5], id="env_negative_int"),
        pytest.param([-5, -3], [-5, -3], id="config_negative_list"),
        pytest.param("[-5, -3]", [-5, -3], id="env_negative_list"),
        # empty list
        pytest.param([], [], id="config_empty_list"),
        pytest.param("[]", [], id="env_empty_list"),
    ],
)
def test_int_or_list_or_eval_valid(value, expected):
    """int_or_list_or_eval accepts valid int scalars and homogeneous int lists."""
    assert int_or_list_or_eval(value) == expected


@pytest.mark.parametrize(
    "value,error_fragment",
    [
        # space-separated strings are no longer accepted; literal_eval fails then int() fails
        pytest.param("3 1", "isn't a valid int or list", id="env_space_sep_two"),
        pytest.param("3 3 3", "isn't a valid int or list", id="env_space_sep_three"),
        pytest.param("  5 10  ", "isn't a valid int or list", id="env_space_sep_padded"),
        # non-numeric strings
        pytest.param("foo", "isn't a valid int or list", id="env_non_numeric"),
        # invalid list element types
        pytest.param(["a", "b"], "isn't an integer", id="config_list_str_elements"),
        pytest.param(["3", "1"], "isn't an integer", id="config_list_quoted_ints_rejected"),
        # leading 0
        pytest.param("033", "isn't a valid int or list", id="env_leading_zero"),
        # wrong scalar type
        pytest.param(3.14, "isn't a valid int or list", id="config_float"),
        pytest.param(False, "isn't a valid int or list", id="config_bool"),
        # bool True is also rejected (symmetry with False)
        pytest.param(True, "isn't a valid int or list", id="config_bool_true"),
        # list element that is a bool (bool subclasses int but must be excluded)
        pytest.param([True, 1], "isn't an integer", id="config_list_bool_element"),
        # empty string
        pytest.param("", "isn't a valid int or list", id="env_empty_string"),
    ],
)
def test_int_or_list_or_eval_invalid(value, error_fragment):
    """int_or_list_or_eval raises ValueError for non-int types, bools, and malformed strings."""
    with pytest.raises(ValueError, match=error_fragment):
        int_or_list_or_eval(value)


# ---------------------------------------------------------------------------
# str_or_list_or_eval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        # config path: YAML-native types
        pytest.param(None, None, id="config_none"),
        pytest.param("eu-west-1", ["eu-west-1"], id="single_str"),
        pytest.param(["eu-west-1", "us-east-1"], ["eu-west-1", "us-east-1"], id="config_list_strs"),
        # env path: raw strings from SCT_* environment variables
        pytest.param("['eu-west-1', 'us-east-1']", ["eu-west-1", "us-east-1"], id="env_py_list"),
        # a plain string with spaces is treated as a single string (not split)
        pytest.param(
            "modify_table and not disruptive", ["modify_table and not disruptive"], id="env_space_str_kept_as_one"
        ),
        # empty inputs
        pytest.param("", [], id="env_empty_string"),
        pytest.param([], [], id="config_empty_list"),
        # single-element list is NOT unwrapped (unlike bool)
        pytest.param(["only"], ["only"], id="config_single_element_list_kept"),
        # JSON double-quoted list literal (ast.literal_eval handles it)
        pytest.param('["eu-west-1", "us-east-1"]', ["eu-west-1", "us-east-1"], id="env_json_list"),
    ],
)
def test_str_or_list_or_eval_valid(value, expected):
    """str_or_list_or_eval accepts plain strings, string lists, and list literals from env vars."""
    assert str_or_list_or_eval(value) == expected


@pytest.mark.parametrize(
    "value,error_fragment",
    [
        pytest.param(1, "isn't a valid string or list", id="non_string_input"),
        pytest.param("['eu-west-1', 'us-east-1', 1]", "isn't a string", id="env_element_not_str"),
        pytest.param("{'key': 'val'}", "parsed to", id="env_parses_to_dict"),
        pytest.param("42", "parsed to", id="env_parses_to_int"),
        # all-int list string — was valid in old validator, now rejected
        pytest.param("[1, 2]", "isn't a string", id="env_all_int_list"),
        # config path: direct list with non-string elements
        pytest.param([True, "a"], "isn't a string", id="config_list_bool_element"),
        pytest.param(["a", 1], "isn't a string", id="config_list_int_element"),
    ],
)
def test_str_or_list_or_eval_invalid(value, error_fragment):
    """str_or_list_or_eval raises ValueError for non-string inputs and lists with non-string elements."""
    with pytest.raises(ValueError, match=error_fragment):
        str_or_list_or_eval(value)


# ---------------------------------------------------------------------------
# bool_or_list_or_eval
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        # config path: YAML-native types
        pytest.param(None, None, id="config_none"),
        pytest.param(True, [True], id="config_true"),
        pytest.param(False, [False], id="config_false"),
        pytest.param([True, False], [True, False], id="config_list_bools"),
        pytest.param([True], [True], id="config_single_item_list_unwrapped"),
        pytest.param([False], [False], id="config_single_false_unwrapped"),
        pytest.param(["true", "false"], [True, False], id="config_list_str_bools"),  # YAML quoted
        pytest.param([True, "0"], [True, False], id="config_mixed_list"),
        # env path: raw strings from SCT_* environment variables
        pytest.param("true", [True], id="env_true"),
        pytest.param("false", [False], id="env_false"),
        pytest.param("1", [True], id="env_one"),
        pytest.param("0", [False], id="env_zero"),
        pytest.param("yes", [True], id="env_yes"),
        pytest.param("no", [False], id="env_no"),
        pytest.param("[true, false]", [True, False], id="env_list_literal_lower"),
        pytest.param("[True, False]", [True, False], id="env_list_literal_python"),
        pytest.param("[true]", [True], id="env_list_literal_single_unwrapped"),
        # Python-capitalized strings (ast.literal_eval path)
        pytest.param("True", [True], id="env_capitalized_true"),
        pytest.param("False", [False], id="env_capitalized_false"),
        # strtobool aliases
        pytest.param("on", [True], id="env_on"),
        pytest.param("off", [False], id="env_off"),
        pytest.param("ON", [True], id="env_on_upper"),
        pytest.param("YES", [True], id="env_yes_upper"),
        pytest.param("NO", [False], id="env_no_upper"),
    ],
)
def test_bool_or_list_or_eval_valid(value, expected):
    """bool_or_list_or_eval accepts booleans, bool lists, and all recognised truthy/falsy string aliases."""
    assert bool_or_list_or_eval(value) == expected


@pytest.mark.parametrize(
    "value,error_fragment",
    [
        # space-separated strings are no longer accepted; strtobool fails on them
        pytest.param("true false", "cannot be converted to bool", id="env_space_sep_bools"),
        pytest.param("1 0", "cannot be converted to bool", id="env_space_sep_int_bools"),
        pytest.param("  true false  ", "cannot be converted to bool", id="env_space_sep_padded"),
        pytest.param("yes no", "cannot be converted to bool", id="env_space_sep_yes_no"),
        # unrecognized boolean string
        pytest.param("maybe", "cannot be converted", id="env_non_bool_string"),
        # list with unrecognized element
        pytest.param(["yes", "maybe"], "isn't a list of booleans", id="config_list_invalid_element"),
        # list with mixed types (both python and yaml style bools)
        pytest.param("[True, false]", "cannot be converted to bool", id="env_list_literal_mixed_types"),
        # int does not pass trough
        pytest.param(1, "isn't a valid bool or list", id="config_int"),
        # empty string is unrecognized by strtobool
        pytest.param("", "cannot be converted to bool", id="env_empty_string"),
        # list containing None cannot be coerced
        pytest.param([True, None], "isn't a list of booleans", id="config_list_with_none"),
    ],
)
def test_bool_or_list_or_eval_invalid(value, error_fragment):
    """bool_or_list_or_eval raises ValueError for unrecognised strings, bare ints, and mixed-type lists."""
    with pytest.raises(ValueError, match=error_fragment):
        bool_or_list_or_eval(value)


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


# ---------------------------------------------------------------------------
# SCTConfiguration._as_list  (stress-cmd loop normalisation helper)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, []),
        ([], []),
        (["cassandra-stress write"], ["cassandra-stress write"]),
        (["cmd1", "cmd2"], ["cmd1", "cmd2"]),
        # scalar string (e.g. gemini_cmd) must become a one-element list
        ("gemini --duration 10m", ["gemini --duration 10m"]),
    ],
)
def test_as_list(input_val, expected):
    assert SCTConfiguration._as_list(input_val) == expected


def test_list_of_stress_tools_with_scalar_gemini_cmd(monkeypatch):
    """list_of_stress_tools must not iterate gemini_cmd char-by-char."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_GEMINI_CMD", "gemini --duration 10m")

    conf = SCTConfiguration()
    tools = conf.list_of_stress_tools
    # "gemini" (the binary name) should appear — not individual characters
    assert "gemini" in tools
    assert "g" not in tools
    assert "e" not in tools


def test_list_of_stress_tools_with_list_stress_cmd(monkeypatch):
    """list_of_stress_tools works correctly with list[str] stress_cmd values."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-stress write n=1000000")

    conf = SCTConfiguration()
    tools = conf.list_of_stress_tools
    assert "cassandra-stress" in tools

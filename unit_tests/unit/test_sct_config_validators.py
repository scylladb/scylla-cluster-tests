"""Tests for sct_config type-coercion validators."""

import pytest
from pydantic import ValidationError

from sdcm.sct_config import (
    SCTConfiguration,
    dict_or_str,
    int_or_list_or_eval,
    str_or_list_or_eval,
)
from sdcm.sct_config.types import strtobool

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
# dict_or_str
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        pytest.param(None, None, id="none"),
        pytest.param({"key": "val"}, {"key": "val"}, id="dict"),
        pytest.param("{'a': 1}", {"a": 1}, id="python-dict-str"),
        pytest.param('{"b": 2}', {"b": 2}, id="json-dict-str"),
        pytest.param("key: value", {"key": "value"}, id="yaml-str"),
    ],
)
def test_dict_or_str_valid(input_val, expected):
    """dict_or_str returns dicts as-is and parses dict-like strings (Python, JSON, YAML)."""
    assert dict_or_str(input_val) == expected


@pytest.mark.parametrize(
    "input_val",
    [
        pytest.param("[1, 2]", id="list-str"),
        pytest.param("3", id="int-str"),
        pytest.param("plain string", id="plain-str"),
    ],
)
def test_dict_or_str_invalid(input_val):
    """dict_or_str raises ValueError when the string does not parse to a dict."""
    with pytest.raises(ValueError):
        dict_or_str(input_val)


# ---------------------------------------------------------------------------
# SCTConfiguration._as_list  (stress-cmd loop normalisation helper)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        pytest.param(None, [], id="none"),
        pytest.param([], [], id="empty-list"),
        pytest.param(["cassandra-stress write"], ["cassandra-stress write"], id="single-cmd-list"),
        pytest.param(["cmd1", "cmd2"], ["cmd1", "cmd2"], id="two-cmd-list"),
        # scalar string (e.g. gemini_cmd) must become a one-element list
        pytest.param("gemini --duration 10m", ["gemini --duration 10m"], id="scalar-str-wrapped"),
    ],
)
def test_as_list(input_val, expected):
    """_as_list normalises None, scalar strings and lists into a list of commands."""
    assert SCTConfiguration._as_list(input_val) == expected


def test_list_of_stress_tools_with_scalar_gemini_cmd(monkeypatch):
    """list_of_stress_tools must not iterate gemini_cmd char-by-char."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
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
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_CMD", "cassandra-stress write n=1000000")

    conf = SCTConfiguration()
    tools = conf.list_of_stress_tools
    assert "cassandra-stress" in tools


# ---------------------------------------------------------------------------
# effective_compression_ratio validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(1.0, id="upper-bound"),
        pytest.param(0.68, id="typical"),
        pytest.param(0.5, id="half"),
        pytest.param(0.01, id="near-zero"),
    ],
)
def test_effective_compression_ratio_valid(monkeypatch, value):
    """effective_compression_ratio accepts values in (0, 1.0]."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_EFFECTIVE_COMPRESSION_RATIO", str(value))

    conf = SCTConfiguration()
    assert conf.effective_compression_ratio == pytest.approx(value)


@pytest.mark.parametrize(
    "value, expected_match",
    [
        pytest.param(0, "greater than 0", id="zero"),
        pytest.param(-0.5, "greater than 0", id="negative"),
        pytest.param(1.1, "less than or equal to 1", id="just-above-one"),
        pytest.param(2.0, "less than or equal to 1", id="two"),
    ],
)
def test_effective_compression_ratio_invalid(monkeypatch, value, expected_match):
    """effective_compression_ratio rejects 0, negative values, and values above 1.0."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_EFFECTIVE_COMPRESSION_RATIO", str(value))

    with pytest.raises(ValidationError, match=expected_match):
        SCTConfiguration()


def test_stress_template_context_accepts_dict(monkeypatch):
    """stress_template_context keeps a dict from SCT_STRESS_TEMPLATE_CONTEXT as-is."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", '{"rows_total": "{{ effective_disk_size_bytes }}"}')

    conf = SCTConfiguration()
    assert conf.stress_template_context == {"rows_total": "{{ effective_disk_size_bytes }}"}


def test_stress_template_context_accepts_yaml_string(monkeypatch):
    """stress_template_context parses a YAML string from SCT_STRESS_TEMPLATE_CONTEXT into a dict."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", "rows_total: '{{ effective_disk_size_bytes }}'")

    conf = SCTConfiguration()
    assert conf.stress_template_context == {"rows_total": "{{ effective_disk_size_bytes }}"}


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("y", id="y"),
        pytest.param("Yes", id="Yes"),
        pytest.param("T", id="T"),
        pytest.param("true", id="true"),
        pytest.param("ON", id="ON"),
        pytest.param("1", id="one"),
        pytest.param(" true ", id="padded-true"),
    ],
)
def test_strtobool_accepts_truthy_spellings(value):
    """Truthy spellings, in any case or padding, make strtobool return True."""
    assert strtobool(value) is True


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("n", id="n"),
        pytest.param("No", id="No"),
        pytest.param("F", id="F"),
        pytest.param("false", id="false"),
        pytest.param("OFF", id="OFF"),
        pytest.param("0", id="zero"),
        pytest.param(" false ", id="padded-false"),
    ],
)
def test_strtobool_accepts_falsy_spellings(value):
    """Falsy spellings, in any case or padding, make strtobool return False."""
    assert strtobool(value) is False


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("maybe", id="word"),
        pytest.param("", id="empty"),
        pytest.param("2", id="out-of-range-int"),
        pytest.param("truthy", id="truthy-prefix"),
    ],
)
def test_strtobool_rejects_anything_else(value):
    """Same contract as the distutils version it replaces -- ValueError, not a silent False."""
    with pytest.raises(ValueError, match="invalid truth value"):
        strtobool(value)

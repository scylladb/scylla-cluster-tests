"""Tests for sct_config type-coercion validators."""

import pytest

from sdcm.sct_config import (
    SCTConfiguration,
    boolean_or_space_separated_booleans,
    dict_or_str,
    dict_or_str_or_int,
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


# ---------------------------------------------------------------------------
# dict_or_str_or_int
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "input_val,expected",
    [
        (None, None),
        (8, 8),
        (0, 0),
        ("8", 8),
        ({"read": 8, "write": 4}, {"read": 8, "write": 4}),
        ("{'read': 8, 'write': 4}", {"read": 8, "write": 4}),
    ],
)
def test_dict_or_str_or_int_valid(input_val, expected):
    assert dict_or_str_or_int(input_val) == expected


@pytest.mark.parametrize(
    "input_val",
    [
        "plain string",
        "[1, 2]",
        1.5,
        True,  # bool must not be accepted as an int
        "True",
    ],
)
def test_dict_or_str_or_int_invalid(input_val):
    with pytest.raises(ValueError):
        dict_or_str_or_int(input_val)


@pytest.mark.parametrize(
    "env_value,expected",
    [
        (None, None),  # no default (like other perf_gradual_* params); value comes from the load-steps fragment
        ("16", 16),
        (
            "{'read': 8, 'write': 16, 'mixed': 32, 'read_disk_only': 8}",
            {"read": 8, "write": 16, "mixed": 32, "read_disk_only": 8},
        ),
    ],
)
def test_perf_gradual_connections_per_host_config(monkeypatch, env_value, expected):
    """perf_gradual_connections_per_host resolves as int or per-workload dict (no default when unset)."""
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2025.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    if env_value is not None:
        monkeypatch.setenv("SCT_PERF_GRADUAL_CONNECTIONS_PER_HOST", env_value)

    conf = SCTConfiguration()
    assert conf["perf_gradual_connections_per_host"] == expected


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

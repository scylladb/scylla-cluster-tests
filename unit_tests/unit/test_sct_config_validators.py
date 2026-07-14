# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2026 ScyllaDB

"""Contract tests for the input converters in `sdcm.sct_config.types`."""

import pytest

from pydantic import ValidationError
from sdcm.sct_config import SCTConfiguration
from sdcm.sct_config.types import strtobool


@pytest.mark.parametrize("value", ["y", "Yes", "T", "true", "ON", "1", " true "])
def test_strtobool_accepts_truthy_spellings(value):
    assert strtobool(value) is True


@pytest.mark.parametrize("value", ["n", "No", "F", "false", "OFF", "0", " false "])
def test_strtobool_accepts_falsy_spellings(value):
    assert strtobool(value) is False


@pytest.mark.parametrize("value", ["maybe", "", "2", "truthy"])
def test_strtobool_rejects_anything_else(value):
    """Same contract as the distutils version it replaces -- ValueError, not a silent False."""
    with pytest.raises(ValueError, match="invalid truth value"):
        strtobool(value)


# ---------------------------------------------------------------------------
# effective_compression_ratio validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [1.0, 0.68, 0.5, 0.01])
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
        (0, "greater than 0"),
        (-0.5, "greater than 0"),
        (1.1, "less than or equal to 1"),
        (2.0, "less than or equal to 1"),
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
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", '{"rows_total": "{{ effective_disk_size_bytes }}"}')

    conf = SCTConfiguration()
    assert conf.stress_template_context == {"rows_total": "{{ effective_disk_size_bytes }}"}


def test_stress_template_context_accepts_yaml_string(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", "rows_total: '{{ effective_disk_size_bytes }}'")

    conf = SCTConfiguration()
    assert conf.stress_template_context == {"rows_total": "{{ effective_disk_size_bytes }}"}

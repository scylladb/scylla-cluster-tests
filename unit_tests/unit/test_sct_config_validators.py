"""Tests for sct_config validation of stress templating options."""

import pytest

from sdcm.sct_config import SCTConfiguration


@pytest.fixture(autouse=True)
def fixture_env(monkeypatch):
    monkeypatch.setenv("SCT_CLUSTER_BACKEND", "docker")
    monkeypatch.setenv("SCT_USE_MGMT", "false")
    monkeypatch.setenv("SCT_SCYLLA_VERSION", "2026.1.0")
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")


# ---------------------------------------------------------------------------
# effective_compression_ratio validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", [1.0, 0.68, 0.5, 0.01])
def test_effective_compression_ratio_valid(monkeypatch, value):
    """effective_compression_ratio accepts values in (0, 1.0]."""
    monkeypatch.setenv("SCT_EFFECTIVE_COMPRESSION_RATIO", str(value))

    conf = SCTConfiguration()
    conf.verify_configuration()
    assert conf.get("effective_compression_ratio") == pytest.approx(value)


@pytest.mark.parametrize("value", [0, -0.5, 1.1, 2.0])
def test_effective_compression_ratio_invalid(monkeypatch, value):
    """effective_compression_ratio rejects 0, negative values, and values above 1.0."""
    monkeypatch.setenv("SCT_EFFECTIVE_COMPRESSION_RATIO", str(value))

    conf = SCTConfiguration()
    with pytest.raises(ValueError, match=r"effective_compression_ratio must be in range \(0, 1.0\]"):
        conf.verify_configuration()


# ---------------------------------------------------------------------------
# stress_template_context parsing
# ---------------------------------------------------------------------------


def test_stress_template_context_accepts_dict(monkeypatch):
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", '{"rows_total": "{{ effective_disk_size_bytes }}"}')

    conf = SCTConfiguration()
    assert conf.get("stress_template_context") == {"rows_total": "{{ effective_disk_size_bytes }}"}


def test_stress_template_context_accepts_yaml_string(monkeypatch):
    monkeypatch.setenv("SCT_STRESS_TEMPLATE_CONTEXT", "rows_total: '{{ effective_disk_size_bytes }}'")

    conf = SCTConfiguration()
    assert conf.get("stress_template_context") == {"rows_total": "{{ effective_disk_size_bytes }}"}

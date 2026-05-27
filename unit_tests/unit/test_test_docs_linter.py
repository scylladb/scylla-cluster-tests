import yaml

from sdcm.utils.lint.docs_linter import LintResult, lint_test_metadata, cross_reference_config


def write_yaml(tmp_path, name, data):
    p = tmp_path / name
    p.write_text(yaml.dump(data))
    return p


def test_lint_result_passed_no_errors():
    r = LintResult(file_path="x.yaml")
    assert r.passed is True


def test_lint_result_failed_with_errors():
    r = LintResult(file_path="x.yaml", errors=["TD-001: missing"])
    assert r.passed is False


def test_td001_missing_test_metadata(tmp_path):
    p = write_yaml(tmp_path, "test.yaml", {"test_duration": 60})
    result = lint_test_metadata(p)
    assert not result.passed
    assert len(result.errors) == 1
    assert "TD-001" in result.errors[0]


def test_td001_null_test_metadata(tmp_path):
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": None})
    result = lint_test_metadata(p)
    assert not result.passed
    assert len(result.errors) == 1
    assert "TD-001" in result.errors[0]


def test_td002_missing_description(tmp_path):
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": {"test_type": "longevity"}})
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-002" in e for e in result.errors)


def test_td002_short_description(tmp_path):
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": {"description": "Too short"}})
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-002" in e for e in result.errors)


def test_td002_valid_description(tmp_path):
    p = write_yaml(
        tmp_path, "test.yaml", {"test_metadata": {"description": "A valid description that is long enough to pass."}}
    )
    result = lint_test_metadata(p)
    assert not any("TD-002" in e for e in result.errors)


def test_td003_invalid_tier(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "tier": "gold",
            }
        },
    )
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-003" in e for e in result.errors)


def test_td003_invalid_backend(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "supported_backends": ["not-a-backend"],
            }
        },
    )
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-003" in e for e in result.errors)


def test_td008_duration_class_mismatch(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_duration": 60,
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "duration_class": "long",
            },
        },
    )
    result = lint_test_metadata(p)
    assert any("TD-008" in w for w in result.warnings)


def test_td008_duration_class_correct(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_duration": 60,
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "duration_class": "short",
            },
        },
    )
    result = lint_test_metadata(p)
    assert not any("TD-008" in w for w in result.warnings)


def test_td010_sanity_with_long_duration(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "tier": "sanity",
                "duration_class": "long",
            }
        },
    )
    result = lint_test_metadata(p)
    assert any("TD-010" in w for w in result.warnings)


def test_td011_backend_not_in_supported(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "cluster_backend": "gce",
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "supported_backends": ["aws"],
            },
        },
    )
    result = lint_test_metadata(p)
    assert any("TD-011" in w for w in result.warnings)


def test_valid_full_metadata_passes(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "test_duration": 255,
            "nemesis_class_name": "SisyphusMonkey",
            "stress_cmd": "cassandra-stress write cl=QUORUM duration=180m",
            "test_metadata": {
                "description": "Basic longevity test running cassandra-stress write workload for 4 hours.",
                "test_type": "longevity",
                "tier": "tier1",
                "duration_class": "short",
                "supported_backends": ["aws", "gce"],
                "stress_tools": ["cassandra-stress"],
                "workload": "write",
                "features": [],
            },
        },
    )
    result = lint_test_metadata(p)
    assert result.passed
    assert result.errors == []


def test_cross_reference_td006_stress_tool_missing(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "stress_cmd": "cassandra-stress write cl=QUORUM duration=60m",
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "stress_tools": [],
            },
        },
    )
    result = cross_reference_config(p)
    assert any("TD-006" in w for w in result.warnings)


def test_cross_reference_td009_multidc_missing_feature(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "n_db_nodes": "6 6 6",
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "features": [],
            },
        },
    )
    result = cross_reference_config(p)
    assert any("TD-009" in w for w in result.warnings)


def test_cross_reference_td009_tls_missing_feature(tmp_path):
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "server_encrypt": True,
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "features": [],
            },
        },
    )
    result = cross_reference_config(p)
    assert any("TD-009" in w for w in result.warnings)


def test_cross_reference_returns_lint_result(tmp_path):
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": None})
    result = cross_reference_config(p)
    assert isinstance(result, LintResult)


def test_invalid_yaml_returns_error(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text("key: [unclosed")
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-000" in e for e in result.errors)

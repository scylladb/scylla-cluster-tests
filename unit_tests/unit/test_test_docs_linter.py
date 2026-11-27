import yaml

from sdcm.utils.lint.docs_linter import LintResult, cross_reference_config, lint_test_metadata


def write_yaml(tmp_path, name, data):
    """Dump data as YAML into a file under tmp_path and return its path."""
    p = tmp_path / name
    p.write_text(yaml.dump(data))
    return p


def test_lint_result_passed_no_errors():
    """A LintResult with no errors reports passed."""
    r = LintResult(file_path="x.yaml")
    assert r.passed is True


def test_lint_result_failed_with_errors():
    """A LintResult with an error reports failed."""
    r = LintResult(file_path="x.yaml", errors=["TD-001: missing"])
    assert r.passed is False


def test_td001_missing_test_metadata(tmp_path):
    """Reports a single TD-001 error when the test-case has no test_metadata section."""
    p = write_yaml(tmp_path, "test.yaml", {"test_duration": 60})
    result = lint_test_metadata(p)
    assert not result.passed
    assert len(result.errors) == 1
    assert "TD-001" in result.errors[0]


def test_td001_null_test_metadata(tmp_path):
    """Reports a single TD-001 error when test_metadata is null."""
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": None})
    result = lint_test_metadata(p)
    assert not result.passed
    assert len(result.errors) == 1
    assert "TD-001" in result.errors[0]


def test_td002_missing_description(tmp_path):
    """Reports TD-002 when test_metadata has no description."""
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": {"test_type": "longevity"}})
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-002" in e for e in result.errors)


def test_td002_short_description(tmp_path):
    """Reports TD-002 when the description is too short."""
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": {"description": "Too short"}})
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-002" in e for e in result.errors)


def test_td002_valid_description(tmp_path):
    """Accepts a long enough description with no TD-002 error."""
    p = write_yaml(
        tmp_path, "test.yaml", {"test_metadata": {"description": "A valid description that is long enough to pass."}}
    )
    result = lint_test_metadata(p)
    assert not any("TD-002" in e for e in result.errors)


def test_td003_invalid_tier(tmp_path):
    """Reports TD-003 for a tier outside the taxonomy."""
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
    """Reports TD-003 for an unknown supported backend."""
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
    """Warns TD-008 when a 60-minute test_duration is classed as long."""
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
    """Accepts a 60-minute test_duration classed as short with no TD-008 warning."""
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
    """Warns TD-010 when a sanity-tier test has a long duration class."""
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
    """Warns TD-011 when cluster_backend is missing from supported_backends."""
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
    """Passes a complete, consistent test_metadata section with no errors."""
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
    """Warns TD-006 when stress_cmd uses a tool missing from stress_tools."""
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
    """Warns TD-009 when a multi-DC config lacks the matching feature label."""
    p = write_yaml(
        tmp_path,
        "test.yaml",
        {
            "n_db_nodes": [6, 6, 6],
            "test_metadata": {
                "description": "A valid description that is long enough to pass.",
                "features": [],
            },
        },
    )
    result = cross_reference_config(p)
    assert any("TD-009" in w for w in result.warnings)


def test_cross_reference_td009_tls_missing_feature(tmp_path):
    """Warns TD-009 when server_encrypt is on but the TLS feature label is missing."""
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
    """cross_reference_config() returns a LintResult even for null test_metadata."""
    p = write_yaml(tmp_path, "test.yaml", {"test_metadata": None})
    result = cross_reference_config(p)
    assert isinstance(result, LintResult)


def test_invalid_yaml_returns_error(tmp_path):
    """Reports a TD-000 error for a file that is not valid YAML."""
    p = tmp_path / "bad.yaml"
    p.write_text("key: [unclosed")
    result = lint_test_metadata(p)
    assert not result.passed
    assert any("TD-000" in e for e in result.errors)

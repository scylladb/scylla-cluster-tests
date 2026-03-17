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

import logging
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from sdcm.utils.trigger_matrix import (
    JobConfig,
    MatrixConfig,
    MatrixValidationError,
    TriggerMatrixError,
    build_job_parameters,
    determine_job_folder,
    filter_jobs,
    load_matrix_config,
    resolve_job_path,
    trigger_matrix,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_matrix_yaml(tmp_path):
    """Create a sample YAML matrix file and return its path."""
    data = {
        "defaults": {
            "provision_type": "spot",
            "post_behavior_db_nodes": "destroy",
        },
        "cron_triggers": [
            {"schedule": "00 06 * * 6", "params": {"scylla_version": "master:latest"}},
        ],
        "jobs": [
            {
                "job_name": "tier1/longevity-50gb-3days-test",
                "backend": "aws",
                "region": "eu-west-1",
                "labels": ["weekly"],
                "exclude_versions": [],
                "params": {"stress_duration": "4320"},
            },
            {
                "job_name": "tier1/longevity-1tb-5days-azure-test",
                "backend": "azure",
                "region": "eastus",
                "labels": ["weekly"],
                "exclude_versions": ["2024.1"],
            },
            {
                "job_name": "longevity/longevity-10gb-3h-gce-test",
                "backend": "gce",
                "region": "us-east1",
                "labels": [],
                "exclude_versions": [],
            },
            {
                "job_name": "/scylla-enterprise/perf-regression/perf-test",
                "backend": "aws",
                "region": "us-east-1",
                "labels": ["master-weekly", "additional"],
                "exclude_versions": ["master"],
                "params": {"sub_tests": '["test_read"]'},
            },
        ],
    }
    path = tmp_path / "test-matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


@pytest.fixture()
def sample_jobs():
    """Return a list of sample JobConfig objects for filter tests."""
    return [
        JobConfig(job_name="job-a", backend="aws", region="eu-west-1", labels=["weekly"], exclude_versions=[]),
        JobConfig(
            job_name="job-b",
            backend="azure",
            region="eastus",
            labels=["weekly", "additional"],
            exclude_versions=["2024.1"],
        ),
        JobConfig(job_name="job-c", backend="gce", region="us-east1", labels=[], exclude_versions=[]),
        JobConfig(
            job_name="job-d", backend="aws", region="us-east-1", labels=["master-weekly"], exclude_versions=["master"]
        ),
    ]


# ---------------------------------------------------------------------------
# Phase 1: load_matrix_config tests
# ---------------------------------------------------------------------------


class TestLoadMatrixConfig:
    def test_load_valid_yaml(self, sample_matrix_yaml):
        config = load_matrix_config(sample_matrix_yaml)

        assert isinstance(config, MatrixConfig)
        assert len(config.jobs) == 4
        assert config.jobs[0].job_name == "tier1/longevity-50gb-3days-test"
        assert config.jobs[0].backend == "aws"
        assert config.jobs[0].region == "eu-west-1"
        assert config.jobs[0].labels == ["weekly"]
        assert config.jobs[0].params == {"stress_duration": "4320"}

    def test_load_defaults(self, sample_matrix_yaml):
        config = load_matrix_config(sample_matrix_yaml)
        assert config.defaults["provision_type"] == "spot"
        assert config.defaults["post_behavior_db_nodes"] == "destroy"

    def test_load_cron_triggers(self, sample_matrix_yaml):
        config = load_matrix_config(sample_matrix_yaml)
        assert len(config.cron_triggers) == 1
        assert config.cron_triggers[0].schedule == "00 06 * * 6"
        assert config.cron_triggers[0].params["scylla_version"] == "master:latest"

    def test_load_file_not_found(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_matrix_config(tmp_path / "nonexistent.yaml")

    def test_load_invalid_not_mapping(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("- just a list")
        with pytest.raises(MatrixValidationError, match="YAML mapping"):
            load_matrix_config(path)

    def test_load_missing_jobs_key(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({"defaults": {}}))
        with pytest.raises(MatrixValidationError, match="'jobs' key"):
            load_matrix_config(path)

    def test_load_jobs_not_list(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({"jobs": "not-a-list"}))
        with pytest.raises(MatrixValidationError, match="'jobs' must be a list"):
            load_matrix_config(path)

    def test_load_job_missing_name(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({"jobs": [{"backend": "aws"}]}))
        with pytest.raises(MatrixValidationError, match="missing required field 'job_name'"):
            load_matrix_config(path)

    def test_load_job_missing_backend(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text(yaml.dump({"jobs": [{"job_name": "test"}]}))
        with pytest.raises(MatrixValidationError, match="missing required field 'backend'"):
            load_matrix_config(path)

    def test_load_empty_jobs_list(self, tmp_path):
        path = tmp_path / "empty.yaml"
        path.write_text(yaml.dump({"jobs": []}))
        config = load_matrix_config(path)
        assert config.jobs == []

    def test_load_minimal_job(self, tmp_path):
        path = tmp_path / "minimal.yaml"
        path.write_text(yaml.dump({"jobs": [{"job_name": "test", "backend": "aws"}]}))
        config = load_matrix_config(path)
        assert config.jobs[0].region == ""
        assert config.jobs[0].labels == []
        assert config.jobs[0].exclude_versions == []
        assert config.jobs[0].params == {}


# ---------------------------------------------------------------------------
# Phase 2: determine_job_folder tests
# ---------------------------------------------------------------------------


class TestDetermineJobFolder:
    @pytest.mark.parametrize(
        "version,expected",
        [
            pytest.param("master:latest", "scylla-master", id="master_latest"),
            pytest.param("master:all", "scylla-master", id="master_all"),
            pytest.param("master", "scylla-master", id="bare_master"),
            pytest.param("2025.4", "branch-2025.4", id="two_part_version"),
            pytest.param("2025.4.0", "branch-2025.4", id="three_part_version"),
            pytest.param("5.2.1", "branch-5.2", id="oss_version"),
            pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", "branch-2024.2", id="full_tag"),
            pytest.param("2025.1.3-0.20260101.abcdef1234-1", "branch-2025.1", id="full_tag_2025"),
        ],
    )
    def test_version_to_folder(self, version, expected):
        assert determine_job_folder(version) == expected

    def test_explicit_override(self):
        assert determine_job_folder("master:latest", job_folder="my-folder") == "my-folder"

    def test_branch_qualifier(self):
        assert determine_job_folder("branch-2019.1:all") == "branch-branch-2019.1"

    def test_invalid_version_raises(self):
        with pytest.raises(TriggerMatrixError, match="Cannot determine job folder"):
            determine_job_folder("not-a-version")


# ---------------------------------------------------------------------------
# Phase 2: filter_jobs tests
# ---------------------------------------------------------------------------


class TestFilterJobs:
    def test_no_filters_returns_all(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="master:latest")
        assert len(result) == 4

    def test_filter_by_labels_single(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly")
        assert len(result) == 2
        assert all("weekly" in j.labels for j in result)

    def test_filter_by_labels_multiple_and_logic(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly,additional")
        assert len(result) == 1
        assert result[0].job_name == "job-b"

    def test_filter_by_backend(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="2025.4", backend="aws")
        assert len(result) == 2
        assert all(j.backend == "aws" for j in result)

    def test_filter_by_version_exclusion(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="2024.1.5")
        # job-b excludes "2024.1" prefix → should be filtered out
        assert len(result) == 3
        assert all(j.job_name != "job-b" for j in result)

    def test_filter_by_version_exclusion_master(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="master:latest")
        # job-d excludes "master" prefix → should be filtered out
        assert len(result) == 3
        assert all(j.job_name != "job-d" for j in result)

    def test_filter_skip_list(self, sample_jobs):
        result = filter_jobs(sample_jobs, scylla_version="2025.4", skip_jobs=["job-a", "job-c"])
        assert len(result) == 2
        names = {j.job_name for j in result}
        assert names == {"job-b", "job-d"}

    def test_no_selector_returns_all_eligible(self, sample_jobs):
        """When no labels_selector, all jobs are eligible (label filter not applied)."""
        result = filter_jobs(sample_jobs, scylla_version="2025.4")
        assert len(result) == 4

    def test_combined_filters(self, sample_jobs):
        result = filter_jobs(
            sample_jobs,
            scylla_version="2025.4",
            labels_selector="weekly",
            backend="aws",
        )
        assert len(result) == 1
        assert result[0].job_name == "job-a"


# ---------------------------------------------------------------------------
# Phase 2: build_job_parameters tests
# ---------------------------------------------------------------------------


class TestBuildJobParameters:
    def test_defaults_applied(self):
        job = JobConfig(job_name="test", backend="aws", region="eu-west-1")
        defaults = {"provision_type": "spot", "post_behavior_db_nodes": "destroy"}
        params = build_job_parameters(job, defaults, "master:latest", {})

        assert params["provision_type"] == "spot"
        assert params["scylla_version"] == "master:latest"
        assert params["region"] == "eu-west-1"

    def test_job_params_override_defaults(self):
        job = JobConfig(job_name="test", backend="aws", region="eu-west-1", params={"provision_type": "on_demand"})
        defaults = {"provision_type": "spot"}
        params = build_job_parameters(job, defaults, "master:latest", {})

        assert params["provision_type"] == "on_demand"

    def test_cli_overrides_take_precedence(self):
        job = JobConfig(job_name="test", backend="aws", region="eu-west-1", params={"provision_type": "on_demand"})
        defaults = {"provision_type": "spot"}
        overrides = {"provision_type": "spot_fleet", "stress_duration": "120"}
        params = build_job_parameters(job, defaults, "2025.4", overrides)

        assert params["provision_type"] == "spot_fleet"
        assert params["stress_duration"] == "120"
        assert params["scylla_version"] == "2025.4"

    def test_version_always_included(self):
        job = JobConfig(job_name="test", backend="aws", region="")
        params = build_job_parameters(job, {}, "2024.2.5-0.20250221.cb9e2a54ae6d-1", {})
        assert params["scylla_version"] == "2024.2.5-0.20250221.cb9e2a54ae6d-1"

    def test_none_overrides_ignored(self):
        job = JobConfig(job_name="test", backend="aws", region="eu-west-1")
        defaults = {"provision_type": "spot"}
        overrides = {"provision_type": None, "region": None}
        params = build_job_parameters(job, defaults, "master:latest", overrides)
        assert params["provision_type"] == "spot"


# ---------------------------------------------------------------------------
# Phase 2: resolve_job_path tests
# ---------------------------------------------------------------------------


class TestResolveJobPath:
    def test_relative_path(self):
        assert resolve_job_path("tier1/longevity-test", "scylla-master") == "scylla-master/tier1/longevity-test"

    def test_absolute_path(self):
        assert resolve_job_path("/scylla-enterprise/perf/test", "scylla-master") == "scylla-enterprise/perf/test"

    def test_absolute_path_strips_leading_slash(self):
        result = resolve_job_path("/enterprise/perf/test", "branch-2025.4")
        assert not result.startswith("/")
        assert result == "enterprise/perf/test"


# ---------------------------------------------------------------------------
# Phase 2: trigger_matrix end-to-end tests
# ---------------------------------------------------------------------------


class TestTriggerMatrix:
    def test_dry_run_produces_output(self, sample_matrix_yaml, caplog):
        """Dry run triggers no API calls but logs all jobs."""
        with caplog.at_level(logging.INFO):
            results = trigger_matrix(
                matrix_file=str(sample_matrix_yaml),
                scylla_version="2025.4",
                dry_run=True,
            )

        assert len(results["triggered"]) == 3  # job with exclude_versions=["master"] still included
        assert len(results["failed"]) == 0
        assert any("[DRY-RUN]" in record.message for record in caplog.records)

    def test_dry_run_with_label_filter(self, sample_matrix_yaml):
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            labels_selector="weekly",
            dry_run=True,
        )
        assert len(results["triggered"]) == 2

    def test_dry_run_with_backend_filter(self, sample_matrix_yaml):
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            backend="aws",
            dry_run=True,
        )
        assert len(results["triggered"]) == 2

    def test_dry_run_version_exclusion(self, sample_matrix_yaml):
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="master:latest",
            dry_run=True,
        )
        # The enterprise perf job excludes "master" → 3 jobs triggered
        assert len(results["triggered"]) == 3

    @patch("sdcm.utils.trigger_matrix.trigger_jenkins_job")
    def test_full_flow_with_mocked_api(self, mock_trigger, sample_matrix_yaml):
        mock_trigger.return_value = True
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
        )
        assert mock_trigger.call_count == 3  # 4 jobs minus 1 excluded by "master"
        assert len(results["triggered"]) == 3

    @patch("sdcm.utils.trigger_matrix.trigger_jenkins_job")
    def test_failed_jobs_reported(self, mock_trigger, sample_matrix_yaml):
        mock_trigger.return_value = False
        results = trigger_matrix(
            matrix_file=str(sample_matrix_yaml),
            scylla_version="2025.4",
            labels_selector="weekly",
        )
        assert len(results["failed"]) == 2
        assert len(results["triggered"]) == 0


# ---------------------------------------------------------------------------
# YAML matrix file validation tests
# ---------------------------------------------------------------------------


class TestYamlMatrixFiles:
    """Validate that the actual YAML matrix files in configurations/triggers/ load correctly."""

    TRIGGERS_DIR = Path(__file__).parent.parent / "configurations" / "triggers"

    @pytest.mark.parametrize(
        "filename,min_jobs",
        [
            pytest.param("tier1.yaml", 10, id="tier1"),
            pytest.param("sanity.yaml", 9, id="sanity"),
            pytest.param("perf-regression.yaml", 20, id="perf-regression"),
        ],
    )
    def test_yaml_loads_and_has_expected_job_count(self, filename, min_jobs):
        path = self.TRIGGERS_DIR / filename
        if not path.exists():
            pytest.skip(f"{filename} not found")
        config = load_matrix_config(path)
        assert len(config.jobs) >= min_jobs, f"{filename} has {len(config.jobs)} jobs, expected at least {min_jobs}"

    @pytest.mark.parametrize("filename", ["tier1.yaml", "sanity.yaml", "perf-regression.yaml"])
    def test_yaml_jobs_have_required_fields(self, filename):
        path = self.TRIGGERS_DIR / filename
        if not path.exists():
            pytest.skip(f"{filename} not found")
        config = load_matrix_config(path)
        for job in config.jobs:
            assert job.job_name, f"Job missing job_name in {filename}"
            assert job.backend in ("aws", "gce", "azure", "docker"), (
                f"Invalid backend '{job.backend}' for job '{job.job_name}' in {filename}"
            )

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

import pytest
import yaml

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.errors import MatrixValidationError
from sdcm.utils.trigger_matrix.models import MatrixConfig


def test_load_valid_yaml(sample_matrix_yaml):
    config = load_matrix_config(sample_matrix_yaml)

    assert isinstance(config, MatrixConfig)
    assert len(config.jobs) == 4
    assert config.jobs[0].job_name == "tier1/longevity-50gb-3days-test"
    assert config.jobs[0].backend == "aws"
    assert config.jobs[0].labels == ["weekly"]
    assert config.jobs[0].params == {"region": "eu-west-1", "stress_duration": "4320"}


def test_load_defaults(sample_matrix_yaml):
    config = load_matrix_config(sample_matrix_yaml)
    assert config.defaults["provision_type"] == "spot"
    assert config.defaults["post_behavior_db_nodes"] == "destroy"


def test_load_cron_triggers(sample_matrix_yaml):
    config = load_matrix_config(sample_matrix_yaml)
    assert len(config.cron_triggers) == 1
    assert config.cron_triggers[0].schedule == "00 06 * * 6"
    assert config.cron_triggers[0].params["scylla_version"] == "master:latest"


def test_load_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_matrix_config(tmp_path / "nonexistent.yaml")


def test_load_invalid_not_mapping(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text("- just a list")
    with pytest.raises(MatrixValidationError, match="YAML mapping"):
        load_matrix_config(path)


def test_load_missing_jobs_key(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.dump({"defaults": {}}))
    with pytest.raises(MatrixValidationError, match="'jobs' key"):
        load_matrix_config(path)


def test_load_jobs_not_list(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.dump({"jobs": "not-a-list"}))
    with pytest.raises(MatrixValidationError, match="'jobs' must be a list"):
        load_matrix_config(path)


def test_load_job_missing_name(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.dump({"jobs": [{"backend": "aws"}]}))
    with pytest.raises(MatrixValidationError, match="job_name"):
        load_matrix_config(path)


def test_load_job_missing_backend(tmp_path):
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "test"}]}))
    with pytest.raises(MatrixValidationError, match="backend"):
        load_matrix_config(path)


def test_load_empty_jobs_list(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text(yaml.dump({"jobs": []}))
    config = load_matrix_config(path)
    assert config.jobs == []


def test_load_minimal_job(tmp_path):
    path = tmp_path / "minimal.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "test", "backend": "aws"}]}))
    config = load_matrix_config(path)
    assert config.jobs[0].labels == []
    assert config.jobs[0].exclude_versions == []
    assert config.jobs[0].params == {}


def test_invalid_backend_raises(tmp_path):
    path = tmp_path / "bad-backend.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "test", "backend": "gec"}]}))
    with pytest.raises(MatrixValidationError, match=r"'gec'"):
        load_matrix_config(path)


@pytest.mark.parametrize("backend", sorted(["aws", "gce", "azure", "docker", "oci"]))
def test_valid_backends_accepted(tmp_path, backend):
    path = tmp_path / "valid.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "test", "backend": backend}]}))
    config = load_matrix_config(path)
    assert config.jobs[0].backend == backend


def test_same_job_under_two_arches_rejected(tmp_path):
    """SCT-1168: an x86 + aarch64 twin of one Jenkins job fires it twice on the same arch."""
    path = tmp_path / "twins.yaml"
    twin = {"job_name": "tier1/twcs", "backend": "aws"}
    path.write_text(yaml.dump({"jobs": [twin, {**twin, "labels": ["aarch64"]}]}))
    with pytest.raises(MatrixValidationError, match=r"more than one arch for the same versions: \['tier1/twcs'\]"):
        load_matrix_config(path)


@pytest.mark.parametrize(
    "x86_range, arm_range",
    [
        pytest.param({"exclude_versions": ["2026.1"]}, {"include_versions": ["2026"]}, id="2026.2-is-in-both"),
        pytest.param({"include_versions": ["2025"]}, {"exclude_versions": ["2026"]}, id="master-is-in-both"),
    ],
)
def test_same_job_under_two_arches_with_overlapping_versions_rejected(tmp_path, x86_range, arm_range):
    path = tmp_path / "twins.yaml"
    job = {"job_name": "tier1/twcs", "backend": "aws"}
    path.write_text(yaml.dump({"jobs": [{**job, **x86_range}, {**job, "arch": "aarch64", **arm_range}]}))
    with pytest.raises(MatrixValidationError, match="more than one arch"):
        load_matrix_config(path)


def test_same_job_under_two_arches_split_by_release_allowed(tmp_path):
    """A job whose instance type moved to Graviton on newer branches gets one entry per range."""
    path = tmp_path / "split.yaml"
    job = {"job_name": "tier1/mv-si", "backend": "aws"}
    older = ["2024", "2025", "2026.1"]
    path.write_text(
        yaml.dump(
            {
                "jobs": [
                    {**job, "arch": "x86_64", "include_versions": older},
                    {**job, "arch": "aarch64", "exclude_versions": older},
                ]
            }
        )
    )
    assert [j.arch for j in load_matrix_config(path).jobs] == ["x86_64", "aarch64"]


def test_same_job_repeated_on_one_arch_allowed(tmp_path):
    path = tmp_path / "repeats.yaml"
    twin = {"job_name": "perf", "backend": "aws", "arch": "aarch64"}
    path.write_text(yaml.dump({"jobs": [{**twin, "include_versions": ["master"]}, {**twin, "labels": ["weekly"]}]}))
    assert len(load_matrix_config(path).jobs) == 2


def test_pre_release_loaded_from_yaml(tmp_path):
    data = {
        "jobs": [
            {
                "job_name": "latte-test",
                "backend": "gce",
                "pre_release": ["rc1", "rc3"],
                "labels": ["gce-custom-monthly"],
            },
        ],
    }
    path = tmp_path / "pre-release.yaml"
    path.write_text(yaml.dump(data))
    config = load_matrix_config(path)
    assert config.jobs[0].pre_release == ["rc1", "rc3"]


def test_include_versions_loaded_from_yaml(tmp_path):
    data = {
        "jobs": [
            {
                "job_name": "weekly-test",
                "backend": "aws",
                "include_versions": ["master", "2025.1"],
            },
        ],
    }
    path = tmp_path / "include.yaml"
    path.write_text(yaml.dump(data))
    config = load_matrix_config(path)
    assert config.jobs[0].include_versions == ["master", "2025.1"]

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

"""Tests for the layout validation that keeps Jenkins parameters under `params:`."""

from pathlib import Path

import pytest
import yaml

from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.errors import MatrixValidationError
from sdcm.utils.trigger_matrix.models import JOB_LEVEL_KEYS

TRIGGERS_DIR = Path(__file__).parent.parent.parent.parent / "configurations" / "triggers"


def write_matrix(tmp_path, data) -> Path:
    """Dump the matrix data to matrix.yaml under tmp_path and return the path."""
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


def test_job_level_region_rejected(tmp_path):
    """`region` at job level is the pre-SCT-693 layout — it must point at `params:`."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "region": "eu-west-1"}]})
    with pytest.raises(MatrixValidationError, match=r"unknown job-level key 'region'.*under 'params:'"):
        load_matrix_config(path)


def test_unknown_job_level_key_rejected(tmp_path):
    """An unrecognised job-level key is rejected with MatrixValidationError naming the key."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "instance_type_db": "i4i.4xlarge"}]})
    with pytest.raises(MatrixValidationError, match=r"unknown job-level key 'instance_type_db'"):
        load_matrix_config(path)


def test_typo_in_job_level_key_suggests_correction(tmp_path):
    """A misspelt job-level key is rejected with a suggestion of the closest valid key."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "label": ["weekly"]}]})
    with pytest.raises(MatrixValidationError, match=r"did you mean 'labels'\?"):
        load_matrix_config(path)


def test_error_names_the_offending_job(tmp_path):
    """The layout error names the offending job and its index in the jobs list."""
    path = write_matrix(
        tmp_path,
        {
            "jobs": [
                {"job_name": "good", "backend": "aws"},
                {"job_name": "tier1/bad-job", "backend": "aws", "region": "eu-west-1"},
            ]
        },
    )
    with pytest.raises(MatrixValidationError, match=r"job 'tier1/bad-job' \(jobs\[1\]\)"):
        load_matrix_config(path)


def test_all_layout_errors_reported_at_once(tmp_path):
    """Layout errors of every job are collected and raised together in one MatrixValidationError."""
    path = write_matrix(
        tmp_path,
        {
            "jobs": [
                {"job_name": "job-a", "backend": "aws", "region": "eu-west-1"},
                {"job_name": "job-b", "backend": "aws", "availability_zone": "a"},
            ]
        },
    )
    with pytest.raises(MatrixValidationError) as exc_info:
        load_matrix_config(path)
    assert "job-a" in str(exc_info.value)
    assert "job-b" in str(exc_info.value)


@pytest.mark.parametrize("key", sorted(JOB_LEVEL_KEYS - {"params"}))
def test_job_level_key_nested_under_params_rejected(tmp_path, key):
    """A job-level key placed under params: is rejected as belonging at job level."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": {key: "x"}}]})
    with pytest.raises(MatrixValidationError, match=rf"'{key}' is a job-level key"):
        load_matrix_config(path)


def test_list_valued_param_rejected(tmp_path):
    """A list value in a job's params is rejected; Jenkins parameters are scalars."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": {"sub_tests": ["a"]}}]})
    with pytest.raises(MatrixValidationError, match=r"'sub_tests' must be a scalar"):
        load_matrix_config(path)


def test_list_valued_region_suggests_json_string(tmp_path):
    """A list-valued region is rejected with a hint to write it as a JSON string."""
    path = write_matrix(
        tmp_path,
        {"jobs": [{"job_name": "test", "backend": "aws", "params": {"region": ["eu-west-1", "eu-west-2"]}}]},
    )
    with pytest.raises(MatrixValidationError, match=r"use a JSON string"):
        load_matrix_config(path)


def test_multi_region_json_string_accepted(tmp_path):
    """A multi-region JSON string in params is accepted and kept verbatim."""
    path = write_matrix(
        tmp_path,
        {"jobs": [{"job_name": "test", "backend": "aws", "params": {"region": '["eu-west-1", "eu-west-2"]'}}]},
    )
    config = load_matrix_config(path)
    assert config.jobs[0].params["region"] == '["eu-west-1", "eu-west-2"]'


def test_params_not_a_mapping_rejected(tmp_path):
    """A params value that is a list instead of a mapping is rejected."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": ["region=eu-west-1"]}]})
    with pytest.raises(MatrixValidationError, match=r"must be a mapping of Jenkins parameters"):
        load_matrix_config(path)


def test_unknown_top_level_key_rejected(tmp_path):
    """An unrecognised top-level matrix key is rejected with MatrixValidationError naming it."""
    path = write_matrix(tmp_path, {"jobs": [], "job": []})
    with pytest.raises(MatrixValidationError, match=r"unknown top-level key 'job'"):
        load_matrix_config(path)


def test_job_level_key_in_defaults_rejected(tmp_path):
    """A job-level key such as labels inside defaults is rejected."""
    path = write_matrix(tmp_path, {"defaults": {"labels": ["weekly"]}, "jobs": []})
    with pytest.raises(MatrixValidationError, match=r"defaults: 'labels' is a job-level key"):
        load_matrix_config(path)


def test_list_valued_default_rejected(tmp_path):
    """A list value in defaults is rejected; defaults hold scalar Jenkins parameters."""
    path = write_matrix(tmp_path, {"defaults": {"post_behavior_db_nodes": ["destroy"]}, "jobs": []})
    with pytest.raises(MatrixValidationError, match=r"defaults: 'post_behavior_db_nodes' must be a scalar"):
        load_matrix_config(path)


def test_list_valued_cron_param_rejected(tmp_path):
    """A list value in a cron trigger's params is rejected, naming the cron entry index."""
    path = write_matrix(
        tmp_path,
        {"cron_triggers": [{"schedule": "0 6 * * 6", "params": {"labels_selector": ["weekly"]}}], "jobs": []},
    )
    with pytest.raises(MatrixValidationError, match=r"cron_triggers\[0\]: params: 'labels_selector' must be a scalar"):
        load_matrix_config(path)


def test_gce_job_region_param_rejected(tmp_path):
    """A GCE region in `region` becomes SCT_REGION_NAME and breaks AWS AMI resolution."""
    path = write_matrix(
        tmp_path, {"jobs": [{"job_name": "test", "backend": "gce", "params": {"region": "us-central1"}}]}
    )
    with pytest.raises(MatrixValidationError, match="a gce job takes its region as 'gce_datacenter'"):
        load_matrix_config(path)


@pytest.mark.parametrize("filename", sorted(p.name for p in TRIGGERS_DIR.glob("*.yaml")))
def test_production_matrices_pass_layout_validation(filename):
    """Every shipped trigger matrix must keep its Jenkins parameters under `params:`."""
    config = load_matrix_config(TRIGGERS_DIR / filename)
    assert config.jobs


@pytest.mark.parametrize("filename", sorted(p.name for p in TRIGGERS_DIR.glob("*.yaml")))
def test_production_matrices_have_no_job_level_region(filename):
    """Every shipped trigger matrix keeps region under params: of each job."""
    raw = yaml.safe_load((TRIGGERS_DIR / filename).read_text(encoding="utf-8"))
    offenders = [job["job_name"] for job in raw["jobs"] if "region" in job]
    assert not offenders, f"{filename}: region must live under 'params:' for {offenders}"

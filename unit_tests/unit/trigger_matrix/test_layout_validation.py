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

from sdcm.utils.trigger_matrix import JOB_LEVEL_KEYS, MatrixValidationError, load_matrix_config

TRIGGERS_DIR = Path(__file__).parent.parent.parent.parent / "configurations" / "triggers"


def write_matrix(tmp_path, data) -> Path:
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


def test_job_level_region_rejected(tmp_path):
    """`region` at job level is the pre-SCT-693 layout — it must point at `params:`."""
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "region": "eu-west-1"}]})
    with pytest.raises(MatrixValidationError, match=r"unknown job-level key 'region'.*under 'params:'"):
        load_matrix_config(path)


def test_unknown_job_level_key_rejected(tmp_path):
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "instance_type_db": "i4i.4xlarge"}]})
    with pytest.raises(MatrixValidationError, match=r"unknown job-level key 'instance_type_db'"):
        load_matrix_config(path)


def test_typo_in_job_level_key_suggests_correction(tmp_path):
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "label": ["weekly"]}]})
    with pytest.raises(MatrixValidationError, match=r"did you mean 'labels'\?"):
        load_matrix_config(path)


def test_error_names_the_offending_job(tmp_path):
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
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": {key: "x"}}]})
    with pytest.raises(MatrixValidationError, match=rf"'{key}' is a job-level key"):
        load_matrix_config(path)


def test_list_valued_param_rejected(tmp_path):
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": {"sub_tests": ["a"]}}]})
    with pytest.raises(MatrixValidationError, match=r"'sub_tests' must be a scalar"):
        load_matrix_config(path)


def test_list_valued_region_suggests_json_string(tmp_path):
    path = write_matrix(
        tmp_path,
        {"jobs": [{"job_name": "test", "backend": "aws", "params": {"region": ["eu-west-1", "eu-west-2"]}}]},
    )
    with pytest.raises(MatrixValidationError, match=r"use a JSON string"):
        load_matrix_config(path)


def test_multi_region_json_string_accepted(tmp_path):
    path = write_matrix(
        tmp_path,
        {"jobs": [{"job_name": "test", "backend": "aws", "params": {"region": '["eu-west-1", "eu-west-2"]'}}]},
    )
    config = load_matrix_config(path)
    assert config.jobs[0].params["region"] == '["eu-west-1", "eu-west-2"]'


def test_params_not_a_mapping_rejected(tmp_path):
    path = write_matrix(tmp_path, {"jobs": [{"job_name": "test", "backend": "aws", "params": ["region=eu-west-1"]}]})
    with pytest.raises(MatrixValidationError, match=r"must be a mapping of Jenkins parameters"):
        load_matrix_config(path)


def test_unknown_top_level_key_rejected(tmp_path):
    path = write_matrix(tmp_path, {"jobs": [], "job": []})
    with pytest.raises(MatrixValidationError, match=r"unknown top-level key 'job'"):
        load_matrix_config(path)


def test_job_level_key_in_defaults_rejected(tmp_path):
    path = write_matrix(tmp_path, {"defaults": {"labels": ["weekly"]}, "jobs": []})
    with pytest.raises(MatrixValidationError, match=r"defaults: 'labels' is a job-level key"):
        load_matrix_config(path)


def test_list_valued_default_rejected(tmp_path):
    path = write_matrix(tmp_path, {"defaults": {"post_behavior_db_nodes": ["destroy"]}, "jobs": []})
    with pytest.raises(MatrixValidationError, match=r"defaults: 'post_behavior_db_nodes' must be a scalar"):
        load_matrix_config(path)


def test_list_valued_cron_param_rejected(tmp_path):
    path = write_matrix(
        tmp_path,
        {"cron_triggers": [{"schedule": "0 6 * * 6", "params": {"labels_selector": ["weekly"]}}], "jobs": []},
    )
    with pytest.raises(MatrixValidationError, match=r"cron_triggers\[0\]: params: 'labels_selector' must be a scalar"):
        load_matrix_config(path)


@pytest.mark.parametrize("filename", sorted(p.name for p in TRIGGERS_DIR.glob("*.yaml")))
def test_production_matrices_pass_layout_validation(filename):
    """Every shipped trigger matrix must keep its Jenkins parameters under `params:`."""
    config = load_matrix_config(TRIGGERS_DIR / filename)
    assert config.jobs


@pytest.mark.parametrize("filename", sorted(p.name for p in TRIGGERS_DIR.glob("*.yaml")))
def test_production_matrices_have_no_job_level_region(filename):
    raw = yaml.safe_load((TRIGGERS_DIR / filename).read_text(encoding="utf-8"))
    offenders = [job["job_name"] for job in raw["jobs"] if "region" in job]
    assert not offenders, f"{filename}: region must live under 'params:' for {offenders}"

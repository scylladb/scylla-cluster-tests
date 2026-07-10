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

import yaml

from sdcm.utils import trigger_matrix as tm
from sdcm.utils.trigger_matrix import JobConfig, JenkinsTriggerError, load_matrix_config, trigger_matrix


def test_job_config_wait_defaults_false():
    job = JobConfig(job_name="test", backend="aws", region="eu-west-1")
    assert job.wait is False
    assert job.fail_on_error is False
    assert job.collect_results == []


def test_load_yaml_with_wait_fields(tmp_path):
    data = {
        "jobs": [
            {
                "job_name": "artifacts/artifacts-ami",
                "backend": "aws",
                "region": "eu-west-1",
                "wait": True,
                "fail_on_error": True,
                "collect_results": ["scylla_doctor_*_analysis.json"],
            },
        ],
    }
    path = tmp_path / "wait-matrix.yaml"
    path.write_text(yaml.dump(data))
    config = load_matrix_config(path)
    assert config.jobs[0].wait is True
    assert config.jobs[0].fail_on_error is True
    assert config.jobs[0].collect_results == ["scylla_doctor_*_analysis.json"]


def test_dry_run_wait_mode(tmp_path, caplog):
    data = {
        "jobs": [
            {
                "job_name": "artifacts/artifacts-ami",
                "backend": "aws",
                "region": "eu-west-1",
                "wait": True,
                "fail_on_error": True,
                "collect_results": ["*.json"],
            },
        ],
    }
    path = tmp_path / "wait-matrix.yaml"
    path.write_text(yaml.dump(data))
    with caplog.at_level(logging.INFO):
        results = trigger_matrix(
            matrix_file=str(path),
            scylla_version="master:latest",
            dry_run=True,
        )
    assert "scylla-master/artifacts/artifacts-ami" in results["triggered"]
    assert len(results["waited"]) == 1
    assert results["waited"][0]["build"].result == "SUCCESS"


def test_fail_on_error_aborts_remaining(tmp_path, monkeypatch):
    data = {
        "jobs": [
            {
                "job_name": "/gating/job-1",
                "backend": "aws",
                "region": "eu-west-1",
                "wait": True,
                "fail_on_error": True,
            },
            {
                "job_name": "/gating/job-2",
                "backend": "aws",
                "region": "eu-west-1",
                "wait": False,
            },
        ],
    }
    path = tmp_path / "gating-matrix.yaml"
    path.write_text(yaml.dump(data))

    def mock_trigger_with_queue(*args, **kwargs):
        raise JenkinsTriggerError("Build failed")

    monkeypatch.setattr(tm, "trigger_jenkins_job_with_queue", mock_trigger_with_queue)
    monkeypatch.setattr(tm, "trigger_jenkins_job", lambda *a, **kw: True)

    results = trigger_matrix(
        matrix_file=str(path),
        scylla_version="master:latest",
        dry_run=False,
    )
    assert "gating/job-1" in results["failed"]
    assert "gating/job-2" in results["triggered"]

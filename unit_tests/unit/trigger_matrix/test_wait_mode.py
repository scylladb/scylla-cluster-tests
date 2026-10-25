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

import pytest
import yaml

from sdcm.utils.trigger_matrix import reporting
from sdcm.utils.trigger_matrix.config import load_matrix_config
from sdcm.utils.trigger_matrix.errors import JenkinsTriggerError, TriggerMatrixError
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient
from sdcm.utils.trigger_matrix.matrix import trigger_matrix, trigger_matrix_versions
from sdcm.utils.trigger_matrix.models import JobConfig


def test_job_config_wait_defaults_false():
    job = JobConfig(job_name="test", backend="aws")
    assert job.wait is False
    assert job.fail_on_error is False
    assert job.collect_results == []


def test_load_yaml_with_wait_fields(tmp_path):
    data = {
        "jobs": [
            {
                "job_name": "artifacts/artifacts-ami",
                "backend": "aws",
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
                "wait": True,
                "fail_on_error": True,
            },
            {
                "job_name": "/gating/job-2",
                "backend": "aws",
                "wait": False,
            },
        ],
    }
    path = tmp_path / "gating-matrix.yaml"
    path.write_text(yaml.dump(data))

    def mock_trigger_with_queue(*args, **kwargs):
        raise JenkinsTriggerError("Build failed")

    monkeypatch.setattr(JenkinsClient, "trigger_with_queue", mock_trigger_with_queue)
    monkeypatch.setattr(JenkinsClient, "trigger", lambda *a, **kw: True)

    results = trigger_matrix(
        matrix_file=str(path),
        scylla_version="master:latest",
        dry_run=False,
    )
    assert "gating/job-1" in results["failed"]
    assert "gating/job-2" in results["triggered"]


def test_several_versions_are_waited_for_together(tmp_path, monkeypatch):
    data = {
        "jobs": [
            {"job_name": "/gating/job-1", "backend": "docker", "wait": True},
            {"job_name": "/gating/job-2", "backend": "docker"},
        ],
    }
    path = tmp_path / "gating-matrix.yaml"
    path.write_text(yaml.dump(data))
    waits = []
    real_wait = JenkinsClient.wait_for_builds

    def record_wait(self, pending_jobs, **kwargs):
        waits.append([(p.job_name, p.scylla_version) for p in pending_jobs])
        return real_wait(self, pending_jobs, **kwargs)

    monkeypatch.setattr(JenkinsClient, "wait_for_builds", record_wait)

    results = trigger_matrix_versions(
        matrix_file=str(path),
        versions=[("2025.4.3", "2025.4.3"), ("2026.1.1", "2026.1.1")],
        dry_run=True,
    )

    assert waits == [[("gating/job-1", "2025.4.3"), ("gating/job-1", "2026.1.1")]]
    assert [entry["build"].scylla_version for entry in results["waited"]] == ["2025.4.3", "2026.1.1"]
    assert results["triggered"] == ["gating/job-1", "gating/job-2", "gating/job-1", "gating/job-2"]
    assert results["versions"] == {"gating/job-1": "2025.4.3, 2026.1.1", "gating/job-2": "2025.4.3, 2026.1.1"}
    assert results["skipped"] == []
    reporting.Email.return_value.send.assert_called_once()


@pytest.mark.parametrize(
    "filters",
    [
        pytest.param({"labels_selector": "os-nonexistent"}, id="labels_match_nothing"),
        pytest.param({"skip_jobs": "/gating/job-1"}, id="all_wait_jobs_skipped"),
    ],
)
def test_no_wait_job_triggered_fails(tmp_path, filters):
    data = {
        "jobs": [
            {"job_name": "/gating/job-1", "backend": "docker", "wait": True, "labels": ["os-a"]},
            {"job_name": "/gating/job-2", "backend": "docker", "wait": True, "disabled": True},
        ],
    }
    path = tmp_path / "gating-matrix.yaml"
    path.write_text(yaml.dump(data))

    with pytest.raises(TriggerMatrixError, match="No wait-mode job was triggered"):
        trigger_matrix(matrix_file=str(path), scylla_version="2025.4.3", dry_run=True, **filters)

    reporting.Email.return_value.send.assert_not_called()


def test_no_job_triggered_without_wait_jobs_does_not_fail(tmp_path):
    data = {"jobs": [{"job_name": "/sanity/job-1", "backend": "docker", "labels": ["weekly"]}]}
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))

    results = trigger_matrix(matrix_file=str(path), scylla_version="2025.4.3", labels_selector="monthly", dry_run=True)

    assert results["triggered"] == []
    assert results["failed"] == []


@pytest.mark.parametrize(
    "cli_recipients,expected",
    [
        pytest.param(None, ["matrix@example.com"], id="matrix"),
        pytest.param(["cli@example.com"], ["cli@example.com"], id="cli-wins"),
    ],
)
def test_report_recipients(tmp_path, cli_recipients, expected):
    data = {
        "email_recipients": "matrix@example.com",
        "jobs": [{"job_name": "/gating/job-1", "backend": "docker", "wait": True}],
    }
    path = tmp_path / "gating-matrix.yaml"
    path.write_text(yaml.dump(data))

    trigger_matrix(matrix_file=str(path), scylla_version="2025.4.3", dry_run=True, email_recipients=cli_recipients)

    assert reporting.Email.return_value.send.call_args.kwargs["recipients"] == expected

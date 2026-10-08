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

"""The arch audit reads every release's configs from git, so these tests build a small repo."""

import subprocess
from unittest.mock import patch

import pytest
import yaml

from sdcm.utils.trigger_matrix import reporting
from sdcm.utils.trigger_matrix.audit import ArchDrift, AuditReport, audit_matrices

JENKINSFILE = """longevityPipeline(
    backend: 'aws',
    test_config: '''["test-cases/mv-si.yaml"]''',
)
"""


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


def _commit_tree(repo, instance_type_db):
    (repo / "jenkins-pipelines/oss/tier1").mkdir(parents=True, exist_ok=True)
    (repo / "jenkins-pipelines/oss/tier1/mv-si.jenkinsfile").write_text(JENKINSFILE)
    (repo / "test-cases").mkdir(exist_ok=True)
    (repo / "test-cases/mv-si.yaml").write_text(yaml.dump({"instance_type_db": instance_type_db}))
    _git(repo, "add", "-A")
    _git(repo, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", instance_type_db)


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """A repo where the job runs Graviton on master and x86 on branch-2026.1."""
    repo = tmp_path / "sct"
    repo.mkdir()
    _git(repo, "init", "-q")
    _commit_tree(repo, "i4i.4xlarge")
    _git(repo, "update-ref", "refs/remotes/origin/branch-2026.1", "HEAD")
    _commit_tree(repo, "i8g.4xlarge")
    _git(repo, "update-ref", "refs/remotes/origin/master", "HEAD")
    monkeypatch.chdir(repo)
    return repo


def _matrix(tmp_path, *jobs):
    path = tmp_path / "tier1.yaml"
    path.write_text(yaml.dump({"jobs": list(jobs)}))
    return [path]


def test_drift_is_reported_only_for_releases_whose_branch_disagrees(repo, tmp_path):
    matrix = _matrix(tmp_path, {"job_name": "tier1/mv-si-test", "backend": "aws"})

    report = audit_matrices(matrix, ["master", "2026.1"])

    assert report.drifts == [ArchDrift("tier1", "tier1/mv-si-test", "x86_64", "aarch64", "i8g.4xlarge", ["master"])]
    assert report.versions == ["master", "2026.1"]


def test_entries_split_by_release_have_no_drift(repo, tmp_path):
    job = {"job_name": "tier1/mv-si-test", "backend": "aws"}
    matrix = _matrix(
        tmp_path,
        {**job, "arch": "x86_64", "include_versions": ["2026.1"]},
        {**job, "arch": "aarch64", "exclude_versions": ["2026.1"]},
    )

    report = audit_matrices(matrix, ["master", "2026.1"])

    assert report.drifts == []
    assert report.unchecked == []


def test_job_without_a_jenkinsfile_is_reported_unchecked(repo, tmp_path):
    matrix = _matrix(tmp_path, {"job_name": "tier1/not-there-test", "backend": "aws"})

    report = audit_matrices(matrix, ["master"])

    assert report.drifts == []
    assert [(u.job_name, u.reason, u.versions) for u in report.unchecked] == [
        ("tier1/not-there-test", "no jenkinsfile found for the job", ["master"])
    ]


def test_release_without_a_branch_is_skipped(repo, tmp_path):
    matrix = _matrix(tmp_path, {"job_name": "tier1/mv-si-test", "backend": "aws", "arch": "aarch64"})

    report = audit_matrices(matrix, ["master", "2099.1"])

    assert report.versions == ["master"]
    assert report.drifts == []


def test_drift_email_lists_each_entry():
    report = AuditReport(
        drifts=[ArchDrift("tier1", "tier1/mv-si-test", "x86_64", "aarch64", "i8g.4xlarge", ["master", "2026.3"])],
        versions=["master", "2026.3"],
    )
    with patch.object(reporting, "Email") as email:
        reporting.send_arch_audit_email(report, ["someone@example.com"])

    sent = email.return_value.send.call_args.kwargs
    assert sent["recipients"] == ["someone@example.com"]
    assert sent["subject"] == "[Trigger Matrix] arch drift in 1 entries"
    assert "tier1/mv-si-test" in sent["content"] and "master, 2026.3" in sent["content"]

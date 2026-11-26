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

from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from sct import cli
from sdcm.utils.trigger_matrix import images
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient

FETCH = "utils.get_supported_scylla_base_versions.fetch_official_supported_versions"


@pytest.fixture()
def triggered(monkeypatch):
    """Parameters of every job the CLI triggers, by job path."""
    calls = {}

    def trigger(self, job_name, parameters, dry_run=False):
        calls[job_name] = parameters
        return True

    monkeypatch.setattr(JenkinsClient, "trigger", trigger)
    monkeypatch.setattr("sct.add_file_logger", lambda *args, **kwargs: None)
    return calls


@pytest.fixture()
def matrix(tmp_path):
    """Matrix file with one docker gating job and post_behavior_db_nodes defaulting to destroy."""
    data = {
        "defaults": {"post_behavior_db_nodes": "destroy"},
        "jobs": [{"job_name": "/gating/job-1", "backend": "docker"}],
    }
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


@pytest.fixture()
def supported_only_matrix(tmp_path):
    """Matrix file with supported_versions_only set and one docker gating job."""
    path = tmp_path / "supported-only.yaml"
    path.write_text(
        yaml.dump({"supported_versions_only": True, "jobs": [{"job_name": "/gating/job-1", "backend": "docker"}]})
    )
    return str(path)


def run(matrix, *args):
    """Invoke trigger-matrix with the given matrix and Scylla version 2025.4.3, expecting exit code 0."""
    result = CliRunner().invoke(cli, ["trigger-matrix", "--matrix", matrix, "--scylla-version", "2025.4.3", *args])
    assert result.exit_code == 0, result.output
    return result


def test_post_behavior_db_nodes_overrides_the_matrix(matrix, triggered):
    """--post-behavior-db-nodes on the CLI overrides the matrix default."""
    run(matrix, "--post-behavior-db-nodes", "keep-on-failure")
    assert triggered["gating/job-1"]["post_behavior_db_nodes"] == "keep-on-failure"


def test_post_behavior_db_nodes_defaults_to_the_matrix(matrix, triggered):
    """Without the flag, the job gets the post_behavior_db_nodes default of the matrix."""
    run(matrix)
    assert triggered["gating/job-1"]["post_behavior_db_nodes"] == "destroy"


def test_report_dir_saves_the_wait_mode_report(tmp_path, triggered):
    """--report-dir writes the summary of a wait-mode job."""
    path = tmp_path / "wait.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "/gating/job-1", "backend": "docker", "wait": True}]}))
    run(str(path), "--dry-run", "--report-dir", str(tmp_path / "report"))
    assert (tmp_path / "report" / "trigger-matrix-summary.txt").read_text() == "PASSED | 1/1 passed | 2025.4.3\n"


def test_supported_only_matrix_refuses_other_versions(supported_only_matrix, triggered):
    """A supported-only matrix exits 1 and triggers nothing when a requested version is not supported."""
    with patch(FETCH, return_value=["2026.3", "2025.1"]):
        result = CliRunner().invoke(
            cli, ["trigger-matrix", "--matrix", supported_only_matrix, "--scylla-version", "2026.3.3 2025.4.3"]
        )
    assert result.exit_code == 1
    assert "not supported: 2025.4.3" in result.output
    assert triggered == {}


def test_supported_only_matrix_accepts_supported_versions(supported_only_matrix, triggered):
    """A supported-only matrix triggers the job with a requested version that is supported."""
    with patch(FETCH, return_value=["2026.3", "2025.4"]):
        run(supported_only_matrix)
    assert triggered["gating/job-1"]["scylla_version"] == "2025.4.3"


def test_supported_only_matrix_runs_every_supported_version_without_scylla_version(
    supported_only_matrix, triggered, monkeypatch
):
    """Without --scylla-version, a supported-only matrix runs the latest release of each supported line."""
    versions = []
    monkeypatch.setattr(
        JenkinsClient,
        "trigger",
        lambda self, job, params, dry_run=False: versions.append(params["scylla_version"]) or True,
    )
    monkeypatch.setattr(
        images, "latest_release_of_line", lambda line, region: {"2026.3": "2026.3.3", "2025.1": "2025.1.16"}[line]
    )
    with patch(FETCH, return_value=["2026.3", "2025.1"]):
        result = CliRunner().invoke(cli, ["trigger-matrix", "--matrix", supported_only_matrix])
    assert result.exit_code == 0, result.output
    assert versions == ["2026.3.3", "2025.1.16"]


def test_other_matrix_still_needs_a_scylla_version(matrix, triggered):
    """A matrix without supported_versions_only refuses to run without --scylla-version."""
    result = CliRunner().invoke(cli, ["trigger-matrix", "--matrix", matrix])
    assert result.exit_code == 1
    assert "Either --scylla-version" in result.output
    assert triggered == {}


@pytest.mark.parametrize(
    "requested,expected",
    [
        pytest.param("2026.3", ["2026.3.3"], id="line-becomes-its-release"),
        pytest.param("2026.3.1", ["2026.3.1"], id="release-kept"),
        pytest.param("2026.3,2025.1.10", ["2026.3.3", "2025.1.10"], id="mixed"),
    ],
)
def test_supported_only_matrix_runs_the_release_of_a_typed_in_line(
    supported_only_matrix, triggered, monkeypatch, requested, expected
):
    """A requested line resolves to its latest release, while a full release is kept as typed."""
    versions = []
    monkeypatch.setattr(
        JenkinsClient,
        "trigger",
        lambda self, job, params, dry_run=False: versions.append(params["scylla_version"]) or True,
    )
    monkeypatch.setattr(images, "latest_release_of_line", lambda line, region: {"2026.3": "2026.3.3"}[line])
    with patch(FETCH, return_value=["2026.3", "2025.1"]):
        result = CliRunner().invoke(
            cli, ["trigger-matrix", "--matrix", supported_only_matrix, "--scylla-version", requested]
        )
    assert result.exit_code == 0, result.output
    assert versions == expected


def test_supported_versions_are_fetched_once_without_scylla_version(supported_only_matrix, triggered, monkeypatch):
    """Without --scylla-version, the supported versions are fetched once per run, not once per job."""
    monkeypatch.setattr(images, "latest_release_of_line", lambda line, region: f"{line}.1")
    with patch(FETCH, return_value=["2026.3", "2025.1"]) as fetch:
        result = CliRunner().invoke(cli, ["trigger-matrix", "--matrix", supported_only_matrix])
    assert result.exit_code == 0, result.output
    assert fetch.call_count == 1

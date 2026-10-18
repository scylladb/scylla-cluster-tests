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
from click.testing import CliRunner

from sct import cli
from sdcm.utils.trigger_matrix.jenkins_client import JenkinsClient


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
    data = {
        "defaults": {"post_behavior_db_nodes": "destroy"},
        "jobs": [{"job_name": "/gating/job-1", "backend": "docker"}],
    }
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return str(path)


def run(matrix, *args):
    result = CliRunner().invoke(cli, ["trigger-matrix", "--matrix", matrix, "--scylla-version", "2025.4.3", *args])
    assert result.exit_code == 0, result.output
    return result


def test_post_behavior_db_nodes_overrides_the_matrix(matrix, triggered):
    run(matrix, "--post-behavior-db-nodes", "keep-on-failure")
    assert triggered["gating/job-1"]["post_behavior_db_nodes"] == "keep-on-failure"


def test_post_behavior_db_nodes_defaults_to_the_matrix(matrix, triggered):
    run(matrix)
    assert triggered["gating/job-1"]["post_behavior_db_nodes"] == "destroy"


def test_report_dir_saves_the_wait_mode_report(tmp_path, triggered):
    path = tmp_path / "wait.yaml"
    path.write_text(yaml.dump({"jobs": [{"job_name": "/gating/job-1", "backend": "docker", "wait": True}]}))
    run(str(path), "--dry-run", "--report-dir", str(tmp_path / "report"))
    assert (tmp_path / "report" / "trigger-matrix-summary.txt").read_text() == "PASSED | 1/1 passed | 2025.4.3\n"

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

"""Tests for job selection and parameter building against a synthetic matrix.

These use an in-memory matrix (not configurations/triggers/perf-regression.yaml) so they
exercise filter_jobs()/build_job_parameters() logic and stay green when the real matrix
changes — see PR #15380 discussion.
"""

from pathlib import Path

import pytest
import yaml

from sdcm.utils.trigger_matrix import build_job_parameters, filter_jobs, load_matrix_config


def write_matrix(tmp_path, data) -> Path:
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


@pytest.fixture()
def perf_config(tmp_path):
    path = write_matrix(
        tmp_path,
        {
            "jobs": [
                {
                    "job_name": "aws-perf-i8g-tablets-master",
                    "backend": "aws",
                    "labels": ["master-2weeks"],
                },
                {
                    "job_name": "gce-perf-latte-1",
                    "backend": "gce",
                    "labels": ["gce-custom-monthly"],
                    "pre_release": ["rc1"],
                },
                {
                    "job_name": "gce-perf-latte-2",
                    "backend": "gce",
                    "labels": ["gce-custom-monthly"],
                    "pre_release": ["rc1"],
                },
                {
                    "job_name": "aws-rolling-upgrade",
                    "backend": "aws",
                    "params": {
                        "rolling_upgrade_test": "true",
                        "new_scylla_repo": "http://downloads.scylladb.com/deb/scylla/{branch_id}/deb/"
                        "unstable/scylladb-{branch}/scylla.list",
                    },
                },
                {
                    "job_name": "gce-rolling-upgrade",
                    "backend": "gce",
                    "params": {
                        "rolling_upgrade_test": "true",
                        "new_scylla_repo": "http://downloads.scylladb.com/deb/scylla/{branch_id}/deb/"
                        "unstable/scylladb-{branch}/scylla.list",
                    },
                },
            ]
        },
    )
    return load_matrix_config(path)


def test_master_2weeks_selects_expected_jobs(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="master:latest",
        resolved_version="2026.3.0~dev-0.20260525.69a5b417d1dc",
        labels_selector="master-2weeks",
    )
    assert len(result) == 1
    names = {j.job_name for j in result}
    assert any("i8g-tablets" in n for n in names)


def test_gce_custom_monthly_with_master_selects_latte(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="master:latest",
        resolved_version="2026.3.0~dev-0.20260525.69a5b417d1dc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 2
    assert all("latte" in j.job_name for j in result)


def test_gce_custom_monthly_non_rc_excluded(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="2025.1:latest",
        resolved_version="2025.1.3-0.20250525.abc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 0


def test_gce_custom_monthly_rc1_included(perf_config):
    result = filter_jobs(
        perf_config.jobs,
        scylla_version="2025.1:latest",
        resolved_version="2025.1.3-rc1-0.20250525.abc",
        labels_selector="gce-custom-monthly",
    )
    assert len(result) == 2


def test_rolling_upgrade_jobs_resolve_new_scylla_repo(perf_config):
    """SCT-782: rolling_upgrade_test jobs must get scylla_version cleared and a fully
    resolved new_scylla_repo from build_job_parameters(), with the directory segment
    branch-prefixed (e.g. 'branch-2025.1' / 'master') and the filename segment bare
    (e.g. 'scylladb-2025.1' / 'scylladb-master').
    """
    rolling_upgrade_jobs = [
        job for job in perf_config.jobs if str(job.params.get("rolling_upgrade_test", "")).lower() == "true"
    ]
    assert len(rolling_upgrade_jobs) == 2, (
        f"Expected 2 rolling_upgrade_test jobs in the synthetic matrix, found {len(rolling_upgrade_jobs)}"
    )

    for job in rolling_upgrade_jobs:
        params = build_job_parameters(job, perf_config.defaults, "2025.1:latest", {})
        assert params["scylla_version"] == "", f"Job {job.job_name} should have blank scylla_version"
        new_scylla_repo = params.get("new_scylla_repo", "")
        assert new_scylla_repo, f"Job {job.job_name} missing new_scylla_repo"
        assert "{branch}" not in new_scylla_repo, (
            f"Job {job.job_name} has unresolved {{branch}} placeholder in new_scylla_repo: {new_scylla_repo}"
        )
        assert "{branch_id}" not in new_scylla_repo, (
            f"Job {job.job_name} has unresolved {{branch_id}} placeholder in new_scylla_repo: {new_scylla_repo}"
        )
        assert "/branch-2025.1/deb/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo directory segment must be branch-prefixed "
            f"('branch-2025.1'): {new_scylla_repo}"
        )
        assert "/scylladb-2025.1/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo filename segment must stay bare ('scylladb-2025.1'): {new_scylla_repo}"
        )

    for job in rolling_upgrade_jobs:
        params = build_job_parameters(job, perf_config.defaults, "master:latest", {})
        new_scylla_repo = params.get("new_scylla_repo", "")
        assert "/scylla/master/deb/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo directory segment for master must stay "
            f"unprefixed ('master', not 'branch-master'): {new_scylla_repo}"
        )
        assert "/scylladb-master/" in new_scylla_repo, (
            f"Job {job.job_name} new_scylla_repo filename segment for master must be "
            f"'scylladb-master': {new_scylla_repo}"
        )

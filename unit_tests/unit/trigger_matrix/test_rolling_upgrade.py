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

from pathlib import Path

import pytest

from sdcm.utils.trigger_matrix import (
    JobConfig,
    _extract_branch_from_version,
    build_job_parameters,
    filter_jobs,
    load_matrix_config,
)

ROLLING_UPGRADE_YAML = Path(__file__).parents[3] / "configurations" / "triggers" / "rolling-upgrade.yaml"


@pytest.mark.parametrize(
    "version,expected",
    [
        ("master:latest", "master"),
        ("branch-2025.4:latest", "branch-2025.4"),
        ("2025.4", "2025.4"),
        ("2025.4.1-0.20250601.abc123def456-1", "2025.4"),
        ("2026.2.0~dev-0.20260322.f51126483167", "2026.2"),
        ("master", "master"),
        ("", ""),
    ],
)
def test_extract_branch_from_version(version, expected):
    assert _extract_branch_from_version(version) == expected


def test_branch_template_resolved_in_params():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={
            "new_scylla_repo": "http://downloads.scylladb.com/unstable/scylla/{branch}/rpm/centos/latest/scylla.repo"
        },
    )
    params = build_job_parameters(job, {}, "master:latest", {})
    assert (
        params["new_scylla_repo"]
        == "http://downloads.scylladb.com/unstable/scylla/master/rpm/centos/latest/scylla.repo"
    )


def test_branch_template_from_simple_version():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={"new_scylla_repo": "http://repo/{branch}/deb/scylla.list"},
    )
    params = build_job_parameters(job, {}, "2025.4", {})
    assert params["new_scylla_repo"] == "http://repo/2025.4/deb/scylla.list"


def test_branch_template_from_full_version_tag():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="aws",
        region="eu-west-1",
        params={"new_scylla_repo": "http://repo/{branch}/rpm/scylla.repo"},
    )
    params = build_job_parameters(job, {}, "2025.4.1-0.20250601.abc123def456-1", {})
    assert params["new_scylla_repo"] == "http://repo/2025.4/rpm/scylla.repo"


def test_no_template_no_change():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="aws",
        region="eu-west-1",
        params={"new_scylla_repo": "http://explicit-repo/scylla.repo"},
    )
    params = build_job_parameters(job, {}, "master:latest", {})
    assert params["new_scylla_repo"] == "http://explicit-repo/scylla.repo"


def test_cli_override_wins_over_template():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={"new_scylla_repo": "http://downloads/{branch}/rpm/scylla.repo"},
    )
    params = build_job_parameters(job, {}, "master:latest", {"new_scylla_repo": "http://custom/release/scylla.repo"})
    assert params["new_scylla_repo"] == "http://custom/release/scylla.repo"


def test_empty_version_no_resolution():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={"new_scylla_repo": "http://downloads/{branch}/rpm/scylla.repo"},
    )
    params = build_job_parameters(job, {}, "", {})
    assert params["new_scylla_repo"] == "http://downloads/{branch}/rpm/scylla.repo"


def test_rolling_upgrade_yaml_loads():
    config = load_matrix_config(ROLLING_UPGRADE_YAML)
    assert len(config.jobs) > 0
    assert config.cron_triggers[0].schedule == "00 06 * * 6"


def test_all_jobs_have_new_scylla_repo():
    config = load_matrix_config(ROLLING_UPGRADE_YAML)
    for job in config.jobs:
        assert "new_scylla_repo" in job.params, f"Job {job.job_name} missing new_scylla_repo param"


def test_weekly_label_filter():
    config = load_matrix_config(ROLLING_UPGRADE_YAML)
    weekly_jobs = filter_jobs(config.jobs, scylla_version="master:latest", labels_selector="weekly")
    assert len(weekly_jobs) >= 9


def test_branch_source_version_overrides_resolved_version():
    """When trigger resolves master:latest → 2026.3.0~dev-..., {branch} should still be 'master'."""
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={
            "new_scylla_repo": "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/{branch}/deb/unified/latest/scylladb-{branch}/scylla.list"
        },
    )
    params = build_job_parameters(
        job, {}, "2026.3.0~dev-0.20260710.9cda315bbab0", {}, branch_source_version="master:latest"
    )
    assert params["new_scylla_repo"] == (
        "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list"
    )
    assert params["scylla_version"] == "2026.3.0~dev-0.20260710.9cda315bbab0"


def test_branch_source_version_none_falls_back_to_scylla_version():
    """Without branch_source_version, {branch} is still extracted from scylla_version."""
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        region="us-central1",
        params={"new_scylla_repo": "http://repo/{branch}/rpm/scylla.repo"},
    )
    params = build_job_parameters(job, {}, "2025.4.1-0.20250601.abc123def456-1", {}, branch_source_version=None)
    assert params["new_scylla_repo"] == "http://repo/2025.4/rpm/scylla.repo"

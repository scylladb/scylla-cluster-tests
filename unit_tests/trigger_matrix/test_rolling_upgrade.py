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
    _branch_directory_id,
    _extract_branch_from_version,
    build_job_parameters,
    filter_jobs,
    load_matrix_config,
)

ROLLING_UPGRADE_YAML = Path(__file__).parent.parent.parent / "configurations" / "triggers" / "rolling-upgrade.yaml"


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


@pytest.mark.parametrize(
    "branch,expected",
    [
        pytest.param("master", "master", id="master"),
        pytest.param("2025.1", "branch-2025.1", id="non_master_branch"),
        pytest.param("2026.1", "branch-2026.1", id="non_master_branch_2026"),
        pytest.param("branch-2025.4", "branch-2025.4", id="already_prefixed"),
        pytest.param("", "", id="empty"),
    ],
)
def test_branch_directory_id(branch, expected):
    """SCT-782: {branch_id} must be branch-prefixed for non-master branches, but
    unprefixed for master.
    """
    assert _branch_directory_id(branch) == expected


@pytest.mark.parametrize(
    "scylla_version,expected_repo",
    [
        pytest.param(
            "2025.1:latest",
            (
                "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/"
                "branch-2025.1/deb/unified/latest/scylladb-2025.1/scylla.list"
            ),
            id="non_master",
        ),
        pytest.param(
            "master:latest",
            (
                "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/"
                "master/deb/unified/latest/scylladb-master/scylla.list"
            ),
            id="master",
        ),
    ],
)
def test_branch_id_template_resolved_in_params(scylla_version, expected_repo):
    """SCT-782: {branch_id} resolves to the branch-prefixed directory segment while
    {branch} stays bare — non-master branches get a 'branch-' prefixed directory
    ('branch-2025.1') with a bare filename ('scylladb-2025.1'); master stays
    unprefixed in both ('master' / 'scylladb-master').
    """
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="aws",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": (
                "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/"
                "{branch_id}/deb/unified/latest/scylladb-{branch}/scylla.list"
            ),
        },
    )
    params = build_job_parameters(job, {}, scylla_version, {})
    assert params["new_scylla_repo"] == expected_repo


def test_branch_template_resolved_in_params():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads.scylladb.com/unstable/scylla/{branch}/rpm/centos/latest/scylla.repo",
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
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://repo/{branch}/deb/scylla.list",
        },
    )
    params = build_job_parameters(job, {}, "2025.4", {})
    assert params["new_scylla_repo"] == "http://repo/2025.4/deb/scylla.list"


def test_branch_template_from_full_version_tag():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="aws",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://repo/{branch}/rpm/scylla.repo",
        },
    )
    params = build_job_parameters(job, {}, "2025.4.1-0.20250601.abc123def456-1", {})
    assert params["new_scylla_repo"] == "http://repo/2025.4/rpm/scylla.repo"


def test_no_template_no_change():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="aws",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://explicit-repo/scylla.repo",
        },
    )
    params = build_job_parameters(job, {}, "master:latest", {})
    assert params["new_scylla_repo"] == "http://explicit-repo/scylla.repo"


def test_cli_override_wins_over_template():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads/{branch}/rpm/scylla.repo",
        },
    )
    params = build_job_parameters(job, {}, "master:latest", {"new_scylla_repo": "http://custom/release/scylla.repo"})
    assert params["new_scylla_repo"] == "http://custom/release/scylla.repo"


def test_empty_version_no_resolution():
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads/{branch}/rpm/scylla.repo",
        },
    )
    params = build_job_parameters(job, {}, "", {})
    assert params["new_scylla_repo"] == "http://downloads/{branch}/rpm/scylla.repo"


def test_rolling_upgrade_yaml_loads():
    config = load_matrix_config(ROLLING_UPGRADE_YAML)
    assert len(config.jobs) > 0
    # Two biweekly cron lines, both at Saturday 06:00 UTC (SCT-716).
    assert [cron.schedule for cron in config.cron_triggers] == [
        "00 06 1-7,15-21,29-31 * 6",
        "00 06 8-14,22-28 * 6",
    ]


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
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/{branch}/deb/unified/latest/scylladb-{branch}/scylla.list",
        },
    )
    params = build_job_parameters(
        job, {}, "2026.3.0~dev-0.20260710.9cda315bbab0", {}, branch_source_version="master:latest"
    )
    assert params["new_scylla_repo"] == (
        "http://downloads.scylladb.com.s3.amazonaws.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list"
    )
    assert params["scylla_version"] == ""


def test_branch_source_version_none_falls_back_to_scylla_version():
    """Without branch_source_version, {branch} is still extracted from scylla_version."""
    job = JobConfig(
        job_name="rolling-upgrade/test",
        backend="gce",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://repo/{branch}/rpm/scylla.repo",
        },
    )
    params = build_job_parameters(job, {}, "2025.4.1-0.20250601.abc123def456-1", {}, branch_source_version=None)
    assert params["new_scylla_repo"] == "http://repo/2025.4/rpm/scylla.repo"


def test_rolling_upgrade_test_clears_scylla_version():
    """When rolling_upgrade_test is true, scylla_version is sent empty."""
    job = JobConfig(
        job_name="perf-regression/rolling-upgrade-test",
        backend="aws",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads/{branch}/deb/scylla.list",
        },
    )
    params = build_job_parameters(
        job, {}, "2026.3.0~dev-0.20260710.9cda315bbab0", {}, branch_source_version="master:latest"
    )
    assert params["scylla_version"] == ""
    assert params["new_scylla_repo"] == "http://downloads/master/deb/scylla.list"
    assert params["rolling_upgrade_test"] == "true"


def test_rolling_upgrade_test_keeps_base_versions_from_override():
    """base_versions from CLI overrides is passed to rolling upgrade jobs."""
    job = JobConfig(
        job_name="perf-regression/rolling-upgrade-test",
        backend="aws",
        params={
            "rolling_upgrade_test": "true",
            "new_scylla_repo": "http://downloads/{branch}/deb/scylla.list",
        },
    )
    overrides = {"base_versions": "2025.1,2025.2"}
    params = build_job_parameters(job, {}, "master:latest", overrides)
    assert params["scylla_version"] == ""
    assert params["base_versions"] == "2025.1,2025.2"


def test_non_rolling_upgrade_keeps_scylla_version():
    """Non-rolling-upgrade jobs still get scylla_version set normally."""
    job = JobConfig(
        job_name="perf-regression/throughput-test",
        backend="aws",
        params={"sub_tests": '["test_read"]'},
    )
    params = build_job_parameters(job, {}, "2026.3.0~dev-0.20260710.9cda315bbab0", {})
    assert params["scylla_version"] == "2026.3.0~dev-0.20260710.9cda315bbab0"


def test_non_rolling_upgrade_strips_new_scylla_repo_from_overrides():
    """new_scylla_repo passed via CLI overrides must not reach non-upgrade jobs."""
    job = JobConfig(
        job_name="perf-regression/throughput-test",
        backend="aws",
        params={"sub_tests": '["test_read"]'},
    )
    overrides = {"new_scylla_repo": "http://downloads/master/rpm/scylla.repo"}
    params = build_job_parameters(job, {}, "2026.3.0~dev-0.20260710.9cda315bbab0", overrides)
    assert "new_scylla_repo" not in params
    assert params["scylla_version"] == "2026.3.0~dev-0.20260710.9cda315bbab0"


def test_non_rolling_upgrade_strips_new_scylla_repo_from_defaults():
    """new_scylla_repo in matrix defaults must not reach non-upgrade jobs."""
    job = JobConfig(
        job_name="perf-regression/latency-test",
        backend="aws",
        params={},
    )
    defaults = {"new_scylla_repo": "http://downloads/master/rpm/scylla.repo"}
    params = build_job_parameters(job, defaults, "master:latest", {})
    assert "new_scylla_repo" not in params


def test_rolling_upgrade_jobs_have_new_scylla_repo():
    """SCT-782: every job with rolling_upgrade_test == 'true' across all trigger matrices
    must define new_scylla_repo (directly or via defaults) — otherwise build_job_parameters()
    blanks scylla_version with no repo to fall back on, and provisioning fails downstream
    with 'missing options: [ami_id_db_scylla]'.
    """
    triggers_dir = Path(__file__).parent.parent.parent / "configurations" / "triggers"
    for yaml_path in sorted(triggers_dir.glob("*.yaml")):
        config = load_matrix_config(yaml_path)
        for job in config.jobs:
            merged_params = {**config.defaults, **job.params}
            is_rolling_upgrade = str(merged_params.get("rolling_upgrade_test", "")).lower() == "true"
            if is_rolling_upgrade:
                assert merged_params.get("new_scylla_repo"), (
                    f"SCT-782: job '{job.job_name}' in {yaml_path.name} has "
                    f"rolling_upgrade_test == 'true' but no new_scylla_repo defined "
                    f"(neither in job.params nor in defaults)"
                )

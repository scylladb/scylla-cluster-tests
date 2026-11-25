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

from sdcm.utils.trigger_matrix.filters import filter_jobs
from sdcm.utils.trigger_matrix.models import JobConfig


def test_no_filters_returns_all(sample_jobs):
    """Without selectors every job is returned for a version that no job excludes."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4")
    assert len(result) == 4


def test_version_exclusion_always_applies(sample_jobs):
    """A job excluded for master is dropped even when no other selector is given."""
    result = filter_jobs(sample_jobs, scylla_version="master:latest")
    assert len(result) == 3
    assert all(j.job_name != "job-d" for j in result)


def test_filter_by_labels_single(sample_jobs):
    """labels_selector keeps only jobs carrying the single given label."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly")
    assert len(result) == 2
    assert all("weekly" in j.labels for j in result)


def test_filter_by_labels_multiple_and_logic(sample_jobs):
    """Comma-separated labels_selector entries must all match (AND)."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly,additional")
    assert len(result) == 1
    assert result[0].job_name == "job-b"


def test_filter_by_labels_alternatives_or_logic(sample_jobs):
    """Slash-separated labels_selector alternatives match if any one label matches (OR)."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="additional/master-weekly")
    assert {j.job_name for j in result} == {"job-b", "job-d"}


def test_filter_by_labels_alternatives_combined_with_and(sample_jobs):
    """Comma-separated groups are ANDed while slash-separated alternatives inside a group are ORed."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly,additional/master-weekly")
    assert [j.job_name for j in result] == ["job-b"]


def test_filter_by_backend(sample_jobs):
    """Backend keeps only jobs configured for that backend."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", backend="aws")
    assert len(result) == 2
    assert all(j.backend == "aws" for j in result)


def test_filter_by_version_exclusion(sample_jobs):
    """A job whose exclude_versions matches the Scylla version is dropped."""
    result = filter_jobs(sample_jobs, scylla_version="2024.1.5")
    assert len(result) == 3
    assert all(j.job_name != "job-b" for j in result)


def test_filter_by_version_exclusion_master(sample_jobs):
    """A job excluded for master is dropped when the Scylla version is master."""
    result = filter_jobs(sample_jobs, scylla_version="master:latest")
    assert len(result) == 3
    assert all(j.job_name != "job-d" for j in result)


def test_filter_skip_list(sample_jobs):
    """skip_jobs removes the named jobs from the result."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4", skip_jobs=["job-a", "job-c"])
    assert len(result) == 2
    names = {j.job_name for j in result}
    assert names == {"job-b", "job-d"}


def test_no_selector_returns_all_eligible(sample_jobs):
    """With no selector, every eligible job is returned."""
    result = filter_jobs(sample_jobs, scylla_version="2025.4")
    assert len(result) == 4


def test_combined_filters(sample_jobs):
    """Label, backend and version filters combine, leaving only the job that matches all of them."""
    result = filter_jobs(
        sample_jobs,
        scylla_version="2025.4",
        labels_selector="weekly",
        backend="aws",
    )
    assert len(result) == 1
    assert result[0].job_name == "job-a"


def test_include_versions_passes_matching_prefix():
    """A job whose include_versions contains the Scylla version prefix is kept."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="master:latest")
    assert len(result) == 1


def test_include_versions_blocks_non_matching():
    """A job whose include_versions does not match the Scylla version is dropped."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest")
    assert len(result) == 0


def test_include_versions_prefix_match():
    """include_versions matches by prefix, so 2025.1 also matches 2025.1.3."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=["2025.1"])]
    result = filter_jobs(jobs, scylla_version="2025.1.3:latest")
    assert len(result) == 1


def test_include_versions_multiple():
    """A job listing several include_versions is kept for each listed version only."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=["master", "2025.1"])]
    assert len(filter_jobs(jobs, scylla_version="master:latest")) == 1
    assert len(filter_jobs(jobs, scylla_version="2025.1:latest")) == 1
    assert len(filter_jobs(jobs, scylla_version="2024.2:latest")) == 0


def test_empty_include_versions_allows_all():
    """An empty include_versions list does not restrict the job to any version."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=[])]
    assert len(filter_jobs(jobs, scylla_version="anything")) == 1


def test_include_versions_uses_original_not_resolved():
    """include_versions is matched against the original scylla_version, not the resolved one."""
    jobs = [JobConfig(job_name="job-a", backend="aws", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-0.20260525.abc")
    assert len(result) == 1


def test_pre_release_master_always_passes():
    """Master passes the pre_release filter regardless of the resolved version's tag."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-0.20260525.abc")
    assert len(result) == 1


def test_pre_release_rc1_in_resolved_passes():
    """A resolved rc1 version passes when rc1 is listed in pre_release."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_rc3_in_resolved_passes():
    """A resolved rc3 version passes when rc3 is listed in pre_release."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc3-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_non_rc_blocked():
    """A resolved version with no pre-release tag is blocked when pre_release is set."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.20250525.abc")
    assert len(result) == 0


@pytest.mark.parametrize(
    "resolved_version",
    [
        pytest.param("2026.4.0-rc1", id="dash-separator"),
        pytest.param("2026.4.0~rc1", id="tilde-separator"),
        pytest.param("2026.4.0.rc1", id="dot-separator"),
        pytest.param("2026.3.0.rc1.0.20260730.726f67a532e2", id="dot-separator-with-build"),
        pytest.param("2026.4.0~rc1-0.20261026.09174ad4a5a7", id="tilde-separator-with-build"),
    ],
)
def test_pre_release_matches_any_separator(resolved_version):
    """The rc1 tag is matched whatever separator precedes it in the resolved version."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    assert len(filter_jobs(jobs, scylla_version=resolved_version, resolved_version=resolved_version)) == 1


@pytest.mark.parametrize(
    "resolved_version",
    [
        pytest.param("2026.4.0-rc10", id="rc10"),
        pytest.param("2026.4.0~rc11-0.20261026.abc", id="rc11-with-build"),
        pytest.param("2026.4.0.rc13", id="rc13"),
    ],
)
def test_pre_release_tag_does_not_match_longer_number(resolved_version):
    """rc1 and rc3 do not match rc10, rc11 or rc13, since the tag must end at the number."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    assert len(filter_jobs(jobs, scylla_version=resolved_version, resolved_version=resolved_version)) == 0


def test_pre_release_rc2_not_in_list_blocked():
    """A resolved rc2 version is blocked when rc2 is not listed in pre_release."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc2-0.20250525.abc")
    assert len(result) == 0


def test_pre_release_empty_allows_all():
    """An empty pre_release list lets a non-rc resolved version through."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=[])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_fallback_to_scylla_version():
    """Without resolved_version, pre_release is matched against scylla_version."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1"])]
    result = filter_jobs(jobs, scylla_version="2025.1.3-rc1-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_with_include_versions_combined():
    """A job needs both a matching include_versions entry and a matching pre_release tag."""
    jobs = [
        JobConfig(
            job_name="job-a",
            backend="aws",
            include_versions=["master"],
            pre_release=["rc1", "rc3"],
        ),
    ]
    assert len(filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-abc")) == 1
    assert len(filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-abc")) == 0


def test_include_versions_checks_original_exclude_checks_original():
    """include_versions and exclude_versions both use the original scylla_version, not the resolved one."""
    jobs = [
        JobConfig(job_name="included", backend="aws", include_versions=["master"]),
        JobConfig(job_name="excluded", backend="aws", exclude_versions=["master"]),
    ]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-abc")
    assert len(result) == 1
    assert result[0].job_name == "included"


def test_pre_release_checks_resolved_version():
    """pre_release is checked against resolved_version, so the tag decides whether the job is kept."""
    jobs = [JobConfig(job_name="job-a", backend="aws", pre_release=["rc1"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-0.abc")
    assert len(result) == 1
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.abc")
    assert len(result) == 0


def test_disabled_jobs_are_skipped():
    """Jobs marked disabled are dropped from the result."""
    jobs = [
        JobConfig(job_name="active-job", backend="aws"),
        JobConfig(job_name="disabled-job", backend="aws", disabled=True),
    ]
    result = filter_jobs(jobs, scylla_version="2025.4")
    assert len(result) == 1
    assert result[0].job_name == "active-job"


def test_arm64_image_keeps_only_aarch64_jobs(sample_jobs_with_arm):
    """An aarch64 image keeps only the aarch64 jobs, by arch field or by label."""
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch="aarch64")
    assert {j.job_name for j in result} == {"job-arm", "job-arm-weekly", "job-arm-no-label"}


def test_x86_64_image_excludes_aarch64_jobs(sample_jobs_with_arm):
    """An x86_64 image keeps only the x86_64 job and drops the aarch64 jobs."""
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch="x86_64")
    assert {j.job_name for j in result} == {"job-x86"}


def test_no_image_arch_preserves_all_jobs(sample_jobs_with_arm):
    """Without image_arch no job is dropped for its architecture."""
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch=None)
    assert len(result) == 4


def test_arch_field_takes_precedence_over_missing_label():
    """The arch field decides the architecture, so a job with an aarch64 label but no arch field still matches."""
    jobs = [
        JobConfig(job_name="arm-via-field", backend="aws", arch="aarch64", labels=["weekly"]),
        JobConfig(job_name="x86-via-field", backend="aws", arch="x86_64", labels=["weekly"]),
        JobConfig(job_name="arm-via-label", backend="aws", labels=["aarch64"]),
    ]
    result = filter_jobs(jobs, scylla_version="2025.4", image_arch="aarch64")
    assert {j.job_name for j in result} == {"arm-via-field", "arm-via-label"}

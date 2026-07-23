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


from sdcm.utils.trigger_matrix import JobConfig, filter_jobs


def test_no_filters_returns_all(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4")
    assert len(result) == 4


def test_version_exclusion_always_applies(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="master:latest")
    assert len(result) == 3
    assert all(j.job_name != "job-d" for j in result)


def test_filter_by_labels_single(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly")
    assert len(result) == 2
    assert all("weekly" in j.labels for j in result)


def test_filter_by_labels_multiple_and_logic(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4", labels_selector="weekly,additional")
    assert len(result) == 1
    assert result[0].job_name == "job-b"


def test_filter_by_backend(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4", backend="aws")
    assert len(result) == 2
    assert all(j.backend == "aws" for j in result)


def test_filter_by_version_exclusion(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2024.1.5")
    assert len(result) == 3
    assert all(j.job_name != "job-b" for j in result)


def test_filter_by_version_exclusion_master(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="master:latest")
    assert len(result) == 3
    assert all(j.job_name != "job-d" for j in result)


def test_filter_skip_list(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4", skip_jobs=["job-a", "job-c"])
    assert len(result) == 2
    names = {j.job_name for j in result}
    assert names == {"job-b", "job-d"}


def test_no_selector_returns_all_eligible(sample_jobs):
    result = filter_jobs(sample_jobs, scylla_version="2025.4")
    assert len(result) == 4


def test_combined_filters(sample_jobs):
    result = filter_jobs(
        sample_jobs,
        scylla_version="2025.4",
        labels_selector="weekly",
        backend="aws",
    )
    assert len(result) == 1
    assert result[0].job_name == "job-a"


def test_include_versions_passes_matching_prefix():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="master:latest")
    assert len(result) == 1


def test_include_versions_blocks_non_matching():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest")
    assert len(result) == 0


def test_include_versions_prefix_match():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=["2025.1"])]
    result = filter_jobs(jobs, scylla_version="2025.1.3:latest")
    assert len(result) == 1


def test_include_versions_multiple():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=["master", "2025.1"])]
    assert len(filter_jobs(jobs, scylla_version="master:latest")) == 1
    assert len(filter_jobs(jobs, scylla_version="2025.1:latest")) == 1
    assert len(filter_jobs(jobs, scylla_version="2024.2:latest")) == 0


def test_empty_include_versions_allows_all():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=[])]
    assert len(filter_jobs(jobs, scylla_version="anything")) == 1


def test_include_versions_uses_original_not_resolved():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", include_versions=["master"])]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-0.20260525.abc")
    assert len(result) == 1


def test_pre_release_master_always_passes():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-0.20260525.abc")
    assert len(result) == 1


def test_pre_release_rc1_in_resolved_passes():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_rc3_in_resolved_passes():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc3-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_non_rc_blocked():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.20250525.abc")
    assert len(result) == 0


def test_pre_release_rc2_not_in_list_blocked():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1", "rc3"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc2-0.20250525.abc")
    assert len(result) == 0


def test_pre_release_empty_allows_all():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=[])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_fallback_to_scylla_version():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1"])]
    result = filter_jobs(jobs, scylla_version="2025.1.3-rc1-0.20250525.abc")
    assert len(result) == 1


def test_pre_release_with_include_versions_combined():
    jobs = [
        JobConfig(
            job_name="job-a",
            backend="aws",
            region="",
            include_versions=["master"],
            pre_release=["rc1", "rc3"],
        ),
    ]
    assert len(filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-abc")) == 1
    assert len(filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-abc")) == 0


def test_include_versions_checks_original_exclude_checks_original():
    jobs = [
        JobConfig(job_name="included", backend="aws", region="", include_versions=["master"]),
        JobConfig(job_name="excluded", backend="aws", region="", exclude_versions=["master"]),
    ]
    result = filter_jobs(jobs, scylla_version="master:latest", resolved_version="2026.3.0~dev-abc")
    assert len(result) == 1
    assert result[0].job_name == "included"


def test_pre_release_checks_resolved_version():
    jobs = [JobConfig(job_name="job-a", backend="aws", region="", pre_release=["rc1"])]
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-rc1-0.abc")
    assert len(result) == 1
    result = filter_jobs(jobs, scylla_version="2025.1:latest", resolved_version="2025.1.3-0.abc")
    assert len(result) == 0


def test_disabled_jobs_are_skipped():
    jobs = [
        JobConfig(job_name="active-job", backend="aws", region=""),
        JobConfig(job_name="disabled-job", backend="aws", region="", disabled=True),
    ]
    result = filter_jobs(jobs, scylla_version="2025.4")
    assert len(result) == 1
    assert result[0].job_name == "active-job"


def test_arm64_image_keeps_only_aarch64_jobs(sample_jobs_with_arm):
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch="aarch64")
    assert {j.job_name for j in result} == {"job-arm", "job-arm-weekly", "job-arm-no-label"}


def test_x86_64_image_excludes_aarch64_jobs(sample_jobs_with_arm):
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch="x86_64")
    assert {j.job_name for j in result} == {"job-x86"}


def test_no_image_arch_preserves_all_jobs(sample_jobs_with_arm):
    result = filter_jobs(sample_jobs_with_arm, scylla_version="2025.4", image_arch=None)
    assert len(result) == 4


def test_arch_field_takes_precedence_over_missing_label():
    jobs = [
        JobConfig(job_name="arm-via-field", backend="aws", region="", arch="aarch64", labels=["weekly"]),
        JobConfig(job_name="x86-via-field", backend="aws", region="", arch="x86_64", labels=["weekly"]),
        JobConfig(job_name="arm-via-label", backend="aws", region="", labels=["aarch64"]),
    ]
    result = filter_jobs(jobs, scylla_version="2025.4", image_arch="aarch64")
    assert {j.job_name for j in result} == {"arm-via-field", "arm-via-label"}

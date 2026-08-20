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

from sdcm.utils.trigger_matrix import (
    BackendTarget,
    JobConfig,
    TriggerMatrixError,
    _as_branch_qualifier,
    _gce_label_to_version,
    is_resolvable_partial_version,
    job_uses_scylla_version,
    resolve_versions_for_targets,
    split_regions,
    target_for_job,
    trigger_matrix,
    version_exists_for_backend,
)


AWS_BUILD = "2026.4.0~dev-0.20260804.9a3aba9e452a"
GCE_BUILD = "2026.4.0~dev-0.20260803.1122334455aa"

AWS_TARGET = BackendTarget("aws", "eu-west-1")
GCE_TARGET = BackendTarget("gce")
AZURE_TARGET = BackendTarget("azure", "eastus")


@pytest.mark.parametrize(
    ("label", "expected"),
    [
        pytest.param("2026-4-0-dev-0-20260804-9a3aba9e452a", "2026.4.0~dev-0.20260804.9a3aba9e452a", id="dev"),
        pytest.param("2026-3-0-rc1-0-20260730-726f67a532e2", "2026.3.0.rc1.0.20260730.726f67a532e2", id="rc"),
        pytest.param("2025-4-10-0-20260609-99f4121cd8e1", "2025.4.10-0.20260609.99f4121cd8e1", id="release"),
        pytest.param("2025-4-10", "", id="not_a_build"),
        pytest.param("garbage", "", id="garbage"),
    ],
)
def test_gce_label_converted_back_to_version_tag(label, expected):
    assert _gce_label_to_version(label) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        pytest.param("master:latest", "master:latest", id="branch_qualifier"),
        pytest.param("2025.4", "branch-2025.4:latest", id="two_part_version"),
        pytest.param("2025.4.0", "branch-2025.4:latest", id="three_part_version"),
        pytest.param("master", "master", id="bare_branch_left_alone"),
    ],
)
def test_partial_version_normalized_to_branch_qualifier(version, expected):
    assert _as_branch_qualifier(version) == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        pytest.param("master:latest", True, id="branch_qualifier"),
        pytest.param("2025.4", True, id="simple_version"),
        pytest.param(AWS_BUILD, False, id="full_tag"),
        pytest.param("2026.1.8", False, id="release_version"),
        pytest.param("master", False, id="bare_branch"),
        pytest.param("", False, id="empty"),
    ],
)
def test_only_open_ended_versions_need_resolution(version, expected):
    assert is_resolvable_partial_version(version) is expected


def test_per_backend_gives_every_backend_its_own_build():
    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve:
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else GCE_BUILD
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET],
            strategy="per-backend",
        )

    assert versions == {AWS_TARGET: AWS_BUILD, GCE_TARGET: GCE_BUILD}
    assert not unavailable


def test_per_backend_skips_a_backend_without_images():
    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve:
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else ""
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET],
            strategy="per-backend",
        )

    assert versions == {AWS_TARGET: AWS_BUILD}
    assert list(unavailable) == [GCE_TARGET]
    assert "no image found for 'master:latest'" in unavailable[GCE_TARGET]


def test_per_backend_leaves_an_explicit_build_alone():
    """A version that already points at one build is passed through, no lookups needed."""
    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve:
        versions, unavailable = resolve_versions_for_targets(
            original_version=AWS_BUILD,
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET],
            strategy="per-backend",
        )

    assert versions == {AWS_TARGET: AWS_BUILD, GCE_TARGET: AWS_BUILD}
    assert not unavailable
    mock_resolve.assert_not_called()


def test_per_backend_passes_the_request_through_for_backends_without_images():
    """Docker jobs have no image to resolve against — they get the original request."""
    docker_target = BackendTarget("docker")
    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend", return_value=AWS_BUILD):
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, docker_target],
            strategy="per-backend",
        )

    assert versions == {AWS_TARGET: AWS_BUILD, docker_target: "master:latest"}
    assert not unavailable


def test_aws_strict_drops_backends_missing_the_aws_build():
    with patch("sdcm.utils.trigger_matrix.version_exists_for_backend") as mock_exists:
        mock_exists.side_effect = lambda version, backend, region, arch: backend == "aws"
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET],
            strategy="aws-strict",
        )

    assert versions == {AWS_TARGET: AWS_BUILD}
    assert list(unavailable) == [GCE_TARGET]
    assert AWS_BUILD in unavailable[GCE_TARGET]


def test_common_picks_the_newest_build_published_everywhere():
    """AWS is one nightly ahead of GCE, so everyone runs the older build both have."""
    with (
        patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve,
        patch("sdcm.utils.trigger_matrix.version_exists_for_backend") as mock_exists,
    ):
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else GCE_BUILD
        # only the older build exists on both backends
        mock_exists.side_effect = lambda version, backend, region, arch: version == GCE_BUILD or backend == "aws"
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET],
            strategy="common",
        )

    assert versions == {AWS_TARGET: GCE_BUILD, GCE_TARGET: GCE_BUILD}
    assert not unavailable


def test_common_raises_when_no_build_is_shared():
    with (
        patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve,
        patch("sdcm.utils.trigger_matrix.version_exists_for_backend", return_value=False),
    ):
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else GCE_BUILD
        with pytest.raises(TriggerMatrixError, match="is published on all of"):
            resolve_versions_for_targets(
                original_version="master:latest",
                reference_version=AWS_BUILD,
                targets=[AWS_TARGET, GCE_TARGET],
                strategy="common",
            )


def test_common_raises_when_nothing_resolves():
    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend", return_value=""):
        with pytest.raises(TriggerMatrixError, match="Cannot resolve 'master:latest'"):
            resolve_versions_for_targets(
                original_version="master:latest",
                reference_version="",
                targets=[AWS_TARGET, GCE_TARGET],
                strategy="common",
            )


def test_common_verifies_an_explicit_build_on_every_backend():
    with patch("sdcm.utils.trigger_matrix.version_exists_for_backend", return_value=True) as mock_exists:
        versions, unavailable = resolve_versions_for_targets(
            original_version=AWS_BUILD,
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET, GCE_TARGET, AZURE_TARGET],
            strategy="common",
        )

    assert set(versions.values()) == {AWS_BUILD}
    assert not unavailable
    assert mock_exists.call_count == 3


def test_unknown_strategy_is_rejected():
    with pytest.raises(TriggerMatrixError, match="Unknown version resolution strategy"):
        resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[AWS_TARGET],
            strategy="whatever",
        )


def test_docker_backend_is_always_considered_available():
    assert version_exists_for_backend(AWS_BUILD, "docker") is True


def test_target_carries_backend_region_and_arch():
    job = JobConfig(job_name="perf-i8g", backend="aws", params={"region": "eu-west-2"}, arch="aarch64")
    assert target_for_job(job) == BackendTarget("aws", "eu-west-2", "aarch64")
    assert str(target_for_job(job)) == "aws/eu-west-2/aarch64"


def test_target_arch_falls_back_to_the_aarch64_label():
    """Jobs predating the `arch` field only say so through a label — resolve them on ARM images."""
    job = JobConfig(job_name="job", backend="aws", params={"region": "eu-west-2"}, labels=["aarch64"])
    assert target_for_job(job).arch == "aarch64"
    assert target_for_job(JobConfig(job_name="job", backend="aws")).arch == "x86_64"


def test_the_job_region_wins_over_the_region_override():
    """SCT-693: `--region` fills in for jobs that don't pin one, it never overwrites one."""
    pinned = JobConfig(job_name="job", backend="aws", params={"region": "eu-west-1"})
    assert target_for_job(pinned, region_override="us-east-1").region == "eu-west-1"

    unpinned = JobConfig(job_name="job", backend="aws")
    assert target_for_job(unpinned, region_override="us-east-1").region == "us-east-1"


def test_target_region_falls_back_to_the_matrix_defaults():
    job = JobConfig(job_name="job", backend="aws")
    assert target_for_job(job, {"region": "eu-west-2"}).region == "eu-west-2"


@pytest.mark.parametrize(
    ("region", "expected"),
    [
        pytest.param("eu-west-1", ["eu-west-1"], id="single"),
        pytest.param('["eu-west-1", "eu-west-2"]', ["eu-west-1", "eu-west-2"], id="json_list"),
        pytest.param("eu-west-1,eu-west-2", ["eu-west-1", "eu-west-2"], id="comma_separated"),
        pytest.param("", [], id="empty"),
        pytest.param("[not json", ["[not", "json"], id="broken_list_falls_back"),
    ],
)
def test_region_values_are_split(region, expected):
    assert split_regions(region) == expected


def test_multi_dc_job_keeps_all_its_regions():
    job = JobConfig(job_name="multidc", backend="aws", params={"region": '["eu-west-1", "eu-west-2"]'})
    assert target_for_job(job) == BackendTarget("aws", "eu-west-1,eu-west-2")


def test_multi_dc_build_must_exist_in_every_region():
    """SCTConfiguration looks up an AMI per region, so one missing region is a no-go."""
    with patch("sdcm.utils.trigger_matrix._version_exists_in_region") as mock_exists:
        mock_exists.side_effect = lambda version, backend, region, arch: region == "eu-west-1"
        assert version_exists_for_backend(AWS_BUILD, "aws", "eu-west-1") is True
        assert version_exists_for_backend(AWS_BUILD, "aws", '["eu-west-1", "eu-west-2"]') is False


def test_multi_dc_latest_resolved_in_the_first_region():
    with patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami", return_value=AWS_BUILD) as mock_resolve:
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[BackendTarget("aws", "eu-west-1,eu-west-2")],
            strategy="per-backend",
        )

    assert not unavailable
    assert set(versions.values()) == {AWS_BUILD}
    mock_resolve.assert_called_once_with("master:latest", "eu-west-1", "x86_64")


def test_multi_dc_job_skipped_until_the_build_reaches_every_region():
    multi_dc = BackendTarget("aws", "eu-west-1,eu-west-2")
    with (
        patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami", return_value=AWS_BUILD),
        patch("sdcm.utils.trigger_matrix._version_exists_in_region") as mock_exists,
    ):
        mock_exists.side_effect = lambda version, backend, region, arch: region == "eu-west-1"
        versions, unavailable = resolve_versions_for_targets(
            original_version="master:latest",
            reference_version=AWS_BUILD,
            targets=[multi_dc],
            strategy="per-backend",
        )

    assert not versions
    assert "not published in every region" in unavailable[multi_dc]


def test_gce_target_has_no_region():
    """GCE images are global — resolving them per region would only duplicate lookups."""
    job = JobConfig(job_name="job", backend="gce", params={"region": "us-east1"})
    assert target_for_job(job) == BackendTarget("gce", "", "x86_64")


@pytest.mark.parametrize(
    ("defaults", "params", "expected"),
    [
        pytest.param({}, {}, True, id="regular_job"),
        pytest.param({"rolling_upgrade_test": "true"}, {}, False, id="rolling_upgrade_from_defaults"),
        pytest.param({}, {"rolling_upgrade_test": "true"}, False, id="rolling_upgrade_from_job"),
        pytest.param({}, {"unified_package": "http://example.com/x.tar.gz"}, False, id="pgo_unified_package"),
    ],
)
def test_jobs_that_dont_install_from_an_image_need_no_version(defaults, params, expected):
    job = JobConfig(job_name="job", backend="aws", params={"region": "eu-west-1"} | params)
    assert job_uses_scylla_version(job, defaults) is expected


def _write_matrix(tmp_path, version_resolution=None):
    data = {
        "defaults": {"provision_type": "on_demand"},
        "jobs": [
            {"job_name": "tier1/aws-test", "backend": "aws", "params": {"region": "eu-west-1"}},
            {"job_name": "tier1/gce-test", "backend": "gce", "params": {"region": "us-east1"}},
        ],
    }
    if version_resolution:
        data["version_resolution"] = version_resolution
    path = tmp_path / "matrix.yaml"
    path.write_text(yaml.dump(data))
    return path


def test_trigger_matrix_stamps_each_backend_with_its_own_build(tmp_path):
    """SCT-665: a GCE job must not be handed a version that only AWS published."""
    matrix_file = _write_matrix(tmp_path)

    with patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve:
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else GCE_BUILD
        results = trigger_matrix(
            matrix_file=str(matrix_file),
            scylla_version=AWS_BUILD,
            filter_version="master:latest",
            job_folder="scylla-master",
            dry_run=True,
        )

    assert results["versions"] == {
        "scylla-master/tier1/aws-test": AWS_BUILD,
        "scylla-master/tier1/gce-test": GCE_BUILD,
    }
    assert len(results["triggered"]) == 2


def test_trigger_matrix_aws_strict_skips_backends_without_the_build(tmp_path):
    matrix_file = _write_matrix(tmp_path, version_resolution="aws-strict")

    with patch("sdcm.utils.trigger_matrix.version_exists_for_backend") as mock_exists:
        mock_exists.side_effect = lambda version, backend, region, arch: backend == "aws"
        results = trigger_matrix(
            matrix_file=str(matrix_file),
            scylla_version=AWS_BUILD,
            filter_version="master:latest",
            job_folder="scylla-master",
            dry_run=True,
        )

    assert results["triggered"] == ["scylla-master/tier1/aws-test"]
    assert "tier1/gce-test" in results["skipped"]
    assert results["versions"] == {"scylla-master/tier1/aws-test": AWS_BUILD}


def test_trigger_matrix_common_runs_one_build_everywhere(tmp_path):
    matrix_file = _write_matrix(tmp_path, version_resolution="common")

    with (
        patch("sdcm.utils.trigger_matrix._resolve_latest_version_for_backend") as mock_resolve,
        patch("sdcm.utils.trigger_matrix.version_exists_for_backend") as mock_exists,
    ):
        mock_resolve.side_effect = lambda version, backend, region, arch: AWS_BUILD if backend == "aws" else GCE_BUILD
        mock_exists.side_effect = lambda version, backend, region, arch: version == GCE_BUILD or backend == "aws"
        results = trigger_matrix(
            matrix_file=str(matrix_file),
            scylla_version=AWS_BUILD,
            filter_version="master:latest",
            job_folder="scylla-master",
            dry_run=True,
        )

    assert set(results["versions"].values()) == {GCE_BUILD}
    assert len(results["triggered"]) == 2


def test_cli_strategy_overrides_the_matrix_file(tmp_path):
    matrix_file = _write_matrix(tmp_path, version_resolution="common")

    with patch("sdcm.utils.trigger_matrix.version_exists_for_backend") as mock_exists:
        mock_exists.side_effect = lambda version, backend, region, arch: backend == "aws"
        results = trigger_matrix(
            matrix_file=str(matrix_file),
            scylla_version=AWS_BUILD,
            filter_version="master:latest",
            job_folder="scylla-master",
            dry_run=True,
            version_resolution="aws-strict",
        )

    # `common` would have raised — aws-strict just drops the GCE job
    assert results["triggered"] == ["scylla-master/tier1/aws-test"]

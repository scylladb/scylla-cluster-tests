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
# Copyright (c) 2021 ScyllaDB

from unittest.mock import patch

import pytest

from utils.get_supported_scylla_base_versions import UpgradeBaseVersion

url_base = "http://downloads.scylladb.com/unstable/scylla"


def general_test(scylla_repo="", linux_distro="", cloud_provider=None, base_version_all_sts_versions=False):
    """Return the base-version list that UpgradeBaseVersion selects for the given repo and distro."""
    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None, base_version_all_sts_versions)
    version_detector.set_start_support_version(cloud_provider)
    _, version_list = version_detector.get_version_list()
    return version_list


@pytest.mark.parametrize(
    "test_case",
    [
        {
            "target_branch": "2025.1",
            "target_rc_only": False,
            "base_version_all_sts_versions": False,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2024.1", "2025.1", "2024.2"},
        },
        {
            "target_branch": "2025.2",
            "target_rc_only": False,
            "base_version_all_sts_versions": False,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2"},
        },
        {
            "target_branch": "2025.3",
            "target_rc_only": False,
            "base_version_all_sts_versions": False,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3"},
        },
        {
            "target_branch": "2025.4",
            "target_rc_only": True,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3"},
        },
        {
            "target_branch": "2025.4",
            "target_rc_only": False,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3", "2025.4"},
        },
        {
            "target_branch": "2026.1",
            "target_rc_only": True,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3", "2025.4"},
        },
        {
            "target_branch": "2026.1",
            "target_rc_only": False,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3", "2025.4", "2026.1"},
        },
        {
            "target_branch": "2026.1",
            "target_rc_only": False,
            "base_version_all_sts_versions": False,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"],
            "expected_version_set": {"2025.1", "2025.4", "2026.1"},
        },
    ],
)
def test_upgrade_matrix_stages(test_case):
    """
    Test that target branch release on Ubuntu returns correct versions while mocking available versions.

    This test validates the upgrade matrix logic by testing different scenarios:

    Args:
        test_case (dict): A dictionary containing:
            - target_branch (str): The target Scylla version branch being tested (e.g., "2025.1")
            - target_rc_only (bool): Whether only RC (release candidate) versions are available for the target branch
            - supported_versions (list): List of all Scylla versions that are mocked to be available in the system
            - expected_version_set (set): Expected set of versions that should be returned as valid upgrade paths
            - base_version_all_sts_versions(bool, optional): Whether to consider all STS versions as base versions (default is False)

    The test logic:
    - When target_rc_only is True, it simulates that only RC versions exist for the target branch
    - When target_rc_only is False, it simulates that stable releases exist for the target branch
    - The upgrade matrix should return appropriate versions based on upgrade compatibility rules

    ``unsupported_versions`` is emptied for the duration of the test so the cases keep describing
    the selection algorithm alone: skipping a specific release (see ``test_unsupported_version_is_skipped``)
    is an operational decision that changes over time and must not silently rewrite these expectations.
    """
    target_branch = test_case["target_branch"]
    target_rc_only = test_case["target_rc_only"]
    supported_versions = test_case["supported_versions"]
    expected_version_set = test_case["expected_version_set"]
    base_version_all_sts_versions = test_case.get("base_version_all_sts_versions", False)

    scylla_repo = url_base + f"/branch-{target_branch}/deb/unified/latest/scylladb-{target_branch}/scylla.list"
    linux_distro = "ubuntu-focal"
    # Mock get_all_versions and get_s3_scylla_repos_mapping to simulate available versions

    mock_versions = supported_versions
    mock_repo_map = {v: f"mock_url/{v}" for v in mock_versions}
    if target_rc_only:
        mock_versions = [
            f"{target_branch}.0~rc1",
        ]
    with (
        patch("utils.get_supported_scylla_base_versions.get_all_versions", return_value=mock_versions),
        patch("utils.get_supported_scylla_base_versions.get_s3_scylla_repos_mapping", return_value=mock_repo_map),
        patch("utils.get_supported_scylla_base_versions.unsupported_versions", []),
    ):
        version_list = general_test(
            scylla_repo, linux_distro, base_version_all_sts_versions=base_version_all_sts_versions
        )
    assert set(version_list) == expected_version_set


def test_unsupported_version_is_skipped():
    """A release listed in ``unsupported_versions`` is never returned as an upgrade base version.

    2025.2 is skipped that way: its images carry a baked-in unstable repo snapshot that is pruned
    from S3 by now, so the rollback step of the rolling upgrade test cannot reinstall it (SCYLLADB-3508).
    """
    target_branch = "2026.1"
    supported_versions = ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"]
    scylla_repo = url_base + f"/branch-{target_branch}/deb/unified/latest/scylladb-{target_branch}/scylla.list"
    mock_repo_map = {v: f"mock_url/{v}" for v in supported_versions}

    with (
        patch("utils.get_supported_scylla_base_versions.get_all_versions", return_value=supported_versions),
        patch("utils.get_supported_scylla_base_versions.get_s3_scylla_repos_mapping", return_value=mock_repo_map),
        patch("utils.get_supported_scylla_base_versions.unsupported_versions", ["2025.2"]),
    ):
        version_list = general_test(scylla_repo, "ubuntu-focal", base_version_all_sts_versions=True)

    assert set(version_list) == {"2025.1", "2025.3", "2025.4", "2026.1"}


def test_master_all_sts_versions():
    """Test master branch returns both last LTS and last STS when base_version_all_sts_versions is True."""
    scylla_repo = url_base + "/master/rpm/centos/latest/scylla.repo"
    linux_distro = "centos"
    # Provide a repo map with multiple enterprise versions including LTS and STS
    mock_supported_versions = [
        "2024.1",  # LTS previous year
        "2024.2",  # STS
        "2024.5",  # STS later in year
        "2025.1",  # LTS current year
        "2025.2",  # Latest STS
    ]
    mock_repo_map = {v: f"mock_url/{v}" for v in mock_supported_versions}
    # get_all_versions should return at least one non-rc artifact per version
    with (
        patch(
            "utils.get_supported_scylla_base_versions.get_all_versions",
            return_value=["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"],
        ),
        patch("utils.get_supported_scylla_base_versions.get_s3_scylla_repos_mapping", return_value=mock_repo_map),
        # keep this an algorithm test: the real skip list changes over time, see test_unsupported_version_is_skipped
        patch("utils.get_supported_scylla_base_versions.unsupported_versions", []),
    ):
        version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None, base_version_all_sts_versions=True)
        version_detector.set_start_support_version(None)
        _, version_list = version_detector.get_version_list()
    # Expect last LTS (2025.1) and last STS (2025.2)
    assert set(version_list) == {"2025.1", "2025.2"}

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

from sdcm.utils.version_utils import ComparableScyllaVersion
from utils.get_supported_scylla_base_versions import UpgradeBaseVersion

"""
This module contains tests for the UpgradeBaseVersion class, which is used to determine
the supported Scylla versions for upgrades based on the provided repository, backend, and Linux distribution.
"""


def general_test(scylla_repo='', linux_distro='', cloud_provider=None, base_version_all_sts_versions=False):
    """
    General test function to retrieve the list of supported Scylla versions for upgrade.
    """
    scylla_version = None

    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, scylla_version, base_version_all_sts_versions)
    version_detector.set_start_support_version(cloud_provider)
    _, version_list = version_detector.get_version_list()
    return version_list


download_url_base = 'http://downloads.scylladb.com'
url_base = f'{download_url_base}/unstable/scylla'


def test_master_rpm():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + '/master/rpm/centos/2021-08-29T00:58:58Z/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_master_deb():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + '/master/deb/unified/2025-02-16T22:46:42Z/scylladb-2025.1/scylla.list'
    linux_distro = 'ubuntu-jammy'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_2024_1_with_centos9():
    """ Test that development branch of 2024.1 with centos9 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.1/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'5.4', '2024.1'}


def test_2024_1():
    """ Test that development branch of 2024.1 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.1/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'5.4', '2023.1', '2024.1'}


def test_2024_2():
    """ Test that development branch of 2024.2 is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.2/rpm/centos/latest/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'6.0', '2024.1', '2024.2'}


def test_2024_2_ubuntu():
    """ Test that development branch of 2024.2 on ubuntu is returned correct versions """
    scylla_repo = url_base + '-enterprise/enterprise-2024.2/deb/unified/latest/scylladb-2024.2/scylla.list'
    linux_distro = 'ubuntu-focal'
    version_list = general_test(scylla_repo, linux_distro)
    assert set(version_list) == {'6.0', '2024.1', '2024.2'}


def test_2025_1_dev():
    """ Test that development branch of 2025.1 is returned correct versions """
    scylla_repo = url_base + '/master/rpm/centos/2025-01-15T09:25:01Z/scylla.repo'
    linux_distro = 'centos'
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= '2025.1'


def test_2025_1_release_ubuntu():
    """ Test that 2025.1 release on Ubuntu returns correct versions """
    scylla_repo = url_base + '/branch-2025.1/deb/unified/2025-02-16T22:46:42Z/scylladb-2025.1/scylla.list'
    linux_distro = 'ubuntu-focal'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'6.2', '2024.1', '2024.2'}.issubset(set(version_list))


def test_2025_1_release_rocky():
    """ Test that 2025.1 release on centos is returned correct versions """
    scylla_repo = url_base + '/branch-2025.4/rpm/centos/2025-02-23T16:19:08Z/scylla.repo'
    linux_distro = 'rocky-10'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'2025.3'} == set(version_list)


def test_2025_1_release_centos():
    """ Test that 2025.1 release on centos is returned correct versions """
    scylla_repo = url_base + '/branch-2025.1/rpm/centos/2025-02-23T16:19:08Z/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'6.2', '2024.1', '2024.2'}.issubset(set(version_list))


def test_2025_3_release_ubuntu():
    """ Test that 2025.3 release on Ubuntu returns correct versions """
    scylla_repo = url_base + '/branch-2025.3/deb/unified/2026-02-16T22:46:42Z/scylladb-2025.3/scylla.list'
    linux_distro = 'ubuntu-focal'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'2025.1', '2025.2', '2025.3'}.issubset(set(version_list))


def test_2025_3_release_centos():
    """ Test that 2025.3 release on centos returns correct versions """
    scylla_repo = url_base + '/branch-2025.3/rpm/centos/2025-02-23T16:19:08Z/scylla.repo'
    linux_distro = 'centos-9'
    version_list = general_test(scylla_repo, linux_distro)
    assert {'2025.1', '2025.2', '2025.3'}.issubset(set(version_list))


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
            "expected_version_set": {"2025.1", "2025.2"}},
        {
            "target_branch": "2025.3",
            "target_rc_only": False,
            "base_version_all_sts_versions": False,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3"}
        },
        {
            "target_branch": "2025.4",
            "target_rc_only": True,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3"}
        },
        {
            "target_branch": "2025.4",
            "target_rc_only": False,
            "base_version_all_sts_versions": True,
            "supported_versions": ["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4"],
            "expected_version_set": {"2025.1", "2025.2", "2025.3", "2025.4"}
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
    """
    target_branch = test_case["target_branch"]
    target_rc_only = test_case["target_rc_only"]
    supported_versions = test_case["supported_versions"]
    expected_version_set = test_case["expected_version_set"]
    base_version_all_sts_versions = test_case.get("base_version_all_sts_versions", False)

    scylla_repo = url_base + \
        f"/branch-{target_branch}/deb/unified/2026-02-16T22:46:42Z/scylladb-{target_branch}/scylla.list"
    linux_distro = 'ubuntu-focal'
    # Mock get_all_versions and get_s3_scylla_repos_mapping to simulate available versions

    mock_versions = supported_versions
    mock_repo_map = {v: f"mock_url/{v}" for v in mock_versions}
    if target_rc_only:
        mock_versions = [f"{target_branch}.0~rc1", ]
    with patch('utils.get_supported_scylla_base_versions.get_all_versions', return_value=mock_versions), \
            patch('utils.get_supported_scylla_base_versions.get_s3_scylla_repos_mapping', return_value=mock_repo_map):
        version_list = general_test(scylla_repo, linux_distro,
                                    base_version_all_sts_versions=base_version_all_sts_versions)
    assert set(version_list) == expected_version_set


def test_master_all_sts_versions():
    """Test master branch returns both last LTS and last STS when base_version_all_sts_versions is True."""
    scylla_repo = url_base + '/master/rpm/centos/2025-08-01T00:00:00Z/scylla.repo'
    linux_distro = 'centos'
    # Provide a repo map with multiple enterprise versions including LTS and STS
    mock_supported_versions = [
        '2024.1',  # LTS previous year
        '2024.2',  # STS
        '2024.5',  # STS later in year
        '2025.1',  # LTS current year
        '2025.2',  # Latest STS
    ]
    mock_repo_map = {v: f'mock_url/{v}' for v in mock_supported_versions}
    # get_all_versions should return at least one non-rc artifact per version
    with patch('utils.get_supported_scylla_base_versions.get_all_versions', return_value=["2024.1", "2024.2", "2025.1", "2025.2", "2025.3", "2025.4", "2026.1"]), \
            patch('utils.get_supported_scylla_base_versions.get_s3_scylla_repos_mapping', return_value=mock_repo_map):
        version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None, base_version_all_sts_versions=True)
        version_detector.set_start_support_version(None)
        _, version_list = version_detector.get_version_list()
    # Expect last LTS (2025.1) and last STS (2025.2)
    assert set(version_list) == {'2025.1', '2025.2'}

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
import logging
from unittest.mock import patch

import pytest
import requests
from botocore.exceptions import BotoCoreError, ClientError

from sdcm.utils.parallel_object import ParallelObjectException
from sdcm.utils.version_utils import ComparableScyllaVersion, get_all_versions
from utils.get_supported_scylla_base_versions import (
    UpgradeBaseVersion,
    fetch_official_supported_versions,
)

pytestmark = pytest.mark.integration

LOGGER = logging.getLogger(__name__)

"""
This module contains tests for the UpgradeBaseVersion class, which is used to determine
the supported Scylla versions for upgrades based on the provided repository, backend, and Linux distribution.

The version-specific coverage is driven off the list of *currently supported* (live) ScyllaDB
releases published by the ScyllaDB docs homepage (see fetch_official_supported_versions). This keeps
the tests correct as versions roll: they never hardcode EOL versions (which point at repositories that
have since been removed and cause the tests to fail), and they assert invariants instead of exact
version sets that would churn over time.

The download S3 buckets (downloads.scylladb.com) are cleaned up regularly: EOL releases and old
timestamped artifacts get removed. Tests must therefore NOT hardcode values (timestamps or specific
release versions) - those rot the moment the buckets are cleaned and cause spurious CI failures. That
is why versions are derived dynamically from the docs-homepage source of truth and repo URLs use the
``latest`` branch path instead of a pinned timestamp (the URL is only parsed for its version string,
never fetched, so ``latest`` is sufficient and never goes stale).
"""


def general_test(scylla_repo="", linux_distro="", cloud_provider=None, base_version_all_sts_versions=False):
    """
    General test function to retrieve the list of supported Scylla versions for upgrade.
    """
    scylla_version = None

    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, scylla_version, base_version_all_sts_versions)
    version_detector.set_start_support_version(cloud_provider)
    _, version_list = version_detector.get_version_list()
    return version_list


download_url_base = "http://downloads.scylladb.com"
url_base = f"{download_url_base}/unstable/scylla"

# Transient/connectivity errors that should skip an integration test rather than fail it. These are
# raised by the network paths the tests exercise: boto3/S3 listing (botocore) during
# ``UpgradeBaseVersion`` construction, and requests/ParallelObject while reading repositories. Real
# assertion failures and logic errors are AssertionError/other types and are NOT in this tuple, so
# they are never swallowed.
TRANSIENT_NETWORK_ERRORS = (BotoCoreError, ClientError, requests.RequestException, ParallelObjectException)


def _build_target_repo(version: str, linux_distro: str) -> str:
    """Build an unstable target repo URL for the given release and distro.

    The ``latest`` path is used on purpose: the URL is only parsed for its version string (never
    fetched), so there is no need for a concrete timestamp - and a pinned timestamp would rot once the
    download S3 buckets get their regular cleanup.
    """
    if linux_distro.split("-")[0] in ("centos", "rocky", "rhel"):
        return f"{url_base}/branch-{version}/rpm/centos/latest/scylla.repo"
    return f"{url_base}/branch-{version}/deb/unified/latest/scylladb-{version}/scylla.list"


@pytest.fixture(scope="session")
def live_supported_versions():
    """Session-scoped list of currently-supported (live) ScyllaDB versions.

    Fetched at execution time (never at import/collection) so that test collection stays identical
    across xdist workers regardless of the network outcome, and so nothing runs when the integration
    marker is deselected. On a fetch failure an empty list is returned and the consuming test skips.
    """
    try:
        return fetch_official_supported_versions()
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("Could not fetch official supported versions (%s); live-version test will skip", exc)
        return []


def test_master_rpm():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + "/master/rpm/centos/latest/scylla.repo"
    linux_distro = "centos"
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= "2025.1"


def test_master_deb():
    """
    Test that master branch select on specific version for upgrade.
    (not hardcoding the version, since it keep changing)
    """
    scylla_repo = url_base + "/master/deb/unified/latest/scylladb-2025.1/scylla.list"
    linux_distro = "ubuntu-jammy"
    version_list = general_test(scylla_repo, linux_distro)
    assert len(version_list) == 1
    assert ComparableScyllaVersion(version_list[0]) >= "2025.1"


@pytest.mark.parametrize("linux_distro", ["centos-9", "ubuntu-focal"])
def test_live_supported_version_base_versions(linux_distro, live_supported_versions):
    """Base-version detection for every currently-supported (live) ScyllaDB release.

    Instead of hardcoding version sets (which go EOL and point at removed repositories), this test is
    driven off ``fetch_official_supported_versions()`` (obtained via a session-scoped fixture, not at
    import time) and asserts invariants that stay true as versions roll, for each live version:

    - a non-empty base-version list is returned;
    - every returned version has a real repository in the S3 repo mapping;
    - every returned version is <= the target (live) version;
    - the target live version itself is included (it is released, so its repo exists).

    Transient repository/network outages are skipped rather than reported as failures, but genuine
    assertion failures are never masked.
    """
    if not live_supported_versions:
        pytest.skip("could not fetch official supported versions (network issue?)")

    for live_version in live_supported_versions:
        scylla_repo = _build_target_repo(live_version, linux_distro)
        # Construction reaches S3 (boto3.list_objects -> botocore errors) and get_version_list reads
        # repositories (requests/ParallelObject); route genuine connectivity errors to a skip.
        try:
            version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None)
            version_detector.set_start_support_version(None)
            _, version_list = version_detector.get_version_list()
        except TRANSIENT_NETWORK_ERRORS as exc:
            pytest.skip(f"repository/S3 access failed for {live_version}/{linux_distro} (network issue?): {exc}")

        assert version_list, f"no base versions returned for live version {live_version} on {linux_distro}"

        # every returned version must be backed by a real repository in the S3 mapping
        for version in version_list:
            assert version in version_detector.repo_maps, (
                f"base version {version!r} has no repository in the S3 mapping "
                f"(target {live_version}, distro {linux_distro})"
            )

        # no returned base version may be newer than the target version
        for version in version_list:
            assert ComparableScyllaVersion(version) <= live_version, (
                f"base version {version!r} is newer than target {live_version} (distro {linux_distro})"
            )

        # the live target version itself is released, so it must be part of the returned list. If it is
        # missing, distinguish a transient repo outage (skip) from a genuine regression (fail).
        if live_version not in version_list:
            try:
                get_all_versions(version_detector.repo_maps[live_version])
            except TRANSIENT_NETWORK_ERRORS:
                pytest.skip(f"target repository for {live_version} is transiently unreachable")
            pytest.fail(
                f"live version {live_version} is missing from its own base-version list {version_list} "
                f"(distro {linux_distro})"
            )


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
    ):
        version_list = general_test(
            scylla_repo, linux_distro, base_version_all_sts_versions=base_version_all_sts_versions
        )
    assert set(version_list) == expected_version_set


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
    ):
        version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None, base_version_all_sts_versions=True)
        version_detector.set_start_support_version(None)
        _, version_list = version_detector.get_version_list()
    # Expect last LTS (2025.1) and last STS (2025.2)
    assert set(version_list) == {"2025.1", "2025.2"}

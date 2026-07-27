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

import pytest
import requests
from botocore.exceptions import BotoCoreError, ClientError

from sdcm.utils.decorators import retrying
from sdcm.utils.parallel_object import ParallelObjectException
from sdcm.utils.version_utils import ComparableScyllaVersion
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


def general_test(scylla_repo="", linux_distro="", cloud_provider=None):
    """
    General test function to retrieve the list of supported Scylla versions for upgrade.
    """
    scylla_version = None

    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, scylla_version)
    version_detector.set_start_support_version(cloud_provider)
    _, version_list = version_detector.get_version_list()
    return version_list


download_url_base = "http://downloads.scylladb.com"
url_base = f"{download_url_base}/unstable/scylla"

# Errors raised by the network paths these tests exercise: boto3/S3 listing (botocore) during
# ``UpgradeBaseVersion`` construction, and requests/ParallelObject while reading repositories. They
# are retried, never swallowed: if the operation keeps failing the exception is re-raised and the
# test reds, so a genuine regression (including one wrapped in ``ParallelObjectException``) is still
# reported instead of being silently skipped.
RETRIABLE_REPO_ERRORS = (BotoCoreError, ClientError, requests.RequestException, ParallelObjectException)


def _build_target_repo(version: str, linux_distro: str) -> str:
    """Build an unstable target repo URL for the given release and distro.

    The ``latest`` path is used on purpose: the URL is only parsed for its version string (never
    fetched), so there is no need for a concrete timestamp - and a pinned timestamp would rot once the
    download S3 buckets get their regular cleanup.
    """
    if linux_distro.split("-")[0] in ("centos", "rocky", "rhel"):
        return f"{url_base}/branch-{version}/rpm/centos/latest/scylla.repo"
    return f"{url_base}/branch-{version}/deb/unified/latest/scylladb-{version}/scylla.list"


@retrying(n=3, sleep_time=5, allowed_exceptions=RETRIABLE_REPO_ERRORS, message="Retrying repository access")
def get_base_versions(scylla_repo: str, linux_distro: str) -> tuple[UpgradeBaseVersion, list[str]]:
    """Resolve the base-version list for a target repo, retrying transient repository/S3 failures.

    ``UpgradeBaseVersion`` construction reaches S3 (``boto3.list_objects``) and ``get_version_list``
    reads the repositories over HTTP, so both are covered by the retry. Once the retries are
    exhausted the original exception propagates and the test fails - nothing is skipped.
    """
    version_detector = UpgradeBaseVersion(scylla_repo, linux_distro, None)
    version_detector.set_start_support_version(None)
    _, version_list = version_detector.get_version_list()
    return version_detector, version_list


@pytest.fixture(scope="session")
def live_supported_versions():
    """Session-scoped list of currently-supported (live) ScyllaDB versions.

    Fetched at execution time (never at import/collection) so that test collection stays identical
    across xdist workers regardless of the network outcome, and so nothing runs when the integration
    marker is deselected. ``fetch_official_supported_versions`` already retries transient HTTP
    failures internally; anything it still raises is propagated, and an empty result is an error -
    the test must fail rather than run over an empty list.
    """
    versions = fetch_official_supported_versions()
    assert versions, "no officially supported ScyllaDB versions were returned - cannot verify base versions"
    return versions


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
    scylla_repo = url_base + "/master/deb/unified/latest/scylladb-master/scylla.list"
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

    Transient repository/network outages are retried (see ``get_base_versions``); a failure that
    survives the retries reds the test rather than skipping it.
    """
    for live_version in live_supported_versions:
        scylla_repo = _build_target_repo(live_version, linux_distro)
        version_detector, version_list = get_base_versions(scylla_repo, linux_distro)

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

        # the live target version itself is released, so it must be part of the returned list
        assert live_version in version_list, (
            f"live version {live_version} is missing from its own base-version list {version_list} "
            f"(distro {linux_distro})"
        )

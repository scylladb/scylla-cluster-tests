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
# Copyright (c) 2020 ScyllaDB

"""
Integration tests for version_utils that require network access (S3, DockerHub, etc.).
"""

import pytest

from unit_tests.lib.s3_utils import get_latest_branches_from_s3
from sdcm.utils.version_utils import (
    get_branched_repo,
    get_relocatable_pkg_url,
    get_specific_tag_of_docker_image,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.need_network,
]


def _generate_test_params_for_get_branched_repo():
    """
    Generate test parameters dynamically based on available OSS branches in S3.

    This function discovers the latest OSS branches from S3 and creates
    test parameters for various distribution types (centos, ubuntu, debian).

    Returns:
        list: Test parameters as tuples of (scylla_version, distro, expected_repo)
    """
    branches = get_latest_branches_from_s3()
    test_params = []

    # Add tests for OSS branches with centos
    for branch in branches:
        test_params.append((f"{branch}:latest", "centos", f"unstable/scylla/{branch}/rpm/centos/latest/scylla.repo"))

    # Add tests for one OSS branch with ubuntu and debian
    for branch in branches:
        if branch.startswith("branch-"):
            branch_id = branch.replace("branch-", "")
            test_params.append(
                (
                    f"{branch}:latest",
                    "ubuntu",
                    f"unstable/scylla/{branch}/deb/unified/latest/scylladb-{branch_id}/scylla.list",
                )
            )
            test_params.append(
                (
                    f"{branch}:latest",
                    "debian",
                    f"unstable/scylla/{branch}/deb/unified/latest/scylladb-{branch_id}/scylla.list",
                )
            )
            # Test multiple distros only for the first branch
            break

    return test_params


@pytest.mark.parametrize("docker_repo", ["scylladb/scylla-nightly", "scylladb/scylla-enterprise-nightly"])
def test_get_specific_tag_of_docker_image(docker_repo):
    assert get_specific_tag_of_docker_image(docker_repo=docker_repo) != "latest"


@pytest.mark.parametrize(
    "scylla_version,distro,expected_repo",
    _generate_test_params_for_get_branched_repo(),
)
def test_get_branched_repo(scylla_version, distro, expected_repo):
    expected_template = "https://s3.amazonaws.com/downloads.scylladb.com/{}"
    actual_repo = get_branched_repo(scylla_version, distro)
    assert actual_repo == expected_template.format(expected_repo)


@pytest.mark.parametrize(
    "scylla_version, expected_result",
    [
        pytest.param(
            "2025.1.10 with build-id 6facdbdabc830d767b848ff2f47b418350f96a72",
            "https://downloads.scylladb.com/unstable/scylla/branch-2025.1/relocatable/2025-11-21T14:15:54Z/scylla-unified-2025.1.10-0.20251121.cb1f72dc8134.aarch64.tar.gz",
            id="valid build-id for 2025.1.10",
        ),
        pytest.param("6.2.0", None, id="version without build-id"),
        pytest.param("", None, id="empty version string"),
        pytest.param("6.2.0 with build-id 000", None, id="invalid_build-id"),
    ],
)
def test_get_relocatable_pkg_url(scylla_version, expected_result):
    """Test get_relocatable_pkg_url with a valid build-id from staging backtrace service."""
    result = get_relocatable_pkg_url(scylla_version)
    assert result == expected_result

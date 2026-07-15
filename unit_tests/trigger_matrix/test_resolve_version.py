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

from sdcm.utils.trigger_matrix import TriggerMatrixError, resolve_to_full_version


FULL_TAG = "2025.4.1-0.20250601.abc123def456-1"


@pytest.mark.parametrize(
    "full_version",
    [
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", id="release_tag"),
        pytest.param("2026.2.0~dev-0.20260322.f51126483167", id="dev_nightly_tag"),
        pytest.param("2025.1.3-0.20260101.abcdef123456-1", id="full_tag_with_suffix"),
    ],
)
def test_full_version_tag_returned_as_is(full_version):
    result = resolve_to_full_version(full_version)
    assert result == full_version


@pytest.mark.parametrize(
    "release_version",
    [
        pytest.param("2026.1.8", id="release_2026"),
        pytest.param("2025.4.1", id="release_2025"),
        pytest.param("5.2.1", id="oss_release"),
    ],
)
def test_release_version_returned_as_is(release_version):
    result = resolve_to_full_version(release_version)
    assert result == release_version


@patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami")
def test_branch_qualifier_version_passes_as_is(mock_resolve):
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("master:latest")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("master:latest", "eu-west-1")


@patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami")
def test_simple_version_passed_as_is_not_with_latest(mock_resolve):
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("2025.4")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("2025.4", "eu-west-1")


@patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami")
def test_custom_region_forwarded(mock_resolve):
    mock_resolve.return_value = FULL_TAG

    result = resolve_to_full_version("2025.4", region="us-east-1")

    assert result == FULL_TAG
    mock_resolve.assert_called_once_with("2025.4", "us-east-1")


@patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami")
def test_raises_when_version_cannot_be_resolved(mock_resolve):
    mock_resolve.return_value = ""

    with pytest.raises(TriggerMatrixError, match="Cannot resolve '2025.4'"):
        resolve_to_full_version("2025.4")


@patch("sdcm.utils.trigger_matrix._resolve_version_via_branched_ami")
def test_branch_version_fallthrough_raises(mock_resolve):
    mock_resolve.return_value = ""

    with pytest.raises(TriggerMatrixError, match="Cannot resolve 'branch-2025.4:latest'"):
        resolve_to_full_version("branch-2025.4:latest")

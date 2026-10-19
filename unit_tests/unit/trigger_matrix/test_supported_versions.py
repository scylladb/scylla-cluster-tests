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

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from sdcm.utils.trigger_matrix import images
from sdcm.utils.trigger_matrix.errors import TriggerMatrixError
from sdcm.utils.trigger_matrix.images import latest_release_of_line
from sdcm.utils.trigger_matrix.resolution import check_only_supported_versions, supported_releases

FETCH = "utils.get_supported_scylla_base_versions.fetch_official_supported_versions"


def ami(scylla_version):
    return SimpleNamespace(tags=[{"Key": "scylla_version", "Value": scylla_version}])


def test_latest_release_is_the_highest_patch_of_the_line():
    amis = [
        ami("2025.4.2-0.20260101.aaaaaaaaaaaa-1"),
        ami("2025.4.10-0.20260301.bbbbbbbbbbbb-1"),
        ami("2025.4.9-0.20260401.cccccccccccc-1"),
        # "*2025.4*" also matches other lines and release candidates; neither is a 2025.4 release
        ami("2025.40.1-0.20260401.dddddddddddd-1"),
        ami("2025.4.11~rc0-0.20260501.eeeeeeeeeeee-1"),
    ]
    with patch("sdcm.utils.common.get_scylla_ami_versions", return_value=amis):
        assert latest_release_of_line("2025.4") == "2025.4.10"


def test_latest_release_is_empty_without_released_amis():
    with patch("sdcm.utils.common.get_scylla_ami_versions", return_value=[]):
        assert latest_release_of_line("2025.4") == ""


def test_a_failed_ami_lookup_raises_its_cause():
    with (
        patch("sdcm.utils.common.get_scylla_ami_versions", side_effect=RuntimeError("expired AWS token")),
        pytest.raises(RuntimeError, match="expired AWS token"),
    ):
        latest_release_of_line("2025.4")


def test_supported_releases_are_the_latest_release_of_each_supported_line(monkeypatch):
    monkeypatch.setattr(
        images, "latest_release_of_line", lambda line, region: {"2026.1": "2026.1.3", "2025.4": "2025.4.10"}[line]
    )
    with patch(FETCH, return_value=["2026.1", "2025.4"]):
        assert supported_releases() == ["2026.1.3", "2025.4.10"]


def test_supported_releases_fail_when_a_supported_line_has_no_release(monkeypatch):
    monkeypatch.setattr(images, "latest_release_of_line", lambda line, region: "")
    with patch(FETCH, return_value=["2026.1"]), pytest.raises(TriggerMatrixError, match="2026.1"):
        supported_releases()


def test_supported_releases_fail_without_supported_lines():
    with patch(FETCH, return_value=[]), pytest.raises(TriggerMatrixError, match="No officially supported"):
        supported_releases()


@pytest.mark.parametrize(
    "version",
    [
        "2026.3",
        "2026.3.3",
        "2026.3.3-0.20260901.abcdef123456-1",
        "2026.3.0~rc1-0.20260801.abcdef123456",
        "branch-2026.3:latest",
    ],
)
def test_versions_of_a_supported_line_are_accepted(version):
    with patch(FETCH, return_value=["2026.3", "2025.1"]):
        check_only_supported_versions([version, "2025.1.16"])


@pytest.mark.parametrize("version", ["master:latest", "2025.4.3", "2026.30.1", "", "latest"])
def test_other_versions_are_rejected(version):
    with patch(FETCH, return_value=["2026.3", "2025.1"]), pytest.raises(TriggerMatrixError, match="not supported"):
        check_only_supported_versions(["2026.3.3", version])

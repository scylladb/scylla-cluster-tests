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

from sdcm.utils.trigger_matrix import TriggerMatrixError, determine_job_folder, resolve_job_path


@pytest.mark.parametrize(
    "version,expected",
    [
        pytest.param("master:latest", "scylla-master", id="master_latest"),
        pytest.param("master:all", "scylla-master", id="master_all"),
        pytest.param("master", "scylla-master", id="bare_master"),
        pytest.param("2025.4", "scylla-2025.4", id="two_part_version"),
        pytest.param("2025.4.0", "scylla-2025.4", id="three_part_version"),
        pytest.param("5.2.1", "scylla-5.2", id="oss_version"),
        pytest.param("2024.2.5-0.20250221.cb9e2a54ae6d-1", "scylla-2024.2", id="full_tag"),
        pytest.param("2025.1.3-0.20260101.abcdef1234-1", "scylla-2025.1", id="full_tag_2025"),
        pytest.param("2026.3.0.rc0.0.20260719.a64da1e635f3", "scylla-2026.3", id="rc_dot_separator"),
        pytest.param("2026.3.0~rc0.0.20260719.a64da1e635f3", "scylla-2026.3", id="rc_tilde_separator"),
        pytest.param("2026.3.0-rc0.0.20260719.a64da1e635f3", "scylla-2026.3", id="rc_dash_separator"),
        pytest.param("2026.1.0.rc1.0.20260501.abcdef123456", "scylla-2026.1", id="rc1_dot_separator"),
        pytest.param("2024.1.0.rc2.0.20231218.a063c2c16185.1", "scylla-2024.1", id="rc_with_revision"),
    ],
)
def test_version_to_folder(version, expected):
    assert determine_job_folder(version) == expected


def test_explicit_override():
    assert determine_job_folder("master:latest", job_folder="my-folder") == "my-folder"


def test_branch_qualifier():
    assert determine_job_folder("branch-2019.1:all") == "scylla-branch-2019.1"


def test_invalid_version_raises():
    with pytest.raises(TriggerMatrixError, match="Cannot determine job folder"):
        determine_job_folder("not-a-version")


def test_empty_version_raises():
    with pytest.raises(TriggerMatrixError, match="Cannot determine job folder"):
        determine_job_folder("")


def test_relative_path():
    assert resolve_job_path("tier1/longevity-test", "scylla-master") == "scylla-master/tier1/longevity-test"


def test_absolute_path():
    assert resolve_job_path("/scylla-enterprise/perf/test", "scylla-master") == "scylla-enterprise/perf/test"


def test_absolute_path_strips_leading_slash():
    result = resolve_job_path("/enterprise/perf/test", "branch-2025.4")
    assert not result.startswith("/")
    assert result == "enterprise/perf/test"

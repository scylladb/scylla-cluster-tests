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
# Copyright (c) 2025 ScyllaDB

"""
Verify that the version submitted to Argus is consistent across all cloud backends.

GCE image labels cannot contain '~', so they use '-' as separator (e.g., '2025.4.0-rc5').
After .replace("-", ".") this becomes '2025.4.0.rc5'. The Argus submission must normalize
this back to '~' format so it matches what nodes report via package versions.
"""

import unittest.mock

import pytest

from sdcm import sct_config


@pytest.fixture(autouse=True)
def minimal_config(monkeypatch):
    monkeypatch.setenv("SCT_CONFIG_FILES", "unit_tests/test_configs/minimal_test_case.yaml")


@pytest.fixture()
def mock_argus_client():
    """Mock the Argus client, letting update_argus_with_version run its real logic.

    Lets update_argus_with_version run its ARGUS_VERSION_RE parsing logic,
    but mocks the TestConfig/argus client to capture the final version string.
    """
    client = unittest.mock.MagicMock()
    mock_test_config = unittest.mock.MagicMock()
    mock_test_config.argus_client.return_value = client

    with unittest.mock.patch("sdcm.sct_config.TestConfig", return_value=mock_test_config):
        yield client


@pytest.fixture()
def resolve_version(monkeypatch):
    """Set up backend config and resolve version."""

    def _resolve(backend, env_overrides, mock_target, tag_value):
        monkeypatch.setenv("SCT_CLUSTER_BACKEND", backend)
        for key, value in env_overrides.items():
            monkeypatch.setenv(key, value)

        with unittest.mock.patch(mock_target, return_value={"scylla_version": tag_value}):
            conf = sct_config.SCTConfiguration()
            conf.verify_configuration()
            conf.get_version_based_on_conf()

    return _resolve


@pytest.mark.parametrize(
    "gce_tag,expected_argus_version",
    [
        pytest.param("2025.4.0-rc5", "2025.4.0~rc5", id="rc"),
        pytest.param("2025.4.0-dev", "2025.4.0~dev", id="dev"),
        pytest.param("2025.4.0", "2025.4.0", id="ga"),
        pytest.param("6.2.1-rc3", "6.2.1~rc3", id="enterprise-rc"),
    ],
)
@pytest.mark.parametrize(
    "backend,env_overrides,mock_target,use_gce_tag",
    [
        pytest.param("aws", {"SCT_AMI_ID_DB_SCYLLA": "ami-test123"}, "sdcm.sct_config.get_ami_tags", False, id="aws"),
        pytest.param(
            "gce",
            {"SCT_GCE_IMAGE_DB": "projects/test/global/images/scylla-test"},
            "sdcm.sct_config.get_gce_image_tags",
            True,
            id="gce",
        ),
        pytest.param(
            "azure",
            {"SCT_AZURE_IMAGE_DB": "/subscriptions/t/resourceGroups/t/providers/Microsoft.Compute/images/scylla-test"},
            "sdcm.sct_config.azure_utils.get_image_tags",
            False,
            id="azure",
        ),
        pytest.param(
            "oci",
            {"SCT_OCI_IMAGE_DB": "ocid1.image.oc1.test", "SCT_OCI_REGION_NAME": "us-ashburn-1"},
            "sdcm.sct_config.oci_utils.get_image_tags",
            False,
            id="oci",
        ),
    ],
)
def test_argus_version_consistency(
    resolve_version,
    mock_argus_client,
    backend,
    env_overrides,
    mock_target,
    use_gce_tag,
    gce_tag,
    expected_argus_version,
):
    """All backends must submit the same canonical version string to Argus."""
    tag_value = gce_tag if use_gce_tag else expected_argus_version
    resolve_version(backend, env_overrides, mock_target, tag_value)

    mock_argus_client.update_scylla_version.assert_called_once_with(expected_argus_version)


@pytest.mark.parametrize(
    "gce_tag,expected_argus_version",
    [
        pytest.param(
            "2026.1-2-0-20260421-c4681d0975fd",
            "2026.1.2.0.20260421.c4681d0975fd",
            id="dev-build-with-date",
        ),
        pytest.param(
            "2025.4.0-rc5-0-20250501-abc123def",
            "2025.4.0~rc5.0.20250501.abc123def",
            id="rc-build-with-date",
        ),
    ],
)
def test_gce_dev_build_argus_version(resolve_version, mock_argus_client, gce_tag, expected_argus_version):
    """GCE dev builds with date/commit metadata must not be incorrectly transformed."""
    resolve_version(
        "gce",
        {"SCT_GCE_IMAGE_DB": "projects/test/global/images/scylla-test"},
        "sdcm.sct_config.get_gce_image_tags",
        gce_tag,
    )

    mock_argus_client.update_scylla_version.assert_called_once_with(expected_argus_version)

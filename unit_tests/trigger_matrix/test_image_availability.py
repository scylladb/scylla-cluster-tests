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

"""Tests for the "is this build published on that backend?" check.

`version_exists_for_backend` decides whether a job is triggered at all, so a wrong answer is
silent: too strict and a whole backend is skipped with only a log line, too lax and the job
fails much later, during provisioning.

The autouse conftest fixture stubs these very functions to keep the suite offline, so each test
here re-patches what it means to exercise -- `unittest.mock.patch` inside a test wins over the
fixture.
"""

from unittest.mock import patch

import pytest

from sdcm.provision.provisioner import VmArch
from sdcm.utils.trigger_matrix.backends import _aws_arch, _vm_arch
from sdcm.utils.trigger_matrix.images import (
    _extract_version_from_tags,
    _version_exists_in_region,
    version_exists_for_backend,
)

VERSION = "2025.4.1-0.20250601.abc123def456-1"


# --------------------------------------------------------------------------------------
# architecture translation — each cloud spells ARM differently
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(("arch", "expected"), [("aarch64", VmArch.ARM), ("x86_64", VmArch.X86)])
def test_vm_arch_translation(arch, expected):
    assert _vm_arch(arch) == expected


def test_unknown_arch_falls_back_to_x86():
    assert _vm_arch("sparc") == VmArch.X86


@pytest.mark.parametrize(("arch", "expected"), [("aarch64", "arm64"), ("x86_64", "x86_64")])
def test_aws_names_arm_arm64(arch, expected):
    """AWS calls it arm64; passing `aarch64` through would find no AMIs at all."""
    assert _aws_arch(arch) == expected


# --------------------------------------------------------------------------------------
# per-backend dispatch
# --------------------------------------------------------------------------------------


def test_aws_lookup_gets_the_aws_arch_spelling():
    with patch("sdcm.utils.common.get_scylla_ami_versions", return_value=["ami-1"]) as lookup:
        assert _version_exists_in_region(VERSION, "aws", "eu-west-1", "aarch64") is True
    lookup.assert_called_once_with(version=VERSION, region_name="eu-west-1", arch="arm64")


def test_gce_lookup_is_region_less_and_uses_vmarch():
    with patch("sdcm.utils.common.get_scylla_gce_images_versions", return_value=["img"]) as lookup:
        assert _version_exists_in_region(VERSION, "gce", "", "aarch64") is True
    lookup.assert_called_once_with(version=VERSION, arch=VmArch.ARM)


def test_azure_lookup_passes_region_and_vmarch():
    with patch("sdcm.provision.azure.utils.get_scylla_images", return_value=["img"]) as lookup:
        assert _version_exists_in_region(VERSION, "azure", "eastus", "x86_64") is True
    lookup.assert_called_once_with(scylla_version=VERSION, region_name="eastus", arch=VmArch.X86)


def test_oci_lookup_maps_empty_region_to_none():
    with patch("sdcm.utils.oci_utils.get_scylla_images_by_version", return_value=["img"]) as lookup:
        assert _version_exists_in_region(VERSION, "oci", "", "x86_64") is True
    lookup.assert_called_once_with(version=VERSION, region=None, arch=VmArch.X86)


def test_backend_without_an_image_lookup_is_always_available():
    """docker builds from a repo, so there is no image to look up — never block it."""
    assert _version_exists_in_region(VERSION, "docker", "", "x86_64") is True


def test_empty_lookup_result_means_not_published():
    with patch("sdcm.utils.common.get_scylla_ami_versions", return_value=[]):
        assert _version_exists_in_region(VERSION, "aws", "eu-west-1", "x86_64") is False


def test_a_failing_cloud_lookup_is_reported_as_not_published():
    """Best-effort by design: a credentials or network error must not abort the whole trigger."""
    with patch("sdcm.utils.common.get_scylla_ami_versions", side_effect=RuntimeError("no creds")):
        assert _version_exists_in_region(VERSION, "aws", "eu-west-1", "x86_64") is False


# --------------------------------------------------------------------------------------
# multi-region semantics
# --------------------------------------------------------------------------------------


def test_multi_dc_job_needs_the_build_in_every_region():
    """A multi-DC job provisions in all its regions, so one missing region is a no-go."""
    with patch("sdcm.utils.trigger_matrix.images._version_exists_in_region", side_effect=[True, False]) as exists:
        assert version_exists_for_backend(VERSION, "aws", '["eu-west-1", "eu-west-2"]') is False
    assert [call.args[2] for call in exists.call_args_list] == ["eu-west-1", "eu-west-2"]


def test_all_regions_present_is_available():
    with patch("sdcm.utils.trigger_matrix.images._version_exists_in_region", return_value=True):
        assert version_exists_for_backend(VERSION, "aws", '["eu-west-1", "eu-west-2"]') is True


def test_region_less_backend_is_checked_once_with_no_region():
    with patch("sdcm.utils.trigger_matrix.images._version_exists_in_region", return_value=True) as exists:
        assert version_exists_for_backend(VERSION, "gce") is True
    exists.assert_called_once_with(VERSION, "gce", "", "x86_64")


def test_aws_without_a_region_falls_back_to_the_default():
    with patch("sdcm.utils.trigger_matrix.images._version_exists_in_region", return_value=True) as exists:
        version_exists_for_backend(VERSION, "aws")
    assert exists.call_args.args[2] == "eu-west-1"


# --------------------------------------------------------------------------------------
# tag extraction
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("tags", "expected"),
    [
        ({"scylla_version": VERSION}, VERSION),
        ({"ScyllaVersion": VERSION}, VERSION),  # Azure/OCI capitalise it differently
        ({"scylla_version": VERSION, "ScyllaVersion": "other"}, VERSION),  # first key wins
        ({"unrelated": "x"}, ""),
        ({}, ""),
        ({"scylla_version": ""}, ""),  # an empty tag is not a version
    ],
)
def test_extract_version_from_tags(tags, expected):
    assert _extract_version_from_tags(tags) == expected

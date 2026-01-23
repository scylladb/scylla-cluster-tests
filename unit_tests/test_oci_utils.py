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

"""Unit tests for OCI utilities."""

from unittest.mock import MagicMock, patch

import pytest

from sdcm.utils.oci_utils import (
    OciService,
    filter_oci_by_tags,
    get_ubuntu_image_ocid,
    list_instances_oci,
    oci_tags_to_dict,
    resolve_availability_domain,
    wait_for_instance_state,
)

# --- Tests for oci_tags_to_dict ---


def test_oci_tags_to_dict_with_tags():
    """Test with valid freeform tags."""
    tags = {"NodeType": "sct-runner", "TestId": "test-123"}
    result = oci_tags_to_dict(tags)
    assert result == tags


def test_oci_tags_to_dict_with_none():
    """Test with None tags."""
    result = oci_tags_to_dict(None)
    assert result == {}


def test_oci_tags_to_dict_with_empty_dict():
    """Test with empty dict."""
    result = oci_tags_to_dict({})
    assert result == {}


# --- Tests for filter_oci_by_tags ---


@pytest.fixture
def mock_instances():
    """Set up mock instances."""
    instance1 = MagicMock()
    instance1.defined_tags = {"sct": {"NodeType": "sct-runner", "TestId": "test-1"}}

    instance2 = MagicMock()
    instance2.defined_tags = {"sct": {"NodeType": "sct-runner", "TestId": "test-2"}}

    instance3 = MagicMock()
    instance3.defined_tags = {"sct": {"NodeType": "db-node", "TestId": "test-1"}}

    return [instance1, instance2, instance3]


def test_filter_oci_by_tags_single_tag(mock_instances):
    """Test filtering by a single tag."""
    result = filter_oci_by_tags({"NodeType": "sct-runner"}, mock_instances)

    assert len(result) == 2
    assert mock_instances[0] in result
    assert mock_instances[1] in result


def test_filter_oci_by_tags_multiple_tags(mock_instances):
    """Test filtering by multiple tags."""
    result = filter_oci_by_tags({"NodeType": "sct-runner", "TestId": "test-1"}, mock_instances)

    assert len(result) == 1
    assert result[0] == mock_instances[0]


def test_filter_oci_by_tags_no_match(mock_instances):
    """Test when no instances match."""
    result = filter_oci_by_tags({"NodeType": "nonexistent"}, mock_instances)

    assert len(result) == 0


def test_filter_oci_by_tags_empty_instances():
    """Test with empty instances list."""
    result = filter_oci_by_tags({"NodeType": "sct-runner"}, [])
    assert result == []


# --- Tests for resolve_availability_domain ---


@patch("sdcm.utils.oci_utils.get_availability_domains")
def test_resolve_availability_domain_ad1(mock_get_ads):
    """Test resolving AD-1."""
    mock_get_ads.return_value = [
        "Uocm:US-ASHBURN-AD-1",
        "Uocm:US-ASHBURN-AD-2",
        "Uocm:US-ASHBURN-AD-3",
    ]

    result = resolve_availability_domain("compartment-id", "AD-1")
    assert result == "Uocm:US-ASHBURN-AD-1"


@patch("sdcm.utils.oci_utils.get_availability_domains")
def test_resolve_availability_domain_numeric(mock_get_ads):
    """Test resolving with just the number."""
    mock_get_ads.return_value = [
        "Uocm:US-ASHBURN-AD-1",
        "Uocm:US-ASHBURN-AD-2",
        "Uocm:US-ASHBURN-AD-3",
    ]

    result = resolve_availability_domain("compartment-id", "2")
    assert result == "Uocm:US-ASHBURN-AD-2"


@patch("sdcm.utils.oci_utils.get_availability_domains")
def test_resolve_availability_domain_lowercase(mock_get_ads):
    """Test resolving with lowercase input."""
    mock_get_ads.return_value = [
        "Uocm:US-ASHBURN-AD-1",
        "Uocm:US-ASHBURN-AD-2",
    ]

    result = resolve_availability_domain("compartment-id", "ad-1")
    assert result == "Uocm:US-ASHBURN-AD-1"


@patch("sdcm.utils.oci_utils.get_availability_domains")
def test_resolve_availability_domain_single_ad_region(mock_get_ads):
    """Test resolving in a region with single AD."""
    mock_get_ads.return_value = ["Uocm:EU-FRANKFURT-1-AD-1"]

    # Should return the only AD regardless of input
    result = resolve_availability_domain("compartment-id", "AD-2")
    assert result == "Uocm:EU-FRANKFURT-1-AD-1"


@patch("sdcm.utils.oci_utils.get_availability_domains")
def test_resolve_availability_domain_not_found(mock_get_ads):
    """Test when AD cannot be resolved."""
    mock_get_ads.return_value = [
        "Uocm:US-ASHBURN-AD-1",
        "Uocm:US-ASHBURN-AD-2",
    ]

    with pytest.raises(ValueError, match="Could not resolve availability domain"):
        resolve_availability_domain("compartment-id", "AD-5")


# --- Tests for get_ubuntu_image_ocid ---


@patch("sdcm.utils.oci_utils.get_oci_compute_client")
def test_get_ubuntu_image_ocid(mock_get_client):
    """Test getting Ubuntu image OCID."""
    mock_client = MagicMock()
    mock_image = MagicMock()
    mock_image.id = "ocid1.image.oc1..ubuntu2404"
    mock_image.display_name = "Canonical-Ubuntu-24.04-2024.01.01-0"

    mock_client.list_images.return_value.data = [mock_image]
    mock_get_client.return_value = (mock_client, {})

    result = get_ubuntu_image_ocid("compartment-id", region="us-ashburn-1")

    assert result == "ocid1.image.oc1..ubuntu2404"
    mock_client.list_images.assert_called_once()


@patch("sdcm.utils.oci_utils.get_oci_compute_client")
def test_get_ubuntu_image_ocid_filters_arm(mock_get_client):
    """Test that ARM images are filtered out."""
    mock_client = MagicMock()
    mock_arm_image = MagicMock()
    mock_arm_image.id = "ocid1.image.oc1..ubuntu2404-aarch64"
    mock_arm_image.display_name = "Canonical-Ubuntu-24.04-aarch64-2024.01.01-0"

    mock_amd64_image = MagicMock()
    mock_amd64_image.id = "ocid1.image.oc1..ubuntu2404-amd64"
    mock_amd64_image.display_name = "Canonical-Ubuntu-24.04-2024.01.01-0"

    mock_client.list_images.return_value.data = [mock_arm_image, mock_amd64_image]
    mock_get_client.return_value = (mock_client, {})

    result = get_ubuntu_image_ocid("compartment-id")

    assert result == "ocid1.image.oc1..ubuntu2404-amd64"


@patch("sdcm.utils.oci_utils.get_oci_compute_client")
def test_get_ubuntu_image_ocid_not_found(mock_get_client):
    """Test when no matching image is found."""
    mock_client = MagicMock()
    mock_client.list_images.return_value.data = []
    mock_get_client.return_value = (mock_client, {})

    with pytest.raises(ValueError, match="No Ubuntu"):
        get_ubuntu_image_ocid("compartment-id")


# --- Tests for list_instances_oci ---


@patch("sdcm.utils.oci_utils.get_oci_compartment_id")
@patch("sdcm.utils.oci_utils.get_oci_compute_client")
def test_list_instances_oci_single_region(mock_get_client, mock_get_compartment):
    """Test listing instances in a single region."""
    mock_get_compartment.return_value = "compartment-id"

    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.freeform_tags = {"NodeType": "sct-runner"}
    mock_instance.lifecycle_state = "RUNNING"

    mock_client.list_instances.return_value.data = [mock_instance]
    mock_get_client.return_value = (mock_client, {})

    result = list_instances_oci(region_name="us-ashburn-1", verbose=False)

    assert len(result) == 1
    assert result[0] == mock_instance


@patch("sdcm.utils.oci_utils.get_oci_compartment_id")
@patch("sdcm.utils.oci_utils.get_oci_compute_client")
def test_list_instances_oci_with_tag_filter(mock_get_client, mock_get_compartment):
    """Test listing instances with tag filter."""
    mock_get_compartment.return_value = "compartment-id"

    mock_client = MagicMock()
    mock_instance1 = MagicMock()
    mock_instance1.defined_tags = {"sct": {"NodeType": "sct-runner"}}
    mock_instance1.lifecycle_state = "RUNNING"

    mock_instance2 = MagicMock()
    mock_instance2.defined_tags = {"sct": {"NodeType": "db-node"}}
    mock_instance2.lifecycle_state = "RUNNING"

    mock_client.list_instances.return_value.data = [mock_instance1, mock_instance2]
    mock_get_client.return_value = (mock_client, {})

    result = list_instances_oci(
        tags_dict={"NodeType": "sct-runner"},
        region_name="us-ashburn-1",
        verbose=False,
    )

    assert len(result) == 1
    assert result[0] == mock_instance1


# --- Tests for wait_for_instance_state ---


def test_wait_for_instance_state_running():
    """Test waiting for instance to become RUNNING."""
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.lifecycle_state = "RUNNING"

    mock_client.get_instance.return_value.data = mock_instance

    result = wait_for_instance_state(mock_client, "instance-id", "RUNNING", timeout=10)

    assert result == mock_instance
    mock_client.get_instance.assert_called_with(instance_id="instance-id")


def test_wait_for_instance_state_timeout():
    """Test timeout when instance doesn't reach target state."""
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.lifecycle_state = "PROVISIONING"

    mock_client.get_instance.return_value.data = mock_instance

    with pytest.raises(TimeoutError):
        wait_for_instance_state(mock_client, "instance-id", "RUNNING", timeout=1, poll_interval=1)


def test_wait_for_instance_state_unexpected_termination():
    """Test when instance is unexpectedly terminated."""
    mock_client = MagicMock()
    mock_instance = MagicMock()
    mock_instance.lifecycle_state = "TERMINATED"

    mock_client.get_instance.return_value.data = mock_instance

    with pytest.raises(RuntimeError, match="terminated unexpectedly"):
        wait_for_instance_state(mock_client, "instance-id", "RUNNING", timeout=10)


# --- Tests for OciService ---


@patch("sdcm.utils.oci_utils.KeyStore")
def test_oci_service_compartment_id(mock_keystore_class):
    """Test getting compartment_id."""

    # Clear singleton cache
    OciService._instances = {}

    mock_keystore = MagicMock()
    mock_keystore.get_oci_credentials.return_value = {
        "tenancy": "ocid1.tenancy.oc1..test",
        "user": "ocid1.user.oc1..test",
        "fingerprint": "aa:bb:cc:dd",
        "key_content": "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",
        "region": "us-ashburn-1",
        "compartment_id": "ocid1.compartment.oc1..test",
    }
    mock_keystore_class.return_value = mock_keystore

    service = OciService()
    assert service.compartment_id == "ocid1.compartment.oc1..test"
    assert service.tenancy_id == "ocid1.tenancy.oc1..test"

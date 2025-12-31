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

"""Integration tests for OCI utilities.

These tests require valid OCI credentials and network access.
Run with: pytest -m integration unit_tests/test_oci_utils_integration.py
"""

import os

import pytest

from sdcm.keystore import KeyStore
from sdcm.utils.oci_utils import (
    get_availability_domains,
    get_oci_compartment_id,
    get_oci_compute_client,
    get_oci_identity_client,
    get_oci_network_client,
    get_ubuntu_image_ocid,
    list_instances_oci,
    OciService,
    resolve_availability_domain,
)


# Skip all tests in this module if OCI credentials are not available
def oci_credentials_available():
    """Check if OCI credentials are available."""
    try:
        KeyStore().get_oci_credentials()
        return True
    except Exception:  # noqa: BLE001
        # Check for local config
        return os.path.exists(os.path.expanduser("~/.oci/config"))


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not oci_credentials_available(), reason="OCI credentials not available"),
]


# --- Tests for OCI Credentials ---


@pytest.mark.integration
def test_get_oci_credentials():
    """Test that OCI credentials can be loaded."""
    creds = KeyStore().get_oci_credentials()

    assert "tenancy" in creds
    assert "user" in creds
    assert "fingerprint" in creds
    assert "key_content" in creds
    assert "region" in creds
    assert "compartment_id" in creds

    # Verify OCIDs have correct format
    assert creds["tenancy"].startswith("ocid1.tenancy.")
    assert creds["user"].startswith("ocid1.user.")


# --- Tests for OCI Compute Client ---


@pytest.mark.integration
def test_get_compute_client():
    """Test creating a compute client."""
    client, config = get_oci_compute_client()

    assert client is not None
    assert "region" in config


@pytest.mark.integration
def test_get_compute_client_with_region():
    """Test creating a compute client for a specific region."""
    client, config = get_oci_compute_client(region="us-phoenix-1")

    assert client is not None
    assert config["region"] == "us-phoenix-1"


# --- Tests for OCI Identity Client ---


@pytest.mark.integration
def test_get_identity_client():
    """Test creating an identity client."""
    client, config = get_oci_identity_client()

    assert client is not None


@pytest.mark.integration
def test_list_availability_domains():
    """Test listing availability domains."""
    compartment_id = get_oci_compartment_id()
    ads = get_availability_domains(compartment_id)

    assert isinstance(ads, list)
    assert len(ads) > 0

    # Verify AD name format
    for ad in ads:
        assert "-AD-" in ad.upper()


@pytest.mark.integration
def test_resolve_availability_domain():
    """Test resolving availability domain from short name."""
    compartment_id = get_oci_compartment_id()

    # Should be able to resolve AD-1
    ad = resolve_availability_domain(compartment_id, "AD-1")
    assert "-AD-1" in ad.upper()


# --- Tests for OCI Image Lookup ---


@pytest.mark.integration
def test_get_ubuntu_image_ocid():
    """Test getting Ubuntu image OCID."""
    compartment_id = get_oci_compartment_id()
    image_id = get_ubuntu_image_ocid(compartment_id, version="24.04")

    assert image_id.startswith("ocid1.image.")


@pytest.mark.integration
def test_get_ubuntu_image_different_version():
    """Test getting Ubuntu image with different version."""
    compartment_id = get_oci_compartment_id()

    # Test with 22.04 LTS
    image_id = get_ubuntu_image_ocid(compartment_id, version="22.04")
    assert image_id.startswith("ocid1.image.")


# --- Tests for OCI List Instances ---


@pytest.mark.integration
def test_list_instances_empty():
    """Test listing instances (may be empty)."""
    # List instances with a filter that likely won't match anything
    instances = list_instances_oci(
        tags_dict={"NonExistentTag": "NonExistentValue"},
        region_name="us-ashburn-1",
        verbose=False,
    )

    assert isinstance(instances, list)


@pytest.mark.integration
def test_list_instances_all_regions():
    """Test listing instances across all supported regions."""
    # This tests that we can connect to all supported regions
    instances = list_instances_oci(verbose=True)

    assert isinstance(instances, list)


# --- Tests for OCI Network Client ---


@pytest.mark.integration
def test_get_network_client():
    """Test creating a network client."""
    client, config = get_oci_network_client()

    assert client is not None


# --- Tests for OciService ---


@pytest.mark.integration
def test_oci_service_clients():
    """Test that OciService can create clients."""

    # Clear singleton cache for clean test
    OciService._instances = {}

    service = OciService()

    # Verify properties
    assert service.compartment_id is not None
    assert service.compartment_id.startswith("ocid1.")

    assert service.tenancy_id is not None
    assert service.tenancy_id.startswith("ocid1.tenancy.")

    # Verify clients can be created
    compute_client = service.get_compute_client()
    assert compute_client is not None

    identity_client = service.get_identity_client()
    assert identity_client is not None

    network_client = service.get_network_client()
    assert network_client is not None

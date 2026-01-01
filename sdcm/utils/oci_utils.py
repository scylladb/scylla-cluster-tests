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

"""Oracle Cloud Infrastructure (OCI) utility functions for SCT."""

from __future__ import annotations

import logging
import time
from functools import cached_property
import oci
from oci.core import ComputeClient, VirtualNetworkClient
from oci.core.models import Instance, Image
from oci.identity import IdentityClient

from sdcm.keystore import KeyStore
from sdcm.utils.metaclasses import Singleton


LOGGER = logging.getLogger(__name__)

# Suppress verbose OCI SDK logging
logging.getLogger("oci").setLevel(logging.WARNING)

# Supported OCI regions for SCT
SUPPORTED_REGIONS = [
    "us-ashburn-1",
    "us-phoenix-1",
]


def get_oci_config() -> dict:
    """Get OCI configuration from keystore.

    Returns a dict suitable for use with OCI SDK clients.
    """
    creds = KeyStore().get_oci_credentials()
    return {
        "tenancy": creds["tenancy"],
        "user": creds["user"],
        "fingerprint": creds["fingerprint"],
        "key_content": creds["key_content"],
        "region": creds["region"],
    }


def get_oci_compartment_id() -> str:
    """Get the default compartment ID from keystore."""
    creds = KeyStore().get_oci_credentials()
    return creds["compartment_id"]


def get_oci_compute_client(region: str | None = None) -> tuple[ComputeClient, dict]:
    """Get OCI Compute client for the specified region.

    Args:
        region: OCI region name. If None, uses the default from config.

    Returns:
        Tuple of (ComputeClient, config_dict)
    """
    config = get_oci_config()
    if region:
        config["region"] = region
    return ComputeClient(config), config


def get_oci_identity_client(region: str | None = None) -> tuple[IdentityClient, dict]:
    """Get OCI Identity client for compartment and availability domain operations.

    Args:
        region: OCI region name. If None, uses the default from config.

    Returns:
        Tuple of (IdentityClient, config_dict)
    """
    config = get_oci_config()
    if region:
        config["region"] = region
    return IdentityClient(config), config


def get_oci_network_client(region: str | None = None) -> tuple[VirtualNetworkClient, dict]:
    """Get OCI Virtual Network client.

    Args:
        region: OCI region name. If None, uses the default from config.

    Returns:
        Tuple of (VirtualNetworkClient, config_dict)
    """
    config = get_oci_config()
    if region:
        config["region"] = region
    return VirtualNetworkClient(config), config


def oci_tags_to_dict(freeform_tags: dict | None) -> dict:
    """Convert OCI freeform tags to a standard dict.

    OCI uses freeform_tags as a dict[str, str], so this is mostly a pass-through
    with None handling.

    Args:
        freeform_tags: OCI instance freeform_tags attribute

    Returns:
        Dict of tags, empty dict if None
    """
    return freeform_tags or {}


def get_availability_domains(compartment_id: str, region: str | None = None) -> list[str]:
    """Get list of availability domain names for a compartment/region.

    Args:
        compartment_id: The compartment OCID
        region: OCI region name

    Returns:
        List of availability domain names (e.g., ["Uocm:US-ASHBURN-AD-1", ...])
    """
    identity_client, _ = get_oci_identity_client(region=region)
    ads = identity_client.list_availability_domains(compartment_id=compartment_id).data
    return [ad.name for ad in ads]


def resolve_availability_domain(compartment_id: str, short_name: str, region: str | None = None) -> str:
    """Resolve short availability domain name to full name.

    Args:
        compartment_id: The compartment OCID
        short_name: Short name like "AD-1", "AD-2", "AD-3" or "1", "2", "3"
        region: OCI region name

    Returns:
        Full availability domain name (e.g., "Uocm:US-ASHBURN-AD-1")

    Raises:
        ValueError: If the short name cannot be resolved
    """
    ads = get_availability_domains(compartment_id, region)

    # Normalize short_name - accept "AD-1", "ad-1", "1", etc.
    short_name = short_name.upper().replace("AD-", "").strip()

    for ad in ads:
        # AD names typically end with "-AD-1", "-AD-2", etc.
        if ad.upper().endswith(f"-AD-{short_name}") or ad.upper().endswith(f"-{short_name}"):
            return ad

    # If only one AD exists (some regions), return it
    if len(ads) == 1:
        return ads[0]

    raise ValueError(f"Could not resolve availability domain '{short_name}' in region. Available: {ads}")


def get_ubuntu_image_ocid(compartment_id: str, region: str | None = None, version: str = "24.04") -> str:
    """Get the latest Ubuntu image OCID for the specified region.

    Args:
        compartment_id: The compartment OCID (used for API call context)
        region: OCI region name
        version: Ubuntu version (default: "24.04")

    Returns:
        OCID of the latest Ubuntu image

    Raises:
        ValueError: If no matching image is found
    """
    compute_client, _ = get_oci_compute_client(region=region)

    # List images with Ubuntu filter
    # OCI provides official Ubuntu images from Canonical
    images = compute_client.list_images(
        compartment_id=compartment_id,
        operating_system="Canonical Ubuntu",
        operating_system_version=version,
        sort_by="TIMECREATED",
        sort_order="DESC",
        lifecycle_state="AVAILABLE",
    ).data

    # Filter for amd64/x86_64 images (exclude ARM)
    amd64_images = [
        img for img in images if "aarch64" not in img.display_name.lower() and "arm" not in img.display_name.lower()
    ]

    if not amd64_images:
        raise ValueError(f"No Ubuntu {version} amd64 image found in region {region}")

    latest_image = amd64_images[0]
    LOGGER.info("Found Ubuntu %s image: %s (OCID: %s)", version, latest_image.display_name, latest_image.id)
    return latest_image.id


def list_instances_oci(
    tags_dict: dict | None = None,
    region_name: str | None = None,
    compartment_id: str | None = None,
    running: bool = False,
    verbose: bool = True,
) -> list[Instance]:
    """List OCI instances with optional tag filtering.

    Args:
        tags_dict: Dict of freeform tags to filter by
        region_name: OCI region to search. If None, searches all supported regions.
        compartment_id: Compartment to search. If None, uses default from config.
        running: If True, only return running instances
        verbose: If True, log progress information

    Returns:
        List of OCI Instance objects matching the criteria
    """
    if compartment_id is None:
        compartment_id = get_oci_compartment_id()

    regions = [region_name] if region_name else SUPPORTED_REGIONS
    all_instances = []

    for region in regions:
        if verbose:
            LOGGER.info("Listing OCI instances in region '%s'...", region)

        try:
            compute_client, _ = get_oci_compute_client(region=region)
            if running:
                kwargs = {"lifecycle_state": "RUNNING"}
            else:
                kwargs = {}
            instances = compute_client.list_instances(compartment_id=compartment_id, **kwargs).data

            # Handle pagination if needed
            # Note: For simplicity, assuming results fit in one page
            # In production, should handle pagination with page tokens

            region_instances = list(instances) if instances else []

            if verbose:
                LOGGER.debug("Found %d instances in region '%s'", len(region_instances), region)

            all_instances.extend(region_instances)

        except oci.exceptions.ServiceError as exc:
            LOGGER.warning("Failed to list instances in region '%s': %s", region, exc.message)

    # Filter by tags if specified
    if tags_dict:
        all_instances = filter_oci_by_tags(tags_dict=tags_dict, instances=all_instances)

    # Filter for running instances if requested and not already filtered by API
    if running and not region_name:  # API filter only works when single region specified
        all_instances = [i for i in all_instances if i.lifecycle_state == "RUNNING"]

    if verbose:
        LOGGER.info("Found total of %d OCI instances", len(all_instances))

    return all_instances


def filter_oci_by_tags(tags_dict: dict, instances: list[Instance]) -> list[Instance]:
    """Filter OCI instances by freeform tags.

    Args:
        tags_dict: Dict of tag key-value pairs to match
        instances: List of OCI Instance objects

    Returns:
        Filtered list of instances that match all specified tags
    """
    filtered = []
    for instance in instances:
        instance_tags = oci_tags_to_dict(instance.freeform_tags)
        if all(instance_tags.get(k) == v for k, v in tags_dict.items()):
            filtered.append(instance)
    return filtered


def oci_public_addresses(
    instance: Instance, compute_client: ComputeClient, network_client: VirtualNetworkClient
) -> list[str]:
    """Get public IP addresses for an OCI instance.

    Args:
        instance: OCI Instance object
        compute_client: OCI Compute client
        network_client: OCI Virtual Network client

    Returns:
        List of public IP addresses
    """
    public_ips = []

    try:
        # Get VNIC attachments for the instance
        vnic_attachments = compute_client.list_vnic_attachments(
            compartment_id=instance.compartment_id,
            instance_id=instance.id,
        ).data

        for vnic_attachment in vnic_attachments:
            if vnic_attachment.lifecycle_state != "ATTACHED":
                continue

            vnic = network_client.get_vnic(vnic_id=vnic_attachment.vnic_id).data
            if vnic.public_ip:
                public_ips.append(vnic.public_ip)

    except oci.exceptions.ServiceError as exc:
        LOGGER.warning("Failed to get public IPs for instance %s: %s", instance.id, exc.message)

    return public_ips


def oci_private_addresses(
    instance: Instance, compute_client: ComputeClient, network_client: VirtualNetworkClient
) -> list[str]:
    """Get private IP addresses for an OCI instance.

    Args:
        instance: OCI Instance object
        compute_client: OCI Compute client
        network_client: OCI Virtual Network client

    Returns:
        List of private IP addresses
    """
    private_ips = []

    try:
        vnic_attachments = compute_client.list_vnic_attachments(
            compartment_id=instance.compartment_id,
            instance_id=instance.id,
        ).data

        for vnic_attachment in vnic_attachments:
            if vnic_attachment.lifecycle_state != "ATTACHED":
                continue

            vnic = network_client.get_vnic(vnic_id=vnic_attachment.vnic_id).data
            if vnic.private_ip:
                private_ips.append(vnic.private_ip)

    except oci.exceptions.ServiceError as exc:
        LOGGER.warning("Failed to get private IPs for instance %s: %s", instance.id, exc.message)

    return private_ips


def wait_for_instance_state(
    compute_client: ComputeClient,
    instance_id: str,
    target_state: str,
    timeout: int = 600,
    poll_interval: int = 10,
) -> Instance:
    """Wait for an OCI instance to reach a specific lifecycle state.

    Args:
        compute_client: OCI Compute client
        instance_id: Instance OCID
        target_state: Target lifecycle state (e.g., "RUNNING", "STOPPED", "TERMINATED")
        timeout: Maximum time to wait in seconds
        poll_interval: Time between status checks in seconds

    Returns:
        The instance in the target state

    Raises:
        TimeoutError: If the instance doesn't reach the target state within timeout
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        instance = compute_client.get_instance(instance_id=instance_id).data
        LOGGER.debug("Instance %s state: %s (waiting for %s)", instance_id, instance.lifecycle_state, target_state)

        if instance.lifecycle_state == target_state:
            return instance

        if instance.lifecycle_state == "TERMINATED" and target_state != "TERMINATED":
            raise RuntimeError(f"Instance {instance_id} was terminated unexpectedly")

        time.sleep(poll_interval)

    raise TimeoutError(f"Instance {instance_id} did not reach state '{target_state}' within {timeout}s")


def wait_for_image_state(
    compute_client: ComputeClient,
    image_id: str,
    target_state: str = "AVAILABLE",
    timeout: int = 1800,
    poll_interval: int = 30,
) -> Image:
    """Wait for an OCI image to reach a specific lifecycle state.

    Args:
        compute_client: OCI Compute client
        image_id: Image OCID
        target_state: Target lifecycle state (default: "AVAILABLE")
        timeout: Maximum time to wait in seconds (default: 30 minutes)
        poll_interval: Time between status checks in seconds

    Returns:
        The image in the target state

    Raises:
        TimeoutError: If the image doesn't reach the target state within timeout
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        image = compute_client.get_image(image_id=image_id).data
        LOGGER.debug("Image %s state: %s (waiting for %s)", image_id, image.lifecycle_state, target_state)

        if image.lifecycle_state == target_state:
            return image

        if image.lifecycle_state in ("DELETED", "DISABLED"):
            raise RuntimeError(f"Image {image_id} is in unexpected state: {image.lifecycle_state}")

        time.sleep(poll_interval)

    raise TimeoutError(f"Image {image_id} did not reach state '{target_state}' within {timeout}s")


class OciService(metaclass=Singleton):
    """Singleton service class for OCI operations, similar to AzureService."""

    @cached_property
    def oci_credentials(self) -> dict[str, str]:
        return KeyStore().get_oci_credentials()

    @cached_property
    def compartment_id(self) -> str:
        return self.oci_credentials["compartment_id"]

    @cached_property
    def tenancy_id(self) -> str:
        return self.oci_credentials["tenancy"]

    @cached_property
    def config(self) -> dict:
        return get_oci_config()

    def get_compute_client(self, region: str | None = None) -> ComputeClient:
        """Get Compute client for specified region."""
        config = self.config.copy()
        if region:
            config["region"] = region
        return ComputeClient(config)

    def get_identity_client(self, region: str | None = None) -> IdentityClient:
        """Get Identity client for specified region."""
        config = self.config.copy()
        if region:
            config["region"] = region
        return IdentityClient(config)

    def get_network_client(self, region: str | None = None) -> VirtualNetworkClient:
        """Get Virtual Network client for specified region."""
        config = self.config.copy()
        if region:
            config["region"] = region
        return VirtualNetworkClient(config)

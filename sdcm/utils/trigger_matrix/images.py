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

"""Cloud image lookups for the trigger matrix.

Every call that reaches an AWS, GCE, Azure or OCI image API lives in this module and nowhere
else. That is deliberate: it gives the unit tests a single module to neutralise, which is what
`unit_tests/trigger_matrix/conftest.py::stub_image_lookups` relies on to keep the suite offline.

For the same reason, other submodules must call into this one through the module object
(`images.version_exists_for_backend(...)`) rather than importing the names. `from ... import`
would bind a copy per caller, so a test patching this module would silently miss them."""

import logging

from sdcm.utils.trigger_matrix.backends import _aws_arch, _backend_region, _vm_arch, split_regions
from sdcm.utils.trigger_matrix.constants import DEFAULT_ARCH, DEFAULT_AWS_REGION
from sdcm.utils.trigger_matrix.errors import TriggerMatrixError
from sdcm.utils.trigger_matrix.versions import _as_branch_qualifier, _gce_label_to_version

logger = logging.getLogger(__name__)


def _resolve_latest_version_for_backend(
    scylla_version: str, backend: str, region: str, arch: str = DEFAULT_ARCH
) -> str:
    """Resolve a branch:qualifier version to the full tag of that backend's newest image.

    Multi-DC jobs resolve in their first region; `version_exists_for_backend` is what makes
    sure the build reached every region the job runs in.
    """
    branch_version = _as_branch_qualifier(scylla_version)
    region = next(iter(split_regions(region)), "")
    match backend:
        case "aws":
            return _resolve_version_via_branched_ami(branch_version, region, arch)
        case "gce":
            return _resolve_version_via_branched_gce_image(branch_version, arch)
        case "azure":
            return _resolve_version_via_branched_azure_image(branch_version, region, arch)
        case "oci":
            return _resolve_version_via_branched_oci_image(branch_version, region, arch)
    logger.warning("No image lookup implemented for backend '%s' — cannot resolve '%s'", backend, scylla_version)
    return ""


def resolve_scylla_version_from_image(
    scylla_ami_id: str | None = None,
    gce_image_db: str | None = None,
    azure_image_db: str | None = None,
    oci_image_db: str | None = None,
    region: str | None = None,
) -> str:  # noqa: PLR0913
    """Resolve a full scylla_version from a backend image ID.

    Looks up the image metadata (tags/labels) to extract the scylla version.
    Tries each provided image parameter in order and returns the first resolved version.

    Args:
        scylla_ami_id: AWS AMI ID (e.g., ami-0123456789abcdef0).
        gce_image_db: GCE image URL or name.
        azure_image_db: Azure image ID.
        oci_image_db: OCI image OCID.
        region: AWS region for AMI lookup (defaults to eu-west-1).

    Returns:
        Full version string (e.g., '2024.2.5-0.20250221.cb9e2a54ae6d-1').

    Raises:
        TriggerMatrixError: If the version cannot be resolved from any provided image.
    """
    if scylla_ami_id:
        version = _resolve_version_from_ami(scylla_ami_id, region or DEFAULT_AWS_REGION)
        if version:
            return version

    if gce_image_db:
        version = _resolve_version_from_gce_image(gce_image_db)
        if version:
            return version

    if oci_image_db:
        version = _resolve_version_from_oci_image(oci_image_db, region=region)
        if version:
            return version

    if azure_image_db:
        version = _resolve_version_from_azure_image(azure_image_db)
        if version:
            return version

    provided = {
        k: v
        for k, v in {
            "scylla_ami_id": scylla_ami_id,
            "gce_image_db": gce_image_db,
            "azure_image_db": azure_image_db,
            "oci_image_db": oci_image_db,
        }.items()
        if v
    }
    raise TriggerMatrixError(f"Cannot resolve scylla_version from images: {provided}")


def _resolve_version_via_branched_ami(scylla_version: str, region: str, arch: str = DEFAULT_ARCH) -> str:
    """Resolve a branch:qualifier version to a full tag via AMI lookup.

    Uses get_branched_ami which searches across both Scylla images account and default credentials.
    """
    try:
        from sdcm.utils.common import get_branched_ami  # noqa: PLC0415 - circular import avoidance

        amis = get_branched_ami(scylla_version, region_name=region, arch=_aws_arch(arch))
        if amis:
            tags = {t["Key"]: t["Value"] for t in (amis[0].tags or [])}
            if version := tags.get("scylla_version"):
                logger.info("Resolved '%s' → full version '%s' (via AMI %s)", scylla_version, version, amis[0].image_id)
                return version
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve '%s' via AMI lookup: %s", scylla_version, exc)
    return ""


def _resolve_version_via_branched_gce_image(scylla_version: str, arch: str = DEFAULT_ARCH) -> str:
    """Resolve a branch:qualifier version to a full tag via GCE image lookup."""
    try:
        from sdcm.utils.common import get_branched_gce_images  # noqa: PLC0415 - circular import avoidance

        images = get_branched_gce_images(scylla_version, arch=_vm_arch(arch))
        if images and (label := images[0].labels.get("scylla_version")):
            if version := _gce_label_to_version(label):
                logger.info(
                    "Resolved '%s' → full version '%s' (via GCE image %s)", scylla_version, version, images[0].name
                )
                return version
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve '%s' via GCE image lookup: %s", scylla_version, exc)
    return ""


def _resolve_version_via_branched_azure_image(scylla_version: str, region: str, arch: str = DEFAULT_ARCH) -> str:
    """Resolve a branch:qualifier version to a full tag via Azure image lookup."""
    try:
        import sdcm.provision.azure.utils as azure_utils  # noqa: PLC0415 - optional cloud dependency

        images = azure_utils.get_scylla_images(scylla_version=scylla_version, region_name=region, arch=_vm_arch(arch))
        if images and (version := _extract_version_from_tags(images[0].tags or {})):
            logger.info(
                "Resolved '%s' → full version '%s' (via Azure image %s)", scylla_version, version, images[0].name
            )
            return version
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve '%s' via Azure image lookup: %s", scylla_version, exc)
    return ""


def _resolve_version_via_branched_oci_image(scylla_version: str, region: str, arch: str = DEFAULT_ARCH) -> str:
    """Resolve a branch:qualifier version to a full tag via OCI image lookup."""
    try:
        from sdcm.utils import oci_utils  # noqa: PLC0415 - optional cloud dependency

        # rows are [backend, name, image_id, created, build_id, arch, scylla_version]
        rows = oci_utils.get_scylla_images_by_branch(branch=scylla_version, region=region or None, arch=_vm_arch(arch))
        if rows and (version := rows[0][-1]) and version != "N/A":
            logger.info("Resolved '%s' → full version '%s' (via OCI image %s)", scylla_version, version, rows[0][1])
            return version
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve '%s' via OCI image lookup: %s", scylla_version, exc)
    return ""


def version_exists_for_backend(version: str, backend: str, region: str = "", arch: str = DEFAULT_ARCH) -> bool:
    """Check whether an exact build is published as an image on a backend.

    Mirrors the lookups `SCTConfiguration` runs when it turns `scylla_version` into a
    backend image, so a version that passes here won't abort provisioning downstream.
    A multi-DC job needs the build in *every* region it provisions in, exactly like the
    downstream config does.

    Backends without an image lookup (docker) are reported as available.
    """
    lookup_regions = split_regions(_backend_region(backend, region)) or [""]
    return all(_version_exists_in_region(version, backend, single, arch) for single in lookup_regions)


def _version_exists_in_region(version: str, backend: str, lookup_region: str, arch: str) -> bool:
    try:
        match backend:
            case "aws":
                from sdcm.utils.common import get_scylla_ami_versions  # noqa: PLC0415 - circular import avoidance

                found = bool(get_scylla_ami_versions(version=version, region_name=lookup_region, arch=_aws_arch(arch)))
            case "gce":
                from sdcm.utils.common import (  # noqa: PLC0415 - circular import avoidance
                    get_scylla_gce_images_versions,
                )

                found = bool(get_scylla_gce_images_versions(version=version, arch=_vm_arch(arch)))
            case "azure":
                import sdcm.provision.azure.utils as azure_utils  # noqa: PLC0415 - optional cloud dependency

                found = bool(
                    azure_utils.get_scylla_images(
                        scylla_version=version, region_name=lookup_region, arch=_vm_arch(arch)
                    )
                )
            case "oci":
                from sdcm.utils import oci_utils  # noqa: PLC0415 - optional cloud dependency

                found = bool(
                    oci_utils.get_scylla_images_by_version(
                        version=version, region=lookup_region or None, arch=_vm_arch(arch)
                    )
                )
            case _:
                return True
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to check '%s' availability on %s/%s: %s", version, backend, lookup_region, exc)
        return False

    logger.info(
        "Version '%s' (%s) is %savailable on %s%s",
        version,
        arch,
        "" if found else "NOT ",
        backend,
        f"/{lookup_region}" if lookup_region else "",
    )
    return found


def _extract_version_from_tags(tags: dict, tag_keys: tuple[str, ...] = ("scylla_version", "ScyllaVersion")) -> str:
    """Extract scylla version from a tags dict, trying multiple key names."""
    for key in tag_keys:
        if version := tags.get(key):
            return version
    return ""


def _resolve_version_from_ami(ami_id: str, region: str) -> str:
    """Get scylla_version tag from an AWS AMI.

    Uses get_ami_tags which checks both Scylla images account and default credentials.
    Tag can be 'scylla_version' or 'ScyllaVersion' depending on the AMI.
    """
    try:
        from sdcm.utils.common import get_ami_tags  # noqa: PLC0415 - circular import avoidance

        tags = get_ami_tags(ami_id, region_name=region)
        if version := _extract_version_from_tags(tags):
            logger.info("Resolved AMI %s → scylla_version=%s", ami_id, version)
            return version
        logger.warning(
            "AMI %s has no 'scylla_version' or 'ScyllaVersion' tag. Available tags: %s", ami_id, list(tags.keys())
        )
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from AMI %s: %s", ami_id, exc)
    return ""


def _aws_arch_to_vmarch(aws_arch: str):
    """Convert AWS architecture string to VmArch enum.

    Inverse of vmarch_to_aws: 'arm64' → VmArch.ARM, 'x86_64' → VmArch.X86.
    """
    from sdcm.provision.provisioner import VmArch  # noqa: PLC0415 - avoid circular import
    from sdcm.utils.aws_utils import vmarch_to_aws  # noqa: PLC0415 - avoid circular import

    for member in VmArch:
        if vmarch_to_aws(member) == aws_arch:
            return member
    raise ValueError(f"Unknown AWS architecture: {aws_arch}")


def resolve_architecture_from_ami(ami_id: str, region: str | None = None) -> str:
    """Detect the CPU architecture of an AWS AMI.

    Args:
        ami_id: AWS AMI ID (e.g., ami-0123456789abcdef0).
        region: AWS region. Defaults to DEFAULT_AWS_REGION.

    Returns:
        Normalized architecture string matching VmArch values ('x86_64' or 'aarch64'),
        or empty string on failure.
    """
    region = region or DEFAULT_AWS_REGION
    try:
        import boto3  # noqa: PLC0415 - optional cloud dependency
        from sdcm.utils.aws_utils import get_scylla_images_ec2_resource  # noqa: PLC0415 - circular import avoidance

        # Try Scylla images account first (private AMIs) — uses STS role assumption
        try:
            ec2 = get_scylla_images_ec2_resource(region_name=region)
            image = ec2.Image(ami_id)
            image.reload()
            if image.architecture:
                arch = _aws_arch_to_vmarch(image.architecture)
                logger.info("Resolved AMI %s → architecture=%s (raw: %s)", ami_id, arch, image.architecture)
                return arch.value
        except Exception as exc:  # noqa: BLE001 - fall through to default credentials
            logger.debug("Scylla images account lookup failed for AMI %s: %s", ami_id, exc)

        # Fallback: default credentials (QA account)
        ec2 = boto3.resource("ec2", region_name=region)
        image = ec2.Image(ami_id)
        image.reload()
        if image.architecture:
            arch = _aws_arch_to_vmarch(image.architecture)
            logger.info("Resolved AMI %s → architecture=%s (raw: %s)", ami_id, arch, image.architecture)
            return arch.value
    except Exception as exc:  # noqa: BLE001 - best-effort
        logger.warning("Failed to resolve architecture from AMI %s: %s", ami_id, exc)
    return ""


def _arch_from_image_name(image_name: str) -> str:
    """Infer architecture from image name/URL using SCT naming conventions.

    SCT images across all clouds include 'aarch64' or 'arm64' in the name for ARM builds.
    Returns VmArch-compatible value ('aarch64' or 'x86_64'), or empty string if indeterminate.
    """
    name_lower = image_name.lower()
    if "aarch64" in name_lower or "arm64" in name_lower:
        return "aarch64"
    if "x86_64" in name_lower or "x86-64" in name_lower:
        return "x86_64"
    return ""


def resolve_image_architecture(
    scylla_ami_id: str | None = None,
    gce_image_db: str | None = None,
    azure_image_db: str | None = None,
    oci_image_db: str | None = None,
    region: str | None = None,
) -> str:
    """Resolve architecture from any cloud image parameter.

    Tries each provided image parameter in order:
    - AWS AMI: uses EC2 API to get the architecture attribute
    - GCE/Azure/OCI: infers from image name (SCT naming convention includes arch)

    Returns:
        Normalized architecture string ('x86_64' or 'aarch64'), or empty string on failure.
    """
    if scylla_ami_id:
        arch = resolve_architecture_from_ami(scylla_ami_id, region=region)
        if arch:
            return arch

    if gce_image_db:
        arch = _arch_from_image_name(gce_image_db)
        if arch:
            logger.info("Resolved GCE image %s → architecture=%s (from name)", gce_image_db, arch)
            return arch

    if azure_image_db:
        arch = _arch_from_image_name(azure_image_db)
        if arch:
            logger.info("Resolved Azure image %s → architecture=%s (from name)", azure_image_db, arch)
            return arch

    if oci_image_db:
        arch = _arch_from_image_name(oci_image_db)
        if arch:
            logger.info("Resolved OCI image %s → architecture=%s (from name)", oci_image_db, arch)
            return arch

    return ""


def _resolve_version_from_gce_image(image_name: str) -> str:
    """Get scylla_version label from a GCE image.

    Uses get_gce_image_tags which handles both URL and family-based image references.
    """
    try:
        from sdcm.utils.gce_utils import get_gce_image_tags  # noqa: PLC0415 - optional cloud dependency

        labels = get_gce_image_tags(image_name)
        if version_label := _extract_version_from_tags(labels, tag_keys=("scylla_version",)):
            # GCE labels have dashes instead of dots
            version = version_label.replace("-", ".")
            logger.info("Resolved GCE image %s → scylla_version=%s", image_name, version)
            return version
        logger.warning("GCE image %s has no 'scylla_version' label", image_name)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from GCE image %s: %s", image_name, exc)
    return ""


def _resolve_version_from_oci_image(image_id: str, region: str | None = None) -> str:
    """Get scylla_version from an OCI image using oci_utils.get_image_tags."""
    try:
        from sdcm.utils import oci_utils  # noqa: PLC0415 - optional cloud dependency

        tags = oci_utils.get_image_tags(region or "", image_id, "scylla")
        if version := _extract_version_from_tags(tags, tag_keys=("scylla_version",)):
            logger.info("Resolved OCI image %s → scylla_version=%s", image_id, version)
            return version
        logger.warning("OCI image %s has no 'scylla_version' tag", image_id)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from OCI image %s: %s", image_id, exc)
    return ""


def _resolve_version_from_azure_image(image_id: str) -> str:
    """Get scylla_version from an Azure image using azure_utils.get_image_tags."""
    try:
        import sdcm.provision.azure.utils as azure_utils  # noqa: PLC0415 - optional cloud dependency

        tags = azure_utils.get_image_tags(image_id)
        if version := _extract_version_from_tags(tags, tag_keys=("scylla_version",)):
            logger.info("Resolved Azure image %s → scylla_version=%s", image_id, version)
            return version
        logger.warning("Azure image %s has no 'scylla_version' tag", image_id)
    except Exception as exc:  # noqa: BLE001 - best-effort cloud lookup
        logger.warning("Failed to resolve version from Azure image %s: %s", image_id, exc)
    return ""

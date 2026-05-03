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

"""Cross-cloud instance sizing mapping module.

Provides canonical size names (e.g. "db/2xlarge") that map to equivalent
instance types across AWS, GCE, Azure, and OCI, enabling cloud-agnostic
test configuration and cross-cloud comparisons.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

LOGGER = logging.getLogger(__name__)


@dataclass
class InstanceSpec:
    """Specification for a single cloud instance type.

    Attributes:
        instance_type: Cloud-specific instance type identifier.
        vcpus: Number of virtual CPUs.
        memory_gb: Memory in gigabytes.
        local_ssd_count: Number of local NVMe SSD disks (default 0).
        root_disk_type: Root disk type string, e.g. "pd-ssd" for GCE (default "").
        extra_params: Additional cloud-specific parameters (default empty dict).
    """

    instance_type: str
    vcpus: int
    memory_gb: float
    local_ssd_count: int = 0
    root_disk_type: str = ""
    extra_params: dict = field(default_factory=dict)


@dataclass
class SizeMapping:
    """Cross-cloud mapping for a canonical size across all supported clouds.

    Attributes:
        aws_arm: AWS ARM (Graviton) instance spec.
        aws_x86: AWS x86 instance spec.
        gce: Google Compute Engine instance spec.
        azure: Microsoft Azure instance spec.
        oci: Oracle Cloud Infrastructure instance spec.
    """

    aws_arm: InstanceSpec
    aws_x86: InstanceSpec
    gce: InstanceSpec
    azure: InstanceSpec
    oci: InstanceSpec


# ---------------------------------------------------------------------------
# Sizing registry
# ---------------------------------------------------------------------------
# Keyed by role -> canonical size name -> SizeMapping.
# Roles: "db", "loader", "monitor".
# DB sizes:    large, xlarge, 2xlarge, 4xlarge, 8xlarge, 16xlarge
# Loader sizes: small, medium, large, xlarge
# Monitor sizes: small, medium, large
# ---------------------------------------------------------------------------

SIZING: dict[str, dict[str, SizeMapping]] = {
    # ------------------------------------------------------------------
    # DB role — storage-optimised instances with local NVMe SSDs.
    # AWS: i8g (ARM/Graviton4) and i4i (x86).
    # GCE: z3-highmem — all sizes include 4 local SSD disks + pd-ssd root.
    # Azure: Standard_L*s_v4 local-storage optimised family.
    # OCI: DenseIO.E5.Flex — notation encodes :vcpus:memory_gb.
    # ------------------------------------------------------------------
    "db": {
        "large": SizeMapping(
            aws_arm=InstanceSpec("i8g.large", 2, 16.0),
            aws_x86=InstanceSpec("i4i.large", 2, 16.0),
            gce=InstanceSpec("z3-highmem-4", 4, 32.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L4s_v4", 4, 32.0),
            oci=InstanceSpec("DenseIO.E5.Flex:2:32", 2, 32.0),
        ),
        "xlarge": SizeMapping(
            aws_arm=InstanceSpec("i8g.xlarge", 4, 32.0),
            aws_x86=InstanceSpec("i4i.xlarge", 4, 32.0),
            gce=InstanceSpec("z3-highmem-8", 8, 64.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L8s_v4", 8, 64.0),
            oci=InstanceSpec("DenseIO.E5.Flex:4:64", 4, 64.0),
        ),
        "2xlarge": SizeMapping(
            aws_arm=InstanceSpec("i8g.2xlarge", 8, 64.0),
            aws_x86=InstanceSpec("i4i.2xlarge", 8, 64.0),
            gce=InstanceSpec("z3-highmem-16", 16, 128.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L16s_v4", 16, 128.0),
            oci=InstanceSpec("DenseIO.E5.Flex:8:128", 8, 128.0),
        ),
        "4xlarge": SizeMapping(
            aws_arm=InstanceSpec("i8g.4xlarge", 16, 128.0),
            aws_x86=InstanceSpec("i4i.4xlarge", 16, 128.0),
            gce=InstanceSpec("z3-highmem-32", 32, 256.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L32s_v4", 32, 256.0),
            oci=InstanceSpec("DenseIO.E5.Flex:16:256", 16, 256.0),
        ),
        "8xlarge": SizeMapping(
            aws_arm=InstanceSpec("i8g.8xlarge", 32, 256.0),
            aws_x86=InstanceSpec("i4i.8xlarge", 32, 256.0),
            gce=InstanceSpec("z3-highmem-48", 48, 384.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L64s_v4", 64, 512.0),
            oci=InstanceSpec("DenseIO.E5.Flex:32:512", 32, 512.0),
        ),
        "16xlarge": SizeMapping(
            aws_arm=InstanceSpec("i8g.16xlarge", 64, 512.0),
            aws_x86=InstanceSpec("i4i.16xlarge", 64, 512.0),
            gce=InstanceSpec("z3-highmem-88", 88, 704.0, local_ssd_count=4, root_disk_type="pd-ssd"),
            azure=InstanceSpec("Standard_L80s_v4", 80, 640.0),
            oci=InstanceSpec("DenseIO.E5.Flex:64:1024", 64, 1024.0),
        ),
    },
    # ------------------------------------------------------------------
    # Loader role — compute-optimised instances for stress tools.
    # AWS c6i family has no ARM variant; aws_arm == aws_x86.
    # OCI: VM.Standard3.Flex — notation encodes :vcpus:memory_gb.
    # ------------------------------------------------------------------
    "loader": {
        "small": SizeMapping(
            aws_arm=InstanceSpec("c6i.xlarge", 4, 8.0),
            aws_x86=InstanceSpec("c6i.xlarge", 4, 8.0),
            gce=InstanceSpec("e2-standard-4", 4, 16.0),
            azure=InstanceSpec("Standard_F4s_v2", 4, 8.0),
            oci=InstanceSpec("VM.Standard3.Flex:4:32", 4, 32.0),
        ),
        "medium": SizeMapping(
            aws_arm=InstanceSpec("c6i.2xlarge", 8, 16.0),
            aws_x86=InstanceSpec("c6i.2xlarge", 8, 16.0),
            gce=InstanceSpec("e2-standard-8", 8, 32.0),
            azure=InstanceSpec("Standard_F8s_v2", 8, 16.0),
            oci=InstanceSpec("VM.Standard3.Flex:8:64", 8, 64.0),
        ),
        "large": SizeMapping(
            aws_arm=InstanceSpec("c6i.4xlarge", 16, 32.0),
            aws_x86=InstanceSpec("c6i.4xlarge", 16, 32.0),
            gce=InstanceSpec("e2-standard-16", 16, 64.0),
            azure=InstanceSpec("Standard_F16s_v2", 16, 32.0),
            oci=InstanceSpec("VM.Standard3.Flex:16:128", 16, 128.0),
        ),
        "xlarge": SizeMapping(
            aws_arm=InstanceSpec("c6i.16xlarge", 64, 128.0),
            aws_x86=InstanceSpec("c6i.16xlarge", 64, 128.0),
            gce=InstanceSpec("e2-highcpu-32", 32, 32.0),
            azure=InstanceSpec("Standard_F32s_v2", 32, 64.0),
            oci=InstanceSpec("VM.Standard3.Flex:32:256", 32, 256.0),
        ),
    },
    # ------------------------------------------------------------------
    # Monitor role — general-purpose instances for monitoring stack.
    # AWS t3/m6i family has no ARM variant; aws_arm == aws_x86.
    # OCI: VM.Standard.E4.Flex — notation encodes :vcpus:memory_gb.
    # ------------------------------------------------------------------
    "monitor": {
        "small": SizeMapping(
            aws_arm=InstanceSpec("t3.large", 2, 8.0),
            aws_x86=InstanceSpec("t3.large", 2, 8.0),
            gce=InstanceSpec("n2-highmem-4", 4, 32.0),
            azure=InstanceSpec("Standard_D2_v4", 2, 8.0),
            oci=InstanceSpec("VM.Standard.E4.Flex:2:16", 2, 16.0),
        ),
        "medium": SizeMapping(
            aws_arm=InstanceSpec("m6i.xlarge", 4, 16.0),
            aws_x86=InstanceSpec("m6i.xlarge", 4, 16.0),
            gce=InstanceSpec("n2-highmem-8", 8, 64.0),
            azure=InstanceSpec("Standard_D4_v4", 4, 16.0),
            oci=InstanceSpec("VM.Standard.E4.Flex:4:32", 4, 32.0),
        ),
        "large": SizeMapping(
            aws_arm=InstanceSpec("m6i.2xlarge", 8, 32.0),
            aws_x86=InstanceSpec("m6i.2xlarge", 8, 32.0),
            gce=InstanceSpec("n2-highmem-16", 16, 128.0),
            azure=InstanceSpec("Standard_D8_v4", 8, 32.0),
            oci=InstanceSpec("VM.Standard.E4.Flex:8:64", 8, 64.0),
        ),
    },
}

_VALID_CLOUDS = frozenset({"aws", "gce", "azure", "oci"})
_VALID_ARCH = frozenset({"arm", "x86"})


def resolve_size(role: str, size: str, cloud: str, arch: str = "arm") -> InstanceSpec:
    """Resolve a canonical size name to an InstanceSpec for the given cloud.

    Args:
        role: Node role — one of "db", "loader", "monitor".
        size: Canonical size name, e.g. "2xlarge", "medium".
        cloud: Cloud provider — one of "aws", "gce", "azure", "oci".
        arch: CPU architecture for AWS — "arm" (Graviton) or "x86" (default "arm").
              Ignored for non-AWS clouds.

    Returns:
        InstanceSpec for the requested combination.

    Raises:
        ValueError: If role, size, cloud, or arch is not recognised.

    Example:
        >>> spec = resolve_size("db", "2xlarge", "aws", "arm")
        >>> spec.instance_type
        'i8g.2xlarge'
    """
    if role not in SIZING:
        raise ValueError(f"Unknown role: {role!r}. Valid roles: {sorted(SIZING)}")

    role_sizes = SIZING[role]
    if size not in role_sizes:
        raise ValueError(f"Unknown size {size!r} for role {role!r}. Valid sizes: {sorted(role_sizes)}")

    mapping = role_sizes[size]

    if cloud == "aws":
        if arch not in _VALID_ARCH:
            raise ValueError(f"Unknown arch {arch!r} for cloud 'aws'. Valid values: {sorted(_VALID_ARCH)}")
        return mapping.aws_arm if arch == "arm" else mapping.aws_x86

    if cloud == "gce":
        return mapping.gce

    if cloud == "azure":
        return mapping.azure

    if cloud == "oci":
        return mapping.oci

    raise ValueError(f"Unknown cloud: {cloud!r}. Valid clouds: {sorted(_VALID_CLOUDS)}")


def identify_size(cloud: str, instance_type: str) -> tuple[str, str] | None:
    """Reverse-lookup a canonical (role, size) pair from a cloud instance type.

    Args:
        cloud: Cloud provider — one of "aws", "gce", "azure", "oci".
        instance_type: Cloud-specific instance type string.

    Returns:
        A ``(role, size)`` tuple if found, or ``None`` if the instance type
        is not registered in any mapping.

    Example:
        >>> identify_size("gce", "z3-highmem-16")
        ('db', '2xlarge')
        >>> identify_size("aws", "x99.nonexistent")
        # returns None
    """
    for role, sizes in SIZING.items():
        for size, mapping in sizes.items():
            if cloud == "aws":
                candidates = {mapping.aws_arm.instance_type, mapping.aws_x86.instance_type}
            elif cloud == "gce":
                candidates = {mapping.gce.instance_type}
            elif cloud == "azure":
                candidates = {mapping.azure.instance_type}
            elif cloud == "oci":
                candidates = {mapping.oci.instance_type}
            else:
                return None

            if instance_type in candidates:
                return (role, size)

    return None


def get_cloud_params(role: str, spec: InstanceSpec, cloud: str) -> dict[str, str | int]:
    """Return a dict of SCT config parameter names mapped to values for a given cloud and role.

    The returned dict can be used directly to override SCT configuration for
    the matching backend.

    Args:
        role: Node role — one of "db", "loader", "monitor".
        spec: InstanceSpec to translate into SCT params.
        cloud: Cloud provider — one of "aws", "gce", "azure", "oci".

    Returns:
        Dict of SCT config param names → values.
        For GCE, extra disk params are included when non-zero / non-empty.

    Raises:
        ValueError: If cloud is not recognised.

    Example:
        >>> spec = resolve_size("db", "2xlarge", "gce")
        >>> get_cloud_params("db", spec, "gce")
        {'gce_instance_type_db': 'z3-highmem-16', 'gce_n_local_ssd_disk_db': 4,
         'gce_root_disk_type_db': 'pd-ssd'}
    """
    params: dict[str, str | int] = {}

    if cloud == "aws":
        params[f"instance_type_{role}"] = spec.instance_type

    elif cloud == "gce":
        params[f"gce_instance_type_{role}"] = spec.instance_type
        if spec.local_ssd_count:
            params[f"gce_n_local_ssd_disk_{role}"] = spec.local_ssd_count
        if spec.root_disk_type:
            params[f"gce_root_disk_type_{role}"] = spec.root_disk_type

    elif cloud == "azure":
        params[f"azure_instance_type_{role}"] = spec.instance_type

    elif cloud == "oci":
        params[f"oci_instance_type_{role}"] = spec.instance_type

    else:
        raise ValueError(f"Unknown cloud: {cloud!r}. Valid clouds: {sorted(_VALID_CLOUDS)}")

    return params

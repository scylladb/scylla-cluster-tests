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

"""Backend, region and architecture helpers.

Turns a job's region field into the list of regions it actually provisions in, and converts
architecture strings between SCT's `VmArch` enum and each cloud's own spelling."""

import json
import logging
import re

from sdcm.utils.trigger_matrix.constants import DEFAULT_AWS_REGION, DEFAULT_AZURE_REGION, REGIONAL_BACKENDS

logger = logging.getLogger(__name__)


def split_regions(region: str | None) -> list[str]:
    """Split a matrix `region` value into single regions.

    Multi-DC jobs carry a JSON list (`'["eu-west-1", "eu-west-2"]'`), single-DC jobs a plain
    region name.

    Examples:
        >>> split_regions("eu-west-1")
        ['eu-west-1']
        >>> split_regions('["eu-west-1", "eu-west-2"]')
        ['eu-west-1', 'eu-west-2']
        >>> split_regions("")
        []
    """
    value = (region or "").strip()
    if not value:
        return []
    if value.startswith("["):
        try:
            return [str(item).strip() for item in json.loads(value) if str(item).strip()]
        except json.JSONDecodeError:
            logger.warning("Could not parse region list %r — treating it as a plain region", value)
    return [part for part in re.split(r"[,\s]+", value) if part]


def _backend_region(backend: str, region: str | None) -> str:
    """Regions to use for a backend's image lookups, comma separated.

    Empty for region-less backends (GCE); the backend default when a job declares no region.
    """
    if backend not in REGIONAL_BACKENDS:
        return ""
    regions = split_regions(region)
    if not regions:
        return {"aws": DEFAULT_AWS_REGION, "azure": DEFAULT_AZURE_REGION}.get(backend, "")
    return ",".join(regions)


def _vm_arch(arch: str):
    """Convert an architecture string to the VmArch enum used by the image lookups."""
    from sdcm.provision.provisioner import VmArch  # noqa: PLC0415 - circular import avoidance

    return VmArch.ARM if arch == "aarch64" else VmArch.X86


def _aws_arch(arch: str) -> str:
    """AWS names the ARM architecture `arm64`, not `aarch64`, on both AMIs and instances."""
    from sdcm.utils.aws_utils import vmarch_to_aws  # noqa: PLC0415 - circular import avoidance

    return vmarch_to_aws(_vm_arch(arch))

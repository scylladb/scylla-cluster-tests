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

"""
GCE machine type traits that both the provisioner and the legacy `gce_utils.create_instance` need.

Kept free of `sdcm.provision` imports: `sdcm.provision` imports `gce_utils`, so `gce_utils` cannot import it back.
"""

# Families where every machine type bundles Titanium SSD, including the original z3-highmem-88/176
# names that carry no "lssd" suffix. GCE accepts only on_host_maintenance=MIGRATE for them, so never spot.
# c4/c4a/c4d "-lssd" machine types bundle SSD too but support spot; see bundles_local_ssds().
BUNDLED_LOCAL_SSD_FAMILIES = frozenset({"z3", "z4d"})


def machine_family(instance_type: str) -> str:
    """Family of a machine type name or URL: "z4d" for "zones/us-east1-b/machineTypes/z4d-highmem-16-highlssd"."""
    return instance_type.rsplit("/", maxsplit=1)[-1].split("-", maxsplit=1)[0]


def is_bundled_local_ssd_family(instance_type: str) -> bool:
    """z3 and z4d machine types require on_host_maintenance=MIGRATE and have no spot form."""
    return machine_family(instance_type) in BUNDLED_LOCAL_SSD_FAMILIES


def bundles_local_ssds(instance_type: str) -> bool:
    """Local SSD machine types (z3-*, z4d-*-highlssd, c4-*-lssd, c4d-*-lssd-metal, ...) come with their SSDs,
    GCE rejects explicitly requested ones on top.
    """
    return "lssd" in instance_type or is_bundled_local_ssd_family(instance_type)

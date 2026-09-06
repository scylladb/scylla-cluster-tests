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
# Copyright (c) 2020 ScyllaDB

"""GCE backend configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String


class GceConfigMixin(BaseModel):
    """GCE backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "GCE backend"

    gce_n_local_ssd_disk_monitor: int = SctField(
        description="Number of local SSD disks for monitor nodes in Google Compute Engine",
    )
    gce_instance_type_db: String = SctField(
        description="Instance type for database nodes in Google Compute Engine",
    )
    gce_instance_type_db_oracle: String = SctField(
        description="Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine",
    )
    gce_root_disk_type_db: String = SctField(
        description="Root disk type for database nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_db: int = SctField(
        description="Number of local SSD disks for database nodes in Google Compute Engine",
    )
    gce_pd_standard_disk_size_db: int = SctField(
        description="The size of the standard persistent disk in GB used for GCE database nodes",
    )
    gce_pd_ssd_disk_size_db: int = SctField(
        description="",
    )
    gce_setup_hybrid_raid: Boolean = SctField(
        description="If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data",
    )
    gce_pd_ssd_disk_size_loader: int = SctField(
        description="",
    )
    gce_pd_ssd_disk_size_monitor: int = SctField(
        description="",
    )

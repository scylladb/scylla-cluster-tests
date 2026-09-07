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

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class GceConfigMixin(BaseModel):
    """GCE backend.

    Google Compute Engine provisioning.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "GCE backend"

    gce_datacenter: StringOrList = SctField(
        description="Supported regions: us-east1, us-east4, us-west1, us-central1. Specifying just the region "
        "(e.g., us-east1) means the zone will be selected automatically, or you can mention the zone "
        "explicitly (e.g., us-east1-b)",
        appendable=False,
    )
    gce_image_db: String = SctField(
        description="gce image to use for db nodes",
    )
    gce_image_db_oracle: String = SctField(
        description="GCE image to use for oracle (2nd ref cluster) DB node(s). "
        "If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.",
    )
    gce_image_loader: String = SctField(
        description="Google Compute Engine image to use for loader nodes",
    )
    gce_image_monitor: String = SctField(
        description="gce image to use for monitor nodes",
    )
    gce_image_username: String = SctField(
        description="Username for the Google Compute Engine image",
    )
    gce_instance_type_db: String = SctField(
        description="Instance type for database nodes in Google Compute Engine",
    )
    gce_instance_type_db_oracle: String = SctField(
        description="Instance type for the oracle (2nd ref cluster) DB nodes in Google Compute Engine",
    )
    gce_instance_type_loader: String = SctField(
        description="Instance type for loader nodes in Google Compute Engine",
    )
    gce_instance_type_monitor: String = SctField(
        description="Instance type for monitor nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_db: int = SctField(
        description="Number of local SSD disks for database nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_loader: int = SctField(
        description="Number of local SSD disks for loader nodes in Google Compute Engine",
    )
    gce_n_local_ssd_disk_monitor: int = SctField(
        description="Number of local SSD disks for monitor nodes in Google Compute Engine",
    )
    gce_network: String = SctField(
        description="GCP VPC network the instances are attached to.",
    )
    gce_pd_ssd_disk_size_db: int = SctField(
        description="Size in GB of the persistent SSD disk attached to each DB node.",
    )
    gce_pd_ssd_disk_size_loader: int = SctField(
        description="Size in GB of the persistent SSD disk attached to each loader.",
    )
    gce_pd_ssd_disk_size_monitor: int = SctField(
        description="Size in GB of the persistent SSD disk attached to the monitoring node.",
    )
    gce_pd_standard_disk_size_db: int = SctField(
        description="The size of the standard persistent disk in GB used for GCE database nodes",
    )
    gce_project: String = SctField(
        description="GCP project that owns the provisioned resources.",
    )
    gce_root_disk_type_db: String = SctField(
        description="Root disk type for database nodes in Google Compute Engine",
    )
    gce_root_disk_type_loader: String = SctField(
        description="Root disk type for loader nodes in Google Compute Engine",
    )
    gce_root_disk_type_monitor: String = SctField(
        description="Root disk type for monitor nodes in Google Compute Engine",
    )
    gce_setup_hybrid_raid: Boolean = SctField(
        description="If True, SCT configures a hybrid RAID of NVMEs and an SSD for scylla's data",
    )

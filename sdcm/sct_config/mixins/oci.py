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

"""OCI backend configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String, StringOrList


class OciConfigMixin(BaseModel):
    """OCI backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "OCI backend"

    oci_region_name: StringOrList = SctField(
        description="OCI region where the resources will be deployed",
        appendable=False,
    )
    oci_instance_type_loader: String = SctField(
        description=(
            "Oracle Cloud instance shape to use for loader node(s). "
            "Usage of flex shapes allows setting of the ocpus, memory. "
            "Format is following: <shape-name>:<ocpus>:<ram>"
        ),
    )
    oci_instance_type_monitor: String = SctField(
        description=(
            "Oracle Cloud instance shape to use for monitor node. "
            "Usage of flex shapes allows setting of the ocpus, memory. "
            "Format is following: <shape-name>:<ocpus>:<ram>"
        ),
    )
    oci_instance_type_db: String = SctField(
        description=(
            "Oracle Cloud instance shape to use for DB node(s). "
            "Usage of flex shapes allows setting of the ocpus, memory and nvme disks. "
            "Format is following: <shape-name>:<ocpus>:<ram>:<nvmes> . "
            "For DenseIO shapes it makes sense to specify only 'ocpus' part, "
            "because ram and amount of NVMe disks will be fixed based on the OCPUs count."
        ),
    )
    oci_instance_type_db_oracle: String = SctField(
        description="Oracle Cloud instance shape to use for 'oracle' (2nd ref cluster) ScylladbDB cluster",
    )
    oci_image_db: String = SctField(
        description="Oracle Cloud image to use for DB node(s)",
    )
    oci_image_db_oracle: String = SctField(
        description="Oracle Cloud image to use for oracle (2nd ref cluster) DB node(s). "
        "If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.",
    )
    oci_image_monitor: String = SctField(
        description="Oracle Cloud image to use for the monitor node. Empty value results into latest ubuntu image",
    )
    oci_image_loader: String = SctField(
        description="Oracle Cloud image to use for the loader node(s). Empty value results into latest ubuntu image",
    )
    oci_image_username: String = SctField(
        description="Username used in the Oracle Cloud images utilized by the DB node(s)",
    )

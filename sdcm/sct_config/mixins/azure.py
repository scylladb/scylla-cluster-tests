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

"""Azure backend configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import SctField, String, StringOrList


class AzureConfigMixin(BaseModel):
    """Azure backend configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Azure backend"

    azure_region_name: StringOrList = SctField(
        description="Azure region(s) where the resources will be deployed. Supports single or multiple regions.",
        appendable=False,
    )
    azure_instance_type_loader: String = SctField(
        description="The Azure virtual machine size to be used for loader nodes.",
    )
    azure_instance_type_monitor: String = SctField(
        description="The Azure virtual machine size to be used for monitor nodes.",
    )
    azure_instance_type_db: String = SctField(
        description="The Azure virtual machine size to be used for database nodes.",
    )
    azure_instance_type_db_oracle: String = SctField(
        description="The Azure virtual machine size to be used for Oracle database nodes.",
    )
    azure_image_db: String = SctField(
        description="The Azure image to be used for database nodes.",
    )
    azure_image_db_oracle: String = SctField(
        description="The Azure image to be used for oracle (2nd ref cluster) DB nodes. "
        "If not set and 'oracle_scylla_version' is provided, it will be resolved automatically.",
    )
    azure_image_monitor: String = SctField(
        description="The Azure image to be used for monitor nodes.",
    )
    azure_image_loader: String = SctField(
        description="The Azure image to be used for loader nodes.",
    )
    azure_image_username: String = SctField(
        description="The username for the Azure image.",
    )
    azure_provision_stuck_vm_timeout: int = SctField(
        gt=0,
        description="""
              Seconds to wait for an Azure VM to reach the 'Succeeded' provisioning state before
              treating it as stuck (accepted by Azure but never started by the host - SCT-434) and
              recreating it. Detection is gated on the polled instanceView provisioning state.
        """,
    )
    azure_provision_stuck_vm_recreate_attempts: int = SctField(
        ge=0,
        description="""
              How many times to recreate a stuck Azure VM (full node: VM, NIC and public IP) onto
              fresh capacity before giving up with a non-retryable error.
        """,
    )
    azure_provision_stuck_vm_total_timeout: int = SctField(
        gt=0,
        description="""
              Total timeout (seconds) for the whole stuck-VM recovery attempts.
              Recovery stops with a non-retryable error when either this timeout or
              'azure_provision_stuck_vm_recreate_attempts' is exhausted. This way a degraded Azure
              region cannot keep provisioning running until the CI stage times out SCT.
              This value must be at least 'azure_provision_stuck_vm_timeout', otherwise SCT may
              give up during the initial wait without making even one recreate attempt.
        """,
    )

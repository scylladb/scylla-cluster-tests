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

"""Upgrade tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class UpgradeConfigMixin(BaseModel):
    """Upgrade tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Upgrade tests"

    new_scylla_repo: String = SctField(
        description="URL to the Scylla repository for new versions.",
    )
    new_version: String = SctField(
        description="Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1",
    )
    target_upgrade_version: String = SctField(description="The target version to upgrade Scylla to.")
    disable_raft: Boolean = SctField(
        description="Flag to disable Raft consensus for LWT operations.",
    )
    enable_tablets_on_upgrade: Boolean = SctField(
        description="By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade",
    )
    enable_views_with_tablets_on_upgrade: Boolean = SctField(
        description="Enables creating materialized views in keyspaces using tablets by adding an experimental feature."
        "It should not be used when upgrading to versions before 2025.1 and it should be used for upgrades"
        "where we create such views.",
    )
    upgrade_node_packages: String = SctField(description="Specifies the packages to be upgraded on the node.")
    upgrade_node_system: Boolean = SctField(
        description="Upgrade system packages on nodes before upgrading Scylla. Enabled by default.",
    )
    stress_cmd_1: StringOrList = SctField(
        description="Primary stress command to be executed.",
    )
    stress_cmd_complex_prepare: StringOrList = SctField(
        description="Stress command for complex preparation steps.",
    )
    prepare_write_stress: StringOrList = SctField(
        description="Stress command to prepare write operations.",
    )
    stress_cmd_read_10m: StringOrList = SctField(
        description="Stress command to perform read operations for 10 minutes.",
    )
    stress_cmd_read_cl_one: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level ONE.",
    )
    stress_cmd_read_60m: StringOrList = SctField(
        description="Stress command to perform read operations for 60 minutes.",
    )
    stress_cmd_complex_verify_read: StringOrList = SctField(
        description="Stress command to verify complex read operations.",
    )
    stress_cmd_complex_verify_more: StringOrList = SctField(
        description="Additional stress command to verify complex operations.",
    )
    write_stress_during_entire_test: StringOrList = SctField(
        description="Stress command to perform write operations throughout the entire test.",
    )
    verify_data_after_entire_test: StringOrList = SctField(
        description="Stress command to verify data integrity after the entire test.",
    )
    stress_cmd_read_cl_quorum: StringOrList = SctField(
        description="Stress command to perform read operations with consistency level QUORUM.",
    )
    verify_stress_after_cluster_upgrade: StringOrList = SctField(
        description="""cassandra-stress commands.
        You can specify everything but the -node parameter, which is going to
        be provided by the test suite infrastructure.
        multiple commands can passed as a list""",
    )
    stress_cmd_complex_verify_delete: StringOrList = SctField(
        description="""cassandra-stress commands.
        You can specify everything but the -node parameter, which is going to
        be provided by the test suite infrastructure.
        multiple commands can passed as a list""",
    )
    scylla_encryption_options: String = SctField(
        description="options will be used for enable encryption at-rest for tables",
    )
    kms_key_rotation_interval: int = SctField(
        description="The time interval in minutes which gets waited before the KMS key rotation happens."
        " Applied when the AWS KMS service is configured to be used.",
    )
    enable_kms_key_rotation: Boolean = SctField(
        description="Allows to disable KMS keys rotation. Applicable to AWS, GCP, and Azure backends.",
    )
    enterprise_disable_kms: Boolean = SctField(
        description="An escape hatch to disable KMS for enterprise run, when needed. We enable KMS by default since if we use Scylla 2023.1.3 and up",
    )

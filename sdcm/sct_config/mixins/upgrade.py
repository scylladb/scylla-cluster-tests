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
    """Upgrade tests.

    Rolling upgrade and rollback scenarios: target versions and the load applied across the upgrade.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Upgrade tests"

    disable_raft: Boolean = SctField(
        description="Flag to disable Raft consensus for LWT operations.",
    )
    enable_tablets_on_upgrade: Boolean = SctField(
        description="By default, the tablets feature is disabled. With this parameter, created for the upgrade test, the tablets feature will only be enabled after the upgrade",
    )
    enable_truncate_checks_on_node_upgrade: Boolean = SctField(
        description="Enables or disables truncate checks on each node upgrade and rollback",
    )
    enable_views_with_tablets_on_upgrade: Boolean = SctField(
        description="Enables creating materialized views in keyspaces using tablets by adding an experimental feature."
        "It should not be used when upgrading to versions before 2025.1 and it should be used for upgrades"
        "where we create such views.",
    )
    large_partition_stress_during_upgrade: StringOrList = SctField(
        description="Stress command to be run during rolling upgrade while nodes are being upgraded. "
        "This workload cannot use CL=ALL as not all nodes may be available during the upgrade.",
    )
    new_scylla_repo: String = SctField(
        description="URL to the Scylla repository for new versions.",
    )
    new_version: String = SctField(
        description="Assign new upgrade version, use it to upgrade to specific minor release. eg: 3.0.1",
    )
    num_nodes_to_rollback: int = SctField(
        description="Number of nodes to upgrade and rollback in test_generic_cluster_upgrade",
    )
    run_gemini_in_rolling_upgrade: Boolean = SctField(
        description="Enable running Gemini workload during rolling upgrade test. Default is false.",
    )
    stress_after_cluster_upgrade: StringOrList = SctField(
        description="Stress command to be run after full upgrade - usually used to read the dataset for verification",
    )
    stress_before_upgrade: StringOrList = SctField(
        description="Stress command to be run before upgrade starts (preload/validation stage). "
        "This workload runs before any nodes are upgraded and can use CL=ALL for data validation.",
    )
    stress_during_entire_upgrade: StringOrList = SctField(
        description="Stress command to be run during the upgrade - user should take care for suitable duration",
    )
    target_upgrade_version: String = SctField(description="The target version to upgrade Scylla to.")
    upgrade_node_packages: String = SctField(description="Specifies the packages to be upgraded on the node.")
    upgrade_node_system: Boolean = SctField(
        description="Upgrade system packages on nodes before upgrading Scylla. Enabled by default.",
    )
    upgrade_sstables: Boolean = SctField(
        description="Whether to upgrade sstables as part of upgrade_node or not",
    )
    verify_data_after_entire_test: StringOrList = SctField(
        description="Stress command to verify data integrity after the entire test.",
    )
    verify_stress_after_cluster_upgrade: StringOrList = SctField(
        description="Stress command(s) run after every node has been upgraded, to verify the upgraded cluster. See 'stress_cmd' for the format.",
    )
    verify_stress_after_migration: String = SctField(
        description="Stress command to verify data after migration",
    )
    write_stress_during_entire_test: StringOrList = SctField(
        description="Stress command to perform write operations throughout the entire test.",
    )

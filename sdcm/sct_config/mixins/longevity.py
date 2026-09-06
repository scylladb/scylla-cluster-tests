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

"""Longevity tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class LongevityConfigMixin(BaseModel):
    """Longevity tests configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Longevity tests"

    stress_multiplier: int = SctField(
        description="Multiplier for stress command intensity",
    )
    stress_multiplier_w: int = SctField(
        description="Write stress command intensity multiplier",
    )
    stress_multiplier_r: int = SctField(
        description="Read stress command intensity multiplier",
    )
    stress_multiplier_m: int = SctField(
        description="Mixed operations stress command intensity multiplier",
    )
    run_fullscan: list = SctField(
        description="Enable or disable running full scans during tests",
    )
    run_full_partition_scan: String = SctField(
        description="Enable or disable running full partition scans during tests",
    )
    run_tombstone_gc_verification: String = SctField(
        description="Enable or disable tombstone garbage collection verification during tests",
    )
    keyspace_num: int = SctField(
        description="Number of keyspaces to use in the test",
    )
    round_robin: Boolean = SctField(
        description="Enable or disable round robin selection of nodes for operations",
    )
    batch_size: int = SctField(
        description="Batch size for operations",
    )
    pre_create_schema: Boolean = SctField(
        description="Enable or disable pre-creation of schema before running workload",
    )
    pre_create_keyspace: StringOrList = SctField(
        description="Command to create keyspace to be pre-created before running workload",
    )
    post_prepare_cql_cmds: StringOrList = SctField(
        description="CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)",
    )
    prepare_wait_no_compactions_timeout: int = SctField(
        description="Time to wait for compaction to finish at the end of prepare stage. Use only when compaction affects the test or load",
    )
    compaction_strategy: String = SctField(
        description="Compaction strategy to use for pre-created schema",
    )
    sstable_size: int = SctField(
        description="Configure sstable size for pre-create-schema mode",
    )
    cluster_health_check: Boolean = SctField(
        description="Enable or disable starting cluster health checker for all nodes",
    )
    cluster_health_check_parallel_workers: int = SctField(
        description="Number of parallel workers for health checks. "
        "Values above 10 are not recommended (diminishing returns, risk of API rate limiting). "
        "Default: 5.",
    )
    data_validation: String = SctField(
        description="Specify the type of data validation to perform",
    )
    stress_read_cmd: StringOrList = SctField(
        description="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    prepare_verify_cmd: StringOrList = SctField(
        description="""cassandra-stress commands.
            You can specify everything but the -node parameter, which is going to
            be provided by the test suite infrastructure.
            multiple commands can passed as a list""",
    )
    user_profile_table_count: int = SctField(
        description="Number of user profile tables to create for the test",
    )
    add_cs_user_profiles_extra_tables: Boolean = SctField(
        description="extra tables to create for template user c-s, in addition to pre-created tables",
    )

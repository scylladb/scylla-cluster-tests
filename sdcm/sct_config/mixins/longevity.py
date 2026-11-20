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

"""Longevity tests configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, IntOrList, SctField, String, StringOrList


class LongevityConfigMixin(BaseModel):
    """Longevity tests.

    Options specific to long-running longevity test scenarios.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Longevity tests"

    cluster_target_size: IntOrList = SctField(
        description="""Used for scale test: max size of the cluster""",
    )
    grow_cluster_during_stress: Boolean = SctField(
        description="""
            Used with cluster_target_size in longevity_test.test_custom_time. When true, the stress_cmd
            load and the nemesis are started first and the cluster is grown to cluster_target_size while
            they run. If stress_cmd ends before the cluster reaches its target size, it is run again,
            unless it failed. It is run once more after the cluster reached its target size, so the
            full-size cluster gets a full round of the load and the nemesis.
            When false, the cluster is grown before stress_cmd and the nemesis start.
        """,
    )
    minimum_nemesis_to_execute: int = SctField(
        ge=0,
        description="""
            Used in longevity_test.test_custom_time. When stress_cmd ends and fewer nemeses than this number
            were executed, stress_cmd is run again, one full round after another, until the number is reached.
            A nemesis counts once it ran to its end, whether it succeeded or failed. A skipped nemesis does
            not count. The count is checked only between the rounds. With grow_cluster_during_stress, it is
            checked from the end of the round that runs on the full-size cluster.
            The rounds stop before the number is reached when a round failed. They also stop, with a warning
            event, when no nemesis thread is running or when one more round could outlast test_duration: a
            round is expected to take as long as the last one did, and up to 5 minutes more.
            An integer, 0 or more. 0 means no minimum: stress_cmd is not run again for the nemesis.
            Example: minimum_nemesis_to_execute: 5
        """,
    )
    compaction_strategy: String = SctField(
        description="Compaction strategy to use for pre-created schema",
    )
    data_validation: String = SctField(
        description="Specify the type of data validation to perform",
    )
    post_prepare_cql_cmds: StringOrList = SctField(
        description="CQL Commands to run after prepare stage finished (relevant only to longevity_test.py)",
    )
    pre_create_keyspace: StringOrList = SctField(
        description="Command to create keyspace to be pre-created before running workload",
    )
    pre_create_schema: Boolean = SctField(
        description="Enable or disable pre-creation of schema before running workload",
    )
    run_commit_log_check_thread: Boolean = SctField(
        description="Flag to run a thread that checks commit logs",
    )
    run_fullscan: list = SctField(
        description="Enable or disable running full scans during tests",
    )
    run_tombstone_gc_verification: String = SctField(
        description="Enable or disable tombstone garbage collection verification during tests",
    )
    space_node_threshold: int = SctField(
        description="""
             Space node threshold before starting nemesis (bytes)
             The default value is 6GB (6x1024^3 bytes)
             This value is supposed to reproduce
             https://github.com/scylladb/scylla/issues/1140
         """,
    )
    sstable_size: int = SctField(
        description="Configure sstable size for pre-create-schema mode",
    )
    validate_large_collections: Boolean = SctField(
        description="Flag to validate large collections in the database",
    )

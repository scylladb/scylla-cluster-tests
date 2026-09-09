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

"""Spark migrator (Cassandra to Scylla) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, SctField, String, StringOrList


class SparkMigratorConfigMixin(BaseModel):
    """Spark migrator (Cassandra to Scylla).

    The migration job itself: source and target keyspaces/tables and validation.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Spark migrator (Cassandra to Scylla)"

    migrator_run_validator: Boolean = SctField(
        description="Run the spark-migrator validator after migration to do a row-by-row comparison",
    )
    migrator_source_hosts: StringOrList = SctField(
        description="CQL contact-point IPs for the source Cassandra/Scylla cluster. "
        "Mutually exclusive with migrator_source_test_id.",
    )
    migrator_source_keyspace: String = SctField(
        description="Keyspace to migrate from on the source cluster",
    )
    migrator_source_table: String = SctField(
        description="Table to migrate from on the source cluster",
    )
    migrator_source_test_id: String = SctField(
        description="SCT test_id of a running source cluster. When set, source host IPs are auto-discovered "
        "via EC2 tags (NodeType=cs-db). Mutually exclusive with migrator_source_hosts.",
    )
    migrator_step_timeout_minutes: int = SctField(
        description="Time in minutes to wait for the spark-migrator migration EMR step. Default 360.",
    )
    migrator_target_keyspace: String = SctField(
        description="Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.",
    )
    migrator_target_table: String = SctField(
        description="Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.",
    )
    validator_step_timeout_minutes: int = SctField(
        description="Time in minutes to wait for the spark-migrator validator EMR step. Default 60.",
    )

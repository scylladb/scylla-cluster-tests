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

"""Spark migrator (Cassandra to Scylla) configuration options."""

from typing import ClassVar, Literal
from typing_extensions import Annotated

from pydantic import BaseModel
from pydantic.functional_validators import BeforeValidator

from sdcm.sct_config.types import AdaptiveTimeoutMultipliers, Boolean, DictOrStr, IntOrList, SctField, String, StringOrList, dict_or_str_or_pydantic


class SparkMigratorConfigMixin(BaseModel):
    """Spark migrator (Cassandra to Scylla) configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Spark migrator (Cassandra to Scylla)"

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
    migrator_target_keyspace: String = SctField(
        description="Keyspace to migrate into on the target Scylla cluster. Defaults to migrator_source_keyspace.",
    )
    migrator_target_table: String = SctField(
        description="Table to migrate into on the target Scylla cluster. Defaults to migrator_source_table.",
    )
    migrator_run_validator: Boolean = SctField(
        description="Run the spark-migrator validator after migration to do a row-by-row comparison",
    )
    migrator_step_timeout_minutes: int = SctField(
        description="Time in minutes to wait for the spark-migrator migration EMR step. Default 360.",
    )
    validator_step_timeout_minutes: int = SctField(
        description="Time in minutes to wait for the spark-migrator validator EMR step. Default 60.",
    )
    run_scylla_doctor: Boolean = SctField(
        description="Flag to run Scylla Doctor tool",
    )
    scylla_doctor_version: String = SctField(
        description="""Scylla Doctor version to use for artifact tests. Set to specific version (e.g., '1.10')
                to hardcode the version, or leave empty to use the latest available version. For stability,
                artifact tests should use a hardcoded version to avoid issues from newer scylla-doctor releases.""",
    )
    scylla_doctor_full_tarball_url: String = SctField(
        description="""Direct URL to a full edition Scylla Doctor tarball in S3. When set, bypasses the
                standard version-based S3 lookup and downloads SD directly from this URL.
                Use for testing unofficial or pre-release SD versions.
                Example: 'https://s3.amazonaws.com/my-bucket/scylla-doctor-1.11-rc1.tar.gz'""",
    )
    run_scylla_doctor_only: Boolean = SctField(
        description="""When true, the artifact test runs only the Scylla Doctor validation
                (install, collect vitals, analyze, verify) and skips all other artifact checks
                such as stop/start, cassandra-stress, etc. Useful for fast SD
                release gating. Implies run_scylla_doctor=true.""",
    )
    scylla_doctor_edition: Literal["basic", "full"] = SctField(
        description="""Scylla Doctor edition to use. Allowed values: 'basic', 'full'.
                'basic' fetches the free/open-source edition via HTTP.
                'full' fetches the full/enterprise edition from a private S3 bucket.""",
    )
    skip_test_stages: DictOrStr = SctField(
        description="Skip selected stages of a test scenario",
    )
    use_zero_nodes: Boolean = SctField(
        description="If True, enable support in SCT of zero nodes (configuration, nemesis)",
    )
    n_db_zero_token_nodes: IntOrList = SctField(
        description="Number of zero token nodes in cluster. Value should be set as '0 1 1' "
        "for multidc configuration in same manner as 'n_db_nodes' and should be equal number of regions",
    )
    zero_token_instance_type_db: String = SctField(
        description="Instance type for zero token node",
    )
    sct_aws_account_id: String = SctField(
        description="AWS account id on behalf of which the test is run",
    )
    latency_decorator_error_thresholds: DictOrStr = SctField(
        description="Error thresholds for latency decorator. "
        "Defined by dict: {<write, read, mixed>: {<default|nemesis_name>:{<metric_name>: {<rule>: <value>}}}",
    )
    workload_name: String = SctField(
        description="Workload name, can be: write|read|mixed|unset. "
        "Used for e.g. latency_calculator_decorator (use with 'use_hdrhistogram' set to true). "
        "If unset, workload is taken from test name.",
    )
    adaptive_timeout_store_metrics: Boolean = SctField(
        description="Store adaptive timeout metrics in Argus. Disabled for performance tests only.",
    )
    adaptive_timeout_multipliers: Annotated[AdaptiveTimeoutMultipliers, BeforeValidator(dict_or_str_or_pydantic)] = (
        SctField(
            description="Optional dict of adaptive-timeout multipliers keyed by operation name "
            "(from Operations enum value[0], e.g. decommission, remove_node, new_node, repair, etc.). "
            "If the current operation key is absent, multiplier 1.0 is used.<br>"
            "YAML example:<br>"
            "adaptive_timeout_multipliers:<br>"
            "  decommission: 4<br>"
            "  new_node: 2<br>"
            "Environment variable examples:<br>"
            "SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS=\"{'decommission': 4, 'new_node': 2}\"<br>"
            "Or dot-notation: SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS.decommission=4<br>"
            "Or double-underscore (bash-exportable): SCT_ADAPTIVE_TIMEOUT_MULTIPLIERS__decommission=4",
        )
    )

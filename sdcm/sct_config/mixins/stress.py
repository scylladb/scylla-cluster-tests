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

"""Stress commands and load generation configuration options."""

from typing import ClassVar, Literal

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, DictOrStr, SctField, String, StringOrList


class StressConfigMixin(BaseModel):
    """Stress commands and load generation configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Stress commands and load generation"

    prepare_stress_duration: int = SctField(
        description="""
              Time in minutes, which is required to run prepare stress commands
              defined in prepare_*_cmd for dataset generation, and is used in
              test duration calculation
         """,
    )
    stress_duration: int = SctField(
        description="""
              Time in minutes, Time of execution for stress commands from stress_cmd parameters
              and is used in test duration calculation
        """,
    )
    alternator_stress_rate: int = SctField(
        description="""
           Number of operations per second to achieve in stress commands for alternator testing.
      """,
    )
    alternator_write_always_lwt_stress_rate: int = SctField(
        description="""
              Number of operations per second to achieve in stress commands for alternator testing, in write test with isolation set to always LWT. If non-zero, overwrites alternator_stress_rate.
         """,
    )
    stress_cmd: StringOrList = SctField(
        description="cassandra-stress commands. You can specify everything but the -node parameter, which is going to be provided by the test suite infrastructure. multiple commands can passed as a list",
    )
    gemini_schema_url: String = SctField(
        description="""Path to a local schema JSON file or a remote URL (http/https) that Gemini will use.
                    Local files are uploaded to the loader via send_files and mounted into the Gemini Docker
                    container via --schema.
                    Remote URLs are downloaded on the loader node with curl and then mounted the same way.""",
    )
    gemini_cmd: String = SctField(
        description="gemini command to run (for now used only in GeminiTest)",
    )
    gemini_seed: int = SctField(
        description="Seed number for gemini command",
    )
    gemini_log_cql_statements: Boolean = SctField(
        description="Log CQL statements to file",
    )
    gemini_table_options: list = SctField(
        description="table options for created table. example: ['cdc={'enabled': true}'], ['cdc={'enabled': true}', 'compaction={'class': 'IncrementalCompactionStrategy'}']",
    )
    run_gemini_in_rolling_upgrade: Boolean = SctField(
        description="Enable running Gemini workload during rolling upgrade test. Default is false.",
    )
    bare_loaders: Boolean = SctField(
        description="Don't install anything but node_exporter to the loaders during cluster setup",
    )
    stress_image: DictOrStr = SctField(
        description="Dict of the images to use for the stress tools",
    )
    cs_populating_distribution: String = SctField(
        description="set c-s parameter '-pop' with gauss/uniform distribution for performance gradual throughput grow tests",
    )
    latte_schema_parameters: DictOrStr = SctField(
        description="""Optional. Allows to pass through custom rune script parameters to the 'latte schema' command.
        Also used as a fallback source for keyspace/table in gradual performance tests when
        perf_stress_keyspace/perf_stress_table are not set.
        For example, {'keyspace': 'test_keyspace', 'table': 'test_table'}""",
    )
    c_s_driver_version: Literal["3", "4", "random"] = SctField(
        description="cassandra-stress driver version to use: 3|4|random",
    )

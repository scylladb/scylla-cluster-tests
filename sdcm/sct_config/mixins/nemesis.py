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

"""Nemesis (chaos testing) configuration options."""

from typing import ClassVar

from pydantic import BaseModel

from sdcm.sct_config.types import Boolean, IntOrList, SctField, String, StringOrList


class NemesisConfigMixin(BaseModel):
    """Nemesis (chaos testing) configuration options.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Nemesis (chaos testing)"

    nemesis_class_name: StringOrList = SctField(
        description="""
                Nemesis class to use (possible types in sdcm.nemesis).
                Supported syntax:
                - nemesis_class_name: "NemesisName"
                  Run one nemesis in a single thread.
                - nemesis_class_name: ["NemesisA", "NemesisB"]
                  Run NemesisA and NemesisB each in their own thread.
                - nemesis_class_name: ["SisyphusMonkey", "SisyphusMonkey"]
                  Run two SisyphusMonkey threads in parallel.
                Note: the former 'Class:N' count syntax (e.g. "ChaosMonkey:2") and
                space-separated strings (e.g. "DisruptiveMonkey NonDisruptiveMonkey") are no
                longer supported. Use an explicit YAML list instead.
        """,
    )
    nemesis_interval: int = SctField(
        description="""Nemesis sleep interval to use if None provided specifically in the test""",
    )
    nemesis_sequence_sleep_between_ops: int = SctField(
        description="""Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests""",
    )
    nemesis_during_prepare: Boolean = SctField(
        description="""Run nemesis during prepare stage of the test""",
    )
    nemesis_seed: IntOrList = SctField(
        description="""A seed number in order to repeat nemesis sequence as part of SisyphusMonkey""",
    )
    nemesis_add_node_cnt: int = SctField(
        description="""Add/remove nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_grow_shrink_instance_type: String = SctField(
        description="""Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis""",
    )
    cluster_target_size: IntOrList = SctField(
        description="""Used for scale test: max size of the cluster""",
    )
    space_node_threshold: int = SctField(
        description="""
             Space node threshold before starting nemesis (bytes)
             The default value is 6GB (6x1024^3 bytes)
             This value is supposed to reproduce
             https://github.com/scylladb/scylla/issues/1140
         """,
    )
    nemesis_filter_seeds: Boolean = SctField(
        description="""If true runs the nemesis only on non seed nodes""",
    )
    nemesis_selector: StringOrList = SctField(
        description="""nemesis_selector gets a list of "nemesis properties" and filters IN all the nemesis that has
        ALL the properties in that list which are set to true (the intersection of all properties).
        (In other words filters out all nemesis that doesn't ONE of these properties set to true)
        IMPORTANT: If a property doesn't exist, ALL the nemesis will be included.""",
    )
    nemesis_multiply_factor: int = SctField(
        description="Multiply the list of nemesis to execute by the specified factor",
    )

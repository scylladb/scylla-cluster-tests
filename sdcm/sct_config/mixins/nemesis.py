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

"""Nemesis (chaos testing) configuration options."""

from typing import Annotated, ClassVar

from pydantic import BaseModel, BeforeValidator

from sdcm.sct_config.nemesis_runners import NemesisConfig
from sdcm.sct_config.types import Boolean, IntOrList, SctField, String, StringOrList, dict_or_str_or_pydantic


class NemesisConfigMixin(BaseModel):
    """Nemesis (chaos testing).

    Which disruptions run, how often, and how targets are selected.

    See ``sdcm.sct_config.mixins`` for how these are assembled into ``SCTConfiguration``.
    """

    #: Section title used to group this mixin's options in the generated documentation.
    config_group: ClassVar[str] = "Nemesis (chaos testing)"

    nemesis: Annotated[NemesisConfig, BeforeValidator(dict_or_str_or_pydantic)] = SctField(
        description="""
                Nemesis runners to start, one thread each, and defaults shared by all of them.
                Each runner is a single-key mapping of the runner class to its options; the options
                accepted depend on the runner class. Options a runner leaves unset take the
                `nemesis` defaults; a runner without `seed` gets a random one. `selector` is a
                logical expression over nemesis flags filtering the disruptions a runner runs.
                YAML example:
                  nemesis:
                    interval: 5
                    runners:
                      - SisyphusMonkey:
                          selector: "disruptive"
                          interval: 15
                      - CategoricalMonkey:
                          dist: {StopStartMonkey: 2}
                Environment variable examples:
                  SCT_NEMESIS__INTERVAL=10
                  SCT_NEMESIS__RUNNERS='[{"SisyphusMonkey": {"selector": "disruptive"}}]'
        """,
    )
    nemesis_add_node_cnt: int = SctField(
        description="""Add/remove nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_class_name: StringOrList = SctField(
        description="Not read; replaced by `nemesis.runners`.",
    )
    nemesis_double_load_during_grow_shrink_duration: int = SctField(
        description="After growing (and before shrink) in GrowShrinkCluster nemesis it will double the load for provided duration.",
    )
    nemesis_during_prepare: Boolean = SctField(
        description="""Run nemesis during prepare stage of the test""",
    )
    nemesis_filter_seeds: Boolean = SctField(
        description="Not read; replaced by `nemesis.filter_seeds`.",
    )
    nemesis_grow_shrink_instance_type: String = SctField(
        description="""Instance type to use for adding/removing nodes during GrowShrinkCluster nemesis""",
    )
    nemesis_interval: int = SctField(
        description="Not read; replaced by `nemesis.interval`.",
    )
    nemesis_multiply_factor: int = SctField(
        description="Not read; replaced by `nemesis.multiply_factor`.",
    )
    nemesis_seed: IntOrList = SctField(
        description="Not read; replaced by `nemesis.seed`.",
    )
    nemesis_selector: StringOrList = SctField(
        description="Not read; replaced by `nemesis.selector`.",
    )
    nemesis_sequence_sleep_between_ops: int = SctField(
        description="""Sleep interval between nemesis operations for use in unique_sequence nemesis kind of tests""",
    )
    # Temporary solution. We do not want to run SLA nemeses during not-SLA test until the feature is stable
    sla: Boolean = SctField(
        description="run SLA nemeses if the test is SLA only",
    )
